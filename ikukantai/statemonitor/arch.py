from typing import List, Dict
import logging
import simpy

from logger_config.logger_config import setup_logger

from sim.faas import Function, FaasSystem
from sim.core import Environment
from sim.topology import DockerRegistry
from sim.net import SafeFlow

from skippy.core.model import Node

from ether.core import Node as EtherNode

logger = logging.getLogger(__name__)
setup_logger()


class MainMonitor:
    """
    Manage all function
    """
    def __init__(self):
        self.fn_monitor_map: Dict[str, 'FunctionMonitor'] = {}
        
    def add_fnMonitor(self, fn_monitor: 'FunctionMonitor'):
        self.fn_monitor_map[fn_monitor.function.name] = fn_monitor
        logger.info(f"Finish adding new FunctionMonitor for function '{fn_monitor.function.name}'")
        
class FunctionMonitor:
    """
    Manage all pod with same functions
    """
    def __init__(self, function : Function, mainmonitor: MainMonitor):
        self.name = function.name 
        self.function = function
        self.mainmonitor = mainmonitor
        self.podmonitor_map: Dict[str, 'PodMonitor'] = {}
        
    def add_podmonitor(self, env: Environment, name):
        new_podmonitor = PodMonitor(env = env, function_monitor=self, name=name)
        self.podmonitor_map[name] = new_podmonitor

class PodMonitor:
    """
    Hold simulation specific runtime knowledge about a pod. I this case is the 'life cycle of a pod' that we have designed
    """
    def __init__(self, env: Environment, function_monitor: FunctionMonitor, name):
        self.env = env
        self.function_monitor = function_monitor
        self.name = name
        
        # --- Original State Flags ---
        self.null = False        # ksvc available
        self.cold = True         # identity available
        self.warm = False        # image available
        self.warmdisk = False    # container available
        self.active = False      # receiving request
        self.node: Node = None   # Assign which node to schedule pod
        
        # --- Simpy Integration ---
        # self.event = simpy.Event(env)
        # self.new_state = None
        
        # --- Simpy modified ---
        self.state_queue = simpy.Store(env)
        
        self.lifecycle_process = env.process(self.run_pod_lifecircle())
        
        logger.info(f"[Simtime={self.env.now}] Pod '{self.name}' of function {self.function_monitor.name} reached cold state")
        
    def state_signal(self, new_state):
        if self.lifecycle_process is None or not self.lifecycle_process.is_alive:
            logger.warning(f"[Simtime={self.env.now}] Pod {self.name} of function {self.function_monitor.name}: Cannot send new state signal")
            # if False: yield
            return self.env.timeout(0)
        
        logger.debug(f"Receiving a state signal '{new_state}' pod {self.name} of function {self.function_monitor.name}")
        return self.state_queue.put(new_state)
      
    def run_pod_lifecircle(self):
        """
        Managing pod's state
        """
        faas: FaasSystem = self.env.faas
        
        logger.debug(f"Initializing PodMonitor for tracking lifecircle of pod '{self.name}'")
        
        while not self.null:
            # --- Listening for a signal ---
            # yield self.event
            # new_state = self.new_state
            # self.event = simpy.Event(self.env) # Reset event for next command
            new_state = yield self.state_queue.get()
            
            logger.critical(f"Pod '{self.name}': Dequeued '{new_state}'. Current Flags: C={self.cold}, W={self.warm}, WD={self.warmdisk}, A={self.active}")
            
            logger.info(f"[Simtime={self.env.now}] Pod '{self.name}' of function {self.function_monitor.name} is changing to {new_state} state")
            
            # --- Change to warm state ---
            if new_state == "warmdisk": 
                if self.cold: # Download image for the pod
                    dockerPull = docker_pull(env=self.env,
                                             image_str=self.function_monitor.function.fn_images[0].image, # FIXME: Current use image[0] as default
                                             node=self.env.get_node_state(self.node.name).ether_node)
                    yield self.env.process(dockerPull)
                    self.cold = False
                    self.warmdisk = True
                    logger.info(f"[Simtime={self.env.now}] Pod '{self.name}' of function {self.function_monitor.name} is reached {new_state} state")
                elif self.warm:
                    logger.debug(f"Changing pod '{self.name}' of function '{self.function_monitor.name}' from warm to warmdisk")
                    self.warm = False
                    self.warmdisk = True
                else:
                    logger.warning("Current pod not in valid state ")
                    
            elif new_state == "warm":
                if self.warmdisk:
                    yield faas.scale_up(self.function_monitor.name, int(1))
                    self.warmdisk = False
                    self.warm = True
                    logger.info(f"[Simtime={self.env.now}] Pod '{self.name}' of function {self.function_monitor.name} is reached {new_state} state")
                elif self.active:
                    self.active = False
                    self.warm = True
                    logger.info(f"[Simtime={self.env.now}] Pod '{self.name}' of function {self.function_monitor.name} is reached {new_state} state")
                    
def docker_pull(env: Environment, image_str: str, node: EtherNode):
    """
    Simulate a docker pull command of the given image on the given node.

    :param env: the simulation environment
    :param image_str: the name of the image (<repository[:tag]>)
    :param node: the node on which to run the pull command
    :return: a simpy process (a generator)
    """
    started = env.now
    # TODO: there's a lot of potential to improve fidelity here: consider image layers, simulate extraction time, etc.
    #  e.g., docker pull on a 13MB container takes about 5 seconds. the simulated time at 120 MBit/sec would be <1s

    # find the image in the registry with the node's architecture
    # images = env.container_registry.find(image_str, arch=node.arch)
    images = env.container_registry.find(image_str, arch="x86")
    if not images:
        raise ValueError('image not in registry: %s arch=%s' % (image_str, "x86"))
    image = images[0]

    node_state = env.get_node_state(node.name)
    if node_state:
        if image in node_state.docker_images:
            return
        else:
            node_state.docker_images.add(image)

    size = image.size

    if size <= 0:
        return

    # # FIXME: crude simulation of layer sharing (90% across images is shared)
    # num_images = len(env.cluster.images_on_nodes[node.name]) - 1
    # if num_images > 0:
    #     size = size * 0.1

    route = env.topology.route(DockerRegistry, node)
    flow = SafeFlow(env, size, route)

    yield flow.start()

    # for hop in route.hops:
    #     env.metrics.log_network(size, 'docker_pull', hop)
    env.metrics.log_flow(size, env.now - started, route.source, route.destination, 'docker_pull')

    
    

        
                


    
        

        

        
