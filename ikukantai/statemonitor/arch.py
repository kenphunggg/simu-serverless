from typing import List, Dict
import logging
import simpy

from logger_config.logger_config import setup_logger

from sim.faas import Function, FaasSystem, FunctionReplica, FunctionRequest
from sim.faas.core import FunctionState
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
    def __init__(self, env: Environment):
        self.env = env
        self.run = True
        self.fn_monitor_map: Dict[str, 'FunctionMonitor'] = {}
        self.ram_usage: Dict[str, Dict[int, int]] = {} # nodename:[timestamp:ram_usage]
        self.cpu_usage: Dict[str, Dict[int, int]] = {} # nodename:[timestamp:cpu_usage]
        self.timestamp: Dict[FunctionRequest, Dict[str, int]] = {} # FunctionRequest: [t1: 2, t2: 3, t3: 4, t4: 5, t_drop: 1]
        self.run_metric_monitor = env.process(self.metric_monitor())
        
    def stop(self):
        self.run = False
        
    def add_fnMonitor(self, fn_monitor: 'FunctionMonitor'):
        self.fn_monitor_map[fn_monitor.function.name] = fn_monitor
        logger.info(f"Finish adding new FunctionMonitor for function '{fn_monitor.function.name}'")
        
    def metric_monitor(self):
        while self.run:
            self.update_metric()
            yield self.env.timeout(2)
    
    def update_metric(self):
        for node in self.env.cluster.list_nodes():
            if node not in self.ram_usage:
                self.ram_usage[node] = {}
            self.ram_usage[node][self.env.now] = node.allocatable.memory
            
            if node not in self.cpu_usage:
                self.cpu_usage[node] = {}
            self.cpu_usage[node][self.env.now] = node.allocatable.cpu_millis
            
            # logger.critical(f"RAM usage for {node} at {self.env.now} updated to {node.allocatable.memory}MB and {node.allocatable.cpu_millis}CPU.")
                
        
        
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
        
        self.firstinit = True
        
        # --- Simpy Integration ---
        # self.event = simpy.Event(env)
        # self.new_state = None
        
        # --- Simpy modified ---
        self.state_queue = simpy.Store(env)
        
        self.lifecycle_process = env.process(self.run_pod_lifecircle())
        
    
    def get_current_state(self):
        if self.null:
            return "null"
        elif self.cold:
            return "cold"
        elif self.warm:
            return "warm"
        elif self.warmdisk:
            return "warmdisk"
        elif self.active:
            return "active"
      
    def state_signal(self, new_state):
        if self.lifecycle_process is None or not self.lifecycle_process.is_alive:
            logger.warning(f"[Simtime={self.env.now}] Pod {self.name} of function {self.function_monitor.name}: Cannot send new state signal")
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

            # --- Change to null state ---
            if new_state == "null":
                if self.cold:
                    yield self.env.timeout(0)
                    self.cold = False
                    self.null = True                    
                    logger.info(f"[Simtime={self.env.now}] Pod '{self.name}' of function {self.function_monitor.name} reached null state")
            
            # --- Change to cold state ---
            elif new_state == "cold":
                if self.firstinit:
                    yield self.env.timeout(0)
                    self.firstinit = False
                    logger.info(f"[Simtime={self.env.now}] Pod '{self.name}' of function {self.function_monitor.name} reached cold state")
                    
                elif self.warmdisk:
                    self.warmdisk = False
                    self.cold = True
                    yield self.env.timeout(0)
                    logger.info(f"[Simtime={self.env.now}] Pod '{self.name}' of function {self.function_monitor.name} is reached {new_state} state")
                else:
                    logger.warning("Current pod not in valid state")   
                    logger.warning(self.get_current_state())   
            
            # --- Change to warm state ---
            elif new_state == "warmdisk": 
                if self.cold: # Download image for the pod
                    dockerPull = docker_pull(env=self.env,
                                             image_str=self.function_monitor.function.fn_images[0].image, # FIXME: Current use image[0] as default
                                             node=self.env.get_node_state(self.node.name).ether_node)
                    yield self.env.process(dockerPull)
                    self.cold = False
                    self.warmdisk = True
                    logger.info(f"[Simtime={self.env.now}] Pod '{self.name}' of function {self.function_monitor.name} is reached {new_state} state")
                elif self.warm:
                    # logger.debug(f"Changing pod '{self.name}' of function '{self.function_monitor.name}' from warm to warmdisk")
                    faas: FaasSystem = self.env.faas
                    replicas: List[FunctionReplica] = faas.get_replicas(fn_name=self.function_monitor.name)
                    
                    found_rep = False
                    for replica in replicas:
                        if replica.node.skippy_node == self.node and replica.state == FunctionState.RUNNING:
                            chosen_replica = replica
                            found_rep = True
                            break
                    if found_rep:
                        faas: FaasSystem = self.env.faas
                        yield self.env.process(faas.scale_down(self.function_monitor.name, int(1), chosen_replica))
                    else:
                        logger.warning("Not found rep")
                        
                    self.warm = False
                    self.warmdisk = True
                    logger.info(f"[Simtime={self.env.now}] Pod '{self.name}' of function {self.function_monitor.name} is reached {new_state} state")
                else:
                    logger.warning("Current pod not in valid state")
                    
            # --- Change to warm state ---
            elif new_state == "warm":
                if self.warmdisk:
                    scaleUp = faas.scale_up(self.function_monitor.name, int(1))
                    yield self.env.process(scaleUp)
                    
                    # FIXME by Ken: if multiple apps trigger scaleup the same time, 
                    # it may cause state flag change before scaleUp finish
                    # wait 0.001s may solve this 
                    yield self.env.timeout(0.001) 
                    
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

    
    

        
                


    
        

        

        
