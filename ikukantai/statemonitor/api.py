import logging
from logger_config.logger_config import setup_logger

from ikukantai.statemonitor.arch import MainMonitor, FunctionMonitor, PodMonitor

from sim.core import Environment
from sim.faas import FaasSystem
from sim.topology import DockerRegistry
from sim.net import SafeFlow

from ether.core import Node as EtherNode


from skippy.core.model import Node

logger = logging.getLogger(__name__)
setup_logger()

class StateAPI:
    """
    API to controll life circle of pod
    """
    @staticmethod
    def to_null(env: Environment, main_monitor:MainMonitor, function_name:str, pod_name:str):
        """
        No information of pod on the system
        """
        fn_monitor = main_monitor.fn_monitor_map[function_name]
        if pod_name not in fn_monitor.podmonitor_map:
            logger.warning(f"Pod '{pod_name}' is not intialized")
        else:
            pod_monitor = fn_monitor.podmonitor_map[pod_name]
            if pod_monitor.cold:
                logger.info(f"[Simtime={env.now}] Pod '{pod_name}' of function '{function_name}' is changing to null state")
                yield pod_monitor.state_signal("null")
            else:
                logger.warning("Current pod not in valid state")
                
    @staticmethod
    def to_cold(env: Environment, main_monitor:MainMonitor, function_name:str, pod_name:str):
        """
        Initializing an identification for pod
        """
        # Create new instance with name pod_name
        fn_monitor = main_monitor.fn_monitor_map[function_name]
        if pod_name not in fn_monitor.podmonitor_map:
            logger.info(f"[Simtime={env.now}] Pod '{pod_name}' of function {fn_monitor.name} is changing to cold state")
            fn_monitor.add_podmonitor(env=env, name=pod_name)
            pod_monitor = fn_monitor.podmonitor_map[pod_name]
            yield pod_monitor.state_signal("cold")
        else:
            pod_monitor = fn_monitor.podmonitor_map[pod_name]
            if pod_monitor.warmdisk:   
                logger.info(f"[Simtime={env.now}] Pod '{pod_name}' of function {fn_monitor.name} is changing to cold state")
                pod_monitor = fn_monitor.podmonitor_map[pod_name]
                yield pod_monitor.state_signal("cold")
                # pod_monitor.warmdisk = False
                # pod_monitor.cold = True
            else:
                logger.warning("Current pod not in valid state")
    
    @staticmethod
    def to_warmdisk(env: Environment, main_monitor: MainMonitor, function_name: str, pod_name: str, node: Node):
        """
        Downloading image for pod
        """
        faas: FaasSystem = env.faas
        fn_monitor = main_monitor.fn_monitor_map[function_name]
        if pod_name not in fn_monitor.podmonitor_map:
            logger.warning(f"Pod '{pod_name}' is not intialized")
        else:
            pod_monitor = fn_monitor.podmonitor_map[pod_name]
            pod_monitor.node = node
            if pod_monitor.cold:
                logger.info(f"[Simtime={env.now}] Pod '{pod_name}' of function '{function_name}' is changing to warmdisk state")
                yield pod_monitor.state_signal("warmdisk")
            elif pod_monitor.warm:
                logger.info(f"[Simtime={env.now}] Changing pod '{pod_name}' of function '{function_name}' is changing to warmdisk state hihi")
                pod_monitor = fn_monitor.podmonitor_map[pod_name]
                yield pod_monitor.state_signal("warmdisk")
                yield env.timeout(0.001)
                # pod_monitor.warm = False
                # pod_monitor.warmdisk = True
            else:
                logger.warning("Current pod not in valid state")
        return
           
    @staticmethod
    def to_warm(env: Environment, main_monitor: MainMonitor, function_name: str, pod_name: str, node: Node):
        """
        Using the image to turn the pod on
        """
        faas: FaasSystem = env.faas
        fn_monitor = main_monitor.fn_monitor_map[function_name]
        if pod_name not in fn_monitor.podmonitor_map:
            logger.warning(f"Pod '{pod_name}' is not intialized")
        else:
            pod_monitor = fn_monitor.podmonitor_map[pod_name]
            if pod_monitor.warmdisk:
                logger.info(f"[Simtime={env.now}] Pod '{pod_name}' of function '{function_name}' is changing to warm state")
                pod_monitor = fn_monitor.podmonitor_map[pod_name]
                yield pod_monitor.state_signal("warm")
                logger.warning(f"Prepare scaling up function '{function_name}' by 1")
                pod_monitor.node = node
                # pod_monitor.warmdisk = False
                # pod_monitor.warm = True
            elif pod_monitor.active:
                logger.info(f"Pod '{pod_name}' of function '{function_name}' is changing to warm state")
                pod_monitor = fn_monitor.podmonitor_map[pod_name]
                pod_monitor.active = False
                pod_monitor.warmdisk = True
            else:
                logger.warning("Current pod not in valid state ")
    
    @staticmethod
    def to_active(main_monitor:MainMonitor, function_name:str, pod_name: str, node: Node):
        """
        Receiving requests
        """
        fn_monitor = main_monitor.fn_monitor_map[function_name]
        if pod_name not in fn_monitor.podmonitor_map:
            logger.warning(f"Pod '{pod_name}' is not intialized")
        else:
            pod_monitor = fn_monitor.podmonitor_map[pod_name]
            if pod_monitor.warmdisk:
                logger.info(f"Changing pod '{pod_name}' of function '{function_name}' from warm to active")
                pod_monitor = fn_monitor.podmonitor_map[pod_name]
                pod_monitor.cold = False
                pod_monitor.warm = True
            else:
                logger.warning("Current pod not in valid state")
        return


       
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
    logger.warning(f"Finish docker pull for image '{image.name}' at node {node.name}")

    
    

        
    