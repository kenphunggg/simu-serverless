import logging
from logger_config.logger_config import setup_logger

import random

from ikukantai.statemonitor.arch import MainMonitor, FunctionMonitor, PodMonitor

from skippy.core.clustercontext import ClusterContext
from skippy.core.model import SchedulingResult, Pod

from sim.core import Environment

logger = logging.getLogger(__name__)
setup_logger()

class CustomScheduler:
    """
    Example scheduler implementation that picks a node at random.
    """

    def __init__(self, cluster: ClusterContext):
        self.cluster = cluster

    def schedule(self, pod: Pod, mainmonitor: MainMonitor) -> SchedulingResult:
        """
        Schedule selects a node for a Pod (in Kubernetes language). Our system assumes that Kubernetes or a similar
        platform is used as underlying runtime for the FaaS system.
        """

        function: str = pod.spec.containers[0].image
        
        
        fn_monitor: FunctionMonitor = mainmonitor.fn_monitor_map[function]
        for podmonitor in fn_monitor.podmonitor_map.values():
            if podmonitor.warmdisk == True:
                node = podmonitor.node
                
        # get all available nodes in the cluster from the cluster context
        nodes = self.cluster.list_nodes()
        
        logger.warning("selected node %s for pod %s from total of %d nodes %s", node.name, pod.name, len(nodes), nodes)

        # the last two arguments of SchedulingResult (feasible_nodes, needed_images) are not needed
        return SchedulingResult(node, len(nodes), list())

    @staticmethod
    def create(env: Environment):
        """
        Factory method that is injected into the Simulation
        """
        logger.info('Creating CustomScheduler')
        # the ClusterContext holds the cluster state (concepts from skippy)
        return CustomScheduler(env.cluster)


