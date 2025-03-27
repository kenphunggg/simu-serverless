import logging
from logger_config.logger_config import setup_logger

import random

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

    def schedule(self, pod: Pod) -> SchedulingResult:
        """
        Schedule selects a node for a Pod (in Kubernetes language). Our system assumes that Kubernetes or a similar
        platform is used as underlying runtime for the FaaS system.
        """

        # get all available nodes in the cluster from the cluster context
        nodes = self.cluster.list_nodes()
        

        # pick a node at random
        node = random.choice(nodes)

        logger.info("selected node %s for pod %s from total of %d nodes %s", node.name, pod.name, len(nodes), nodes)

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


