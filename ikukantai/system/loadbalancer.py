import logging
from collections import defaultdict
from typing import List, Dict

from logger_config.logger_config import setup_logger
from setup.config import Config

from sim.topology import Topology
from sim.core import Environment, NodeState
from sim.faas.core import FunctionReplica, FunctionRequest, FunctionState

logger = logging.getLogger(__name__)
setup_logger()   
    
class EdgeLoadBalancer:
    env: Environment
    replicas: Dict[str, List[FunctionReplica]]

    def __init__(self, env: Environment, replicas: FunctionReplica) -> None:
        super().__init__()
        self.env = env
        self.topology: Topology = env.topology
        self.replicas = replicas
        self.counters = defaultdict(lambda: 0)

    def get_running_replicas(self, function: str, source_node: str):
        s_node = self.env.get_node_state(source_node).ether_node
        result = []
        for replica in self.replicas[function]:
            if replica.state == FunctionState.RUNNING:
                d_node = replica.node.ether_node
                if self.topology.latency(s_node, d_node) < Config.edge_delay:
                    # logging.critical(f"lower than 100 {self.topology.latency(s_node, d_node)} | {s_node} - {d_node}")
                    result.append(replica)
                # else:
                #     logging.critical(f"more than 100 {self.topology.latency(s_node, d_node)} | {s_node} - {d_node}")
                #     result.append(replica)
        
        return result
                
    def next_replica(self, request: FunctionRequest) -> FunctionReplica:
        replicas = self.get_running_replicas(request.name)
        i = self.counters[request.name] % len(replicas)
        self.counters[request.name] = (i + 1) % len(replicas)

        replica = replicas[i]

        return replica