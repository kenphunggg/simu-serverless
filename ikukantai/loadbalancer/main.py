import logging
from logger_config.logger_config import setup_logger

import random
import time
from datetime import datetime

from setup.main import ikukantai_topology

import sim.docker as docker
from sim.core import Environment
from sim.faas import FunctionSimulator, FunctionReplica, FunctionRequest, SimulatorFactory, FunctionContainer

logger = logging.getLogger(__name__)
setup_logger()

# Import this
class CustomSimulatorFactory(SimulatorFactory):

    def __init__(self) -> None:
        super().__init__()

    def create(self, env: Environment, fn: FunctionContainer) -> FunctionSimulator:
        return MyFunctionSimulator()


class MyFunctionSimulator(FunctionSimulator):

    def deploy(self, env: Environment, replica: FunctionReplica):
        # simulate a docker pull command for deploying the function (also done by sim.faassim.DockerDeploySimMixin)
        yield from docker.pull(env, replica.container.image, replica.node.ether_node)

    def startup(self, env: Environment, replica: FunctionReplica):
        logger.info('[simtime=%.2f] starting up function replica for function %s', env.now, replica.function.name)

        # you could create a very fine-grained setup routines here
        yield env.timeout(10)  # simulate docker startup

    def setup(self, env: Environment, replica: FunctionReplica):        
        
        # no setup routine
        yield env.timeout(0)

    def invoke(self, env: Environment, replica: FunctionReplica, request: FunctionRequest):
        # you would probably either create one simulator per function, or use a generalized simulator, this is just
        # to demonstrate how the simulators are used to encapsulate simulator behavior.
                
        cloud_region = ["server_0", "server_1"]
        edge_region = ["server_2", "server_3", "server_4"]
        allnode = cloud_region + edge_region
        
        nodeIdx = random.randint(0,4)
        nodeSource = allnode[nodeIdx]
        nodeDes = replica.node.name
        
        delay = 0
        jitter = 0.001
        
        if inRegion(nodeDes, cloud_region):
            if inRegion(nodeSource, cloud_region):
                # logger.warning(f"{nodeDes} From Cloud to Cloud")
                delay = 0.01
            elif inRegion(nodeSource, edge_region):
                # logger.warning(f"{nodeDes} From Edge to Cloud")
                delay = 0.05
        elif inRegion(nodeDes, edge_region):
            if inRegion(nodeSource, edge_region):
                # logger.warning(f"{nodeDes} From Edge to Edge")
                delay = 0.01
            elif inRegion(nodeSource, cloud_region):
                # logger.warning(f"{nodeDes} From Edge to Edge")
                delay = 0.05

        # logger.info('[simtime=%.2f] invoking function %s from node %s to node %s', env.now, request, nodeSource, replica.node.name)

        # for full flexibility you decide the resources used
        cpu_millis = replica.node.capacity.cpu_millis * 0.1
        env.resource_state.put_resource(replica, 'cpu', cpu_millis)
        node = replica.node

        node.current_requests.add(request)
        
        latency = random.uniform((delay - jitter), (delay + jitter))
        
        if replica.function.name == 'python-pi':
            if replica.node.name.startswith('rpi3'):  # those are nodes we created in basic.example_topology()
                yield env.timeout(20 + latency)  # invoking this function takes 20 seconds on a raspberry pi
            else:
                yield env.timeout(2 + latency)  # invoking this function takes 2 seconds on all other nodes in the cluster
            
        elif replica.function.name == 'resnet50-inference':
            yield env.timeout(0.5 + latency)  # invoking this function takes 500 ms
        else:
            yield env.timeout(0 + latency)
            

        # also, you have to release them at the end
        env.resource_state.remove_resource(replica, 'cpu', cpu_millis)
        node.current_requests.remove(request)
        # logger.warning('[endsimtime=%.2f] END invoking function %s from node %s to node %s', env.now, request, nodeSource, replica.node.name)
        

    def teardown(self, env: Environment, replica: FunctionReplica):
        yield env.timeout(0)

def inRegion(node, region):
    for i in range(len(region)):
        if node == region[i]:
            return True
        return False


