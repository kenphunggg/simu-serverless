import logging
from typing import Dict

from logger_config.logger_config import setup_logger
from setup.config import Config

from ikukantai.statemonitor.arch import MainMonitor

import simpy

import sim.docker as docker
from sim.core import Environment
from sim.faas import FunctionSimulator, FunctionReplica, FunctionRequest, SimulatorFactory, FunctionContainer
from sim.core import NodeState

logger = logging.getLogger(__name__)
setup_logger()

# Import this
class CustomSimulatorFactory(SimulatorFactory):

    def __init__(self) -> None:
        super().__init__()

    def create(self, env: Environment, fn: FunctionContainer) -> FunctionSimulator:
        return MyFunctionSimulator()


class MyFunctionSimulator(FunctionSimulator):
    
    def __init__(self):
        self.concurrent_request: Dict[FunctionReplica, simpy.Resource] = dict()

    def deploy(self, env: Environment, replica: FunctionReplica):
        # simulate a docker pull command for deploying the function (also done by sim.faassim.DockerDeploySimMixin)
        # Currently disabled
        yield from docker.pull(env, replica.container.image, replica.node.ether_node)

    def startup(self, env: Environment, replica: FunctionReplica):
        # Startup pod for function
        logger.info('[simtime=%.2f] starting up function replica for function %s', env.now, replica.function.name)

        # you could create a very fine-grained setup routines here
        coldstart = 0 # FIXME by ken: adjust different start up for different functions - coldstart delay
        yield env.timeout(coldstart)  # simulate docker startup
        
        # claim resource consumption by replica
        for container in replica.pod.spec.containers:
            required_cpu_millis = container.resources.requests.get('cpu', container.resources.default_milli_cpu_request)
            required_memory = container.resources.requests.get('memory', container.resources.default_mem_request)
            
            node = replica.node.skippy_node
            node.allocatable.cpu_millis -= required_cpu_millis
            node.allocatable.memory -= required_memory

    def setup(self, env: Environment, replica: FunctionReplica):        
        # Load model, simulate resource consumption by model
        # no setup routine
        self.concurrent_request[replica] = simpy.Resource(env, capacity=Config.concurrent_request)
        yield env.timeout(0)

    def invoke(self, env: Environment, mainmonitor: MainMonitor, source_node: str, replica: FunctionReplica, request: FunctionRequest):
        # you would probably either create one simulator per function, or use a generalized simulator, this is just
        # to demonstrate how the simulators are used to encapsulate simulator behavior.
        
        # Drop request if pod alreary have request
        if self.concurrent_request[replica].count > 0:
            logger.warning(f"[simtime={env.now}] request dropped {request}") # t_drop
            mainmonitor.timestamp[request]["t_drop"] = env.now
            mainmonitor.timestamp[request]["t_2"] = env.now
            mainmonitor.timestamp[request]["t_3"] = -1
            mainmonitor.timestamp[request]["t_4"] = -1
            return
        
        mainmonitor.timestamp[request]["t_drop"] = -1
        mainmonitor.timestamp[request]["t2"] = env.now
        token = self.concurrent_request[replica].request()
        t_wait_start = env.now
        yield token
        t_wait_end = env.now
        mainmonitor.timestamp[request]["t2"] = env.now
        
        logger.info('[simtime=%.2f] invoking function %s from node %s to node %s', env.now, request, source_node, replica.node.name)

        # for full flexibility you decide the resources used
        cpu_millis = replica.node.capacity.cpu_millis * 0.1
        env.resource_state.put_resource(replica, 'cpu', cpu_millis)
        node = replica.node

        node.current_requests.add(request) # Manage cocurrent request in one node
        
        
        # if replica.function.name == 'python-pi':
        #     if replica.node.name.startswith('rpi3'):  # those are nodes we created in basic.example_topology()
        #         yield env.timeout(20)  # invoking this function takes 20 seconds on a raspberry pi
        #         logger.critical(f'1, {request}')
        #     else:
        #         yield env.timeout(2)  # invoking this function takes 2 seconds on all other nodes in the cluster
        #         logger.critical(f'2, {request}')
            
        # elif replica.function.name == 'resnet50-inference':
        #     yield env.timeout(0.5)  # invoking this function takes 500 ms
        #     logger.critical(f'3, {request}')
        # else:
        #     yield env.timeout(0)
        #     logger.critical(f'4, {request}')
        if replica.function.name == 'app1':
            yield env.timeout(0)
            
        # also, you have to release them at the end
        env.resource_state.remove_resource(replica, 'cpu', cpu_millis)
        node.current_requests.remove(request)
        logger.warning('[endsimtime=%.2f] END invoking function %s from node %s to node %s', env.now, request, source_node, replica.node.name)
        
        self.concurrent_request[replica].release(token) 
        
        mainmonitor.timestamp[request]["t4"] = env.now

    def teardown(self, env: Environment, replica: FunctionReplica):
        yield env.timeout(0)
        
    

def inRegion(node, region):
    for i in range(len(region)):
        if node == region[i]:
            return True
        return False


