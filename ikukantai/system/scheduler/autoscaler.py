import logging
from typing import Dict, List

from ikukantai.statemonitor.arch import FunctionMonitor, MainMonitor
from ikukantai.statemonitor.api import StateAPI

from sim.core import Environment
from sim.faas import FunctionDeployment, FaasSystem, FunctionState
from sim.faas.scaling import FaasRequestScaler
from sim.resource import ResourceMonitor, MetricsServer, ResourceState

from skippy.core.model import Node

logger = logging.getLogger(__name__)

class ReinforcementLearningScaler(FaasRequestScaler):
    """
    I have implemented APIs for managing pod's lifecycle
    To implement reinforcement learning algorithm, just use APIs as below
    NOTE by ken: after invoking API, use [yield env.timeout(delay)] to wait API to finish
    if not, it will not reach desired state as we want
    i will implement machanism for warning if it not reached desired state in next few days
    """

    def __init__(self, fn: FunctionDeployment, mainmonitor: MainMonitor, fm: FunctionMonitor, env: Environment):
        self.env = env
        self.function_invocations = dict() 
        self.reconcile_interval = fn.scaling_config.rps_threshold_duration # seconds the rps threshold must be violated to trigger scale up
        self.threshold = fn.scaling_config.rps_threshold # average requests per second threshold for scaling 
        self.alert_window = fn.scaling_config.alert_window # window over which to track the average rps <NOT SUPPORTED>
        self.running = True
        
        self.fn_name = fn.name
        self.fn = fn
        self.main_monitor = mainmonitor
        self.fm = fm
        
        self.test = True

    def run(self):
        env: Environment = self.env
        faas: FaasSystem = env.faas
        resource_monitor: ResourceMonitor = env.resource_monitor
        metrics_server: MetricsServer = env.metrics_server
        resource_state: ResourceState = env.resource_state
        
        
        while self.running:
            logger.info('Invoking scheduling algorithm - reinforcement learning')
            
            # Pause the execution for the duration [reconcile_interval = 10s]
            yield env.timeout(self.reconcile_interval)
            
            node_idx = 3
            chosen_node: Node = self.env.cluster.list_nodes()[node_idx]
            
            # Get current resource allocation
            logger.debug(f"Get resource allocation on node {chosen_node}: {chosen_node.allocatable}")
            
            # Testing session
            # Log all function name (For testing)
            # for name in self.main_monitor.fn_monitor_map.keys():
            #     logger.critical(f"Logging function name for testing: {name}")
            # logger.critical(f"Logging image name for testing: {self.fm.function.fn_images[0].image}")
            # logger.critical(f"Logging all nodes for testing: {self.env.cluster.list_nodes()}")
            # logger.critical(f"Logging current node for testing: {self.env.cluster.list_nodes()[node_idx]}")
            if self.test:
                # Change pod from null to cold state
                yield env.process(
                    StateAPI.to_cold(env=self.env, main_monitor=self.main_monitor, function_name=self.fn_name, pod_name='1')
                )
                
                # Change pod from cold to warmisk state
                # Download image to node
                yield env.process(
                    StateAPI.to_warmdisk(env=self.env, main_monitor=self.main_monitor, function_name=self.fn_name, pod_name='1', node=chosen_node)
                )
                yield env.timeout(20)
                
                # Change pod from warmdisk to warm
                # Make pod available
                yield env.process(
                    StateAPI.to_warm(env=self.env, main_monitor=self.main_monitor, function_name=self.fn_name, pod_name='1', node=chosen_node)  
                )
                yield env.timeout(20)
                
                # Change pod from warm to warmdisk
                # Remove pod
                # yield env.process(
                #     StateAPI.to_warmdisk(env=self.env, main_monitor=self.main_monitor, function_name=self.fn_name, pod_name='1', node=chosen_node)
                # )
                
                # logger.critical(f"WARMDISK {chosen_node.allocatable}")
                
                # yield env.timeout(20)
                
                # Change pod from warmdisk to cold
                # Remove image - implement later
                # yield env.process(
                #     StateAPI.to_cold(env=self.env, main_monitor=self.main_monitor, function_name=self.fn_name, pod_name='1')
                # )
                
                # Change pod from cold to null
                # Delete identification
                # yield env.process(
                #     StateAPI.to_null(env=self.env, main_monitor=self.main_monitor, function_name=self.fn_name, pod_name='1')
                # )
                
                # yield env.timeout(20)
                
                self.test = False
            
                 
            self.running = False 
            logger.debug(f'Scale hanging')        

    def stop(self):
        logger.warning('Stop scheduling algorithm - reinforcement learning')
        self.running = False 
        
        

class SampleScaler(FaasRequestScaler):

    def __init__(self, fn: FunctionDeployment, env: Environment):
        self.env = env
        self.function_invocations = dict() 
        self.reconcile_interval = fn.scaling_config.rps_threshold_duration # seconds the rps threshold must be violated to trigger scale up
        self.threshold = fn.scaling_config.rps_threshold # average requests per second threshold for scaling 
        self.alert_window = fn.scaling_config.alert_window # window over which to track the average rps <NOT SUPPORTED>
        self.running = True
        self.fn_name = fn.name
        self.fn = fn

    def run(self):
        env: Environment = self.env
        faas: FaasSystem = env.faas
        while self.running:
            logger.info('Invoking scheduling algorithm - reinforcement learning')
            
            # Pause the execution for the duration [reconcile_interval = 10s]
            yield env.timeout(self.reconcile_interval)
            
            # Update current invocation
            if self.function_invocations.get(self.fn_name, None) is None:
                self.function_invocations[self.fn_name] = 0
            last_invocations = self.function_invocations.get(self.fn_name, 0)
            current_total_invocations = env.metrics.invocations.get(self.fn_name, 0)
            invocations = current_total_invocations - last_invocations
            self.function_invocations[self.fn_name] += invocations
            
            # TODO divide by alert window, but needs to store the invocations, such that reconcile_interval != alert_window is possible
            config = self.fn.scaling_config
            if (invocations / self.reconcile_interval) >= self.threshold:
                scale = (config.scale_factor / 100) * config.scale_max
                yield from faas.scale_up(self.fn_name, int(scale))
                logger.debug(f'scaled up {self.fn_name} by {scale}')
            else:
                scale = (config.scale_factor / 100) * config.scale_max
                yield from faas.scale_down(self.fn_name, int(scale))
                logger.debug(f'scaled down {self.fn_name} by {scale}')

    def stop(self):
        logger.warning('Stop scheduling algorithm - reinforcement learning')
        self.running = False 