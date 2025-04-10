import logging
import time
from typing import Dict, List

from ikukantai.statemonitor.arch import FunctionMonitor

from sim.core import Environment
from sim.faas import FunctionDeployment, FaasSystem
from sim.faas.scaling import FaasRequestScaler

logger = logging.getLogger(__name__)

class ReinforcementLearningScaler(FaasRequestScaler):

    def __init__(self, fn: FunctionDeployment, fm: FunctionMonitor, env: Environment):
        self.env = env
        self.function_invocations = dict() 
        self.reconcile_interval = fn.scaling_config.rps_threshold_duration # seconds the rps threshold must be violated to trigger scale up
        self.threshold = fn.scaling_config.rps_threshold # average requests per second threshold for scaling 
        self.alert_window = fn.scaling_config.alert_window # window over which to track the average rps <NOT SUPPORTED>
        self.running = True
        self.fn_name = fn.name
        self.fn = fn
        self.fm = fm

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