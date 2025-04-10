import logging
import time
from typing import Dict, List

from system.autoscaler import ReinforcementLearningScaler
from ikukantai.statemonitor.arch import FunctionMonitor

from sim.faas.system import DefaultFaasSystem, simulate_function_start
from sim.core import Environment
from sim.faas import FunctionReplica, FunctionDeployment, FunctionContainer
from sim.faas.scaling import FaasRequestScaler, AverageFaasRequestScaler, AverageQueueFaasRequestScaler
from sim.faas.scaling import FaasRequestScaler

logger = logging.getLogger(__name__)


class IkukantaiSystem(DefaultFaasSystem):
    
    def __init__(self, 
                 env, 
                 scale_by_requests = False, 
                 scale_by_average_requests = False, 
                 scale_by_queue_requests_per_replica = False, 
                 scale_by_reinforcement_learning = False):
        super().__init__(env, scale_by_requests, scale_by_average_requests, scale_by_queue_requests_per_replica)
        
        self.scale_by_reinforcement_learning = scale_by_reinforcement_learning
        self.reinforcement_learning_scaler: Dict[str, ReinforcementLearningScaler] = dict()
    
    def deploy(self, fd: FunctionDeployment, fm: FunctionMonitor):
        if fd.name in self.functions_deployments:
            raise ValueError('function already deployed')

        self.functions_deployments[fd.name] = fd
        # TODO remove specific scaling approaches, it's more extendable to let users start scaling technique that iterates over FDs
        self.faas_scalers[fd.name] = FaasRequestScaler(fd, self.env)
        self.avg_faas_scalers[fd.name] = AverageFaasRequestScaler(fd, self.env)
        self.queue_faas_scalers[fd.name] = AverageQueueFaasRequestScaler(fd, self.env)
        self.reinforcement_learning_scaler[fd.name] = ReinforcementLearningScaler(fd, fm, self.env)
        
        # current using [scale_by_requests] but do not know why
        # see [simu-serverless/ext/raith21/main.py] - line 77
        if self.scale_by_requests: 
            self.env.process(self.faas_scalers[fd.name].run())
        if self.scale_by_average_requests_per_replica:
            self.env.process(self.avg_faas_scalers[fd.name].run())
        if self.scale_by_queue_requests_per_replica:
            self.env.process(self.queue_faas_scalers[fd.name].run())
        if self.scale_by_reinforcement_learning:
            self.env.process(self.reinforcement_learning_scaler[fd.name].run())

        for f in fd.fn_containers:
            self.function_containers[f.image] = f

        # TODO log metadata
        self.env.metrics.log_function_deployment(fd)
        self.env.metrics.log_function_deployment_lifecycle(fd, 'deploy')
        logger.info('deploying function %s with scale_min=%d', fd.name, fd.scaling_config.scale_min)
        yield from self.scale_up(fd.name, fd.scaling_config.scale_min)
        
    def deploy_replica(self, fd: FunctionDeployment, fn: FunctionContainer, services: List[FunctionContainer]):
        """
        Creates and deploys a FunctionReplica for the given FunctionContainer.
        In case no node supports the given FunctionContainer, the services list dictates which FunctionContainer to try next.
        In case no FunctionContainer can be hosted, the scheduling process terminates and logs the failed attempt
        """
        replica = self.create_replica(fd, fn)
        self.replicas[fd.name].append(replica)
        self.env.metrics.log_queue_schedule(replica)
        self.env.metrics.log_function_replica(replica)
        yield self.scheduler_queue.put((replica, services))
    
    def run_scheduler_worker(self): # NOTE HOW TO RUN SCHEDULE KEN
        env = self.env

        while True:
            replica: FunctionReplica
            replica, services = yield self.scheduler_queue.get()

            logger.debug('scheduling next replica %s', replica.function.name)

            # schedule the required pod
            # find a node to schedule pod
            self.env.metrics.log_start_schedule(replica)
            pod = replica.pod
            then = time.time()
            result = env.scheduler.schedule(pod) # it will call [custom_scheduler] to choose which node to schedule pod
            duration = time.time() - then
            # It will log the following line
            # Pod scheduling took 0.13 ms, and yielded SchedulingResult(suggested_host=server_0, feasible_nodes=5, needed_images=[])
            self.env.metrics.log_finish_schedule(replica, result)

            yield env.timeout(duration)  # include scheduling latency in simulation time

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Pod scheduling took %.2f ms, and yielded %s', duration * 1000, result)

            if not result.suggested_host:
                self.replicas[replica.fn_name].remove(replica)
                if len(services) > 0:
                    logger.warning('retry scheduling pod %s', pod.name)
                    yield from self.deploy_replica(replica.function, services[0], services[1:])
                else:
                    logger.error('pod %s cannot be scheduled', pod.name)

                continue

            logger.info('pod %s was scheduled to %s', pod.name, result.suggested_host)

            replica.node = self.env.get_node_state(result.suggested_host.name)
            node = replica.node.skippy_node

            env.metrics.log('allocation', {
                'cpu': 1 - (node.allocatable.cpu_millis / node.capacity.cpu_millis),
                'mem': 1 - (node.allocatable.memory / node.capacity.memory)
            }, node=node.name)

            self.functions_definitions[replica.image] += 1
            self.replica_count[replica.fn_name] += 1

            self.env.metrics.log_function_deploy(replica)
            # start a new process to simulate starting of pod
            env.process(simulate_function_start(env, replica))
            
    def remove(self, fn: FunctionDeployment):
        self.env.metrics.log_function_deployment_lifecycle(fn, 'remove')

        replica_count = self.replica_count[fn.name]
        yield from self.scale_down(fn.name, replica_count)
        # TODO can be removed after using a central scaler for all FDs
        self.faas_scalers[fn.name].stop()
        self.avg_faas_scalers[fn.name].stop()
        self.queue_faas_scalers[fn.name].stop()
        self.reinforcement_learning_scaler[fn.name].stop()

        del self.functions_deployments[fn.name]
        del self.faas_scalers[fn.name]
        del self.avg_faas_scalers[fn.name]
        del self.queue_faas_scalers[fn.name]
        del self.reinforcement_learning_scaler[fn.name]
        del self.replica_count[fn.name]
        for container in fn.fn_containers:
            del self.functions_definitions[container.image]
 
           

        
        
