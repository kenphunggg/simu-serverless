import logging
import time

from skippy.core.scheduler import Scheduler

from sim.benchmark import Benchmark
from sim.core import Environment, timeout_listener
from sim.docker import ContainerRegistry
from sim.faas.system import DefaultFaasSystem
from sim.metrics import Metrics, RuntimeLogger
from sim.resource import MetricsServer, ResourceState, ResourceMonitor
from sim.skippy import SimulationClusterContext
from sim.topology import Topology
from sim.faassim import SimpleSimulatorFactory

logger = logging.getLogger(__name__)

class Simulation:

    def __init__(self, topology: Topology, benchmark: Benchmark, env: Environment = None, timeout=None, name=None):
        self.env = env or Environment()
        self.topology = topology
        self.benchmark = benchmark
        self.timeout = timeout
        self.name = name

    def run(self):
        logger.info('initializing simulation, benchmark: %s, topology nodes: %d',
                    type(self.benchmark).__name__, len(self.topology.nodes))

        env = self.env

        env.benchmark = self.benchmark
        env.topology = self.topology

        self.init_environment(env)

        then = time.time()

        if self.timeout:
            logger.info('starting timeout listener with timeout %d', self.timeout)
            env.process(timeout_listener(env, then, self.timeout))

        logger.info('starting resource monitor')
        env.process(env.resource_monitor.run())

        logger.info('setting up benchmark - populate global image registry') # Changing image needed in the future
        self.benchmark.setup(env)

        logger.info('starting faas system')
        env.faas.start() # run schedule worker

        logger.info('starting benchmark process')
        p = env.process(self.benchmark.run(env))

        logger.info('executing simulation')
        env.run(until=p)

        logger.info('simulation ran %.2fs sim, %.2fs wall', env.now, (time.time() - then))

    def init_environment(self, env):
        if not env.simulator_factory: # Custom in ikukantai/loadbalancer
            env.simulator_factory = env.simulator_factory or self.create_simulator_factory()

        if not env.container_registry: # Current use default
            env.container_registry = self.create_container_registry()

        if not env.faas: # Custom in ikukantai/system/ikukantai.py
            env.faas = self.create_faas_system(env)

        if not env.metrics: # Current use default
            env.metrics = Metrics(env, RuntimeLogger())

        if not env.cluster: # Current use default
            env.cluster = SimulationClusterContext(env)

        if not env.scheduler: # Custom in ikukantai/scheduler/main.py
            env.scheduler = self.create_scheduler(env) # this will only choose which pod to schedule but not know when

        if not env.metrics_server: # Current use default
            env.metrics_server = MetricsServer()

        if not env.resource_state: # Current use default
            env.resource_state = ResourceState()

        if not env.resource_monitor: # Current use default
            env.resource_monitor = ResourceMonitor(env, 1)

    def create_container_registry(self):
        return ContainerRegistry()

    def create_simulator_factory(self):
        return SimpleSimulatorFactory()

    def create_faas_system(self, env):
        return DefaultFaasSystem(env)

    def create_scheduler(self, env):
        return Scheduler(env.cluster)