import logging
from logger_config.logger_config import setup_logger, helloWorld

from ikukantai.setup.benchmark import IkukantaiBenchmark
from ikukantai.setup.topology import ikukantai_topology

from scheduler.main import CustomScheduler as scheduler
from loadbalancer.main import CustomSimulatorFactory as loadBalancer
from system.ikukantai import IkukantaiSystem

from system.faasim import Simulation

logger = logging.getLogger(__name__)
setup_logger()

def main():
    logging.basicConfig(level=logging.DEBUG)
    
    helloWorld()
    
    # Setup topology and Benchmark for the testbed
    # For the benchmark, we can adjust runtime, simulate user request and services(images) used on the testbed
    sim = Simulation(ikukantai_topology(), IkukantaiBenchmark())
    
    # Adjust autoscaling algorithm for kubernetes system
    # Currently we're using Reinforcement Learning algorithm
    # See more about Reinforcement learning algorithm in ./ikukantai/system/autoscaler.py
    # See more about how i implement pod's life cycle in ikukantai/statemonitor
    sim.env.faas = IkukantaiSystem(env=sim.env, scale_by_reinforcement_learning=True)
    
    # This scheduler will choose the node for the incoming pod
    sim.create_scheduler = scheduler.create
    
    # How function is simulated
    sim.create_simulator_factory = loadBalancer
    
    # Run simulation
    sim.run()
    
if __name__ == '__main__':
    main()