import logging
from logger_config.logger_config import setup_logger, helloWorld

from setup.main import ikukantai_topology, IkukantaiBenchmark
from scheduler.main import CustomScheduler as scheduler
from loadbalancer.main import CustomSimulatorFactory as loadBalancer
from system.ikukantai import IkukantaiSystem

from sim.faassim import Simulation
import networkx as nx

logger = logging.getLogger(__name__)
setup_logger()

def main():
    logging.basicConfig(level=logging.DEBUG)
    
    helloWorld()
    
    sim = Simulation(ikukantai_topology(), IkukantaiBenchmark())
    
    sim.env.faas = IkukantaiSystem(env=sim.env, scale_by_reinforcement_learning=True)
    
    sim.create_scheduler = scheduler.create
    
    sim.create_simulator_factory = loadBalancer
    
    sim.run()
    
if __name__ == '__main__':
    main()