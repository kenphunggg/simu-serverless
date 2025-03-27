import logging
from logger_config.logger_config import setup_logger

from setup.main import ikukantai_topology, IkukantaiBenchmark
from scheduler.main import CustomScheduler as scheduler
from loadbalancer.main import CustomSimulatorFactory as loadBalancer

from sim.faassim import Simulation
import networkx as nx

logger = logging.getLogger(__name__)
setup_logger()

def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.info('Initializing for Ikukantai')
    
    sim = Simulation(ikukantai_topology(), IkukantaiBenchmark())
    
    # sim.create_scheduler = scheduler.create
    
    sim.create_simulator_factory = loadBalancer
    
    sim.run()
    
if __name__ == '__main__':
    main()