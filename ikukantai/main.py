import logging
from setup.main import ikukantai_topology, IkukantaiBenchmark
from miporin.main import CustomScheduler as miporin
from logger_config.logger_config import setup_logger

from sim.faassim import Simulation


logger = logging.getLogger(__name__)
setup_logger()

def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.info('Initializing for Ikukantai')
    
    sim = Simulation(ikukantai_topology(), IkukantaiBenchmark())
    
    sim.create_scheduler = miporin
    
    sim.run()
    
if __name__ == '__main__':
    main()