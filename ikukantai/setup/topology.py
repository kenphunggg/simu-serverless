import logging
from logger_config.logger_config import setup_logger

from sim.topology import Topology

import ether.scenarios.cloudregions as scenario

logger = logging.getLogger(__name__)
setup_logger()

def ikukantai_topology() -> Topology:
    t = Topology()
    
    my_regions = ["cloud-region", "edge-region"]
    my_region_sizes = [(2, 1), (3, 1)] 
    
    ''' 
    :param regions: list of region names
    :param region_sizes: server_per_rack x racks
    '''
    my_scenario = scenario.CloudRegionsScenario(regions=my_regions, region_size=my_region_sizes)
    my_scenario.materialize(t)
    
    t.init_docker_registry()

    return t