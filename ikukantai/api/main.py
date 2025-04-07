from typing import List
import logging
from logger_config.logger_config import setup_logger

from skippy.core.utils import parse_size_string

from sim import docker
from sim.docker import ImageProperties
from sim.core import Environment
from sim.faas import Function, FunctionImage, FunctionContainer, FunctionDeployment, ScalingConfiguration

logger = logging.getLogger(__name__)
setup_logger()

class FunctionMonitor:
    """
    Manage all pod with same functions
    """
    def __init__(self, function, name):
        self.function = function
        self.name = name
        self.podmonitor_map = []
        
    def add_podmonitor(self, podmonitor):
        self.podmonitor_map.append(podmonitor)
        

class PodMonitor:
    """
    Hold simulation specific runtime knowledge about a pod. I this case is the 'life cycle of a pod' that we have designed
    """
    def __init__(self, function_monitor, name, size ='58M', arch ='arm32'):
        self.function_monitor = function_monitor
        
        self.name = name      
        self.size = size
        self.arch = arch
        
        self.null = False # ksvc available
        self.cold = True # identity available
        self.warm = False # image available
        self.warmdisk = False # container available
        self.active = False # receiving request
        
        if isinstance(function_monitor, FunctionMonitor):
            function_monitor.add_podmonitor(self)
            logger.info(f'Pod {self.name} is set to cold')
        else:
            logger.warning(f'Err when adding podmonitor {self.name} to FunctionMonitor {self.function_monitor.name}')
        
    def prepare_deployment(self):
        # Design time
        fn = Function(self.function_name, fn_images=[FunctionImage(image=self.function_name)])
        
        # Run time
        fn_container = FunctionContainer(FunctionImage(image=self.function_name))
        
        fn_funcdeployment = FunctionDeployment(
            fn,
            [fn_container],
            ScalingConfiguration
        )
        

    def to_warm(self, env: Environment): # make image available
        if self.cold == True:
            # download image
            containers: docker.ContainerRegistry = env.container_registry
            
            logger.info(f'Pod {self.name} is set to warm')
            self.warm = True
            self.cold = False
            return
        elif self.warmdisk == True:
            # remove container
            self.warm = True
            self.warmdisk = False
            return
        
