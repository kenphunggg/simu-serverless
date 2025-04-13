import logging
from logger_config.logger_config import setup_logger

from ikukantai.statemonitor.arch import MainMonitor, FunctionMonitor, PodMonitor

import sim.docker as docker
from sim.core import Environment
from sim.faas import FaasSystem

from skippy.core.model import Node

logger = logging.getLogger(__name__)
setup_logger()

class StateAPI:
    """
    API to controll life circle of pod
    """
    @staticmethod
    def to_null(main_monitor:MainMonitor, function_name, pod_name):
        """
        No information of pod on the system
        """
        return

    @staticmethod
    def to_cold(main_monitor:MainMonitor, function_name:str, pod_name:str):
        """
        Initializing an identification for pod
        """
        # Create new instance with name pod_name
        fn_monitor = main_monitor.fn_monitor_map[function_name]
        if pod_name not in fn_monitor.podmonitor_map:
            logger.info(f"Changing pod '{pod_name}' of function '{function_name}' from null to cold")
            fn_monitor.add_podmonitor(name=pod_name)
        else:
            pod_monitor = fn_monitor.podmonitor_map[pod_name]
            if pod_monitor.warm:   
                logger.info(f"Changing pod '{pod_name}' of function '{function_name}' from warm to cold")
                pod_monitor = fn_monitor.podmonitor_map[pod_name]
                pod_monitor.warm = False
                pod_monitor.cold = True
            else:
                logger.warning("Current pod not in valid state")
    
    @staticmethod
    def to_warmdisk(env: Environment, main_monitor: MainMonitor, function_name: str, pod_name: str):
        """
        Downloading image for pod
        """
        logger.debug(f"to_warmdisk called with env: {env}, faas: {env.faas}") # Check env and its faas
        fn_monitor = main_monitor.fn_monitor_map[function_name]
        if pod_name not in fn_monitor.podmonitor_map:
            logger.warning(f"Pod '{pod_name}' is not intialized")
        else:
            pod_monitor = fn_monitor.podmonitor_map[pod_name]
            if pod_monitor.cold:
                logger.info(f"Changing pod '{pod_name}' of function '{function_name}' from cold to warmdisk")
                pod_monitor = fn_monitor.podmonitor_map[pod_name]
                # yield from docker.pull(env, fn_monitor.function.fn_images[0].image, )
                pod_monitor.cold = False
                pod_monitor.warm = True
            elif pod_monitor.warmdisk:
                logger.info(f"Changing pod '{pod_name}' of function '{function_name}' from warm to warmdisk")
                pod_monitor = fn_monitor.podmonitor_map[pod_name]
                pod_monitor.warmdisk = False
                pod_monitor.warm = True
            else:
                logger.warning("Current pod not in valid state")
        return
    
    @staticmethod
    def to_warm(env: Environment, main_monitor: MainMonitor, function_name: str, pod_name: str, node: Node):
        """
        Using the image to turn the pod on
        """
        faas: FaasSystem = env.faas
        logger.debug(f"to_warm called with env: {env}, faas: {env.faas}") # Check env and its faas
        fn_monitor = main_monitor.fn_monitor_map[function_name]
        if pod_name not in fn_monitor.podmonitor_map:
            logger.warning(f"Pod '{pod_name}' is not intialized")
        else:
            pod_monitor = fn_monitor.podmonitor_map[pod_name]
            if pod_monitor.warm:
                logger.info(f"Changing pod '{pod_name}' of function '{function_name}' from warmdisk to warm")
                pod_monitor = fn_monitor.podmonitor_map[pod_name]
                yield from faas.scale_up(function_name, int(1))
                logger.debug(f"Scaled up {function_name} by 1")
                pod_monitor.warm = False
                pod_monitor.warmdisk = True
            elif pod_monitor.active:
                logger.info(f"Changing pod '{pod_name}' of function '{function_name}' from active to warm")
                pod_monitor = fn_monitor.podmonitor_map[pod_name]
                pod_monitor.active = False
                pod_monitor.warmdisk = True
            else:
                logger.warning("Current pod not in valid state")
        return
    
    @staticmethod
    def to_active(main_monitor:MainMonitor, function_name:str, pod_name: str, node: Node):
        """
        Receiving requests
        """
        fn_monitor = main_monitor.fn_monitor_map[function_name]
        if pod_name not in fn_monitor.podmonitor_map:
            logger.warning(f"Pod '{pod_name}' is not intialized")
        else:
            pod_monitor = fn_monitor.podmonitor_map[pod_name]
            if pod_monitor.warmdisk:
                logger.info(f"Changing pod '{pod_name}' of function '{function_name}' from warm to active")
                pod_monitor = fn_monitor.podmonitor_map[pod_name]
                pod_monitor.cold = False
                pod_monitor.warm = True
            else:
                logger.warning("Current pod not in valid state")
        return
    
    
    

        
    