from typing import List, Dict
import logging
from logger_config.logger_config import setup_logger

from sim.faas import Function

from skippy.core.model import Node

logger = logging.getLogger(__name__)
setup_logger()


class MainMonitor:
    """
    Manage all function
    """
    def __init__(self):
        self.fn_monitor_map: Dict[str, 'FunctionMonitor'] = {}
        
    def add_fnMonitor(self, fn_monitor: 'FunctionMonitor'):
        self.fn_monitor_map[fn_monitor.function.name] = fn_monitor
        logger.info(f"Finish adding new FunctionMonitor for function '{fn_monitor.function.name}'")
        
class FunctionMonitor:
    """
    Manage all pod with same functions
    """
    def __init__(self, function : Function, mainmonitor: MainMonitor):
        self.name = function.name 
        self.function = function
        self.mainmonitor = mainmonitor
        self.podmonitor_map: Dict[str, 'PodMonitor'] = {}
        
    def add_podmonitor(self, name):
        new_podmonitor = PodMonitor(function_monitor=self, name=name)
        self.podmonitor_map[name] = new_podmonitor

class PodMonitor:
    """
    Hold simulation specific runtime knowledge about a pod. I this case is the 'life cycle of a pod' that we have designed
    """
    def __init__(self, function_monitor: FunctionMonitor, name):
        self.function_monitor = function_monitor
        
        self.name = name
        
        self.null = False # ksvc available
        self.cold = True # identity available
        self.warm = False # image available
        self.warmdisk = False # container available
        self.active = False # receiving request
        
        self.node: Node = None
        # self.schedule_sig = True
        
        # if isinstance(function_monitor, FunctionMonitor):
        #     function_monitor.add_podmonitor(self)
        #     logger.info(f'Pod {self.name} is set to cold')
        # else:
        #     logger.warning(f'Err when adding podmonitor {self.name} to FunctionMonitor {self.function_monitor.name}')
        

    
        

        

        
