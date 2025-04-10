from ikukantai.statemonitor.arch import MainMonitor, FunctionMonitor, PodMonitor

class StateAPI:
    """
    API to controll life circle of pod
    """
    def to_null(main_monitor:MainMonitor, function_name, pod_name):
        """
        No information of pod on the system
        """
        return

    def to_cold(main_monitor:MainMonitor, function_name, pod_name):
        """
        Initializing an identification for pod
        """
        # Create new instance with name pod_name
        
        return 
    
    def to_warm(main_monitor:MainMonitor, function_name, pod_name, node):
        """
        Downloading image for pod
        """
        return
    
    def to_warmdisk(main_monitor:MainMonitor, function_name, pod_name, node):
        """
        Using the image to turn the pod on
        """
        return
    
    def to_active(main_monitor:MainMonitor, function_name, pod_name, node):
        """
        Receiving requests
        """
        return
        
    