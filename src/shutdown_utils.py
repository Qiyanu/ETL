import os
import signal
import threading
import psutil

from src.logging_utils import logger

class ShutdownHandler:
    """
    A thread-safe shutdown handler that can detect shutdown requests 
    from multiple sources.
    """
    
    def __init__(self):
        """
        Initialize the shutdown handler.
        
        Tracks shutdown requests from:
        - Signal handlers (SIGINT, SIGTERM)
        - Environment variables
        - Manual flag setting
        """
        self._shutdown_requested = threading.Event()
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
    
    def _handle_signal(self, signum, frame):
        """
        Handle incoming shutdown signals.
        
        Args:
            signum (int): Signal number
            frame (frame): Current stack frame
        """
        sig_name = signal.Signals(signum).name
        logger.warning(f"Received {sig_name} signal, initiating graceful shutdown")
        self._shutdown_requested.set()
    
    def request_shutdown(self):
        """
        Manually request a shutdown.
        """
        logger.warning("Shutdown manually requested")
        self._shutdown_requested.set()
    
    def is_shutdown_requested(self) -> bool:
        """
        Check if a shutdown has been requested.
        
        Checks multiple sources:
        - Signal handlers
        - Environment variable
        - Manual shutdown flag
        
        Returns:
            bool: Whether shutdown has been requested
        """
        # Check environment variable for explicit shutdown request
        env_shutdown = os.environ.get('ETL_SHUTDOWN', '').lower() in ('true', '1', 'yes')
        
        # Check if shutdown has been requested via signal or environment
        return (self._shutdown_requested.is_set() or 
                env_shutdown or 
                self._check_external_shutdown_indicators())
    
    def _check_external_shutdown_indicators(self) -> bool:
        """
        Check for additional external shutdown indicators.
        
        This can include:
        - Checking specific files
        - Querying external systems
        - Monitoring system resources
        
        Returns:
            bool: Whether external shutdown indicators are present
        """
        # Example: Check for a shutdown flag file
        shutdown_flag_file = '/tmp/etl_shutdown_flag'
        if os.path.exists(shutdown_flag_file):
            logger.warning(f"Shutdown flag file detected: {shutdown_flag_file}")
            return True
        
        # Example: Check for low system resources
        try:
            # Check if memory is critically low
            memory = psutil.virtual_memory()
            if memory.percent > 95:  # 95% memory usage
                logger.warning("Critical memory usage detected, initiating shutdown")
                return True
            
            # Check if disk is critically full
            disk = psutil.disk_usage('/')
            if disk.percent > 95:  # 95% disk usage
                logger.warning("Critical disk space usage detected, initiating shutdown")
                return True
        except ImportError:
            # psutil not available, skip resource checks
            pass
        
        return False

# Create a global shutdown handler instance
_shutdown_handler = ShutdownHandler()

def is_shutdown_requested() -> bool:
    """
    Global function to check if a shutdown has been requested.
    
    Returns:
        bool: Whether shutdown has been requested
    """
    return _shutdown_handler.is_shutdown_requested()

def request_shutdown():
    """
    Manually trigger a shutdown.
    """
    _shutdown_handler.request_shutdown()