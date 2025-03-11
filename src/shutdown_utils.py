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
        self._shutdown_time = None
        self._lock = threading.Lock()
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        
        logger.info("Shutdown handler initialized")
    
    def _handle_signal(self, signum, frame):
        """
        Handle incoming shutdown signals.
        
        Args:
            signum (int): Signal number
            frame (frame): Current stack frame
        """
        with self._lock:
            sig_name = signal.Signals(signum).name
            logger.warning(f"Received {sig_name} signal, initiating graceful shutdown")
            self._shutdown_requested.set()
            
            # Import time inside the method to avoid circular imports
            import time
            self._shutdown_time = time.time()
    
    def request_shutdown(self):
        """
        Manually request a shutdown.
        """
        with self._lock:
            logger.warning("Shutdown manually requested")
            self._shutdown_requested.set()
            
            # Import time inside the method to avoid circular imports
            import time
            self._shutdown_time = time.time()
    
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
        # Fast path - check event first without lock
        if self._shutdown_requested.is_set():
            return True
            
        with self._lock:
            # Check environment variable for explicit shutdown request
            env_shutdown = os.environ.get('ETL_SHUTDOWN', '').lower() in ('true', '1', 'yes')
            
            # Check if shutdown has been requested via signal or environment
            is_requested = (self._shutdown_requested.is_set() or 
                           env_shutdown or 
                           self._check_external_shutdown_indicators())
                           
            # Set event if detected from other sources
            if is_requested and not self._shutdown_requested.is_set():
                self._shutdown_requested.set()
                
                # Import time inside the method to avoid circular imports
                import time
                self._shutdown_time = time.time()
                
            return is_requested
    
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
        
        # Check for low system resources
        try:
            # Check if memory is critically low (less than 5% available)
            memory = psutil.virtual_memory()
            if memory.available < (0.05 * memory.total):  # Less than 5% available memory
                logger.warning("Critical memory usage detected (less than 5% available), initiating shutdown")
                return True
            
            # Check if memory usage is very high (more than 95%)
            if memory.percent > 95:  # 95% memory usage
                logger.warning("Critical memory usage detected (>95%), initiating shutdown")
                return True
            
            # Check if disk is critically full
            disk = psutil.disk_usage('/')
            if disk.percent > 95:  # 95% disk usage
                logger.warning("Critical disk space usage detected (>95%), initiating shutdown")
                return True
        except (ImportError, AttributeError, OSError) as e:
            # Log but don't fail if psutil has issues
            logger.warning(f"Error checking system resources: {e}")
        
        return False
    
    def get_shutdown_time(self):
        """
        Get the time when shutdown was requested.
        
        Returns:
            float or None: Timestamp when shutdown was requested, or None if not requested
        """
        with self._lock:
            return self._shutdown_time

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

def get_shutdown_time():
    """
    Get the time when shutdown was requested.
    
    Returns:
        float or None: Timestamp when shutdown was requested, or None if not requested
    """
    return _shutdown_handler.get_shutdown_time()