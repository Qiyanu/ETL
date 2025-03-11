import os
import signal
import threading
import psutil
import time

from src.logging_utils import logger

class ShutdownHandler:
    """
    A thread-safe shutdown handler that can detect shutdown requests 
    from multiple sources.
    """
    
    def __init__(self, config=None):
        """
        Initialize the shutdown handler.
        
        Args:
            config (dict, optional): Configuration dictionary with shutdown settings
        
        Tracks shutdown requests from:
        - Signal handlers (SIGINT, SIGTERM)
        - Environment variables
        - Manual flag setting
        """
        self._shutdown_requested = threading.Event()
        self._shutdown_time = None
        self._lock = threading.Lock()
        
        # Initialize configuration
        self.config = config or {}
        
        # Default configuration
        self.shutdown_flag_path = self.config.get("SHUTDOWN_FLAG_PATH", "/tmp/etl_shutdown_flag")
        self.memory_threshold = self.config.get("SHUTDOWN_MEMORY_THRESHOLD", 0.05)
        self.disk_threshold = self.config.get("SHUTDOWN_DISK_THRESHOLD", 0.95)
        self.check_interval = self.config.get("SHUTDOWN_CHECK_INTERVAL", 60)  # seconds
        
        # Cache for system resource checks with expiration
        self._resource_cache = {
            "memory": {"value": None, "timestamp": 0},
            "disk": {"value": None, "timestamp": 0},
        }
        
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
    
    def _get_cached_resource_value(self, resource_type, fetch_func):
        """
        Get a cached resource value or fetch a new one if cache is expired.
        
        Args:
            resource_type (str): Type of resource (memory or disk)
            fetch_func (callable): Function to fetch the current value
            
        Returns:
            The current resource value (cached or fresh)
        """
        current_time = time.time()
        cache_entry = self._resource_cache[resource_type]
        
        # Check if cache is expired
        if current_time - cache_entry["timestamp"] > self.check_interval:
            try:
                cache_entry["value"] = fetch_func()
                cache_entry["timestamp"] = current_time
            except Exception as e:
                logger.warning(f"Error checking {resource_type} resources: {e}")
                # Keep using old value on error if available
                if cache_entry["value"] is None:
                    cache_entry["value"] = False  # Default safe value
                    
        return cache_entry["value"]
        
    def is_shutdown_requested(self) -> bool:
        """
        Check if a shutdown has been requested.
        
        Checks multiple sources:
        - Signal handlers
        - Environment variable
        - Manual shutdown flag
        - Resource constraints (with caching)
        
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
        - Monitoring system resources (with caching)
        
        Returns:
            bool: Whether external shutdown indicators are present
        """
        # Check for a shutdown flag file (path from config)
        if os.path.exists(self.shutdown_flag_path):
            logger.warning(f"Shutdown flag file detected: {self.shutdown_flag_path}")
            return True
        
        # Check for low system resources with caching
        try:
            # Get memory status (cached)
            def check_memory():
                memory = psutil.virtual_memory()
                return {
                    "available_percent": memory.available / memory.total,
                    "percent": memory.percent
                }
            
            memory_status = self._get_cached_resource_value("memory", check_memory)
            
            # Check if memory is critically low
            if memory_status and memory_status["available_percent"] < self.memory_threshold:
                logger.warning(
                    f"Critical memory usage detected (less than {self.memory_threshold*100}% available), "
                    "initiating shutdown"
                )
                return True
            
            # Check if memory usage is very high
            if memory_status and memory_status["percent"] > (self.disk_threshold * 100):
                logger.warning(
                    f"Critical memory usage detected (>{self.disk_threshold*100}%), "
                    "initiating shutdown"
                )
                return True
            
            # Get disk status (cached)
            def check_disk():
                return psutil.disk_usage('/').percent
            
            disk_percent = self._get_cached_resource_value("disk", check_disk)
            
            # Check if disk is critically full
            if disk_percent and disk_percent > (self.disk_threshold * 100):
                logger.warning(
                    f"Critical disk space usage detected (>{self.disk_threshold*100}%), "
                    "initiating shutdown"
                )
                return True
                
        except Exception as e:
            # Log but don't fail if checks have issues
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

# Create a global shutdown handler instance with config
_shutdown_handler = None

def initialize_shutdown_handler(config=None):
    """
    Initialize the global shutdown handler with configuration.
    
    Args:
        config (dict, optional): Configuration dictionary
    """
    global _shutdown_handler
    _shutdown_handler = ShutdownHandler(config)
    return _shutdown_handler

def is_shutdown_requested() -> bool:
    """
    Global function to check if a shutdown has been requested.
    
    Returns:
        bool: Whether shutdown has been requested
    """
    if _shutdown_handler is None:
        # Initialize with default config if not already initialized
        initialize_shutdown_handler()
    return _shutdown_handler.is_shutdown_requested()

def request_shutdown():
    """
    Manually trigger a shutdown.
    """
    if _shutdown_handler is None:
        # Initialize with default config if not already initialized
        initialize_shutdown_handler()
    _shutdown_handler.request_shutdown()

def get_shutdown_time():
    """
    Get the time when shutdown was requested.
    
    Returns:
        float or None: Timestamp when shutdown was requested, or None if not requested
    """
    if _shutdown_handler is None:
        # Initialize with default config if not already initialized
        initialize_shutdown_handler()
    return _shutdown_handler.get_shutdown_time()