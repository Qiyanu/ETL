import os
import signal
import threading
import time

from src.logging_utils import logger

class ShutdownHandler:
    """
    A simplified handler for graceful shutdown requests.
    """
    
    def __init__(self, config=None):
        """
        Initialize the shutdown handler.
        
        Args:
            config (dict, optional): Configuration dictionary with shutdown settings
        """
        self._shutdown_requested = threading.Event()
        self._shutdown_time = None
        
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
        sig_name = signal.Signals(signum).name
        logger.warning(f"Received {sig_name} signal, initiating graceful shutdown")
        self._shutdown_requested.set()
        self._shutdown_time = time.time()
    
    def request_shutdown(self):
        """
        Manually request a shutdown.
        """
        logger.warning("Shutdown manually requested")
        self._shutdown_requested.set()
        self._shutdown_time = time.time()
    
    def is_shutdown_requested(self) -> bool:
        """
        Check if a shutdown has been requested.
        
        Returns:
            bool: Whether shutdown has been requested
        """
        if self._shutdown_requested.is_set():
            return True
            
        # Check environment variable for explicit shutdown request
        env_shutdown = os.environ.get('ETL_SHUTDOWN', '').lower() in ('true', '1', 'yes')
        
        if env_shutdown and not self._shutdown_requested.is_set():
            self._shutdown_requested.set()
            self._shutdown_time = time.time()
            
        return self._shutdown_requested.is_set()
    
    def get_shutdown_time(self):
        """
        Get the time when shutdown was requested.
        
        Returns:
            float or None: Timestamp when shutdown was requested, or None if not requested
        """
        return self._shutdown_time

# Create a global shutdown handler instance
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