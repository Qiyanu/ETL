import logging
import sys
import os
import traceback
import json
import threading
from typing import Optional, Union, Dict, Any

class ETLLogger:
    """
    Advanced logging utility for ETL processes with structured logging support.
    
    Provides:
    - Structured JSON logging for cloud environments
    - Correlation ID tracking with thread safety
    - Service name tracking
    - Flexible log level configuration
    """
    
    def __init__(self, service_name: Optional[str] = None, log_level: Optional[str] = None):
        """
        Initialize the ETL logger.
        
        Args:
            service_name (str, optional): Name of the service/application
            log_level (str, optional): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        # Create logger
        self.logger = logging.getLogger("etl")
        
        # Clear existing handlers to avoid duplication
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        # Set service name
        self.service = service_name or os.environ.get("K_SERVICE", "local")
        
        # Add thread-safe context tracking
        self._thread_local = threading.local()
        self._thread_local.correlation_id = "not-set"
        
        # Set log level
        log_level = log_level or os.environ.get("LOG_LEVEL", "INFO")
        self.logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        
        # Create a proper reference to the logger instance for the filter
        logger_instance = self
        
        class ContextFilter(logging.Filter):
            def filter(self, record):
                # Get correlation ID from the thread-local storage
                thread_local_id = getattr(logger_instance._thread_local, 'correlation_id', 'not-set')
                record.correlation_id = thread_local_id
                record.service = logger_instance.service
                return True
        
        # Create handler for stdout (cloud-friendly)
        handler = logging.StreamHandler(sys.stdout)
        
        # Structured JSON formatter
        formatter = logging.Formatter(
            json.dumps({
                "timestamp": "%(asctime)s",
                "severity": "%(levelname)s", 
                "correlation_id": "%(correlation_id)s", 
                "service": "%(service)s", 
                "message": "%(message)s",
                "module": "%(module)s",
                "line": "%(lineno)d"
            })
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        # Add context filter
        context_filter = ContextFilter()
        self.logger.addFilter(context_filter)
    
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log a debug message."""
        self.logger.debug(msg, *args, **kwargs)
    
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log an info message."""
        self.logger.info(msg, *args, **kwargs)
    
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log a warning message."""
        self.logger.warning(msg, *args, **kwargs)
    
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """
        Log an error message.
        
        Optionally includes traceback if exc_info is True.
        """
        self.logger.error(msg, *args, **kwargs)
    
    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log a critical message."""
        self.logger.critical(msg, *args, **kwargs)
    
    def log_error_with_trace(self, error: Union[Exception, str]) -> None:
        """
        Log an error with full traceback.
        
        Args:
            error (Exception or str): Error to log, can be an exception or error message
        """
        if isinstance(error, Exception):
            # Log full exception details
            error_msg = f"{type(error).__name__}: {str(error)}"
            trace = traceback.format_exc()
            self.error(f"Exception occurred: {error_msg}\nFull Traceback:\n{trace}")
        else:
            # Log string error message
            self.error(str(error))
    
    @property
    def correlation_id(self) -> str:
        """Get the current correlation ID for this thread."""
        return getattr(self._thread_local, 'correlation_id', 'not-set')
    
    @correlation_id.setter
    def set_correlation_id(self, correlation_id: str) -> None:
        """
        Set a correlation ID for tracking related log entries.
        Thread-safe implementation that uses thread-local storage.
        
        Args:
            correlation_id (str): Unique identifier for a specific process/request
        """
        self._thread_local.correlation_id = correlation_id
    
    def set_service_name(self, service_name: str) -> None:
        """
        Set the service name for logging.
        
        Args:
            service_name (str): Name of the service/application
        """
        self.service = service_name

# Create a global logger instance with better thread safety
# This can be imported and used across the application
logger = ETLLogger()