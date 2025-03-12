import logging
import sys
import os
import traceback
import json
import threading
from typing import Optional, Any

class ETLLogger:
    """
    Simplified logging utility for ETL processes with structured logging.
    
    Provides:
    - Structured JSON logging
    - Correlation ID tracking with thread safety
    - Service name tracking
    """
    
    def __init__(self, service_name: Optional[str] = None, log_level: Optional[str] = None):
        """
        Initialize the ETL logger.
        
        Args:
            service_name (str, optional): Name of the service/application
            log_level (str, optional): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        # Create logger
        self._logger = logging.getLogger("etl")
        
        # Clear existing handlers to avoid duplication
        if self._logger.handlers:
            self._logger.handlers.clear()
        
        # Set service name
        self._service = service_name or os.environ.get("K_SERVICE", "etl-service")
        
        # Add thread-safe context tracking
        self._thread_local = threading.local()
        self._thread_local.correlation_id = "not-set"
        
        # Set log level
        log_level = log_level or os.environ.get("LOG_LEVEL", "INFO")
        self._logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        
        # Create a proper reference to the logger instance for the filter
        logger_instance = self
        
        class ContextFilter(logging.Filter):
            def filter(self, record):
                # Get correlation ID from the thread-local storage
                thread_local_id = getattr(logger_instance._thread_local, 'correlation_id', 'not-set')
                record.correlation_id = thread_local_id
                record.service = logger_instance._service
                return True
        
        # Create handler for stdout
        handler = logging.StreamHandler(sys.stdout)
        
        # Structured JSON formatter
        formatter = logging.Formatter(
            json.dumps({
                "timestamp": "%(asctime)s",
                "level": "%(levelname)s", 
                "correlation_id": "%(correlation_id)s", 
                "service": "%(service)s", 
                "message": "%(message)s"
            })
        )
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)
        
        # Add context filter
        context_filter = ContextFilter()
        self._logger.addFilter(context_filter)
    
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log a debug message."""
        self._logger.debug(msg, *args, **kwargs)
    
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log an info message."""
        self._logger.info(msg, *args, **kwargs)
    
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log a warning message."""
        self._logger.warning(msg, *args, **kwargs)
    
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log an error message."""
        self._logger.error(msg, *args, **kwargs)
    
    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log a critical message."""
        self._logger.critical(msg, *args, **kwargs)
    
    def log_error_with_trace(self, error: Exception) -> None:
        """
        Log an error with full traceback.
        
        Args:
            error (Exception): Error to log
        """
        if isinstance(error, Exception):
            error_msg = f"{type(error).__name__}: {str(error)}"
            trace = traceback.format_exc()
            self.error(f"Exception occurred: {error_msg}\nTraceback:\n{trace}")
        else:
            self.error(str(error))
    
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
        self._service = service_name

# Global logger instance
logger = ETLLogger()