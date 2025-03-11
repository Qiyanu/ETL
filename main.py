import logging
import os
import sys
import time
import json
import yaml
import uuid
import signal
import threading
import psutil
import requests
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Tuple, Any, Optional, Union
from google.cloud import bigquery
from google.api_core import exceptions as gcp_exceptions

# Set up production-ready structured logging
def setup_logging(log_level=None):
    """Set up structured logging for Cloud Run."""
    logger = logging.getLogger("etl")
    
    # Clear existing handlers to avoid duplication
    if logger.handlers:
        logger.handlers.clear()
    
    # Set log level
    log_level = log_level or os.environ.get("LOG_LEVEL", "INFO")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    
    # Create a filter to add correlation_id and service to log records
    class ContextFilter(logging.Filter):
        def filter(self, record):
            # Add the attributes to the LogRecord
            record.correlation_id = getattr(logger, 'correlation_id', 'not-set')
            record.service = getattr(logger, 'service', 'local')
            return True
    
    # Create handler for Cloud Run (stdout)
    handler = logging.StreamHandler(sys.stdout)
    
    # Use structured JSON format for Cloud Logging
    formatter = logging.Formatter(
        '{"severity": "%(levelname)s", "correlation_id": "%(correlation_id)s", '
        '"service": "%(service)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    # Add correlation_id and service as attributes to the logger object
    logger.correlation_id = "not-set"
    logger.service = os.environ.get("K_SERVICE", "local")
    
    # Add the filter to the logger
    logger.addFilter(ContextFilter())
    
    return logger

# Global logger instance
logger = setup_logging()

# Load configuration from YAML file or environment variables
def load_config(config_file=None):
    """Load configuration with sensible defaults."""
    # Default configuration
    config = {
        "DEST_TABLE": "c4-marketing-dev-347012.customer_data.customer_data_test",
        # Source tables (shared by all countries)
        "SOURCE_LINE_TABLE": "c4-united-datasharing-prd.datasharing_marketing_group_dashboard.a_ww_sales_trx_line",
        "SOURCE_HEADER_TABLE": "c4-united-datasharing-prd.datasharing_marketing_group_dashboard.a_ww_sales_trx_header",
        "SOURCE_CARD_TABLE": "c4-united-datasharing-prd.datasharing_marketing_group_dashboard.d_card",
        "SOURCE_SITE_TABLE": "c4-united-datasharing-prd.datasharing_marketing_group_dashboard.d_ww_oc_site",
        "SOURCE_STORE_TABLE": "c4-united-datasharing-prd.datasharing_marketing_group_dashboard.d_ww_store",
        "MAX_WORKERS": 8,  # Increased from 4 to 8 for better parallelism
        "QUERY_TIMEOUT": 3600,
        "CHUNK_SIZE": 20,
        "ALLOWED_COUNTRIES": ["ITA", "ESP"],
        "COUNTRY_MAPPING": {"ITA": "IT", "ESP": "SP"},
        "EXCLUDED_CHAIN_TYPES": ["SUPECO", "Galerie", "AUTRES"],
        "SPAIN_FILTERS": {
            "excluded_business_brands": ["supeco"],
            "excluded_ecm_combinations": [
                {"delivery_channel": "PICKUP_STORE"},
                {"business_service": "Home Delivery Non Food"}
            ]
        },
        "ENABLE_RETRIES": True,
        "MAX_RETRY_ATTEMPTS": 5,  # Increased from 3 for better reliability
        "CIRCUIT_BREAKER_THRESHOLD": 5,
        "CIRCUIT_BREAKER_TIMEOUT": 300,
        "LOCATION": "EU",
        "JOB_MAX_RUNTIME": 86400,  # Maximum job runtime in seconds (24 hours)
        "JOB_TIMEOUT_SAFETY_MARGIN": 1800,  # 30 minutes safety margin before job timeout
        "LOG_LEVEL": "INFO",
        # Temp table management
        "TEMP_TABLE_EXPIRATION_HOURS": 24,
        "TEMP_TABLE_ADD_DESCRIPTIONS": True,
        # Job-specific parameters with environment variable overrides
        "START_MONTH": os.environ.get("START_MONTH", None),  # Format: YYYY-MM-DD
        "END_MONTH": os.environ.get("END_MONTH", None),  # Format: YYYY-MM-DD
        "LAST_N_MONTHS": os.environ.get("LAST_N_MONTHS", None),  # Number of months to process
        "PARALLEL": os.environ.get("PARALLEL", "true").lower() in ('true', 'yes', '1')
    }
    
    # Try to load from YAML file if provided
    if config_file and os.path.exists(config_file):
        try:
            with open(config_file, 'r') as f:
                yaml_config = yaml.safe_load(f)
                
                # Extract key settings from nested YAML
                if 'tables' in yaml_config:
                    if 'destination' in yaml_config['tables']:
                        config["DEST_TABLE"] = yaml_config['tables']['destination']
                    
                    if 'sources' in yaml_config['tables']:
                        sources = yaml_config['tables']['sources']
                        if 'line_table' in sources:
                            config["SOURCE_LINE_TABLE"] = sources['line_table']
                        if 'header_table' in sources:
                            config["SOURCE_HEADER_TABLE"] = sources['header_table']
                        if 'card_table' in sources:
                            config["SOURCE_CARD_TABLE"] = sources['card_table']
                        if 'site_table' in sources:
                            config["SOURCE_SITE_TABLE"] = sources['site_table']
                        if 'store_table' in sources:
                            config["SOURCE_STORE_TABLE"] = sources['store_table']
                
                if 'processing' in yaml_config:
                    p = yaml_config['processing']
                    config["MAX_WORKERS"] = p.get('max_workers', config["MAX_WORKERS"])
                    config["QUERY_TIMEOUT"] = p.get('query_timeout', config["QUERY_TIMEOUT"])
                    config["LOCATION"] = p.get('location', config["LOCATION"])
                
                if 'filters' in yaml_config:
                    f = yaml_config['filters']
                    if 'allowed_countries' in f:
                        config["ALLOWED_COUNTRIES"] = f['allowed_countries']
                    if 'country_mapping' in f:
                        config["COUNTRY_MAPPING"] = f['country_mapping']
                    if 'excluded_chain_types' in f:
                        config["EXCLUDED_CHAIN_TYPES"] = f['excluded_chain_types']
                    if 'spain_filters' in f:
                        config["SPAIN_FILTERS"] = f['spain_filters']
                
                if 'resilience' in yaml_config:
                    r = yaml_config['resilience']
                    config["ENABLE_RETRIES"] = r.get('enable_retries', config["ENABLE_RETRIES"])
                    config["MAX_RETRY_ATTEMPTS"] = r.get('max_retry_attempts', config["MAX_RETRY_ATTEMPTS"])
                    config["CIRCUIT_BREAKER_THRESHOLD"] = r.get('circuit_breaker_threshold', config["CIRCUIT_BREAKER_THRESHOLD"])
                    config["CIRCUIT_BREAKER_TIMEOUT"] = r.get('circuit_breaker_timeout', config["CIRCUIT_BREAKER_TIMEOUT"])
                
                if 'temp_tables' in yaml_config:
                    tt = yaml_config['temp_tables']
                    config["TEMP_TABLE_EXPIRATION_HOURS"] = tt.get('expiration_hours', config["TEMP_TABLE_EXPIRATION_HOURS"])
                    config["TEMP_TABLE_ADD_DESCRIPTIONS"] = tt.get('add_descriptions', config["TEMP_TABLE_ADD_DESCRIPTIONS"])
                
                if 'job' in yaml_config:
                    jr = yaml_config['job']
                    config["JOB_MAX_RUNTIME"] = jr.get('max_runtime', config["JOB_MAX_RUNTIME"])
                    config["JOB_TIMEOUT_SAFETY_MARGIN"] = jr.get('timeout_safety_margin', config["JOB_TIMEOUT_SAFETY_MARGIN"])
                    config["START_MONTH"] = jr.get('start_month', config["START_MONTH"])
                    config["END_MONTH"] = jr.get('end_month', config["END_MONTH"])
                    config["LAST_N_MONTHS"] = jr.get('last_n_months', config["LAST_N_MONTHS"])
                    config["PARALLEL"] = jr.get('parallel', config["PARALLEL"])
                
                if 'logging' in yaml_config:
                    config["LOG_LEVEL"] = yaml_config['logging'].get('level', config["LOG_LEVEL"])
                
                logger.info(f"Loaded configuration from {config_file}")
        except Exception as e:
            logger.error(f"Error loading config file {config_file}: {e}")
    
    # Override with environment variables (type conversion included)
    for key in config:
        env_value = os.environ.get(key)
        if env_value is not None:
            # Type conversion based on default value type
            default_type = type(config[key])
            try:
                if default_type == list:
                    config[key] = env_value.split(",")
                elif default_type == bool:
                    config[key] = env_value.lower() in ('true', 'yes', '1')
                elif default_type == int:
                    config[key] = int(env_value)
                elif default_type == float:
                    config[key] = float(env_value)
                else:
                    config[key] = env_value
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid env value for {key}: {env_value}, using default. Error: {e}")
    
    # Parse destination table into components
    parts = config["DEST_TABLE"].split('.')
    if len(parts) == 3:
        config["DEST_PROJECT"] = parts[0]
        config["DEST_DATASET"] = parts[1]
        config["DEST_TABLE_NAME"] = parts[2]
    
    # Update logger level
    logger.setLevel(getattr(logging, config["LOG_LEVEL"], logging.INFO))
    
    return config

# Simplified metrics collection
class Metrics:
    """Lightweight metrics collection."""
    
    def __init__(self):
        self.metrics = {}
        self.start_times = {}
        self.lock = threading.Lock()
    
    def start_timer(self, metric_name):
        """Start a timer for a specific metric."""
        self.start_times[metric_name] = time.time()
    
    def stop_timer(self, metric_name):
        """Stop a timer and record the elapsed time."""
        if metric_name in self.start_times:
            elapsed = time.time() - self.start_times[metric_name]
            self.record_value(f"{metric_name}_seconds", elapsed)
            del self.start_times[metric_name]
            return elapsed
        return None
    
    def record_value(self, metric_name, value):
        """Record a metric value."""
        with self.lock:
            if metric_name not in self.metrics:
                self.metrics[metric_name] = []
            self.metrics[metric_name].append(value)
    
    def increment_counter(self, metric_name, increment=1):
        """Increment a counter metric."""
        with self.lock:
            if metric_name not in self.metrics:
                self.metrics[metric_name] = 0
            self.metrics[metric_name] += increment
    
    def get_summary(self):
        """Get a summary of all collected metrics."""
        summary = {}
        with self.lock:
            for name, values in self.metrics.items():
                if isinstance(values, list):
                    summary[name] = {
                        "count": len(values),
                        "sum": sum(values),
                        "avg": sum(values) / len(values) if values else None,
                        "min": min(values) if values else None,
                        "max": max(values) if values else None
                    }
                else:
                    summary[name] = values
        return summary

# Initialize metrics collector
metrics = Metrics()

# Circuit breaker for resilience
class CircuitBreaker:
    """Simple circuit breaker pattern implementation."""
    
    def __init__(self, failure_threshold, reset_timeout):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN
        self.last_failure_time = 0
        self.lock = threading.Lock()
    
    def record_success(self):
        """Record successful operation."""
        with self.lock:
            if self.state == "HALF-OPEN":
                self.state = "CLOSED"
                logger.info("Circuit breaker reset to CLOSED state")
            self.failure_count = 0
    
    def record_failure(self):
        """Record failed operation."""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.state == "CLOSED" and self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
                metrics.increment_counter("circuit_breaker_open_count")
    
    def allow_request(self):
        """Check if request should be allowed based on circuit state."""
        with self.lock:
            if self.state == "CLOSED":
                return True
            
            if self.state == "OPEN":
                # Check if enough time has passed to try again
                if time.time() - self.last_failure_time > self.reset_timeout:
                    self.state = "HALF-OPEN"
                    logger.info("Circuit breaker transitioning to HALF-OPEN state")
                    return True
                return False
            
            # In HALF-OPEN state, allow request to test if system has recovered
            return True

# Initialize circuit breaker
circuit_breaker = CircuitBreaker(
    failure_threshold=5,  # Will be overridden by config
    reset_timeout=300     # Will be overridden by config
)

class BigQueryConnectionPool:
    """
    A connection pool that manages and reuses BigQuery client connections.
    Enhanced with graceful shutdown capabilities.
    """
    
    def __init__(self, config, pool_size=None):
        """Initialize the connection pool."""
        self.config = config
        self.pool_size = pool_size or config.get("MAX_CONNECTIONS", 10)
        self.location = config.get("LOCATION", "EU")
        
        # Connection pool and management
        self.available_clients = []
        self.in_use_clients = set()
        self.lock = threading.Lock()
        self.semaphore = threading.Semaphore(self.pool_size)
        
        # Shutdown management
        self.shutdown_requested = False
        self.shutdown_event = threading.Event()
        
        # Metrics for monitoring pool usage
        self.checkout_count = 0
        self.connection_wait_time = 0
        self.created_connections = 0
        
        # Preload some connections for faster startup
        preload_count = min(3, self.pool_size)
        for _ in range(preload_count):
            client = self._create_client()
            self.available_clients.append(client)
            self.created_connections += 1
    
    def _create_client(self):
        """Create a new BigQuery client with proper configuration."""
        return bigquery.Client(location=self.location)
    
    def get_client(self):
        """Get a BigQuery client from the pool or create a new one if needed."""
        if self.shutdown_requested:
            raise RuntimeError("Connection pool is shutting down, no new connections allowed")
            
        # Acquire semaphore to control maximum concurrent connections
        self.semaphore.acquire()
        
        with self.lock:
            self.checkout_count += 1
            
            if self.available_clients:
                # Reuse an existing client from the pool
                client = self.available_clients.pop()
                self.in_use_clients.add(client)
                return client
            
            # No available clients, create a new one
            client = self._create_client()
            self.in_use_clients.add(client)
            self.created_connections += 1
            return client
    
    def release_client(self, client):
        """Return a client to the pool for reuse."""
        with self.lock:
            if client in self.in_use_clients:
                self.in_use_clients.remove(client)
                
                if self.shutdown_requested:
                    # During shutdown, close immediately instead of returning to pool
                    try:
                        client.close()
                    except:
                        pass
                    
                    # If this was the last connection, signal shutdown completion
                    if not self.in_use_clients:
                        self.shutdown_event.set()
                elif len(self.available_clients) < self.pool_size:
                    # Return to pool during normal operation
                    self.available_clients.append(client)
                else:
                    # Close excess connections
                    try:
                        client.close()
                    except:
                        pass
        
        # Release the semaphore to allow another operation to acquire a connection
        self.semaphore.release()
    
    def close_all(self, timeout=30):
        """
        Close all connections in the pool with graceful shutdown.
        
        Args:
            timeout: Maximum time in seconds to wait for in-use connections
                    to be returned before forcefully closing them.
        """
        logger.info("Starting connection pool shutdown")
        
        with self.lock:
            # Mark pool as shutting down
            self.shutdown_requested = True
            
            # Close available clients immediately
            for client in self.available_clients:
                try:
                    client.close()
                except:
                    pass
            self.available_clients.clear()
            
            # Log info about in-use clients
            in_use_count = len(self.in_use_clients)
            if in_use_count:
                logger.info(f"Waiting for {in_use_count} active connections to complete (timeout: {timeout}s)")
                # Set shutdown event if there are no in-use connections
                if in_use_count == 0:
                    self.shutdown_event.set()
            else:
                # No in-use connections, set event immediately
                self.shutdown_event.set()
        
        # Wait for all in-use connections to be returned
        if not self.shutdown_event.wait(timeout=timeout):
            # Timeout occurred, force close remaining connections
            with self.lock:
                remaining = len(self.in_use_clients)
                if remaining:
                    logger.warning(f"Forcefully closing {remaining} connections that didn't complete in time")
                    for client in list(self.in_use_clients):
                        try:
                            client.close()
                        except:
                            pass
                    self.in_use_clients.clear()
        
        logger.info("Connection pool shutdown complete")
    
    def get_stats(self):
        """Get statistics about the connection pool."""
        with self.lock:
            return {
                "pool_size": self.pool_size,
                "available_connections": len(self.available_clients),
                "in_use_connections": len(self.in_use_clients),
                "total_created_connections": self.created_connections,
                "checkout_count": self.checkout_count,
                "shutting_down": self.shutdown_requested
            }

class BigQueryOperations:
    """Streamlined BigQuery operations with connection pooling and query profiling."""
    
    def __init__(self, config):
        """Initialize with configuration and optional connection pool and profiler."""
        # Initialize connection pool and query profiler
        self.connection_pool = BigQueryConnectionPool(config)
        self.query_profiler = QueryProfiler(config)
        
        self.config = config
        self.start_time = time.time()
        
        # Update circuit breaker settings from config
        circuit_breaker.failure_threshold = config.get("CIRCUIT_BREAKER_THRESHOLD", 5)
        circuit_breaker.reset_timeout = config.get("CIRCUIT_BREAKER_TIMEOUT", 300)
        
        # Define retryable exceptions - expanded list
        self.retryable_exceptions = (
            # GCP API exceptions
            gcp_exceptions.ServiceUnavailable,
            gcp_exceptions.ServerError,
            gcp_exceptions.InternalServerError,
            gcp_exceptions.GatewayTimeout,
            gcp_exceptions.DeadlineExceeded,
            gcp_exceptions.ResourceExhausted,
            gcp_exceptions.RetryError,
            # Network/socket errors
            ConnectionError,
            TimeoutError,
            # Common HTTP errors (if requests is used)
            requests.exceptions.ConnectionError if 'requests' in globals() else None,
            requests.exceptions.Timeout if 'requests' in globals() else None,
            requests.exceptions.ConnectTimeout if 'requests' in globals() else None,
            requests.exceptions.ReadTimeout if 'requests' in globals() else None,
            # Google API client errors
            google.api_core.exceptions.Aborted,
            google.api_core.exceptions.TooManyRequests,
            google.api_core.exceptions.ServiceUnavailable,
        )
        
        # Filter out None values (if requests not available)
        self.retryable_exceptions = tuple(e for e in self.retryable_exceptions if e is not None)
    
    def _is_retryable_exception(self, exception):
        """Check if an exception is retryable."""
        # Check if it's one of our defined retryable types
        if isinstance(exception, self.retryable_exceptions):
            return True
        
        # Check for socket or transport errors in the error message
        error_msg = str(exception).lower()
        if any(keyword in error_msg for keyword in [
            "socket", "timeout", "deadline", "connection reset", 
            "broken pipe", "transport", "network", "reset by peer",
            "connection refused", "temporarily unavailable"
        ]):
            return True
        
        return False
    
    def _check_timeout(self, operation_timeout=None):
        """Check if we're approaching job timeout."""
        elapsed = time.time() - self.start_time
        timeout = self.config.get("JOB_MAX_RUNTIME", 86400)  # 24 hours default
        safety_margin = self.config.get("JOB_TIMEOUT_SAFETY_MARGIN", 1800)  # 30 min default
        
        remaining = timeout - elapsed
        if remaining <= safety_margin:
            logger.warning(f"Approaching job timeout: {elapsed:.1f}s elapsed, {remaining:.1f}s remaining")
            return False
                
        # Adjust operation timeout if needed
        if operation_timeout and operation_timeout > remaining - safety_margin:
            adjusted_timeout = max(1, remaining - safety_margin)
            logger.warning(f"Adjusting timeout from {operation_timeout}s to {adjusted_timeout:.1f}s")
            return adjusted_timeout
                
        # Return the original timeout instead of True
        return operation_timeout
    
    def execute_query(self, query, params=None, timeout=None, return_job=False, job_config=None):
        """Execute a BigQuery query with connection pooling, error handling, and profiling."""
        if timeout is None:
            timeout = self.config.get("QUERY_TIMEOUT", 3600)
        
        # Check if we're approaching job timeout
        adjusted_timeout = self._check_timeout(timeout)
        if adjusted_timeout is False:  # We're too close to timeout
            raise Exception("Job timeout approaching, refusing operation")
        elif adjusted_timeout is not None:
            timeout = adjusted_timeout
        
        # Check circuit breaker
        if not circuit_breaker.allow_request():
            raise Exception("Circuit breaker is open, refusing operation")
        
        # Create or use the provided job config
        if job_config is None:
            job_config = bigquery.QueryJobConfig()
        
        # Apply parameters if provided
        if params:
            job_config.query_parameters = params
        
        # Apply settings from config
        job_config.use_query_cache = self.config.get("use_query_cache", True)
        
        # Set priority
        priority_str = self.config.get("priority", "INTERACTIVE").upper()
        if hasattr(bigquery.QueryPriority, priority_str):
            job_config.priority = getattr(bigquery.QueryPriority, priority_str)
        
        # Set bytes billed limit if available
        max_bytes = self.config.get("maximum_bytes_billed")
        if max_bytes:
            job_config.maximum_bytes_billed = max_bytes
        
        # Set SQL dialect
        job_config.use_legacy_sql = self.config.get("use_legacy_sql", False)
        
        # Get client from connection pool
        client = self.connection_pool.get_client()
        
        # Start tracking execution time
        metrics.start_timer("query_execution")
        query_start_time = time.time()
        query_job = None
        
        try:
            # Execute with retries
            retry_count = 0
            max_retries = self.config.get("MAX_RETRY_ATTEMPTS", 3)
            base_delay = 1.0
            max_delay = 60.0
            
            while True:
                try:
                    # Execute the query
                    query_job = client.query(query, job_config=job_config)
                    result = query_job.result(timeout=timeout)
                    
                    # Record success and metrics
                    circuit_breaker.record_success()
                    execution_time = time.time() - query_start_time
                    metrics.stop_timer("query_execution")
                    
                    # Record bytes processed if available
                    if hasattr(query_job, 'total_bytes_processed'):
                        metrics.record_value("bytes_processed", query_job.total_bytes_processed)
                    
                    # Profile the query for performance analysis
                    self.query_profiler.profile_query(query, params, query_job, execution_time)
                    
                    # Return the appropriate result
                    if return_job:
                        return query_job
                    return result
                    
                except Exception as e:
                    # Check if this exception is retryable
                    if self._is_retryable_exception(e):
                        retry_count += 1
                        metrics.increment_counter("bigquery_transient_errors")
                        
                        if retry_count <= max_retries and self.config.get("ENABLE_RETRIES", True):
                            delay = min(max_delay, base_delay * (2 ** (retry_count - 1)))
                            logger.warning(f"Transient error (attempt {retry_count}/{max_retries}): {e}. Retrying in {delay}s")
                            time.sleep(delay)
                        else:
                            metrics.stop_timer("query_execution")
                            circuit_breaker.record_failure()
                            logger.error(f"Exceeded maximum retries for transient error: {e}")
                            raise
                    else:
                        # Non-retryable errors
                        metrics.stop_timer("query_execution")
                        circuit_breaker.record_failure()
                        logger.error(f"Query execution failed with non-retryable error: {e}")
                        raise
        finally:
            # Always return the client to the pool
            self.connection_pool.release_client(client)
    
    def create_dataset_if_not_exists(self, dataset_id):
        """Create a dataset if it doesn't exist, using connection pool."""
        client = self.connection_pool.get_client()
        try:
            try:
                client.get_dataset(dataset_id)
                logger.info(f"Dataset {dataset_id} already exists")
            except Exception:
                dataset = bigquery.Dataset(dataset_id)
                dataset.location = self.config.get("LOCATION", "EU")
                client.create_dataset(dataset, exists_ok=True)
                logger.info(f"Created dataset: {dataset_id}")
        finally:
            self.connection_pool.release_client(client)
    
    def create_table_with_schema(self, table_id, schema, partitioning_field=None, clustering_fields=None):
        """Create a table with the specified schema, using connection pool."""
        client = self.connection_pool.get_client()
        try:
            try:
                client.get_table(table_id)
                logger.info(f"Table {table_id} already exists")
                return
            except Exception:
                # Table doesn't exist, create it
                table = bigquery.Table(table_id, schema=schema)
                
                # Add partitioning if specified
                if partitioning_field:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.MONTH,
                        field=partitioning_field
                    )
                
                # Add clustering if specified
                if clustering_fields:
                    table.clustering_fields = clustering_fields
                
                client.create_table(table, exists_ok=True)
                logger.info(f"Created table: {table_id}")
        finally:
            self.connection_pool.release_client(client)
    
    def create_table_from_query(self, table_id, query, params=None):
        """Create a table from a query, applying configuration settings."""
        # Create job config with destination table
        job_config = bigquery.QueryJobConfig(destination=table_id)
        
        # Apply write and create disposition from config
        create_disp = self.config.get("create_disposition", "CREATE_IF_NEEDED")
        if hasattr(bigquery.CreateDisposition, create_disp):
            job_config.create_disposition = getattr(bigquery.CreateDisposition, create_disp)
        
        write_disp = self.config.get("write_disposition", "WRITE_TRUNCATE")
        if hasattr(bigquery.WriteDisposition, write_disp):
            job_config.write_disposition = getattr(bigquery.WriteDisposition, write_disp)
        
        # Execute the query with the configured job_config
        return self.execute_query(query, params, job_config=job_config)
    
    def delete_table_rows(self, table_id, condition, params=None):
        """Delete rows from a table based on a condition."""
        query = f"DELETE FROM `{table_id}` WHERE {condition}"
        return self.execute_query(query, params)
    
    def delete_table(self, table_id, not_found_ok=False):
        """Delete a BigQuery table using connection pool."""
        client = self.connection_pool.get_client()
        try:
            return client.delete_table(table_id, not_found_ok=not_found_ok)
        finally:
            self.connection_pool.release_client(client)

    def get_table(self, table_id):
        """Get a BigQuery table reference using connection pool."""
        client = self.connection_pool.get_client()
        try:
            return client.get_table(table_id)
        finally:
            self.connection_pool.release_client(client)

    def update_table(self, table, field_mask=None):
        """Update a BigQuery table using connection pool."""
        client = self.connection_pool.get_client()
        try:
            return client.update_table(table, field_mask)
        finally:
            self.connection_pool.release_client(client)

# Query Profiler class to add
class QueryProfiler:
    """
    Profiles BigQuery queries to identify slow or large queries.
    """
    
    def __init__(self, config):
        """Initialize the query profiler with configuration settings."""
        self.config = config
        
        # Thresholds for identifying problematic queries (7min/20GB)
        self.time_threshold_seconds = config.get("SLOW_QUERY_THRESHOLD_SECONDS", 420)  # 7 minutes
        self.size_threshold_bytes = config.get("LARGE_QUERY_THRESHOLD_BYTES", 20 * 1024 * 1024 * 1024)  # 20GB
        
        # Storage for query profiles and synchronization
        self.profiles = []
        self.lock = threading.Lock()
        
        # Enable query plan analysis for detailed profiling
        self.enable_plan_analysis = config.get("ENABLE_QUERY_PLAN_ANALYSIS", True)
    
    def profile_query(self, query, params, query_job, execution_time):
        """Profile a BigQuery query for performance metrics."""
        # Skip if not enough information is available
        if not query_job or not hasattr(query_job, 'total_bytes_processed'):
            return
        
        # Extract basic query metrics
        bytes_processed = query_job.total_bytes_processed or 0
        bytes_billed = getattr(query_job, 'total_bytes_billed', None)
        is_cached = getattr(query_job, 'cache_hit', False)
        
        # Check if query exceeds thresholds for slow or large queries
        is_slow = execution_time >= self.time_threshold_seconds
        is_large = bytes_processed >= self.size_threshold_bytes
        
        # Only profile problematic queries
        if not (is_slow or is_large):
            return
        
        # Build profile data dictionary
        profile = {
            'timestamp': datetime.now().isoformat(),
            'job_id': query_job.job_id,
            'query': query[:2000],  # Truncate very long queries
            'params': str(params) if params else None,
            'execution_time_seconds': execution_time,
            'bytes_processed': bytes_processed,
            'bytes_billed': bytes_billed,
            'gb_processed': bytes_processed / (1024**3) if bytes_processed else 0,
            'is_slow': is_slow,
            'is_large': is_large,
            'cache_hit': is_cached,
            'slot_ms': getattr(query_job, 'slot_millis', None),
        }
        
        # Extract query plan if available and enabled
        if self.enable_plan_analysis and hasattr(query_job, 'query_plan'):
            try:
                profile['bottleneck_stages'] = self._identify_bottlenecks(query_job)
            except Exception as e:
                logger.warning(f"Failed to extract query plan: {e}")
        
        # Store the profile
        with self.lock:
            self.profiles.append(profile)
        
        # Log information about problematic query
        self._log_query_issue(profile)
    
    def _identify_bottlenecks(self, query_job):
        """Identify bottleneck stages in the query execution."""
        if not hasattr(query_job, 'query_plan'):
            return None
            
        bottlenecks = []
        
        # Find the top 3 stages by duration
        stages = []
        for step in query_job.query_plan:
            if step.start_ms and step.end_ms:
                duration = step.end_ms - step.start_ms
                stages.append((step.id, step.name, duration))
        
        # Sort by duration (descending) and take top 3
        stages.sort(key=lambda x: x[2], reverse=True)
        for i, (stage_id, stage_name, duration) in enumerate(stages[:3]):
            bottlenecks.append({
                'stage_id': stage_id,
                'stage_name': stage_name,
                'duration_ms': duration,
                'rank': i+1
            })
            
        return bottlenecks
    
    def _log_query_issue(self, profile):
        """Log information about problematic queries."""
        issue_type = []
        if profile['is_slow']:
            issue_type.append(f"slow ({profile['execution_time_seconds']:.1f}s)")
        if profile['is_large']:
            issue_type.append(f"large ({profile['gb_processed']:.1f}GB)")
            
        issue_description = " and ".join(issue_type)
        
        # Create a meaningful log message
        logger.warning(
            f"Performance issue detected: {issue_description} query. "
            f"Job ID: {profile['job_id']}. "
            f"Cache hit: {profile['cache_hit']}."
        )
        
        # Log bottleneck information if available
        if 'bottleneck_stages' in profile and profile['bottleneck_stages']:
            bottleneck = profile['bottleneck_stages'][0]  # Get the worst bottleneck
            logger.info(
                f"Primary bottleneck: Stage {bottleneck['stage_id']} ({bottleneck['stage_name']}), "
                f"Duration: {bottleneck['duration_ms'] / 1000:.1f}s"
            )
    
    def get_profiles(self):
        """Get all collected query profiles."""
        with self.lock:
            return self.profiles.copy()
    
    def write_profiles_to_log(self):
        """Write summary of all profiles to the log."""
        with self.lock:
            if not self.profiles:
                logger.info("No slow or large queries detected during execution.")
                return
            
            # Count issues by type
            slow_count = sum(1 for p in self.profiles if p['is_slow'])
            large_count = sum(1 for p in self.profiles if p['is_large'])
            both_count = sum(1 for p in self.profiles if p['is_slow'] and p['is_large'])
            
            # Log summary information
            logger.info(f"Query profiling summary: {len(self.profiles)} problematic queries detected")
            logger.info(f"  - Slow queries (>{self.time_threshold_seconds/60:.1f}min): {slow_count}")
            logger.info(f"  - Large queries (>{self.size_threshold_bytes/(1024**3):.1f}GB): {large_count}")
            logger.info(f"  - Queries that were both slow and large: {both_count}")


class AdaptiveSemaphore:
    """Thread-safe semaphore that can dynamically adjust its counter."""
    
    def __init__(self, initial_value):
        self.value = initial_value
        self.lock = threading.Lock()
        self._real_semaphore = threading.Semaphore(initial_value)
    
    def acquire(self, blocking=True, timeout=None):
        """Acquire the semaphore."""
        return self._real_semaphore.acquire(blocking, timeout)
    
    def release(self):
        """Release the semaphore."""
        return self._real_semaphore.release()
    
    def adjust_value(self, new_value):
        """Dynamically adjust semaphore value."""
        with self.lock:
            current_value = self.value
            
            if new_value > current_value:
                # Increasing permits - release additional permits
                for _ in range(new_value - current_value):
                    self.release()
                logger.info(f"Increased worker semaphore: {current_value} → {new_value}")
            elif new_value < current_value:
                # Decreasing permits - acquire extra permits to reduce availability
                # This is the key improvement - actively reducing available workers
                acquired_count = 0
                for _ in range(current_value - new_value):
                    # Non-blocking acquire to avoid deadlock
                    if self._real_semaphore.acquire(blocking=False):
                        acquired_count += 1
                
                if acquired_count > 0:
                    logger.info(f"Decreased worker semaphore by {acquired_count} (target: {current_value} → {new_value})")
                
                # If we couldn't acquire all needed permits, adjust our tracking
                self.value = current_value - acquired_count
                return
            
            # Update our tracking value
            self.value = new_value
    
    @property
    def current_value(self):
        """Get the current semaphore value."""
        with self.lock:
            return self.value



# Signal handler for graceful shutdown
class GracefulShutdown:
    """Handle signals for graceful shutdown."""
    
    def __init__(self):
        self.shutdown_requested = False
        self._register_handlers()
    
    def _register_handlers(self):
        """Register signal handlers."""
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        
    def _handle_shutdown_signal(self, signum, frame):
        """Handle shutdown signals."""
        sig_name = signal.Signals(signum).name
        logger.info(f"Received {sig_name} signal, initiating graceful shutdown")
        self.shutdown_requested = True
    
    def is_shutdown_requested(self):
        """Check if shutdown has been requested."""
        return self.shutdown_requested

# Initialize shutdown handler
shutdown_handler = GracefulShutdown()

# Core ETL Functions
def create_customer_data_table_if_not_exists(bq_ops):
    """Creates destination table with optimized schema."""
    # Define the schema with robust typing
    schema = [
        bigquery.SchemaField("Mois", "DATE", description="Month of data"),
        bigquery.SchemaField("Country", "STRING", description="Country code"),
        bigquery.SchemaField("Enseigne", "STRING", description="Store chain type"),
        bigquery.SchemaField("CA_Tous_Clients", "FLOAT64", description="Revenue all clients (€)"),
        bigquery.SchemaField("CA_Tous_Clients_local", "FLOAT64", description="Revenue all clients (local currency)"),
        bigquery.SchemaField("CA_Porteurs", "FLOAT64", description="Revenue cardholders (€)"),
        bigquery.SchemaField("CA_Porteurs_local", "FLOAT64", description="Revenue cardholders (local currency)"),
        bigquery.SchemaField("Taux_CA_encarte", "FLOAT64", description="Cardholder revenue percentage"),
        bigquery.SchemaField("Nb_transactions_Tous_Clients", "INT64", description="Transaction count all clients"),
        bigquery.SchemaField("Nb_transactions_porteurs", "INT64", description="Transaction count cardholders"),
        bigquery.SchemaField("Taux_transactions_encartees", "FLOAT64", description="Cardholder transaction percentage"),
        bigquery.SchemaField("Nb_foyers", "INT64", description="Number of households"),
        bigquery.SchemaField("Frequence_porteurs", "FLOAT64", description="Cardholder frequency"),
        bigquery.SchemaField("Panier_moyen_Porteurs", "FLOAT64", description="Average basket cardholders"),
        bigquery.SchemaField("nb_articles", "INT64", description="Number of articles"),
        bigquery.SchemaField("nb_articles_porteur", "INT64", description="Number of articles cardholders"),
        bigquery.SchemaField("Families", "INT64", description="Number of families"),
        bigquery.SchemaField("Seniors", "INT64", description="Number of seniors"),
        bigquery.SchemaField("CA_promo", "FLOAT64", description="Promotional revenue (€)"),
        bigquery.SchemaField("CA_promo_local", "FLOAT64", description="Promotional revenue (local currency)"),
        bigquery.SchemaField("nb_foyers_constants", "INT64", description="Stable households"),
        bigquery.SchemaField("nb_foyers_gagnes", "INT64", description="New households"),
        bigquery.SchemaField("nb_foyers_perdus", "INT64", description="Lost households")
    ]
    
    # Ensure dataset exists
    dataset_id = f"{bq_ops.config['DEST_PROJECT']}.{bq_ops.config['DEST_DATASET']}"
    bq_ops.create_dataset_if_not_exists(dataset_id)
    
    # Create table with partitioning and clustering
    bq_ops.create_table_with_schema(
        bq_ops.config["DEST_TABLE"],
        schema,
        partitioning_field="Mois",
        clustering_fields=["Country", "Enseigne"]
    )

def create_temp_table_for_month(bq_ops, the_month, temp_table_id, country, request_id=None):
    """Creates a temporary table with aggregated data for the month and specific country."""
    request_id = request_id or str(uuid.uuid4())[:8]
    logger.info(f"Creating temp table for {country}: {temp_table_id}")
    
    # Start timer for this operation
    metrics.start_timer("create_temp_table")
    
    try:
        # Create the appropriate query based on the country
        if country == "ITA":
            create_temp_table_query = create_italy_query(bq_ops, temp_table_id)
        elif country == "ESP":
            create_temp_table_query = create_spain_query(bq_ops, temp_table_id)
        else:
            raise ValueError(f"Unsupported country: {country}")
        
        # Set up query parameters
        params = [
            bigquery.ScalarQueryParameter("theMonth", "DATE", the_month),
        ]
        
        # Execute query to create temporary table
        bq_ops.execute_query(create_temp_table_query, params)
        
        # Explicitly update expiration if not set in SQL
        try:
            # Use the get_table method from bq_ops instead of client directly
            table_ref = bq_ops.get_table(temp_table_id)
            if not table_ref.expires:
                expiration_hours = bq_ops.config.get("TEMP_TABLE_EXPIRATION_HOURS", 24)
                table_ref.expires = datetime.now() + timedelta(hours=expiration_hours)
                if bq_ops.config.get("TEMP_TABLE_ADD_DESCRIPTIONS", True):
                    table_ref.description = f"Temporary table for {country} data for {the_month}, job ID: {request_id}"
                # Use the update_table method from bq_ops
                bq_ops.update_table(table_ref, ["expires", "description"])
                logger.info(f"Set expiration for table {temp_table_id} to {expiration_hours} hours from now")
        except Exception as e:
            logger.warning(f"Unable to update table expiration: {e}")
        
        # Check row count
        query_result = bq_ops.execute_query(f"SELECT COUNT(*) as row_count FROM `{temp_table_id}`")
        rows = list(query_result)
        row_count = rows[0].row_count if rows else 0
        metrics.record_value("temp_table_row_count", row_count)
        
        if row_count < 1:
            logger.warning(f"Temp table {temp_table_id} has suspiciously low row count: {row_count}")
        
        metrics.stop_timer("create_temp_table")
        return row_count
    except Exception as e:
        metrics.stop_timer("create_temp_table")
        metrics.increment_counter("temp_table_creation_failures")
        logger.error(f"Failed to create temp table: {e}")
        raise

def create_italy_query(bq_ops, temp_table_id):
    """Create BigQuery SQL for Italy data with corrected Total Group retention metrics."""
    # Format the SQL for countries and chain types
    excluded_chains_sql = ','.join([f"'{chain}'" for chain in bq_ops.config["EXCLUDED_CHAIN_TYPES"]])
    
    # Get country mappings from config
    country_mapping = bq_ops.config.get("COUNTRY_MAPPING", {"ITA": "IT", "ESP": "SP"})
    
    # Generate dynamic CASE statement for country mapping
    country_mapping_cases = []
    for source, target in country_mapping.items():
        country_mapping_cases.append(f"WHEN line.COUNTRY_KEY = '{source}' THEN '{target}'")
    
    country_mapping_sql = "\n                ".join(country_mapping_cases)
    if not country_mapping_sql:
        country_mapping_sql = "ELSE line.COUNTRY_KEY"  # Default fallback
    
    # Updated Italy query with dynamic country mapping
    return f"""
    CREATE OR REPLACE TABLE {temp_table_id} AS
    -- Main data aggregation CTE
    WITH data_agg AS (
        SELECT
            DATE_TRUNC(line.DATE_KEY, MONTH) AS month,
            CASE 
                {country_mapping_sql}
                ELSE line.COUNTRY_KEY 
            END AS country_key,
            CASE 
                WHEN header.ACTIVITY_TYPE = 'ECM' THEN 'E-commerce'
                ELSE store.CHAIN_TYPE_DESC
            END AS chain_type_desc,

            -- Pre-aggregate metrics with conditional filtering in one pass
            SUM(line.SAL_AMT_WTAX * line.EURO_RATE) AS CA_Tous_Clients,
            SUM(CASE WHEN line.COUNTRY_KEY IS NULL THEN line.SAL_AMT_WTAX ELSE NULL END) AS CA_Tous_Clients_local,
            SUM(CASE WHEN line.CARD_FLAG = '1' THEN line.SAL_AMT_WTAX * line.EURO_RATE END) AS CA_Porteurs,
            SUM(CASE WHEN line.CARD_FLAG = '0' AND line.COUNTRY_KEY IS NULL THEN line.SAL_AMT_WTAX END) AS CA_Porteurs_local,
            SUM(CASE WHEN line.PROMO_FLAG = '1' THEN line.SAL_AMT_WTAX * line.EURO_RATE END) AS CA_promo,
            SUM(CASE WHEN line.PROMO_FLAG = '0' AND line.COUNTRY_KEY IS NULL THEN line.SAL_AMT_WTAX END) AS CA_promo_local,
            
            -- Transaction counts with FORMAT instead of MD5 for better performance
            COUNT(DISTINCT FORMAT('%s|%s|%s|%s|%s|%s', 
                header.COUNTRY_KEY, CAST(header.DATE_KEY AS STRING), header.SITE_KEY,
                header.TRX_KEY, header.TRX_TYPE, header.REC_SRC)) AS Nb_transactions_Tous_Clients,
            
            COUNT(DISTINCT CASE WHEN line.CARD_FLAG = '1' THEN FORMAT('%s|%s|%s|%s|%s|%s', 
                header.COUNTRY_KEY, CAST(header.DATE_KEY AS STRING), header.SITE_KEY,
                header.TRX_KEY, header.TRX_TYPE, header.REC_SRC) END) AS Nb_transactions_porteurs,
            
            -- Household count
            COUNT(DISTINCT CASE WHEN card.CARD_KEY <> '-1' THEN card.ACCOUNT_KEY END) AS Nb_foyers,
            
            -- Article counts
            SUM(line.sal_unit_qty) AS nb_articles,
            SUM(CASE WHEN line.CARD_FLAG = '1' THEN line.sal_unit_qty END) AS nb_articles_porteur,
            
            -- Placeholder for additional metrics
            CAST(NULL AS INT64) AS Families,
            CAST(NULL AS INT64) AS Seniors

        FROM `{bq_ops.config["SOURCE_LINE_TABLE"]}` AS line
        -- Use JOIN hints to optimize large joins
        LEFT JOIN `{bq_ops.config["SOURCE_HEADER_TABLE"]}` AS header
          ON line.DATE_KEY = header.DATE_KEY
         AND line.COUNTRY_KEY = header.COUNTRY_KEY
         AND line.SITE_KEY = header.SITE_KEY
         AND line.TRX_KEY = header.TRX_KEY
         AND line.TRX_TYPE = header.TRX_TYPE
         AND line.rec_src = header.rec_src
        LEFT JOIN `{bq_ops.config["SOURCE_CARD_TABLE"]}` AS card
          ON header.CARD_KEY = card.CARD_KEY
        -- Use INNER JOIN which is more efficient when we need all records
        INNER JOIN `{bq_ops.config["SOURCE_SITE_TABLE"]}` AS site
          ON line.SITE_KEY = site.site_key
         AND line.COUNTRY_KEY = site.COUNTRY_KEY
        INNER JOIN `{bq_ops.config["SOURCE_STORE_TABLE"]}` AS store
          ON site.MAIN_SITE_KEY = store.STORE_KEY
         AND site.COUNTRY_KEY = store.COUNTRY_KEY

        WHERE DATE_TRUNC(line.date_key, MONTH) = @theMonth
          AND store.chain_type_key != "CAC"
          AND line.COUNTRY_KEY = 'ITA'
          AND store.CHAIN_TYPE_DESC NOT IN ({excluded_chains_sql})

        GROUP BY 
            GROUPING SETS (
                (month, country_key, chain_type_desc), -- Detailed level
                (month, country_key),                  -- Country level
                (month)                                -- Total level
            )
    ),
    
    -- Client retention calculations
    last12_month AS (
        SELECT
            CASE WHEN card.CARD_KEY <> '-1' THEN card.ACCOUNT_KEY END AS id_client,
            CASE 
                {country_mapping_sql}
                ELSE line.COUNTRY_KEY 
            END AS country_key,
            CASE 
                WHEN header.ACTIVITY_TYPE = 'ECM' THEN 'E-commerce'
                ELSE store.CHAIN_TYPE_DESC 
            END AS chain_type_desc
        FROM `{bq_ops.config["SOURCE_LINE_TABLE"]}` AS line
        LEFT JOIN `{bq_ops.config["SOURCE_HEADER_TABLE"]}` AS header
          ON line.DATE_KEY = header.DATE_KEY
         AND line.COUNTRY_KEY = header.COUNTRY_KEY
         AND line.SITE_KEY = header.SITE_KEY
         AND line.TRX_KEY = header.TRX_KEY
         AND line.TRX_TYPE = header.TRX_TYPE
         AND line.rec_src = header.rec_src
        LEFT JOIN `{bq_ops.config["SOURCE_CARD_TABLE"]}` AS card
          ON header.CARD_KEY = card.CARD_KEY
        INNER JOIN `{bq_ops.config["SOURCE_SITE_TABLE"]}` AS site
          ON header.SITE_KEY = site.site_key
         AND header.COUNTRY_KEY = site.COUNTRY_KEY
        INNER JOIN `{bq_ops.config["SOURCE_STORE_TABLE"]}` AS store
          ON site.MAIN_SITE_KEY = store.STORE_KEY
         AND site.COUNTRY_KEY = store.COUNTRY_KEY

        WHERE DATE_TRUNC(line.date_key, MONTH)
              BETWEEN DATE_SUB(@theMonth, INTERVAL 11 MONTH) AND @theMonth
          AND store.chain_type_key != "CAC"
          AND line.COUNTRY_KEY = 'ITA'
          AND store.CHAIN_TYPE_DESC NOT IN ({excluded_chains_sql})
          AND card.CARD_KEY <> '-1'
        GROUP BY 1, 2, 3
    ),

    previous12_month AS (
        SELECT
            CASE WHEN card.CARD_KEY <> '-1' THEN card.ACCOUNT_KEY END AS id_client,
            CASE 
                {country_mapping_sql}
                ELSE line.COUNTRY_KEY 
            END AS country_key,
            CASE 
                WHEN header.ACTIVITY_TYPE = 'ECM' THEN 'E-commerce'
                ELSE store.CHAIN_TYPE_DESC
            END AS chain_type_desc
        FROM `{bq_ops.config["SOURCE_LINE_TABLE"]}` AS line
        LEFT JOIN `{bq_ops.config["SOURCE_HEADER_TABLE"]}` AS header
          ON line.DATE_KEY = header.DATE_KEY
         AND line.COUNTRY_KEY = header.COUNTRY_KEY
         AND line.SITE_KEY = header.SITE_KEY
         AND line.TRX_KEY = header.TRX_KEY
         AND line.TRX_TYPE = header.TRX_TYPE
         AND line.rec_src = header.rec_src
        LEFT JOIN `{bq_ops.config["SOURCE_CARD_TABLE"]}` AS card
          ON header.CARD_KEY = card.CARD_KEY
        INNER JOIN `{bq_ops.config["SOURCE_SITE_TABLE"]}` AS site
          ON line.SITE_KEY = site.site_key
         AND line.COUNTRY_KEY = site.COUNTRY_KEY
        INNER JOIN `{bq_ops.config["SOURCE_STORE_TABLE"]}` AS store
          ON site.MAIN_SITE_KEY = store.STORE_KEY
         AND site.COUNTRY_KEY = store.COUNTRY_KEY

        WHERE DATE_TRUNC(line.date_key, MONTH)
              BETWEEN DATE_SUB(@theMonth, INTERVAL 23 MONTH) 
                  AND DATE_SUB(@theMonth, INTERVAL 12 MONTH)
          AND store.chain_type_key != "CAC"
          AND line.COUNTRY_KEY = 'ITA'
          AND store.CHAIN_TYPE_DESC NOT IN ({excluded_chains_sql})
          AND card.CARD_KEY <> '-1'
        GROUP BY 1, 2, 3
    ),
    
    -- Create country-level aggregates for Total Group calculations
    last12_month_country AS (
        SELECT
            id_client,
            country_key
        FROM last12_month
        GROUP BY 1, 2
    ),

    previous12_month_country AS (
        SELECT 
            id_client,
            country_key
        FROM previous12_month
        GROUP BY 1, 2
    ),
    
    -- Calculate chain-level stable clients
    stable_clients_chain AS (
        SELECT
            a.chain_type_desc,
            a.country_key,
            COUNT(DISTINCT a.id_client) AS nb_foyers_constants
        FROM previous12_month a
        INNER JOIN last12_month b
          ON a.id_client = b.id_client
         AND a.chain_type_desc = b.chain_type_desc
         AND a.country_key = b.country_key
        GROUP BY 1, 2
    ),

    -- Calculate Total Group stable clients
    stable_clients_total AS (
        SELECT
            'Total Group' AS chain_type_desc,
            a.country_key,
            COUNT(DISTINCT a.id_client) AS nb_foyers_constants
        FROM previous12_month_country a
        INNER JOIN last12_month_country b
          ON a.id_client = b.id_client
         AND a.country_key = b.country_key
        GROUP BY 2
    ),

    -- Combine chain and total levels
    stable_clients AS (
        SELECT * FROM stable_clients_chain
        UNION ALL
        SELECT * FROM stable_clients_total
    ),

    -- Calculate chain-level new clients
    new_clients_chain AS (
        SELECT
            a.chain_type_desc,
            a.country_key,
            COUNT(DISTINCT a.id_client) AS nb_foyers_gagnes
        FROM last12_month a
        WHERE NOT EXISTS (
            SELECT 1
            FROM previous12_month b
            WHERE a.id_client = b.id_client
              AND a.chain_type_desc = b.chain_type_desc
              AND a.country_key = b.country_key
        )
        GROUP BY 1, 2
    ),

    -- Calculate Total Group new clients
    new_clients_total AS (
        SELECT
            'Total Group' AS chain_type_desc,
            a.country_key,
            COUNT(DISTINCT a.id_client) AS nb_foyers_gagnes
        FROM last12_month_country a
        WHERE NOT EXISTS (
            SELECT 1
            FROM previous12_month_country b
            WHERE a.id_client = b.id_client
              AND a.country_key = b.country_key
        )
        GROUP BY 2
    ),

    -- Combine chain and total levels
    new_clients AS (
        SELECT * FROM new_clients_chain
        UNION ALL
        SELECT * FROM new_clients_total
    ),

    -- Calculate chain-level lost clients
    lost_clients_chain AS (
        SELECT
            a.chain_type_desc,
            a.country_key,
            COUNT(DISTINCT a.id_client) AS nb_foyers_perdus
        FROM previous12_month a
        WHERE NOT EXISTS (
            SELECT 1
            FROM last12_month b
            WHERE a.id_client = b.id_client
              AND a.chain_type_desc = b.chain_type_desc
              AND a.country_key = b.country_key
        )
        GROUP BY 1, 2
    ),

    -- Calculate Total Group lost clients
    lost_clients_total AS (
        SELECT
            'Total Group' AS chain_type_desc,
            a.country_key,
            COUNT(DISTINCT a.id_client) AS nb_foyers_perdus
        FROM previous12_month_country a
        WHERE NOT EXISTS (
            SELECT 1
            FROM last12_month_country b
            WHERE a.id_client = b.id_client
              AND a.country_key = b.country_key
        )
        GROUP BY 2
    ),

    -- Combine chain and total levels
    lost_clients AS (
        SELECT * FROM lost_clients_chain
        UNION ALL
        SELECT * FROM lost_clients_total
    )
    
    -- Final aggregation with calculated metrics
    SELECT
        data_agg.month AS Mois,
        data_agg.country_key AS Country,
        COALESCE(data_agg.chain_type_desc, 'Total Group') AS Enseigne,
        
        -- Raw metrics
        data_agg.CA_Tous_Clients,
        data_agg.CA_Tous_Clients_local,
        data_agg.CA_Porteurs,
        data_agg.CA_Porteurs_local,
        SAFE_DIVIDE(data_agg.CA_Porteurs, data_agg.CA_Tous_Clients) AS Taux_CA_encarte,
        
        data_agg.Nb_transactions_Tous_Clients,
        data_agg.Nb_transactions_porteurs,
        SAFE_DIVIDE(data_agg.Nb_transactions_porteurs, data_agg.Nb_transactions_Tous_Clients) AS Taux_transactions_encartees,
        data_agg.Nb_foyers,
        SAFE_DIVIDE(data_agg.Nb_transactions_porteurs, data_agg.Nb_foyers) AS Frequence_porteurs,
        SAFE_DIVIDE(data_agg.CA_Porteurs, data_agg.Nb_transactions_porteurs) AS Panier_moyen_Porteurs,
        
        CAST(data_agg.nb_articles AS INT64) AS nb_articles,
        CAST(data_agg.nb_articles_porteur AS INT64) AS nb_articles_porteur,
        
        data_agg.Families,
        data_agg.Seniors,
        
        data_agg.CA_promo,
        data_agg.CA_promo_local,
        
        -- Retention metrics
        sc.nb_foyers_constants,
        nc.nb_foyers_gagnes,
        lc.nb_foyers_perdus
        
    FROM data_agg
    LEFT JOIN stable_clients sc
        ON sc.chain_type_desc = COALESCE(data_agg.chain_type_desc, 'Total Group')
       AND sc.country_key = data_agg.country_key
    LEFT JOIN new_clients nc
        ON nc.chain_type_desc = COALESCE(data_agg.chain_type_desc, 'Total Group')
       AND nc.country_key = data_agg.country_key
    LEFT JOIN lost_clients lc
        ON lc.chain_type_desc = COALESCE(data_agg.chain_type_desc, 'Total Group')
       AND lc.country_key = data_agg.country_key
    
    WHERE data_agg.month IS NOT NULL
      AND data_agg.country_key IS NOT NULL
    """


def create_spain_query(bq_ops, temp_table_id):
    """Create BigQuery SQL for Spain data with corrected Total Group retention metrics."""
    # Get filters for Spain
    spain_filters = bq_ops.config.get("SPAIN_FILTERS", {})
    excluded_business_brands = spain_filters.get("excluded_business_brands", ["supeco"])
    
    # Format excluded business brands
    excluded_business_brands_sql = " OR ".join([f"lower(header.business_brand) = '{brand.lower()}'" for brand in excluded_business_brands])
    if excluded_business_brands_sql:
        excluded_business_brands_condition = f"AND NOT ({excluded_business_brands_sql})"
    else:
        excluded_business_brands_condition = ""
        
    # Format ECM exclusions
    ecm_exclusions = spain_filters.get("excluded_ecm_combinations", [])
    ecm_conditions = []
    for exclusion in ecm_exclusions:
        conditions = []
        for key, value in exclusion.items():
            conditions.append(f"header.{key} = '{value}'")
        if conditions:
            ecm_conditions.append(" AND ".join(conditions))
    
    if ecm_conditions:
        ecm_condition = f"AND NOT (header.activity_type = 'ECM' AND ({' OR '.join(ecm_conditions)}))"
    else:
        ecm_condition = ""
    
    # Get country mappings from config
    country_mapping = bq_ops.config.get("COUNTRY_MAPPING", {"ITA": "IT", "ESP": "SP"})
    
    # Generate dynamic CASE statement for country mapping
    country_mapping_cases = []
    for source, target in country_mapping.items():
        country_mapping_cases.append(f"WHEN line.COUNTRY_KEY = '{source}' THEN '{target}'")
    
    country_mapping_sql = "\n                ".join(country_mapping_cases)
    if not country_mapping_sql:
        country_mapping_sql = "ELSE line.COUNTRY_KEY"  # Default fallback

    # Updated Spain query with fixed Total Group retention metrics
    return f"""
    CREATE OR REPLACE TABLE {temp_table_id} AS
    WITH data_agg AS (
        SELECT
            DATE_TRUNC(line.DATE_KEY, MONTH) AS month,
            CASE WHEN line.COUNTRY_KEY = 'ESP' THEN 'SP' END AS COUNTRY_KEY,
            CASE 
                WHEN header.ACTIVITY_TYPE = 'ECM' THEN 'E-commerce' 
                ELSE store.CHAIN_TYPE_DESC 
            END AS CHAIN_TYPE_DESC,
            
            SUM(line.SAL_AMT_WTAX * line.EURO_RATE) AS total_sales,
            SUM(CASE WHEN header.CUST_ID IS NOT NULL THEN line.SAL_AMT_WTAX * line.EURO_RATE END) AS sales_cardholders,
            
            COUNT(DISTINCT FORMAT('%s|%s|%s|%s|%s|%s', 
                header.COUNTRY_KEY, CAST(header.DATE_KEY AS STRING), header.SITE_KEY,
                header.TRX_KEY, header.TRX_TYPE, header.REC_SRC)) AS nb_trx,
            
            COUNT(DISTINCT CASE WHEN header.CUST_ID IS NOT NULL THEN FORMAT('%s|%s|%s|%s|%s|%s', 
                header.COUNTRY_KEY, CAST(header.DATE_KEY AS STRING), header.SITE_KEY,
                header.TRX_KEY, header.TRX_TYPE, header.REC_SRC) END) AS nb_trx_cardholders,
            
            COUNT(DISTINCT CASE WHEN header.CUST_ID IS NOT NULL THEN header.CUST_ID END) AS nb_cardholders,
            
            SUM(line.sal_unit_qty) AS sales_qty,
            SUM(CASE WHEN header.CUST_ID IS NOT NULL THEN line.sal_unit_qty END) AS sales_qty_carholders,
            
            CAST(NULL AS INT64) AS Families,
            CAST(NULL AS INT64) AS Seniors,
            
            SUM(CASE WHEN line.PROMO_FLAG = '1' THEN line.SAL_AMT_WTAX * line.EURO_RATE END) AS sales_promo
            
        FROM `{bq_ops.config["SOURCE_LINE_TABLE"]}` AS line
        LEFT JOIN `{bq_ops.config["SOURCE_HEADER_TABLE"]}` AS header
            ON line.DATE_KEY = header.DATE_KEY
            AND line.COUNTRY_KEY = header.COUNTRY_KEY
            AND line.SITE_KEY = header.SITE_KEY
            AND line.TRX_KEY = header.TRX_KEY
            AND line.TRX_TYPE = header.TRX_TYPE
            AND line.rec_src = header.rec_src
        INNER JOIN `{bq_ops.config["SOURCE_SITE_TABLE"]}` AS site
            ON line.SITE_KEY = site.site_key
            AND line.COUNTRY_KEY = site.COUNTRY_KEY
        INNER JOIN `{bq_ops.config["SOURCE_STORE_TABLE"]}` AS store
            ON site.MAIN_SITE_KEY = store.STORE_KEY
            AND site.COUNTRY_KEY = store.COUNTRY_KEY
            
        WHERE DATE_TRUNC(line.date_key, MONTH) = @theMonth
            AND store.chain_type_key != "CAC"
            AND line.COUNTRY_KEY = 'ESP'
            {excluded_business_brands_condition}
            {ecm_condition}
            
        GROUP BY 
            GROUPING SETS (
                (month, COUNTRY_KEY, CHAIN_TYPE_DESC),
                (month, COUNTRY_KEY),
                (month)
            )
    ),
    
    last12_month AS (
        SELECT
            CASE WHEN header.CUST_ID IS NOT NULL THEN header.CUST_ID END AS id_client,
            CASE WHEN line.COUNTRY_KEY = 'ESP' THEN 'SP' END AS COUNTRY_KEY,
            CASE 
                WHEN header.ACTIVITY_TYPE = 'ECM' THEN 'E-commerce' 
                ELSE store.CHAIN_TYPE_DESC 
            END AS CHAIN_TYPE_DESC
            
        FROM `{bq_ops.config["SOURCE_LINE_TABLE"]}` AS line
        LEFT JOIN `{bq_ops.config["SOURCE_HEADER_TABLE"]}` AS header
            ON line.DATE_KEY = header.DATE_KEY
            AND line.COUNTRY_KEY = header.COUNTRY_KEY
            AND line.SITE_KEY = header.SITE_KEY
            AND line.TRX_KEY = header.TRX_KEY
            AND line.TRX_TYPE = header.TRX_TYPE
            AND line.rec_src = header.rec_src
        INNER JOIN `{bq_ops.config["SOURCE_SITE_TABLE"]}` AS site
            ON line.SITE_KEY = site.site_key
            AND line.COUNTRY_KEY = site.COUNTRY_KEY
        INNER JOIN `{bq_ops.config["SOURCE_STORE_TABLE"]}` AS store
            ON site.MAIN_SITE_KEY = store.STORE_KEY
            AND site.COUNTRY_KEY = store.COUNTRY_KEY
            
        WHERE DATE_TRUNC(line.date_key, MONTH)
              BETWEEN DATE_SUB(@theMonth, INTERVAL 11 MONTH) AND @theMonth
            AND store.chain_type_key != "CAC"
            AND line.COUNTRY_KEY = 'ESP'
            {excluded_business_brands_condition}
            {ecm_condition}
        
        GROUP BY 1, 2, 3
    ),
    
    previous12_month AS (
        SELECT
            CASE WHEN header.CUST_ID IS NOT NULL THEN header.CUST_ID END AS id_client,
            CASE WHEN line.COUNTRY_KEY = 'ESP' THEN 'SP' END AS COUNTRY_KEY,
            CASE 
                WHEN header.ACTIVITY_TYPE = 'ECM' THEN 'E-commerce' 
                ELSE store.CHAIN_TYPE_DESC 
            END AS CHAIN_TYPE_DESC
            
        FROM `{bq_ops.config["SOURCE_LINE_TABLE"]}` AS line
        LEFT JOIN `{bq_ops.config["SOURCE_HEADER_TABLE"]}` AS header
            ON line.DATE_KEY = header.DATE_KEY
            AND line.COUNTRY_KEY = header.COUNTRY_KEY
            AND line.SITE_KEY = header.SITE_KEY
            AND line.TRX_KEY = header.TRX_KEY
            AND line.TRX_TYPE = header.TRX_TYPE
            AND line.rec_src = header.rec_src
        INNER JOIN `{bq_ops.config["SOURCE_SITE_TABLE"]}` AS site
            ON line.SITE_KEY = site.site_key
            AND line.COUNTRY_KEY = site.COUNTRY_KEY
        INNER JOIN `{bq_ops.config["SOURCE_STORE_TABLE"]}` AS store
            ON site.MAIN_SITE_KEY = store.STORE_KEY
            AND site.COUNTRY_KEY = store.COUNTRY_KEY
            
        WHERE DATE_TRUNC(line.date_key, MONTH)
              BETWEEN DATE_SUB(@theMonth, INTERVAL 23 MONTH) 
                  AND DATE_SUB(@theMonth, INTERVAL 12 MONTH)
            AND store.chain_type_key != "CAC"
            AND line.COUNTRY_KEY = 'ESP'
            {excluded_business_brands_condition}
            {ecm_condition}
            
        GROUP BY 1, 2, 3
    ),
    
    -- Create country-level aggregates for Total Group calculations
    last12_month_country AS (
        SELECT
            id_client,
            COUNTRY_KEY
        FROM last12_month
        GROUP BY 1, 2
    ),

    previous12_month_country AS (
        SELECT 
            id_client,
            COUNTRY_KEY
        FROM previous12_month
        GROUP BY 1, 2
    ),
    
    -- Calculate chain-level stable clients
    stable_clients_chain AS (
        SELECT  
            a.CHAIN_TYPE_DESC,
            a.COUNTRY_KEY,
            COUNT(DISTINCT a.id_client) AS Nb_foyers_constants
        FROM previous12_month a
        INNER JOIN last12_month b
          ON a.id_client = b.id_client
          AND a.CHAIN_TYPE_DESC = b.CHAIN_TYPE_DESC
          AND a.COUNTRY_KEY = b.COUNTRY_KEY
        GROUP BY 1, 2
    ),
    
    -- Calculate Total Group stable clients
    stable_clients_total AS (
        SELECT  
            'Total Group' AS CHAIN_TYPE_DESC,
            a.COUNTRY_KEY,
            COUNT(DISTINCT a.id_client) AS Nb_foyers_constants
        FROM previous12_month_country a
        INNER JOIN last12_month_country b
          ON a.id_client = b.id_client
          AND a.COUNTRY_KEY = b.COUNTRY_KEY
        GROUP BY 2
    ),
    
    -- Combine chain and total levels
    stable_clients AS (
        SELECT * FROM stable_clients_chain
        UNION ALL
        SELECT * FROM stable_clients_total
    ),
    
    -- Calculate chain-level new clients
    New_clients_chain AS (
        SELECT 
            a.CHAIN_TYPE_DESC,
            a.COUNTRY_KEY,
            COUNT(DISTINCT a.id_client) AS Nb_foyers_gagnes
        FROM last12_month a
        WHERE NOT EXISTS (
            SELECT 1
            FROM previous12_month b
            WHERE a.id_client = b.id_client
              AND a.CHAIN_TYPE_DESC = b.CHAIN_TYPE_DESC
              AND a.COUNTRY_KEY = b.COUNTRY_KEY
        )
        GROUP BY 1, 2
    ),
    
    -- Calculate Total Group new clients
    New_clients_total AS (
        SELECT 
            'Total Group' AS CHAIN_TYPE_DESC,
            a.COUNTRY_KEY,
            COUNT(DISTINCT a.id_client) AS Nb_foyers_gagnes
        FROM last12_month_country a
        WHERE NOT EXISTS (
            SELECT 1
            FROM previous12_month_country b
            WHERE a.id_client = b.id_client
              AND a.COUNTRY_KEY = b.COUNTRY_KEY
        )
        GROUP BY 2
    ),
    
    -- Combine chain and total levels
    New_clients AS (
        SELECT * FROM New_clients_chain
        UNION ALL
        SELECT * FROM New_clients_total
    ),
    
    -- Calculate chain-level lost clients
    Lost_clients_chain AS (
        SELECT 
            a.CHAIN_TYPE_DESC,
            a.COUNTRY_KEY,
            COUNT(DISTINCT a.id_client) AS Nb_foyers_perdus
        FROM previous12_month a
        WHERE NOT EXISTS (
            SELECT 1
            FROM last12_month b
            WHERE a.id_client = b.id_client
              AND a.CHAIN_TYPE_DESC = b.CHAIN_TYPE_DESC
              AND a.COUNTRY_KEY = b.COUNTRY_KEY
        )
        GROUP BY 1, 2
    ),
    
    -- Calculate Total Group lost clients
    Lost_clients_total AS (
        SELECT 
            'Total Group' AS CHAIN_TYPE_DESC,
            a.COUNTRY_KEY,
            COUNT(DISTINCT a.id_client) AS Nb_foyers_perdus
        FROM previous12_month_country a
        WHERE NOT EXISTS (
            SELECT 1
            FROM last12_month_country b
            WHERE a.id_client = b.id_client
              AND a.COUNTRY_KEY = b.COUNTRY_KEY
        )
        GROUP BY 2
    ),
    
    -- Combine chain and total levels
    Lost_clients AS (
        SELECT * FROM Lost_clients_chain
        UNION ALL
        SELECT * FROM Lost_clients_total
    )
    
    SELECT 
        a.month AS Mois,
        a.COUNTRY_KEY AS Country,
        COALESCE(a.CHAIN_TYPE_DESC, 'Total Group') AS Enseigne,
        
        a.total_sales AS CA_Tous_Clients,
        CAST(NULL AS FLOAT64) AS CA_Tous_Clients_local,
        a.sales_cardholders AS CA_Porteurs,
        CAST(NULL AS FLOAT64) AS CA_Porteurs_local,
        SAFE_DIVIDE(a.sales_cardholders, a.total_sales) AS Taux_CA_encarte,
        
        a.nb_trx AS Nb_transactions_Tous_Clients,
        a.nb_trx_cardholders AS Nb_transactions_porteurs,
        SAFE_DIVIDE(a.nb_trx_cardholders, a.nb_trx) AS Taux_transactions_encartees,
        
        a.nb_cardholders AS Nb_foyers,
        SAFE_DIVIDE(a.nb_trx_cardholders, a.nb_cardholders) AS Frequence_porteurs,
        SAFE_DIVIDE(a.sales_cardholders, a.nb_trx_cardholders) AS Panier_moyen_Porteurs,
        
        CAST(a.sales_qty AS INT64) AS nb_articles,
        CAST(a.sales_qty_carholders AS INT64) AS nb_articles_porteur,
        
        a.Families,
        a.Seniors,
        
        a.sales_promo AS CA_promo,
        CAST(NULL AS FLOAT64) AS CA_promo_local,
        
        all_c.Nb_foyers_constants,
        new_c.Nb_foyers_gagnes,
        lost_c.Nb_foyers_perdus
        
    FROM data_agg a
    LEFT JOIN stable_clients all_c 
      ON all_c.CHAIN_TYPE_DESC = COALESCE(a.CHAIN_TYPE_DESC, 'Total Group') 
     AND all_c.COUNTRY_KEY = a.COUNTRY_KEY
    LEFT JOIN New_clients new_c 
      ON new_c.CHAIN_TYPE_DESC = COALESCE(a.CHAIN_TYPE_DESC, 'Total Group') 
     AND new_c.COUNTRY_KEY = a.COUNTRY_KEY
    LEFT JOIN Lost_clients lost_c 
      ON lost_c.CHAIN_TYPE_DESC = COALESCE(a.CHAIN_TYPE_DESC, 'Total Group') 
     AND lost_c.COUNTRY_KEY = a.COUNTRY_KEY
     
    WHERE a.month IS NOT NULL
      AND a.COUNTRY_KEY IS NOT NULL
    """

def insert_from_temp_to_final(bq_ops, temp_table_id, the_month):
    """Insert data from temporary table to final table."""
    metrics.start_timer("insert_operation")
    
    try:
        # First, get the list of countries in the temp table
        countries_query = f"""
        SELECT DISTINCT Country 
        FROM `{temp_table_id}`
        """
        countries_result = bq_ops.execute_query(countries_query)
        countries = [row.Country for row in countries_result]
        
        if not countries:
            logger.warning(f"No countries found in temp table {temp_table_id}, skipping delete/insert operations")
            metrics.stop_timer("insert_operation")
            return 0
            
        countries_str = ', '.join([f"'{country}'" for country in countries])
        logger.info(f"Found countries in temp data: {countries_str}")
        
        # Delete existing data only for the specific countries and month
        logger.info(f"Deleting existing data for month {the_month} and countries: {countries_str}")
        delete_query = f"""
        DELETE FROM `{bq_ops.config["DEST_TABLE"]}`
        WHERE Mois = @theMonth
        AND Country IN ({countries_str})
        """
        
        bq_ops.execute_query(
            delete_query,
            [bigquery.ScalarQueryParameter("theMonth", "DATE", the_month)]
        )
        
        # Now insert from temp table
        insert_query = f"""
        INSERT INTO `{bq_ops.config["DEST_TABLE"]}` (
            Mois, Country, Enseigne,
            CA_Tous_Clients, CA_Tous_Clients_local,
            CA_Porteurs, CA_Porteurs_local, Taux_CA_encarte,
            Nb_transactions_Tous_Clients, Nb_transactions_porteurs, Taux_transactions_encartees,
            Nb_foyers, Frequence_porteurs, Panier_moyen_Porteurs,
            nb_articles, nb_articles_porteur,
            Families, Seniors,
            CA_promo, CA_promo_local,
            nb_foyers_constants, nb_foyers_gagnes, nb_foyers_perdus
        )
        SELECT * FROM `{temp_table_id}`
        """
        
        # Execute the insert
        bq_ops.execute_query(insert_query)
        
        # Query to get the actual inserted rows
        count_query = f"""
        SELECT COUNT(*) AS row_count 
        FROM `{bq_ops.config["DEST_TABLE"]}` 
        WHERE Mois = @theMonth
        AND Country IN ({countries_str})
        """
        count_result = bq_ops.execute_query(
            count_query,
            [bigquery.ScalarQueryParameter("theMonth", "DATE", the_month)]
        )
        rows = list(count_result)
        rows_affected = rows[0].row_count if rows else 0
        
        # Record metrics
        metrics.stop_timer("insert_operation")
        metrics.record_value("rows_inserted", rows_affected)
        
        logger.info(f"Inserted {rows_affected} rows for {the_month} and countries: {countries_str}")
        return rows_affected
    except Exception as e:
        metrics.stop_timer("insert_operation")
        metrics.increment_counter("insert_failures")
        logger.error(f"Failed to insert from temp to final: {e}")
        raise

def retry_with_backoff(max_retries=3, initial_backoff=1, max_backoff=60):
    """Retry decorator with exponential backoff."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            retries = 0
            backoff = initial_backoff
            
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries > max_retries:
                        raise
                    
                    wait_time = min(backoff * (2 ** (retries - 1)), max_backoff)
                    logger.warning(f"Operation failed, retrying in {wait_time}s ({retries}/{max_retries}): {e}")
                    time.sleep(wait_time)
        return wrapper
    return decorator

def delete_temp_table(bq_ops, temp_table_id):
    """Delete a temporary table with retries."""
    @retry_with_backoff(max_retries=5, initial_backoff=2, max_backoff=30)
    def _delete_with_retry():
        try:
            bq_ops.delete_table(temp_table_id, not_found_ok=True)
            logger.info(f"Deleted temp table: {temp_table_id}")
            return True
        except Exception as e:
            logger.warning(f"Failed to delete temp table {temp_table_id}: {e}")
            raise  # Re-raise for retry

    try:
        return _delete_with_retry()
    except Exception as e:
        # If all retries fail, log but don't crash the program
        logger.error(f"All deletion attempts failed for temp table {temp_table_id}: {e}")
        metrics.increment_counter("temp_table_deletion_failures")
        return False

class AdaptiveWorkerScaler:
    """
    Dynamically adjusts worker count based on system resource availability.
    Optimized for BigQuery-intensive workloads.
    """
    
    def __init__(self, config):
        """Initialize the worker scaler."""
        self.config = config
        
        # Worker count boundaries
        self.min_workers = config.get("MIN_WORKERS", 1)
        self.max_workers = config.get("MAX_WORKERS", 8)
        self.initial_workers = config.get("INITIAL_WORKERS", min(4, self.max_workers))
        
        # Resource utilization targets - CPU
        self.target_cpu_usage = config.get("TARGET_CPU_USAGE", 0.7)  # 70%
        self.cpu_scale_up_threshold = config.get("CPU_SCALE_UP_THRESHOLD", 0.8)  # 80%
        self.cpu_scale_down_threshold = config.get("CPU_SCALE_DOWN_THRESHOLD", 0.5)  # 50%
        
        # Resource utilization targets - Memory
        # Primary scaling factor for BigQuery workloads (more important than CPU)
        self.target_memory_usage = config.get("TARGET_MEMORY_USAGE", 0.6)  # 60%
        self.memory_scale_up_threshold = config.get("MEMORY_SCALE_UP_THRESHOLD", 0.75)  # 75%
        self.memory_scale_down_threshold = config.get("MEMORY_SCALE_DOWN_THRESHOLD", 0.45)  # 45%
        self.memory_limit_threshold = config.get("MEMORY_LIMIT_THRESHOLD", 0.85)  # 85%
        
        # Memory reservation per worker (MB) - provides better control
        self.memory_per_worker_mb = config.get("MEMORY_PER_WORKER_MB", 512)  # 512MB per worker
        
        # Scaling behavior controls
        self.scaling_cooldown = config.get("SCALING_COOLDOWN_SECONDS", 60)  # 1 minute
        self.scaling_sensitivity = config.get("SCALING_SENSITIVITY", 2)  # Readings needed to confirm a trend
        
        # Internal state
        self.current_workers = self.initial_workers
        self.last_scaling_time = 0
        self.lock = threading.Lock()
        self.worker_history = []
        self.consecutive_high_cpu = 0
        self.consecutive_low_cpu = 0
        self.consecutive_high_memory = 0
        self.consecutive_low_memory = 0
        
        # Sampling and monitoring
        self.sample_count = config.get("METRIC_SAMPLE_COUNT", 3)
        self.sample_interval = config.get("METRIC_SAMPLE_INTERVAL_SECONDS", 1)
        
        logger.info(f"Adaptive worker scaling initialized with memory-aware strategy. "
                   f"Initial workers: {self.initial_workers}, "
                   f"Min: {self.min_workers}, Max: {self.max_workers}, "
                   f"Memory per worker: {self.memory_per_worker_mb}MB")
    
    def get_system_metrics(self):
        """Get detailed system metrics with averaged samples."""
        # Take multiple samples for better accuracy
        cpu_samples = []
        memory_samples = []
        
        # Collect samples
        for _ in range(self.sample_count):
            cpu_samples.append(psutil.cpu_percent(interval=None) / 100.0)
            memory = psutil.virtual_memory()
            memory_samples.append(memory.percent / 100.0)
            time.sleep(self.sample_interval)
        
        # Memory information
        memory = psutil.virtual_memory()
        
        # Compile all metrics
        metrics = {
            'cpu_percent': sum(cpu_samples) / len(cpu_samples),
            'memory_percent': sum(memory_samples) / len(memory_samples),
            'memory_available_gb': memory.available / (1024**3),
            'memory_available_mb': memory.available / (1024**2),
            'timestamp': datetime.now().isoformat(),
        }
        
        return metrics
    
    def get_worker_count(self):
        """Calculate the optimal worker count based on system metrics with memory priority."""
        # Get current system metrics
        metrics = self.get_system_metrics()
        cpu_usage = metrics['cpu_percent']
        memory_usage = metrics['memory_percent']
        memory_available_mb = metrics['memory_available_mb']
        
        # Check if we're in the cooldown period
        current_time = time.time()
        if current_time - self.last_scaling_time < self.scaling_cooldown:
            # Still in cooldown, don't adjust workers
            return self.current_workers
        
        with self.lock:
            previous_workers = self.current_workers
            
            # Calculate maximum workers based on available memory
            # This is the primary scaling factor for BigQuery workloads
            max_workers_by_memory = int(memory_available_mb / self.memory_per_worker_mb)
            max_workers_by_memory = max(self.min_workers, min(max_workers_by_memory, self.max_workers))
            
            # Emergency scale down if memory is critically high
            if memory_usage > self.memory_limit_threshold and self.current_workers > self.min_workers:
                # Calculate a safe number of workers
                safe_worker_count = max(self.min_workers, self.current_workers - 1)
                
                # Apply more aggressive reduction for severe memory pressure
                if memory_usage > 0.92:  # 92% is very high
                    safe_worker_count = self.min_workers
                
                self.current_workers = safe_worker_count
                logger.warning(
                    f"Emergency worker reduction due to high memory usage ({memory_usage:.1%}): "
                    f"{previous_workers} → {self.current_workers}"
                )
                self._record_adjustment(
                    previous_workers, self.current_workers, metrics, "EMERGENCY_MEMORY"
                )
                return self.current_workers
            
            # Memory-based scaling - primary factor
            if memory_usage > self.memory_scale_up_threshold:
                self.consecutive_high_memory += 1
                self.consecutive_low_memory = 0
                
                if self.consecutive_high_memory >= self.scaling_sensitivity:
                    # Scale down to prevent memory exhaustion
                    new_worker_count = min(
                        previous_workers - 1, 
                        max_workers_by_memory
                    )
                    if new_worker_count < previous_workers and new_worker_count >= self.min_workers:
                        self.current_workers = new_worker_count
                        logger.info(
                            f"Scaling down workers due to high memory usage ({memory_usage:.1%}): "
                            f"{previous_workers} → {self.current_workers}"
                        )
                        self._record_adjustment(
                            previous_workers, self.current_workers, metrics, "MEMORY_HIGH"
                        )
                        self.consecutive_high_memory = 0
            
            elif memory_usage < self.memory_scale_down_threshold:
                self.consecutive_low_memory += 1
                self.consecutive_high_memory = 0
                
                if self.consecutive_low_memory >= self.scaling_sensitivity:
                    # Can scale up based on memory availability
                    new_worker_count = min(
                        previous_workers + 1,
                        max_workers_by_memory,
                        self.max_workers
                    )
                    if new_worker_count > previous_workers:
                        self.current_workers = new_worker_count
                        logger.info(
                            f"Scaling up workers due to low memory usage ({memory_usage:.1%}): "
                            f"{previous_workers} → {self.current_workers}"
                        )
                        self._record_adjustment(
                            previous_workers, self.current_workers, metrics, "MEMORY_LOW"
                        )
                        self.consecutive_low_memory = 0
            
            # Secondary scaling factor - CPU utilization
            # Only consider CPU if memory is in the acceptable range
            elif memory_usage >= self.memory_scale_down_threshold and memory_usage <= self.memory_scale_up_threshold:
                if cpu_usage > self.cpu_scale_up_threshold and self.current_workers < self.max_workers:
                    self.consecutive_high_cpu += 1
                    self.consecutive_low_cpu = 0
                    
                    if self.consecutive_high_cpu >= self.scaling_sensitivity:
                        # Check if we have enough memory to add a worker
                        if self.current_workers < max_workers_by_memory:
                            self.current_workers = min(self.current_workers + 1, self.max_workers)
                            logger.info(
                                f"Scaling up workers due to high CPU usage ({cpu_usage:.1%}): "
                                f"{previous_workers} → {self.current_workers}"
                            )
                            self._record_adjustment(
                                previous_workers, self.current_workers, metrics, "CPU_HIGH"
                            )
                            
                        self.consecutive_high_cpu = 0
                
                elif cpu_usage < self.cpu_scale_down_threshold and self.current_workers > self.min_workers:
                    self.consecutive_low_cpu += 1
                    self.consecutive_high_cpu = 0
                    
                    if self.consecutive_low_cpu >= self.scaling_sensitivity:
                        self.current_workers = max(self.min_workers, self.current_workers - 1)
                        
                        logger.info(
                            f"Scaling down workers due to low CPU usage ({cpu_usage:.1%}): "
                            f"{previous_workers} → {self.current_workers}"
                        )
                        self._record_adjustment(
                            previous_workers, self.current_workers, metrics, "CPU_LOW"
                        )
                        
                        self.consecutive_low_cpu = 0
                else:
                    # Reset counters when in the normal range
                    if cpu_usage <= self.cpu_scale_up_threshold and cpu_usage >= self.cpu_scale_down_threshold:
                        self.consecutive_high_cpu = 0
                        self.consecutive_low_cpu = 0
            
            # Print memory status to logs periodically
            if random.random() < 0.1:  # ~10% probability to avoid too many logs
                logger.info(
                    f"Memory status: Usage {memory_usage:.1%}, Available: {metrics['memory_available_gb']:.2f}GB, "
                    f"Max workers by memory: {max_workers_by_memory}, Current: {self.current_workers}"
                )
        
        return self.current_workers
    
    def _record_adjustment(self, previous_workers, new_workers, metrics, reason):
        """Record a worker count adjustment with metrics."""
        # Update last scaling time
        self.last_scaling_time = time.time()
        
        # Create adjustment record
        adjustment = {
            'timestamp': datetime.now().isoformat(),
            'previous_workers': previous_workers,
            'new_workers': new_workers,
            'reason': reason,
            'cpu_usage': metrics['cpu_percent'],
            'memory_usage': metrics['memory_percent'],
            'memory_available_gb': metrics['memory_available_gb'],
        }
        
        # Add to history
        self.worker_history.append(adjustment)
        
        # Log memory allocation per worker
        total_memory_gb = psutil.virtual_memory().total / (1024**3)
        logger.info(
            f"Memory allocation: Total: {total_memory_gb:.2f}GB, "
            f"Per worker: {self.memory_per_worker_mb/1024:.2f}GB, "
            f"Total allocated: {(new_workers * self.memory_per_worker_mb)/1024:.2f}GB"
        )
    
    def get_adjustment_history(self):
        """Get the history of worker count adjustments."""
        with self.lock:
            return self.worker_history.copy()

def process_month_for_country(bq_ops, the_month, country, request_id):
    """Process data for a specific month and country."""
    logger.correlation_id = f"month_{the_month.strftime('%Y%m')}_{country}_{request_id}"
    logger.info(f"Processing month {the_month} for country {country}")
    
    metrics.start_timer(f"process_month_{the_month.strftime('%Y%m')}_{country}")
    temp_table_id = f"{bq_ops.config['DEST_PROJECT']}.{bq_ops.config['DEST_DATASET']}.temp_data_{country}_{the_month.strftime('%Y%m')}_{request_id}"
    
    try:
        # Create a temp table with the aggregated data for this country
        create_temp_table_for_month(bq_ops, the_month, temp_table_id, country, request_id)
        
        # Insert the data into the final table
        rows_inserted = insert_from_temp_to_final(bq_ops, temp_table_id, the_month)
        
        # Record metrics
        metrics.stop_timer(f"process_month_{the_month.strftime('%Y%m')}_{country}")
        
        # Clean up temp table with robust deletion
        delete_temp_table(bq_ops, temp_table_id)
        
        logger.info(f"Successfully processed month {the_month} for country {country}, inserted {rows_inserted} rows")
        return True, rows_inserted
    except Exception as e:
        metrics.stop_timer(f"process_month_{the_month.strftime('%Y%m')}_{country}")
        metrics.increment_counter("month_processing_failures")
        logger.error(f"Failed to process month {the_month} for country {country}: {e}")
        
        # Try to clean up temp table even on failure
        delete_temp_table(bq_ops, temp_table_id)
            
        return False, 0

def process_month(bq_ops, the_month, request_id):
    """Process data for a specific month for all countries."""
    logger.correlation_id = f"month_{the_month.strftime('%Y%m')}_{request_id}"
    logger.info(f"Processing month: {the_month}")
    
    metrics.start_timer(f"process_month_{the_month.strftime('%Y%m')}")
    
    # Process each country in sequence
    successful_countries = 0
    failed_countries = 0
    total_rows = 0
    
    for country in bq_ops.config["ALLOWED_COUNTRIES"]:
        if shutdown_handler.is_shutdown_requested():
            logger.warning("Shutdown requested, stopping further country processing")
            break
            
        country_request_id = f"{request_id}_{country}"
        success, rows = process_month_for_country(bq_ops, the_month, country, country_request_id)
        
        if success:
            successful_countries += 1
            total_rows += rows
        else:
            failed_countries += 1
    
    # Record metrics
    metrics.stop_timer(f"process_month_{the_month.strftime('%Y%m')}")
    
    logger.info(f"Completed processing month {the_month}: {successful_countries} countries successful, {failed_countries} countries failed, {total_rows} total rows")
    
    # Return success only if all countries were processed successfully
    return failed_countries == 0, total_rows

def process_all_country_month_combinations(bq_ops, months, countries, request_id):
    """Process all country/month combinations with adaptive worker scaling."""
    logger.info(f"Processing {len(months)} months × {len(countries)} countries in parallel")
    
    # Generate all combinations
    combinations = [(month, country) for month in months for country in countries]
    total_combinations = len(combinations)
    
    # Record total work
    metrics.record_value("total_combinations", total_combinations)
    
    # Start timer for whole batch
    metrics.start_timer("process_all_combinations")
    
    # Initialize worker scaler
    worker_scaler = AdaptiveWorkerScaler(bq_ops.config)
    
    # Initialize tracking variables
    successful_combinations = 0
    failed_combinations = 0
    failed_combination_pairs = []
    total_rows = 0
    
    # Track jobs
    pending_futures = {}
    completed_futures = {}
    
    # Start an executor with dynamic worker count
    initial_workers = worker_scaler.get_worker_count()
    logger.info(f"Starting with {initial_workers} workers for {total_combinations} combinations")
    
    # Create a dynamic worker pool with adaptive semaphore
    max_possible_workers = bq_ops.config.get("MAX_WORKERS", 8)
    worker_semaphore = AdaptiveSemaphore(initial_workers)
    
    # Create a fixed-size thread pool with the maximum possible workers
    # We'll use the semaphore to control actual concurrency
    with ThreadPoolExecutor(max_workers=max_possible_workers) as executor:
        
        def submit_with_semaphore(fn, *args, **kwargs):
            """Submit a task that acquires and releases the worker semaphore."""
            worker_semaphore.acquire()
            try:
                future = executor.submit(fn, *args, **kwargs)
                # Add callback to release semaphore when done
                future.add_done_callback(lambda f: worker_semaphore.release())
                return future
            except Exception as e:
                worker_semaphore.release()
                raise e
        
        # Submit initial batch of jobs
        for idx, (month, country) in enumerate(combinations):
            if shutdown_handler.is_shutdown_requested():
                logger.warning("Shutdown requested, stopping submissions")
                break
                
            combo_id = f"{request_id}_{month.strftime('%Y%m')}_{country}"
            
            # Submit task with semaphore control
            future = submit_with_semaphore(
                process_month_for_country, bq_ops, month, country, combo_id
            )
            
            pending_futures[(month, country)] = future
        
        # Process results as they complete
        while pending_futures:
            # Check if we need to adjust worker count
            new_worker_count = worker_scaler.get_worker_count()
            
            # Update semaphore count if worker count changed
            if new_worker_count != worker_semaphore.current_value:
                worker_semaphore.adjust_value(new_worker_count)
            
            # Check for completed futures
            done, _ = concurrent.futures.wait(
                list(pending_futures.values()),
                timeout=5,  # Wait for up to 5 seconds
                return_when=concurrent.futures.FIRST_COMPLETED
            )
            
            # Process completed futures
            for future in done:
                # Find which combination this future belongs to
                for combo, f in list(pending_futures.items()):
                    if f == future:
                        month, country = combo
                        del pending_futures[combo]
                        completed_futures[combo] = future
                        
                        try:
                            if shutdown_handler.is_shutdown_requested():
                                continue
                                    
                            success, rows = future.result()
                            
                            if success:
                                successful_combinations += 1
                                total_rows += rows
                            else:
                                failed_combinations += 1
                                failed_combination_pairs.append((month, country))
                            
                            logger.info(f"Combination {month}/{country}: {'Success' if success else 'Failed'}, Rows: {rows}")
                        except Exception as e:
                            failed_combinations += 1
                            failed_combination_pairs.append((month, country))
                            logger.error(f"Error processing {month}/{country}: {e}")
                        
                        break
            
            # If no jobs completed in this iteration, sleep briefly
            if not done:
                time.sleep(1)
            
            # Check for shutdown
            if shutdown_handler.is_shutdown_requested():
                logger.warning("Shutdown requested, cancelling pending jobs")
                for future in pending_futures.values():
                    if not future.done():
                        future.cancel()
                break
    
    # Record metrics
    metrics.stop_timer("process_all_combinations")
    metrics.record_value("successful_combinations", successful_combinations)
    metrics.record_value("failed_combinations", failed_combinations)
    metrics.record_value("total_rows_processed", total_rows)
    
    # Record worker scaling history
    scaling_history = worker_scaler.get_adjustment_history()
    if scaling_history:
        logger.info(f"Worker scaling adjustments: {len(scaling_history)}")
    
    return successful_combinations, failed_combinations, total_rows, failed_combination_pairs

def process_month_range(bq_ops, start_month, end_month, parallel=True, request_id=None):
    """Process a range of months with parallelism and adaptive scaling."""
    # Generate a unique request ID if not provided
    request_id = request_id or str(uuid.uuid4())[:8]
    
    # Set context for logging
    logger.correlation_id = request_id
    
    # Generate a list of months to process
    months = []
    current_month = start_month
    while current_month <= end_month:
        months.append(current_month)
        current_month = (current_month.replace(day=1) + relativedelta(months=1))
    
    # Get countries from config
    countries = bq_ops.config["ALLOWED_COUNTRIES"]
    
    logger.info(f"Processing {len(months)} months from {start_month} to {end_month} for {len(countries)} countries")
    
    # Start timer for whole range
    metrics.start_timer("process_month_range")
    
    try:
        if parallel and (len(months) > 1 or len(countries) > 1):
            # Process all combinations in parallel with adaptive scaling
            successful_combinations, failed_combinations, total_rows, failed_pairs = process_all_country_month_combinations(
                bq_ops, months, countries, request_id
            )
            
            # Map success to the month level
            month_success = {}
            for month in months:
                month_success[month] = True
                for country in countries:
                    if (month, country) in failed_pairs:
                        month_success[month] = False
                        break
            
            successful_months = sum(1 for month, success in month_success.items() if success)
            failed_months = len(months) - successful_months
            
        else:
            # Process months sequentially
            logger.info("Using sequential processing")
            successful_months = 0
            failed_months = 0
            total_rows = 0
            
            for i, month in enumerate(months):
                if shutdown_handler.is_shutdown_requested():
                    logger.warning("Shutdown requested, stopping further processing")
                    break
                
                month_request_id = f"{request_id}_{i}"
                success, rows = process_month(bq_ops, month, month_request_id)
                total_rows += rows
                
                if success:
                    successful_months += 1
                else:
                    failed_months += 1
                    
                logger.info(f"Month {month}: {'Success' if success else 'Failed'}, Rows: {rows}")
    finally:
        # Record metrics
        metrics.stop_timer("process_month_range")
        metrics.record_value("months_processed_successfully", successful_months)
        metrics.record_value("months_processed_with_failure", failed_months)
        metrics.record_value("total_rows_processed", total_rows)
    
    return successful_months, failed_months

def main():
    """Main entrypoint for Cloud Run Job with connection pooling,
    query profiling, and adaptive worker scaling."""
    # Start timer and generate request ID
    start_time = time.time()
    job_id = os.environ.get("CLOUD_RUN_JOB_EXECUTION", str(uuid.uuid4())[:8])
    request_id = f"job_{job_id}"
    
    # Set correlation ID for logging
    logger.correlation_id = request_id
    logger.service = os.environ.get("K_SERVICE", "customer-data-job")
    
    # Load configuration from environment or config file
    config_file = os.environ.get("CONFIG_FILE", "config.yaml")
    if os.path.exists(config_file):
        config = load_config(config_file)
    else:
        config = load_config()
    
    # Add default config values for new features
    config.setdefault("MAX_CONNECTIONS", 10)
    config.setdefault("CONNECTION_POOL_SHUTDOWN_TIMEOUT", 60)  # 60 seconds to wait for active connections
    config.setdefault("SLOW_QUERY_THRESHOLD_SECONDS", 420)  # 7 minutes
    config.setdefault("LARGE_QUERY_THRESHOLD_BYTES", 20 * 1024 * 1024 * 1024)  # 20GB
    config.setdefault("MIN_WORKERS", 1)
    config.setdefault("INITIAL_WORKERS", 4)
    config.setdefault("TARGET_CPU_USAGE", 0.7)  # 70%
    config.setdefault("TARGET_MEMORY_USAGE", 0.6)  # 60%
    config.setdefault("MEMORY_PER_WORKER_MB", 512)  # 512MB per worker
    
    # Log start of process
    logger.info(f"Starting ETL job with ID: {job_id}")
    
    # Initialize BigQuery operations with the new components
    bq_ops = BigQueryOperations(config)
    
    try:
        # Determine date range to process
        if config["START_MONTH"] and config["END_MONTH"]:
            # Use explicitly configured start and end months
            start_month = datetime.strptime(config["START_MONTH"], '%Y-%m-%d').date()
            end_month = datetime.strptime(config["END_MONTH"], '%Y-%m-%d').date()
        elif config["LAST_N_MONTHS"]:
            # Process the last N months
            last_n_months = int(config["LAST_N_MONTHS"])
            end_month = date.today().replace(day=1) - relativedelta(days=1)
            end_month = end_month.replace(day=1)  # First day of last month
            start_month = end_month - relativedelta(months=last_n_months-1)
        else:
            # Default to the last complete month
            start_month = date.today().replace(day=1) - relativedelta(days=1)
            start_month = start_month.replace(day=1)  # First day of last month
            end_month = start_month
        
        # Log parameters
        logger.info(f"Processing from {start_month} to {end_month}, parallel={config['PARALLEL']}")
        logger.info(f"Countries to process: {', '.join(config['ALLOWED_COUNTRIES'])}")
        
        # Ensure destination table exists with optimal schema
        create_customer_data_table_if_not_exists(bq_ops)
        
        # Process the months
        successful_months, failed_months = process_month_range(
            bq_ops, 
            start_month, 
            end_month, 
            config["PARALLEL"], 
            request_id
        )
        
        # Calculate elapsed time
        elapsed = time.time() - start_time
        
        # Get query profiling results
        bq_ops.query_profiler.write_profiles_to_log()
        
        # Prepare successful summary
        metrics_summary = metrics.get_summary()
        summary = {
            "status": "success" if failed_months == 0 else "partial_success",
            "job_id": job_id,
            "start_month": start_month.isoformat(),
            "end_month": end_month.isoformat(),
            "countries": config["ALLOWED_COUNTRIES"],
            "successful_months": successful_months,
            "failed_months": failed_months,
            "processing_time_seconds": round(elapsed, 2),
            "metrics": metrics_summary
        }
        
        # Log completion
        logger.info(f"Processing completed in {elapsed:.2f} seconds: {successful_months} months successful, {failed_months} months failed")
        logger.info(f"Job summary: {json.dumps(summary)}")
        
        # Clean up resources with graceful shutdown
        shutdown_timeout = config.get("CONNECTION_POOL_SHUTDOWN_TIMEOUT", 60)
        logger.info(f"Starting connection pool cleanup with {shutdown_timeout}s timeout")
        bq_ops.connection_pool.close_all(timeout=shutdown_timeout)
        
        # Exit with appropriate code
        if failed_months > 0:
            sys.exit(1)  # Indicate partial failure
        else:
            sys.exit(0)  # Success
    
    except Exception as exc:
        # Calculate elapsed time even for failures
        elapsed = time.time() - start_time
        
        # Log the error
        logger.error(f"Processing error: {exc}", exc_info=True)
        
        # Prepare error summary
        metrics_summary = metrics.get_summary()
        summary = {
            "status": "error",
            "job_id": job_id,
            "error": str(exc),
            "processing_time_seconds": round(elapsed, 2),
            "metrics": metrics_summary
        }
        
        # Clean up resources if available with graceful shutdown
        if hasattr(bq_ops, 'connection_pool'):
            shutdown_timeout = config.get("CONNECTION_POOL_SHUTDOWN_TIMEOUT", 60)
            logger.info(f"Performing connection pool cleanup on error with {shutdown_timeout}s timeout")
            bq_ops.connection_pool.close_all(timeout=shutdown_timeout)
        
        logger.error(f"Job failed: {json.dumps(summary)}")
        sys.exit(1)

if __name__ == "__main__":
    main()