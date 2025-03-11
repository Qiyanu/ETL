import time
import threading
import random
import uuid
from typing import Dict, Any, Optional, Union, List, Tuple, Set
from contextlib import contextmanager

import requests
import psutil

from google.cloud import bigquery
from google.api_core import exceptions as gcp_exceptions

from src.logging_utils import logger
from src.metrics import metrics

class CircuitBreaker:
    """
    Implements the circuit breaker pattern for resilient operations.
    
    Tracks failures and prevents repeated attempts when a service is likely unavailable.
    Features proper state transitions including half-open state testing.
    """
    
    def __init__(self, failure_threshold: int = 5, reset_timeout: int = 300):
        """
        Initialize the circuit breaker.
        
        Args:
            failure_threshold (int): Number of failures before opening the circuit
            reset_timeout (int): Seconds to wait before attempting to close the circuit
        """
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN
        self.last_failure_time = 0
        self.lock = threading.Lock()
        
        # Track half-open state testing
        self.half_open_test_in_progress = False
    
    def record_success(self) -> None:
        """
        Record a successful operation and update circuit state.
        
        In HALF-OPEN state, a success transitions back to CLOSED.
        """
        with self.lock:
            # Update success counters
            self.failure_count = 0
            
            # Handle state transition from HALF-OPEN to CLOSED
            if self.state == "HALF-OPEN":
                if self.half_open_test_in_progress:
                    # Test request succeeded, close the circuit
                    self.state = "CLOSED"
                    self.half_open_test_in_progress = False
                    logger.info("Circuit breaker reset to CLOSED state after successful test")
    
    def record_failure(self) -> None:
        """
        Record a failed operation and potentially open the circuit.
        
        In HALF-OPEN state, a failure immediately returns to OPEN.
        """
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.state == "CLOSED" and self.failure_count >= self.failure_threshold:
                # Too many failures, open the circuit
                self.state = "OPEN"
                logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
                metrics.increment_counter("circuit_breaker_open_count")
            elif self.state == "HALF-OPEN":
                # Test request failed, back to open state with reset timeout
                self.state = "OPEN"
                self.half_open_test_in_progress = False
                logger.warning("Circuit breaker returned to OPEN state after failed test")
                metrics.increment_counter("circuit_breaker_reopen_count")
    
    def allow_request(self) -> bool:
        """
        Determine if a request should be allowed based on circuit state.
        
        In HALF-OPEN state, allows only one test request.
        
        Returns:
            bool: Whether the request can proceed
        """
        with self.lock:
            if self.state == "CLOSED":
                return True
            
            if self.state == "OPEN":
                # Check if enough time has passed to try again
                if time.time() - self.last_failure_time > self.reset_timeout:
                    self.state = "HALF-OPEN"
                    self.half_open_test_in_progress = False
                    logger.info("Circuit breaker transitioning to HALF-OPEN state")
                else:
                    return False
            
            # In HALF-OPEN state, allow only one test request
            if self.state == "HALF-OPEN":
                if not self.half_open_test_in_progress:
                    self.half_open_test_in_progress = True
                    logger.info("Circuit breaker allowing test request in HALF-OPEN state")
                    return True
                return False
            
            return True  # Fallback (should not reach here)
    
    def get_state(self) -> str:
        """
        Get the current circuit breaker state.
        
        Returns:
            str: Current state (CLOSED, OPEN, or HALF-OPEN)
        """
        with self.lock:
            return self.state
    
    def reset(self) -> None:
        """
        Manually reset the circuit breaker to closed state.
        Useful for testing or administrative intervention.
        """
        with self.lock:
            prev_state = self.state
            self.state = "CLOSED"
            self.failure_count = 0
            self.half_open_test_in_progress = False
            logger.info(f"Circuit breaker manually reset from {prev_state} to CLOSED state")

class BigQueryConnectionPool:
    """
    Manages a pool of BigQuery client connections with graceful shutdown capabilities.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the connection pool.
        
        Args:
            config (dict): Configuration dictionary
        """
        self.config = config
        self.pool_size = config.get("MAX_CONNECTIONS", 10)
        self.location = config.get("LOCATION", "EU")
        
        # Connection pool management
        self.available_clients: List[bigquery.Client] = []
        self.in_use_clients = set()
        self.lock = threading.Lock()
        self.semaphore = threading.Semaphore(self.pool_size)
        
        # Shutdown management
        self.shutdown_requested = False
        self.shutdown_event = threading.Event()
        
        # Metrics tracking
        self.checkout_count = 0
        self.created_connections = 0
        
        # Preload some connections
        preload_count = min(3, self.pool_size)
        for _ in range(preload_count):
            client = self._create_client()
            self.available_clients.append(client)
            self.created_connections += 1
    
    def _create_client(self) -> bigquery.Client:
        """
        Create a new BigQuery client with proper configuration.
        
        Returns:
            bigquery.Client: Configured BigQuery client
        """
        return bigquery.Client(location=self.location)
    
    def get_client(self) -> bigquery.Client:
        """
        Retrieve a BigQuery client from the pool.
        
        Returns:
            bigquery.Client: A BigQuery client instance
        
        Raises:
            RuntimeError: If the connection pool is shutting down
        """
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
    
    def release_client(self, client: bigquery.Client) -> None:
        """
        Return a client to the connection pool.
        
        Args:
            client (bigquery.Client): Client to return to the pool
        """
        with self.lock:
            if client in self.in_use_clients:
                self.in_use_clients.remove(client)
                
                if self.shutdown_requested:
                    # During shutdown, close immediately
                    try:
                        client.close()
                    except Exception as e:
                        logger.warning(f"Error closing client during shutdown: {e}")
                    
                    # Signal shutdown completion if no active connections
                    if not self.in_use_clients:
                        self.shutdown_event.set()
                elif len(self.available_clients) < self.pool_size:
                    # Return to pool during normal operation
                    self.available_clients.append(client)
                else:
                    # Close excess connections
                    try:
                        client.close()
                    except Exception as e:
                        logger.warning(f"Error closing excess client: {e}")
        
        # Release the semaphore
        self.semaphore.release()
    
    def close_all(self, timeout: int = 30) -> None:
        """
        Gracefully close all connections in the pool.
        
        Args:
            timeout (int): Maximum time to wait for connections to complete
        """
        logger.info("Initiating connection pool shutdown")
        
        with self.lock:
            # Mark pool as shutting down
            self.shutdown_requested = True
            
            # Close available clients
            for client in self.available_clients:
                try:
                    client.close()
                except Exception as e:
                    logger.warning(f"Error closing available client: {e}")
            self.available_clients.clear()
            
            # Log in-use client count
            in_use_count = len(self.in_use_clients)
            logger.info(f"Waiting for {in_use_count} active connections to complete")
            
            # Set event if no in-use connections
            if in_use_count == 0:
                self.shutdown_event.set()
        
        # Wait for shutdown or timeout
        if not self.shutdown_event.wait(timeout=timeout):
            # Forcefully close remaining connections
            with self.lock:
                remaining = len(self.in_use_clients)
                if remaining:
                    logger.warning(f"Forcefully closing {remaining} connections")
                    for client in list(self.in_use_clients):
                        try:
                            client.close()
                        except Exception as e:
                            logger.warning(f"Error forcefully closing client: {e}")
                    self.in_use_clients.clear()
        
        logger.info("Connection pool shutdown complete")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Retrieve statistics about the connection pool.
        
        Returns:
            dict: Connection pool statistics
        """
        with self.lock:
            return {
                "pool_size": self.pool_size,
                "available_connections": len(self.available_clients),
                "in_use_connections": len(self.in_use_clients),
                "total_created_connections": self.created_connections,
                "checkout_count": self.checkout_count,
                "shutting_down": self.shutdown_requested
            }

class QueryProfiler:
    """
    Profiles BigQuery queries to identify performance bottlenecks.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the query profiler.
        
        Args:
            config (dict): Configuration dictionary
        """
        self.config = config
        
        # Thresholds for problematic queries
        self.time_threshold_seconds = config.get("SLOW_QUERY_THRESHOLD_SECONDS", 420)  # 7 minutes
        self.size_threshold_bytes = config.get("LARGE_QUERY_THRESHOLD_BYTES", 20 * 1024 * 1024 * 1024)  # 20GB
        
        # Profiling storage
        self.profiles: List[Dict[str, Any]] = []
        self.lock = threading.Lock()
        
        # Enable detailed query plan analysis
        self.enable_plan_analysis = config.get("ENABLE_QUERY_PLAN_ANALYSIS", True)
    
    def profile_query(self, query: str, params: Optional[List[Any]], query_job: Any, execution_time: float) -> None:
        """
        Profile a BigQuery query's performance.
        
        Args:
            query (str): SQL query
            params (list, optional): Query parameters
            query_job (Job): BigQuery job object
            execution_time (float): Query execution time in seconds
        """
        # Skip if insufficient information
        if not query_job or not hasattr(query_job, 'total_bytes_processed'):
            return
        
        # Extract query metrics
        bytes_processed = query_job.total_bytes_processed or 0
        bytes_billed = getattr(query_job, 'total_bytes_billed', None)
        is_cached = getattr(query_job, 'cache_hit', False)
        
        # Determine if query is slow or large
        is_slow = execution_time >= self.time_threshold_seconds
        is_large = bytes_processed >= self.size_threshold_bytes
        
        # Only profile problematic queries
        if not (is_slow or is_large):
            return
        
        # Build profile data
        profile: Dict[str, Any] = {
            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
            'job_id': query_job.job_id,
            'query': query[:2000],  # Truncate very long queries
            'params': str(params) if params else None,
            'execution_time_seconds': execution_time,
            'bytes_processed': bytes_processed,
            'bytes_billed': bytes_billed,
            'gb_processed': bytes_processed / (1024**3),
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
        
        # Log query performance issues
        self._log_query_issue(profile)
    
    def _identify_bottlenecks(self, query_job: Any) -> List[Dict[str, Any]]:
        """
        Identify bottleneck stages in query execution.
        
        Args:
            query_job (Job): BigQuery job object
        
        Returns:
            List of bottleneck stage details
        """
        if not hasattr(query_job, 'query_plan'):
            return []
        
        bottlenecks = []
        stages = []
        
        # Find stages by duration
        for step in query_job.query_plan:
            if step.start_ms and step.end_ms:
                duration = step.end_ms - step.start_ms
                stages.append((step.id, step.name, duration))
        
        # Sort and take top bottlenecks
        stages.sort(key=lambda x: x[2], reverse=True)
        for i, (stage_id, stage_name, duration) in enumerate(stages[:3]):
            bottlenecks.append({
                'stage_id': stage_id,
                'stage_name': stage_name,
                'duration_ms': duration,
                'rank': i+1
            })
        
        return bottlenecks
    
    def _log_query_issue(self, profile: Dict[str, Any]) -> None:
        """
        Log details about problematic queries.
        
        Args:
            profile (dict): Query performance profile
        """
        issue_type = []
        if profile['is_slow']:
            issue_type.append(f"slow ({profile['execution_time_seconds']:.1f}s)")
        if profile['is_large']:
            issue_type.append(f"large ({profile['gb_processed']:.1f}GB)")
        
        issue_description = " and ".join(issue_type)
        
        # Log query performance warning
        logger.warning(
            f"Performance issue detected: {issue_description} query. "
            f"Job ID: {profile['job_id']}. "
            f"Cache hit: {profile['cache_hit']}."
        )
        
        # Log bottleneck details if available
        if 'bottleneck_stages' in profile and profile['bottleneck_stages']:
            bottleneck = profile['bottleneck_stages'][0]
            logger.info(
                f"Primary bottleneck: Stage {bottleneck['stage_id']} ({bottleneck['stage_name']}), "
                f"Duration: {bottleneck['duration_ms'] / 1000:.1f}s"
            )

class BigQueryOperations:
    """Streamlined BigQuery operations with connection pooling and query profiling."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize with configuration and optional connection pool and profiler.
        
        Args:
            config: Configuration dictionary with BigQuery settings
        """
        # Validate required configuration
        self._validate_config(config)
        
        # Initialize connection pool and query profiler
        self.connection_pool = BigQueryConnectionPool(config)
        self.query_profiler = QueryProfiler(config)
        
        self.config = config
        self.start_time = time.time()
        
        # Initialize circuit breaker
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=config.get("CIRCUIT_BREAKER_THRESHOLD", 5),
            reset_timeout=config.get("CIRCUIT_BREAKER_TIMEOUT", 300)
        )
        
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
            # Requests exceptions (if used)
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ReadTimeout,
            # Google API client errors
            gcp_exceptions.Aborted,
            gcp_exceptions.TooManyRequests,
            gcp_exceptions.ServiceUnavailable,
        )
    
    def _validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate configuration has all required keys.
        
        Args:
            config: Configuration dictionary to validate
            
        Raises:
            ValueError: If required configuration is missing
        """
        required_keys = [
            "DEST_TABLE", 
            "QUERY_TIMEOUT", 
            "MAX_WORKERS"
        ]
        
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise ValueError(f"Missing required configuration keys: {', '.join(missing_keys)}")
    
    def _is_retryable_exception(self, exception: Exception) -> bool:
        """
        Check if an exception is retryable.
        
        Args:
            exception: The exception to check
            
        Returns:
            bool: True if the exception is retryable
        """
        # Check if it's one of our defined retryable types
        if isinstance(exception, self.retryable_exceptions):
            return True
        
        # Check for socket or transport errors in the error message
        error_msg = str(exception).lower()
        retryable_patterns = [
            "socket", "timeout", "deadline", "connection reset", 
            "broken pipe", "transport", "network", "reset by peer",
            "connection refused", "temporarily unavailable"
        ]
        
        if any(pattern in error_msg for pattern in retryable_patterns):
            return True
        
        return False
    
    def _check_timeout(self, operation_timeout: Optional[float] = None) -> Union[bool, float, None]:
        """
        Check if we're approaching job timeout.
        
        Args:
            operation_timeout: Timeout for the current operation in seconds
            
        Returns:
            False if timeout is approaching, adjusted timeout value, or original timeout
            
        Raises:
            ValueError: If job runtime has been exceeded
        """
        elapsed = time.time() - self.start_time
        timeout = self.config.get("JOB_MAX_RUNTIME", 86400)  # 24 hours default
        safety_margin = self.config.get("JOB_TIMEOUT_SAFETY_MARGIN", 1800)  # 30 min default
        
        # If we've exceeded the job timeout, raise an error
        if elapsed > timeout:
            raise ValueError(f"Job runtime exceeded: {elapsed:.1f}s elapsed, {timeout}s limit")
        
        remaining = timeout - elapsed
        if remaining <= safety_margin:
            logger.warning(f"Approaching job timeout: {elapsed:.1f}s elapsed, {remaining:.1f}s remaining")
            return False
                
        # Adjust operation timeout if needed
        if operation_timeout and operation_timeout > remaining - safety_margin:
            adjusted_timeout = max(1, remaining - safety_margin)
            logger.warning(f"Adjusting timeout from {operation_timeout}s to {adjusted_timeout:.1f}s")
            return adjusted_timeout
                
        # Return the original timeout 
        return operation_timeout
    
    @contextmanager
    def _get_client(self):
        """
        Context manager for acquiring and releasing a BigQuery client.
        
        Yields:
            bigquery.Client: A BigQuery client from the connection pool
            
        Raises:
            Exception: If the circuit breaker is open
        """
        # Check circuit breaker first
        if not self.circuit_breaker.allow_request():
            raise Exception("Circuit breaker is open, refusing operation")
        
        # Get client from connection pool
        client = self.connection_pool.get_client()
        try:
            yield client
        finally:
            # Always return the client to the pool
            self.connection_pool.release_client(client)
    
    def execute_query(self, query: str, params: Optional[List[Any]] = None, 
                     timeout: Optional[float] = None, return_job: bool = False,
                     job_config: Optional[bigquery.QueryJobConfig] = None):
        """
        Execute a BigQuery query with connection pooling, error handling, and profiling.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            timeout: Query timeout in seconds
            return_job: Whether to return the job object instead of results
            job_config: Custom job configuration
            
        Returns:
            Query results or job object
            
        Raises:
            Exception: For query execution failures
        """
        if timeout is None:
            timeout = self.config.get("QUERY_TIMEOUT", 3600)
        
        # Check if we're approaching job timeout
        adjusted_timeout = self._check_timeout(timeout)
        if adjusted_timeout is False:  # We're too close to timeout
            raise Exception("Job timeout approaching, refusing operation")
        elif adjusted_timeout is not None:
            timeout = adjusted_timeout
        
        # Create or use the provided job config
        if job_config is None:
            job_config = bigquery.QueryJobConfig()
        
        # Apply parameters if provided
        if params:
            job_config.query_parameters = params
        
        # Apply settings from config
        job_config.use_query_cache = self.config.get("USE_QUERY_CACHE", True)
        
        # Set priority
        priority_str = self.config.get("PRIORITY", "INTERACTIVE").upper()
        if hasattr(bigquery.QueryPriority, priority_str):
            job_config.priority = getattr(bigquery.QueryPriority, priority_str)
        
        # Set bytes billed limit if available
        max_bytes = self.config.get("MAXIMUM_BYTES_BILLED")
        if max_bytes:
            job_config.maximum_bytes_billed = max_bytes
        
        # Set SQL dialect
        job_config.use_legacy_sql = self.config.get("USE_LEGACY_SQL", False)
        
        # Start tracking execution time
        metrics.start_timer("query_execution")
        query_start_time = time.time()
        query_job = None
        
        with self._get_client() as client:
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
                    self.circuit_breaker.record_success()
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
                            self.circuit_breaker.record_failure()
                            logger.error(f"Exceeded maximum retries for transient error: {e}")
                            raise
                    else:
                        # Non-retryable errors
                        metrics.stop_timer("query_execution")
                        self.circuit_breaker.record_failure()
                        logger.error(f"Query execution failed with non-retryable error: {e}")
                        raise
    
    def create_dataset_if_not_exists(self, dataset_id: str) -> None:
        """
        Create a dataset if it doesn't exist, using connection pool.
        
        Args:
            dataset_id: Dataset ID to create
        """
        with self._get_client() as client:
            try:
                client.get_dataset(dataset_id)
                logger.info(f"Dataset {dataset_id} already exists")
            except gcp_exceptions.NotFound:
                dataset = bigquery.Dataset(dataset_id)
                dataset.location = self.config.get("LOCATION", "EU")
                client.create_dataset(dataset, exists_ok=True)
                logger.info(f"Created dataset: {dataset_id}")
            except Exception as e:
                logger.error(f"Error checking/creating dataset {dataset_id}: {e}")
                raise
    
    def create_table_with_schema(self, table_id: str, schema: List[bigquery.SchemaField], 
                               partitioning_field: Optional[str] = None, 
                               clustering_fields: Optional[List[str]] = None) -> None:
        """
        Create a table with the specified schema, using connection pool.
        
        Args:
            table_id: ID for the new table
            schema: List of SchemaField objects defining the table schema
            partitioning_field: Field to use for time partitioning
            clustering_fields: Fields to use for clustering
        """
        with self._get_client() as client:
            try:
                client.get_table(table_id)
                logger.info(f"Table {table_id} already exists")
                return
            except gcp_exceptions.NotFound:
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
            except Exception as e:
                logger.error(f"Error checking/creating table {table_id}: {e}")
                raise
    
    def create_table_from_query(self, table_id: str, query: str, params: Optional[List[Any]] = None):
        """
        Create a table from a query, applying configuration settings.
        
        Args:
            table_id: Destination table ID
            query: SQL query to use for table creation
            params: Query parameters
            
        Returns:
            Query results
        """
        # Create job config with destination table
        job_config = bigquery.QueryJobConfig(destination=table_id)
        
        # Apply write and create disposition from config
        create_disp = self.config.get("CREATE_DISPOSITION", "CREATE_IF_NEEDED")
        if hasattr(bigquery.CreateDisposition, create_disp):
            job_config.create_disposition = getattr(bigquery.CreateDisposition, create_disp)
        
        write_disp = self.config.get("WRITE_DISPOSITION", "WRITE_TRUNCATE")
        if hasattr(bigquery.WriteDisposition, write_disp):
            job_config.write_disposition = getattr(bigquery.WriteDisposition, write_disp)
        
        # Execute the query with the configured job_config
        return self.execute_query(query, params, job_config=job_config)
    
    def delete_table_rows(self, table_id: str, where_clause: str, params: Optional[List[Any]] = None):
        """
        Delete rows from a table based on a condition.
        
        Args:
            table_id: Table to delete from
            where_clause: WHERE clause for the DELETE statement (should use parameters)
            params: Query parameters for the WHERE clause
            
        Returns:
            Query results
            
        Example:
            delete_table_rows("project.dataset.table", "date = @date", 
                             [bigquery.ScalarQueryParameter("date", "DATE", some_date)])
        """
        query = f"DELETE FROM `{table_id}` WHERE {where_clause}"
        return self.execute_query(query, params)
    
    def delete_table(self, table_id: str, not_found_ok: bool = False):
        """
        Delete a BigQuery table using connection pool.
        
        Args:
            table_id: Table to delete
            not_found_ok: Whether to ignore "not found" errors
            
        Returns:
            Result from the delete operation
        """
        with self._get_client() as client:
            try:
                return client.delete_table(table_id, not_found_ok=not_found_ok)
            except Exception as e:
                if not_found_ok and isinstance(e, gcp_exceptions.NotFound):
                    logger.info(f"Table {table_id} doesn't exist, skipping deletion")
                    return None
                logger.error(f"Error deleting table {table_id}: {e}")
                raise

    def get_table(self, table_id: str):
        """
        Get a BigQuery table reference using connection pool.
        
        Args:
            table_id: Table to retrieve
            
        Returns:
            Table reference
        """
        with self._get_client() as client:
            try:
                return client.get_table(table_id)
            except Exception as e:
                logger.error(f"Error getting table {table_id}: {e}")
                raise

    def update_table(self, table: bigquery.Table, field_mask: Optional[List[str]] = None):
        """
        Update a BigQuery table using connection pool.
        
        Args:
            table: Table object to update
            field_mask: List of fields to update
            
        Returns:
            Updated table
        """
        with self._get_client() as client:
            try:
                return client.update_table(table, field_mask)
            except Exception as e:
                logger.error(f"Error updating table {table.table_id}: {e}")
                raise