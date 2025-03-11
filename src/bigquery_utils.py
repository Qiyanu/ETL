import time
import threading
import random
import uuid
from typing import Dict, Any, Optional, Union, List

import requests
import psutil

from google.cloud import bigquery
from google.api_core import exceptions as gcp_exceptions

from src.logging_utils import logger
from src.metrics import metrics

class CircuitBreaker:
    """
    Implements a simple circuit breaker pattern for resilient operations.
    
    Tracks failures and prevents repeated attempts when a service is likely unavailable.
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
    
    def record_success(self) -> None:
        """Record a successful operation and reset failure tracking."""
        with self.lock:
            if self.state == "HALF-OPEN":
                self.state = "CLOSED"
                logger.info("Circuit breaker reset to CLOSED state")
            self.failure_count = 0
    
    def record_failure(self) -> None:
        """Record a failed operation and potentially open the circuit."""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.state == "CLOSED" and self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
                metrics.increment_counter("circuit_breaker_open_count")
    
    def allow_request(self) -> bool:
        """
        Determine if a request should be allowed based on circuit state.
        
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
                    logger.info("Circuit breaker transitioning to HALF-OPEN state")
                    return True
                return False
            
            # In HALF-OPEN state, allow request to test if system has recovered
            return True

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