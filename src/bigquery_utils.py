import time
import random
from typing import Dict, Any, Optional, Union, List

from google.cloud import bigquery
from google.api_core import exceptions as gcp_exceptions

from src.logging_utils import logger
from src.metrics import metrics

class BigQueryOperations:
    """Simplified BigQuery operations with basic retry capabilities."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize with configuration.
        
        Args:
            config: Configuration dictionary with BigQuery settings
        """
        # Validate required configuration
        self._validate_config(config)
        
        self.config = config
        self.client = bigquery.Client(location=config.get("LOCATION", "EU"))
        
        # Define retryable exceptions
        self.retryable_exceptions = (
            gcp_exceptions.ServiceUnavailable,
            gcp_exceptions.ServerError,
            gcp_exceptions.InternalServerError,
            gcp_exceptions.GatewayTimeout,
            gcp_exceptions.DeadlineExceeded,
            gcp_exceptions.ResourceExhausted,
            gcp_exceptions.RetryError,
            ConnectionError,
            TimeoutError
        )
        
        logger.info(f"BigQuery operations initialized")
        
        # Added for backward compatibility with the original interface
        self.connection_pool = self
    
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
            "MAX_WORKERS",
            "SOURCE_LINE_TABLE",
            "SOURCE_HEADER_TABLE",
            "SOURCE_CARD_TABLE", 
            "SOURCE_SITE_TABLE",
            "SOURCE_STORE_TABLE",
            "ALLOWED_COUNTRIES"
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
            "broken pipe", "transport", "network", "reset by peer"
        ]
        
        if any(pattern in error_msg for pattern in retryable_patterns):
            return True
        
        return False
    
    def execute_query(self, query: str, params: Optional[List[Any]] = None, 
                      timeout: Optional[float] = None, return_job: bool = False,
                      job_config: Optional[bigquery.QueryJobConfig] = None):
        """
        Execute a BigQuery query with retry logic.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            timeout: Query timeout in seconds
            return_job: Whether to return the job object instead of results
            job_config: Custom job configuration
            
        Returns:
            Query results or job object
        """
        if timeout is None:
            timeout = self.config.get("QUERY_TIMEOUT", 3600)
        
        # Create or use the provided job config
        if job_config is None:
            job_config = bigquery.QueryJobConfig()
        
        # Apply parameters if provided
        if params:
            job_config.query_parameters = params
        
        # Start tracking execution time
        metrics.start_timer("query_execution")
        query_start_time = time.time()
        
        # Execute with retries
        retry_count = 0
        max_retries = self.config.get("MAX_RETRY_ATTEMPTS", 3)
        base_delay = 1.0  # 1 second initial delay
        
        while True:
            try:
                # Execute the query
                query_job = self.client.query(query, job_config=job_config)
                result = query_job.result(timeout=timeout)
                
                # Record metrics
                execution_time = time.time() - query_start_time
                metrics.stop_timer("query_execution")
                
                # Record bytes processed if available
                if hasattr(query_job, 'total_bytes_processed'):
                    metrics.record_value("bytes_processed", query_job.total_bytes_processed or 0)
                
                # Return the appropriate result
                if return_job:
                    return query_job
                return result
                
            except Exception as e:
                # Check if this exception is retryable
                if self._is_retryable_exception(e) and retry_count < max_retries:
                    retry_count += 1
                    metrics.increment_counter("bigquery_transient_errors")
                    
                    # Exponential backoff with jitter
                    delay = min(60, base_delay * (2 ** (retry_count - 1)))
                    delay = delay * random.uniform(0.8, 1.2)  # Add jitter
                    
                    logger.warning(f"Transient error (attempt {retry_count}/{max_retries}): {e}. Retrying in {delay:.1f}s")
                    time.sleep(delay)
                else:
                    metrics.stop_timer("query_execution")
                    logger.error(f"Query execution failed: {e}")
                    raise
    
    def create_dataset_if_not_exists(self, dataset_id: str) -> None:
        """
        Create a dataset if it doesn't exist.
        
        Args:
            dataset_id: Dataset ID to create
        """
        try:
            self.client.get_dataset(dataset_id)
            logger.info(f"Dataset {dataset_id} already exists")
        except gcp_exceptions.NotFound:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = self.config.get("LOCATION", "EU")
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created dataset: {dataset_id}")
        except Exception as e:
            logger.error(f"Error checking/creating dataset {dataset_id}: {e}")
            raise
    
    def create_table_with_schema(self, table_id: str, schema: List[bigquery.SchemaField], 
                               partitioning_field: Optional[str] = None, 
                               clustering_fields: Optional[List[str]] = None) -> None:
        """
        Create a table with the specified schema.
        
        Args:
            table_id: ID for the new table
            schema: List of SchemaField objects defining the table schema
            partitioning_field: Field to use for time partitioning
            clustering_fields: Fields to use for clustering
        """
        try:
            self.client.get_table(table_id)
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
            
            self.client.create_table(table, exists_ok=True)
            logger.info(f"Created table: {table_id}")
        except Exception as e:
            logger.error(f"Error checking/creating table {table_id}: {e}")
            raise
    
    def create_table_from_query(self, table_id: str, query: str, params: Optional[List[Any]] = None):
        """
        Create a table from a query.
        
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
            where_clause: WHERE clause for the DELETE statement
            params: Query parameters for the WHERE clause
            
        Returns:
            Query results
        """
        query = f"DELETE FROM `{table_id}` WHERE {where_clause}"
        return self.execute_query(query, params)
    
    def delete_table(self, table_id: str, not_found_ok: bool = False):
        """
        Delete a BigQuery table.
        
        Args:
            table_id: Table to delete
            not_found_ok: Whether to ignore "not found" errors
            
        Returns:
            Result from the delete operation
        """
        try:
            return self.client.delete_table(table_id, not_found_ok=not_found_ok)
        except Exception as e:
            if not_found_ok and isinstance(e, gcp_exceptions.NotFound):
                logger.info(f"Table {table_id} doesn't exist, skipping deletion")
                return None
            logger.error(f"Error deleting table {table_id}: {e}")
            raise

    def get_table(self, table_id: str):
        """
        Get a BigQuery table reference.
        
        Args:
            table_id: Table to retrieve
            
        Returns:
            Table reference
        """
        try:
            return self.client.get_table(table_id)
        except Exception as e:
            logger.error(f"Error getting table {table_id}: {e}")
            raise

    def update_table(self, table: bigquery.Table, field_mask: Optional[List[str]] = None):
        """
        Update a BigQuery table.
        
        Args:
            table: Table object to update
            field_mask: List of fields to update
            
        Returns:
            Updated table
        """
        try:
            return self.client.update_table(table, field_mask)
        except Exception as e:
            logger.error(f"Error updating table {table.table_id}: {e}")
            raise
            
    # For backward compatibility with the original connection pool interface
    def close_all(self, timeout=None):
        """Close BigQuery client (simplified version for compatibility)."""
        logger.info("Closing BigQuery client")
        try:
            if hasattr(self.client, 'close'):
                self.client.close()
        except Exception as e:
            logger.warning(f"Error closing BigQuery client: {e}")