# src/cleanup_utils.py

from datetime import datetime, timezone, timedelta
import time
import random
from typing import List, Tuple, Optional

from src.logging_utils import logger


def cleanup_orphaned_temp_tables(bq_ops, max_age_hours: int = 3, 
                                max_wait_seconds: int = 60,
                                retry_count: int = 3,
                                force_cleanup: bool = False,
                                pattern: str = 'temp_data_%',
                                max_tables: int = 100) -> Tuple[int, List[str]]:
    """
    Cleans up orphaned temporary tables in the destination dataset.
    
    Args:
        bq_ops: BigQuery operations instance
        max_age_hours: Maximum age of temporary tables to keep in hours
        max_wait_seconds: Maximum seconds to wait for the cleanup operation
        retry_count: Number of times to retry deleting each table
        force_cleanup: If True, clean up all temp tables matching pattern regardless of age
        pattern: SQL LIKE pattern to match temporary table names
        max_tables: Maximum number of tables to clean up in one batch
        
    Returns:
        Tuple containing count of deleted tables and list of deleted table IDs
    """
    logger.info(f"Starting cleanup of orphaned temporary tables{' (forced)' if force_cleanup else ''} with pattern '{pattern}'")
    
    # Calculate cutoff time - using timezone-aware datetime
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    
    # Get dataset ID
    dataset_id = f"{bq_ops.config['DEST_PROJECT']}.{bq_ops.config['DEST_DATASET']}"
    
    # Run a simple check first to see if the dataset exists
    try:
        logger.info(f"Checking access to {dataset_id}")
        query = f"SELECT 1 FROM `{dataset_id}.__TABLES__` LIMIT 1"
        bq_ops.execute_query(query, timeout=5)  # Very short timeout
    except Exception as e:
        logger.warning(f"Dataset {dataset_id} may not exist or is not accessible: {e}")
        return 0, []
    
    # Query to find temporary tables
    where_clause = f"table_id LIKE '{pattern}'"
    if not force_cleanup:
        where_clause += f" AND creation_time < TIMESTAMP(\"{cutoff_time.isoformat()}\")"
    
    query = f"""
    SELECT
      table_id,
      creation_time,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), creation_time, HOUR) as age_hours
    FROM
      `{dataset_id}.INFORMATION_SCHEMA.TABLES`
    WHERE
      {where_clause}
    ORDER BY
      creation_time ASC
    LIMIT {max_tables}
    """
    
    try:
        # Use a shorter timeout specifically for this cleanup query
        logger.info(f"Running query to find orphaned tables (timeout: {max_wait_seconds}s)")
        results = bq_ops.execute_query(query, timeout=max_wait_seconds)
        temp_tables = [(row.table_id, row.creation_time, row.age_hours) for row in results]
        logger.info(f"Query completed, found {len(temp_tables)} potential tables to clean up")
        
        if not temp_tables:
            logger.info(f"No orphaned temporary tables found matching pattern '{pattern}'")
            return 0, []
            
        table_count = len(temp_tables)
        logger.info(f"Found {table_count} orphaned temporary tables to clean up")
        
        # Delete each table with retries
        deleted_tables = []
        for table_id, creation_time, age_hours in temp_tables:
            full_table_id = f"{dataset_id}.{table_id}"
            
            success = False
            for attempt in range(1, retry_count + 1):
                try:
                    logger.info(f"Deleting orphaned table {full_table_id} (age: {age_hours:.1f} hours) - attempt {attempt}/{retry_count}")
                    bq_ops.delete_table(full_table_id)
                    deleted_tables.append(full_table_id)
                    success = True
                    break
                except Exception as e:
                    if attempt < retry_count:
                        # Add jitter to retry delay
                        delay = min(5, 2 ** (attempt - 1)) * random.uniform(0.8, 1.2)
                        logger.warning(f"Failed to delete table {full_table_id} (attempt {attempt}/{retry_count}): {e}. Retrying in {delay:.1f}s")
                        time.sleep(delay)
                    else:
                        logger.warning(f"Failed to delete table {full_table_id} after {retry_count} attempts: {e}")
            
            if not success:
                logger.error(f"Could not delete orphaned table {full_table_id} after {retry_count} attempts")
        
        if deleted_tables:
            logger.info(f"Successfully deleted {len(deleted_tables)}/{table_count} orphaned temporary tables")
        else:
            logger.warning(f"Failed to delete any of the {table_count} orphaned temporary tables")
            
        return len(deleted_tables), deleted_tables
        
    except Exception as e:
        logger.error(f"Error during orphaned table cleanup: {e}")
        return 0, []


def get_active_temp_tables_count(bq_ops, max_wait_seconds: int = 15, 
                              pattern: str = 'temp_data_%',
                              timeout_ok: bool = True) -> Optional[int]:
    """
    Get a count of active temporary tables in the destination dataset.
    
    Args:
        bq_ops: BigQuery operations instance
        max_wait_seconds: Maximum seconds to wait for the query
        pattern: SQL LIKE pattern to match temporary table names
        timeout_ok: If True, return None on timeout without raising an exception
        
    Returns:
        Count of active temporary tables or None if query fails
    """
    # Get dataset ID
    dataset_id = f"{bq_ops.config['DEST_PROJECT']}.{bq_ops.config['DEST_DATASET']}"
    
    # Query to count temporary tables
    query = f"""
    SELECT
      COUNT(1) as table_count
    FROM
      `{dataset_id}.INFORMATION_SCHEMA.TABLES`
    WHERE
      table_id LIKE '{pattern}'
    """
    
    try:
        results = bq_ops.execute_query(query, timeout=max_wait_seconds)
        rows = list(results)
        if rows:
            count = rows[0].table_count
            logger.info(f"Found {count} temporary tables matching pattern '{pattern}'")
            return count
        return 0
    except Exception as e:
        if timeout_ok and "timeout" in str(e).lower():
            logger.warning(f"Timed out counting temporary tables: {e}")
            return None
        logger.warning(f"Error counting temporary tables: {e}")
        return None


def cleanup_job_temp_tables(bq_ops, job_id: str, 
                          max_wait_seconds: int = 30,
                          retry_count: int = 2,
                          max_tables: int = 50) -> Tuple[int, List[str]]:
    """
    Clean up temporary tables specifically for this job ID.
    
    Args:
        bq_ops: BigQuery operations instance
        job_id: Unique job identifier
        max_wait_seconds: Maximum seconds to wait for the cleanup
        retry_count: Number of retry attempts per table
        
    Returns:
        Tuple containing count of deleted tables and list of deleted table IDs
    """
    pattern = f"temp_data_%{job_id}%"
    logger.info(f"Cleaning up temporary tables for job ID: {job_id}")
    
    # Force cleanup regardless of age since these are from the current job
    return cleanup_orphaned_temp_tables(
        bq_ops, 
        max_age_hours=0,  # Age doesn't matter for this job's tables
        max_wait_seconds=max_wait_seconds,
        retry_count=retry_count,
        force_cleanup=True,
        pattern=pattern,
        max_tables=max_tables
    )