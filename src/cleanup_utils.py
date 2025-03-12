# src/cleanup_utils.py

from datetime import datetime, timezone, timedelta
from typing import List, Tuple

from src.logging_utils import logger


def cleanup_orphaned_temp_tables(bq_ops, max_age_hours: int = 3, 
                                max_wait_seconds: int = 60) -> Tuple[int, List[str]]:
    """
    Cleans up orphaned temporary tables in the destination dataset.
    
    Args:
        bq_ops: BigQuery operations instance
        max_age_hours: Maximum age of temporary tables to keep in hours
        max_wait_seconds: Maximum seconds to wait for the cleanup operation
        
    Returns:
        Tuple containing count of deleted tables and list of deleted table IDs
    """
    logger.info(f"Starting cleanup of orphaned temporary tables older than {max_age_hours} hours")
    
    # Calculate cutoff time - FIXED to use timezone-aware datetime
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    
    # Get dataset ID
    dataset_id = f"{bq_ops.config['DEST_PROJECT']}.{bq_ops.config['DEST_DATASET']}"
    
    # Check if dataset exists
    try:
        bq_ops.get_table(f"{dataset_id}.__TABLES__")
    except Exception as e:
        logger.warning(f"Dataset {dataset_id} may not exist or is not accessible: {e}")
        return 0, []
    
    # Query to find temporary tables older than the cutoff
    query = f"""
    SELECT
      table_id,
      creation_time
    FROM
      `{dataset_id}.INFORMATION_SCHEMA.TABLES`
    WHERE
      table_id LIKE 'temp_data_%'
      AND creation_time < TIMESTAMP("{cutoff_time.isoformat()}")
    """
    
    try:
        # Use a shorter timeout specifically for this cleanup query
        results = bq_ops.execute_query(query, timeout=max_wait_seconds)
        temp_tables = [(row.table_id, row.creation_time) for row in results]
        
        if not temp_tables:
            logger.info("No orphaned temporary tables found")
            return 0, []
            
        logger.info(f"Found {len(temp_tables)} orphaned temporary tables to clean up")
        
        # Delete each table
        deleted_tables = []
        for table_id, creation_time in temp_tables:
            full_table_id = f"{dataset_id}.{table_id}"
            # Also fixed age calculation to use timezone-aware datetime
            age_hours = (datetime.now(timezone.utc) - creation_time).total_seconds() / 3600
            
            try:
                logger.info(f"Deleting orphaned table {full_table_id} (age: {age_hours:.1f} hours)")
                bq_ops.delete_table(full_table_id)
                deleted_tables.append(full_table_id)
            except Exception as e:
                logger.warning(f"Failed to delete table {full_table_id}: {e}")
        
        logger.info(f"Successfully deleted {len(deleted_tables)} orphaned temporary tables")
        return len(deleted_tables), deleted_tables
        
    except Exception as e:
        logger.error(f"Error during orphaned table cleanup: {e}")
        return 0, []