# src/cleanup_utils.py

import datetime
from typing import List, Tuple, Optional
from google.cloud import bigquery

from src.logging_utils import logger
from src.bigquery_utils import BigQueryOperations

def cleanup_orphaned_temp_tables(bq_ops, max_age_hours: int = 24, 
                                max_wait_seconds: int = 60) -> Tuple[int, List[str]]:
    """
    Cleans up orphaned temporary tables in the destination dataset.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
        max_age_hours (int): Maximum age of temporary tables to keep in hours
        max_wait_seconds (int): Maximum seconds to wait for the cleanup operation
        
    Returns:
        Tuple containing count of deleted tables and list of deleted table IDs
    """
    logger.info(f"Starting cleanup of orphaned temporary tables older than {max_age_hours} hours")
    
    # Calculate cutoff time
    cutoff_time = datetime.datetime.utcnow() - datetime.timedelta(hours=max_age_hours)
    
    # Get list of tables in the dataset
    dataset_id = f"{bq_ops.config['DEST_PROJECT']}.{bq_ops.config['DEST_DATASET']}"
    
    # First check if dataset exists
    try:
        with bq_ops._get_client() as client:
            try:
                # Use a very short timeout just to check dataset existence
                client.get_dataset(dataset_id, timeout=10)
                logger.info(f"Dataset {dataset_id} exists, proceeding with cleanup")
            except Exception as e:
                logger.warning(f"Dataset {dataset_id} may not exist or is not accessible: {e}")
                return 0, []
    except Exception as e:
        logger.warning(f"Failed to check dataset existence: {e}")
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
            age_hours = (datetime.datetime.utcnow() - creation_time).total_seconds() / 3600
            
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

def schedule_temp_table_cleanup(bq_ops) -> None:
    """
    Creates a scheduled query to clean up temporary tables automatically.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
    """
    dataset_id = f"{bq_ops.config['DEST_PROJECT']}.{bq_ops.config['DEST_DATASET']}"
    schedule_query = f"""
    -- Delete temporary tables older than 24 hours
    DECLARE tables ARRAY<STRING>;
    
    -- Find temp tables older than 24 hours
    SET tables = (
      SELECT ARRAY_AGG(table_id)
      FROM `{dataset_id}.INFORMATION_SCHEMA.TABLES`
      WHERE table_id LIKE 'temp_data_%'
      AND creation_time < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    );
    
    -- Loop through and drop tables
    FOR table_record IN (
      SELECT table_name FROM UNNEST(tables) AS table_name
    )
    DO
      EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS `' || '{dataset_id}.' || table_record.table_name || '`';
    END FOR;
    """
    
    # Implementation would depend on how you want to schedule this query
    # Could use Cloud Scheduler with Cloud Functions, or BigQuery scheduled queries if available
    logger.info("Created scheduled cleanup query template - implement scheduling mechanism")
    return schedule_query