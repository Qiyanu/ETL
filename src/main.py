import os
import sys
import time
import uuid
import json
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

from src.config import load_config
from src.logging_utils import logger
from src.metrics import metrics
from src.bigquery_utils import BigQueryOperations
from src.etl_processors import (
    create_customer_data_table_if_not_exists,
    process_month_range
)
from src.shutdown_utils import is_shutdown_requested

def main():
    """
    Main entrypoint for the ETL job with comprehensive error handling and observability.
    """
    # Start timer and generate request ID
    start_time = time.time()
    job_id = os.environ.get("CLOUD_RUN_JOB_EXECUTION", str(uuid.uuid4())[:8])
    request_id = f"job_{job_id}"
    
    # Set correlation ID for logging
    logger.correlation_id = request_id
    logger.service = os.environ.get("K_SERVICE", "customer-data-job")
    
    # Load configuration
    try:
        config_file = os.environ.get("CONFIG_FILE", "config.yaml")
        config = load_config(config_file)
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)
    
    # Initialize BigQuery operations
    try:
        bq_ops = BigQueryOperations(config)
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery operations: {e}")
        sys.exit(1)
    
    # Log start of process
    logger.info(f"Starting ETL job with ID: {job_id}")
    
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
        
        # Prepare job summary
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