import os
import sys
import time
import uuid
import json
from datetime import date, datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
import traceback

from src.config import load_config, get_config_path
from src.logging_utils import logger
from src.metrics import metrics
from src.bigquery_utils import BigQueryOperations
from src.etl_processors import (
    create_customer_data_table_if_not_exists,
    process_month_range
)
from src.shutdown_utils import is_shutdown_requested, request_shutdown, initialize_shutdown_handler
from src.cleanup_utils import cleanup_orphaned_temp_tables, cleanup_job_temp_tables

def main():
    """
    Main entrypoint for the ETL job.
    """
    # Basic initialization
    start_time = time.time()
    job_id = os.environ.get("CLOUD_RUN_JOB_EXECUTION", os.environ.get("JOB_ID", str(uuid.uuid4())[:8]))
    request_id = f"job_{job_id}"
    
    # Set correlation ID and service name
    logger.set_correlation_id(request_id)
    logger.set_service_name(os.environ.get("K_SERVICE", "customer-data-job"))
    
    # Initialize BigQuery operations
    bq_ops = None
    
    try:
        # Load configuration
        try:
            config_file = get_config_path()
            print(f"Loading configuration from: {config_file}")
            config = load_config(config_file)
            print("CONFIG LOADED SUCCESSFULLY")
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            sys.exit(1)
        
        # Initialize the shutdown handler
        initialize_shutdown_handler(config.get("_NESTED_CONFIG", {}))
        
        # Initialize BigQuery operations
        try:
            bq_ops = BigQueryOperations(config)
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery operations: {e}")
            sys.exit(1)
        
        logger.info(f"Starting ETL job with ID: {job_id}")
        
        # Clean up orphaned temporary tables at the start (make it optional)
        if config.get("SKIP_INITIAL_CLEANUP", "false").lower() != "true":
            try:
                cleanup_age_hours = config.get("TEMP_TABLE_CLEANUP_AGE_HOURS", 24)
                logger.info("Starting initial cleanup with fail-safe timeout")
                
                # Use a separate thread with timeout to prevent hanging
                import threading
                from functools import partial
                
                cleanup_done = threading.Event()
                cleanup_result = [0, []]  # [count, deleted_tables]
                
                def do_cleanup():
                    try:
                        count, tables = cleanup_orphaned_temp_tables(
                            bq_ops, 
                            max_age_hours=cleanup_age_hours,
                            max_wait_seconds=15,  # Even shorter timeout
                            max_tables=20,        # Process fewer tables
                            retry_count=1
                        )
                        cleanup_result[0] = count
                        cleanup_result[1] = tables
                    except Exception as e:
                        logger.warning(f"Cleanup thread encountered an error: {e}")
                    finally:
                        cleanup_done.set()
                
                # Start cleanup in background thread
                cleanup_thread = threading.Thread(target=do_cleanup)
                cleanup_thread.daemon = True  # Make it a daemon so it doesn't block process exit
                cleanup_thread.start()
                
                # Wait with timeout
                cleanup_timeout = 25  # seconds
                if not cleanup_done.wait(timeout=cleanup_timeout):
                    logger.warning(f"Initial cleanup operation timed out after {cleanup_timeout}s, continuing with the job")
                else:
                    if cleanup_result[0] > 0:
                        logger.info(f"Cleaned up {cleanup_result[0]} orphaned temporary tables at job start")
            except Exception as e:
                logger.warning(f"Failed to run initial cleanup: {e}")
                logger.info("Continuing with the job despite cleanup issues")
        else:
            logger.info("Initial cleanup skipped as per configuration")

        
        # Determine date range to process
        if config.get("JOB_START_MONTH") and config.get("JOB_END_MONTH"):
            # Use explicitly configured dates
            from datetime import datetime
            start_month = datetime.strptime(config["JOB_START_MONTH"], '%Y-%m-%d').date()
            end_month = datetime.strptime(config["JOB_END_MONTH"], '%Y-%m-%d').date()
        elif config.get("JOB_LAST_N_MONTHS"):
            # Process the last N months
            last_n_months = int(config["JOB_LAST_N_MONTHS"])
            end_month = date.today().replace(day=1) - relativedelta(days=1)
            end_month = end_month.replace(day=1)  # First day of last month
            start_month = end_month - relativedelta(months=last_n_months-1)
        else:
            # Default to the last complete month
            start_month = date.today().replace(day=1) - relativedelta(days=1)
            start_month = start_month.replace(day=1)  # First day of last month
            end_month = start_month
        
        # Validate date range
        if start_month > end_month:
            raise ValueError(f"Invalid date range: start month {start_month} is after end month {end_month}")
                
        if (end_month.year - start_month.year) * 12 + (end_month.month - start_month.month) > 36:
            raise ValueError(f"Date range too large: {start_month} to {end_month} spans more than 36 months")
        
        # Check for enabled countries
        enabled_countries = config.get("ALLOWED_COUNTRIES") or []
        
        # Validate that we have query generators for supported countries
        from src.query_templates import QUERY_GENERATORS
        unsupported_countries = [country for country in enabled_countries if country not in QUERY_GENERATORS]
        if unsupported_countries:
            raise ValueError(
                f"Unsupported countries in configuration: {', '.join(unsupported_countries)}. "
                f"Available countries: {', '.join(QUERY_GENERATORS.keys())}"
            )
        
        # Log parameters
        logger.info(f"Processing from {start_month} to {end_month}, parallel={config.get('JOB_PARALLEL', True)}")
        logger.info(f"Countries to process: {', '.join(enabled_countries)}")
        
        # Ensure destination table exists
        create_customer_data_table_if_not_exists(bq_ops)
        
        # Process the date range with timeouts
        successful_months, failed_months = process_month_range(
            bq_ops, 
            start_month, 
            end_month, 
            config.get("JOB_PARALLEL", True), 
            request_id,
            max_query_time=config.get("QUERY_TIMEOUT", 600),
            worker_timeout=config.get("WORKER_TIMEOUT", 1800)
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
            "countries": enabled_countries,
            "successful_months": successful_months,
            "failed_months": failed_months,
            "processing_time_seconds": round(elapsed, 2),
            "metrics": metrics_summary
        }
        
        # Log completion
        logger.info(f"Processing completed in {elapsed:.2f}s: {successful_months} months successful, {failed_months} months failed")
        logger.info(f"Job summary: {json.dumps(summary)}")
        
        # Clean up this job's temporary tables and other orphaned tables at the end
        try:
            # Make end-cleanup safer with shorter timeouts
            # First, clean up this job's temporary tables
            logger.info("Starting end-of-job cleanup with safe parameters")
            job_tables_deleted, _ = cleanup_job_temp_tables(
                bq_ops, 
                request_id,
                max_wait_seconds=15,
                retry_count=1,
                max_tables=20
            )
            if job_tables_deleted > 0:
                logger.info(f"Cleaned up {job_tables_deleted} temporary tables from this job")
            
            # Skip additional cleanup to avoid hanging on job completion
            # orphaned_tables_deleted, _ = cleanup_orphaned_temp_tables(
            #     bq_ops, 
            #     max_age_hours=cleanup_age_hours,
            #     max_wait_seconds=15,
            #     max_tables=20,
            #     retry_count=1
            # )
            # if orphaned_tables_deleted > 0:
            #     logger.info(f"Cleaned up {orphaned_tables_deleted} other orphaned temporary tables at job end")
            
            # Update metrics
            metrics.record_value("tables_cleaned_up", job_tables_deleted)
        except Exception as e:
            logger.warning(f"Failed to clean up temporary tables at job end: {e}")
            logger.info("Continuing despite cleanup issues")
        
        # Exit with appropriate code
        if failed_months > 0:
            sys.exit(1)  # Partial failure
        else:
            sys.exit(0)  # Success
            
    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt received")
        # Request shutdown to allow graceful termination
        request_shutdown()
        
        # Try to clean up this job's temporary tables on keyboard interrupt
        if bq_ops:
            try:
                logger.info("Attempting to clean up temporary tables after keyboard interrupt")
                deleted_count, _ = cleanup_job_temp_tables(
                    bq_ops, 
                    request_id,
                    max_wait_seconds=10,
                    retry_count=1,
                    max_tables=10
                )
                if deleted_count > 0:
                    logger.info(f"Cleaned up {deleted_count} temporary tables after keyboard interrupt")
            except Exception as e:
                logger.warning(f"Failed to clean up temporary tables after keyboard interrupt: {e}")
                
        sys.exit(130)
        
    except Exception as e:
        # Calculate elapsed time even for failures
        elapsed = time.time() - start_time
        
        # Log the error with traceback
        logger.error(f"Critical error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        # Try to clean up this job's temporary tables on crash
        if bq_ops:
            try:
                logger.info("Attempting to clean up temporary tables after job failure")
                deleted_count, _ = cleanup_job_temp_tables(
                    bq_ops, 
                    request_id,
                    max_wait_seconds=10,
                    retry_count=1,
                    max_tables=10
                )
                if deleted_count > 0:
                    logger.info(f"Cleaned up {deleted_count} temporary tables after job failure")
            except Exception as cleanup_error:
                logger.warning(f"Failed to clean up temporary tables after job failure: {cleanup_error}")
            
        sys.exit(1)

if __name__ == "__main__":
    main()