import os
import sys
import time
import uuid
import json
from datetime import date, datetime
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
from src.cleanup_utils import cleanup_orphaned_temp_tables

def main():
    """
    Main entrypoint for the ETL job with comprehensive error handling and observability.
    """
    # Start timer and generate request ID
    start_time = time.time()
    job_id = os.environ.get("CLOUD_RUN_JOB_EXECUTION", os.environ.get("JOB_ID", str(uuid.uuid4())[:8]))
    request_id = f"job_{job_id}"
    
    # Set correlation ID for logging
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
        
        # Initialize the shutdown handler with the loaded configuration
        initialize_shutdown_handler(config.get("_NESTED_CONFIG", {}))
        
        # Initialize metrics with proper size limits
        metrics_limit = config.get("METRICS_MAX_HISTORY", 1000)
        from src.metrics import Metrics
        global metrics
        metrics = Metrics(max_history_per_metric=metrics_limit)
        
        # Initialize BigQuery operations
        try:
            bq_ops = BigQueryOperations(config)
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery operations: {e}")
            sys.exit(1)
        
        # Log start of process
        logger.info(f"Starting ETL job with ID: {job_id}")
        
        try:
            # First, clean up any orphaned temporary tables from previous runs
            try:
                # Configure cleanup age from config or use default (24 hours)
                cleanup_age_hours = config.get("TEMP_TABLE_CLEANUP_AGE_HOURS", 1)
                cleanup_timeout_seconds = config.get("CLEANUP_TIMEOUT_SECONDS", 60)
                logger.info(f"Starting orphaned table cleanup with {cleanup_timeout_seconds}s timeout")
                import threading
                cleanup_done = threading.Event()
                cleanup_result = [0, []]
                def run_cleanup():
                    try:
                        result = cleanup_orphaned_temp_tables(
                            bq_ops, 
                            max_age_hours=cleanup_age_hours,
                            max_wait_seconds=cleanup_timeout_seconds
                        )
                        cleanup_result[0] = result[0]
                        cleanup_result[1] = result[1]
                    except Exception as e:
                        logger.warning(f"Cleanup thread encountered an error: {e}")
                    finally:
                        cleanup_done.set()
                
                # Start cleanup in a separate thread
                cleanup_thread = threading.Thread(target=run_cleanup)
                cleanup_thread.daemon = True
                cleanup_thread.start()
                
                # Wait for cleanup with timeout
                if not cleanup_done.wait(timeout=cleanup_timeout_seconds + 10):  # Add a small buffer
                    logger.warning("Orphaned table cleanup timed out, continuing with processing")
                else:
                    # Log results
                    deleted_count = cleanup_result[0]
                    deleted_tables = cleanup_result[1]
                    
                    if deleted_count > 0:
                        logger.info(f"Cleaned up {deleted_count} orphaned temporary tables")
                        logger.debug(f"Deleted tables: {', '.join(deleted_tables)}")
                    else:
                        logger.info("No orphaned temporary tables found to clean up")
            except Exception as cleanup_error:
                logger.warning(f"Orphaned table cleanup failed: {cleanup_error}")
            
            # Determine date range to process
            if config.get("JOB_START_MONTH") and config.get("JOB_END_MONTH"):
                # Use explicitly configured start and end months
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
            
            # Check for enabled countries from the new config structure
            enabled_countries = config.get("ALLOWED_COUNTRIES") or []
            
            # Validate that we have query generators for all enabled countries
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
            
            # Ensure destination table exists with optimal schema
            create_customer_data_table_if_not_exists(bq_ops)
            
            # Process the months
            successful_months, failed_months = process_month_range(
                bq_ops, 
                start_month, 
                end_month, 
                config.get("JOB_PARALLEL", True), 
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
                "countries": enabled_countries,
                "successful_months": successful_months,
                "failed_months": failed_months,
                "processing_time_seconds": round(elapsed, 2),
                "metrics": metrics_summary
            }
            
            # Log completion
            logger.info(f"Processing completed in {elapsed:.2f} seconds: {successful_months} months successful, {failed_months} months failed")
            logger.info(f"Job summary: {json.dumps(summary)}")
            
            # Run a final cleanup after processing to catch any tables that were missed
            try:
                # Use a shorter timeout for orphaned tables created during this run
                cleanup_age_hours = config.get("TEMP_TABLE_CLEANUP_AGE_HOURS", 24)
                recent_cleanup_hours = max(4, cleanup_age_hours/6)  # Default to 4 hours minimum
                deleted_count, _ = cleanup_orphaned_temp_tables(bq_ops, max_age_hours=recent_cleanup_hours)
                if deleted_count > 0:
                    logger.info(f"Cleaned up {deleted_count} temporary tables from current run")
            except Exception as cleanup_error:
                logger.warning(f"Final table cleanup failed: {cleanup_error}")
            
            # Clean up resources with graceful shutdown
            shutdown_timeout = config.get("CONNECTION_POOL_SHUTDOWN_TIMEOUT", 60)
            logger.info(f"Starting connection pool cleanup with {shutdown_timeout}s timeout")
            
            if bq_ops and hasattr(bq_ops, 'connection_pool'):
                bq_ops.connection_pool.close_all(timeout=shutdown_timeout)
            
            # Exit with appropriate code
            if failed_months > 0:
                sys.exit(1)  # Indicate partial failure
            else:
                sys.exit(0)  # Success
        
        except Exception as exc:
            # Calculate elapsed time even for failures
            elapsed = time.time() - start_time
            
            # Log the error with traceback
            logger.error(f"Processing error: {exc}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Prepare error summary
            metrics_summary = metrics.get_summary()
            summary = {
                "status": "error",
                "job_id": job_id,
                "error": str(exc),
                "processing_time_seconds": round(elapsed, 2),
                "metrics": metrics_summary
            }
            
            # Try to clean up any temporary tables even if the job failed
            try:
                if bq_ops:
                    # Use a shorter timeout to catch tables from this run
                    deleted_count, _ = cleanup_orphaned_temp_tables(bq_ops, max_age_hours=6)
                    if deleted_count > 0:
                        logger.info(f"Cleaned up {deleted_count} temporary tables after job failure")
            except Exception as cleanup_error:
                logger.warning(f"Cleanup after failure failed: {cleanup_error}")
            
            # Clean up resources if available with graceful shutdown
            if bq_ops and hasattr(bq_ops, 'connection_pool'):
                shutdown_timeout = config.get("CONNECTION_POOL_SHUTDOWN_TIMEOUT", 60)
                logger.info(f"Performing connection pool cleanup on error with {shutdown_timeout}s timeout")
                bq_ops.connection_pool.close_all(timeout=shutdown_timeout)
            
            logger.error(f"Job failed: {json.dumps(summary)}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt received")
        # Request shutdown to allow graceful termination
        request_shutdown()
        
        # Try to clean up any temporary tables
        try:
            if bq_ops:
                deleted_count, _ = cleanup_orphaned_temp_tables(bq_ops, max_age_hours=6)
                if deleted_count > 0:
                    logger.info(f"Cleaned up {deleted_count} temporary tables after interruption")
        except Exception:
            pass
        
        # Minimal cleanup in case of keyboard interrupt
        if bq_ops and hasattr(bq_ops, 'connection_pool'):
            logger.info("Performing connection pool cleanup on interrupt")
            try:
                bq_ops.connection_pool.close_all(timeout=30)
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")
        
        sys.exit(130)  # Standard exit code for keyboard interrupt
    
    except Exception as e:
        # Handle unexpected top-level exceptions
        logger.critical(f"Critical error: {e}")
        logger.critical(f"Traceback: {traceback.format_exc()}")
        
        # Try to clean up temporary tables
        try:
            if bq_ops:
                cleanup_orphaned_temp_tables(bq_ops, max_age_hours=6)
        except Exception:
            pass
        
        # Try to clean up if possible
        if bq_ops and hasattr(bq_ops, 'connection_pool'):
            try:
                bq_ops.connection_pool.close_all(timeout=10)
            except Exception as cleanup_error:
                logger.error(f"Error during emergency cleanup: {cleanup_error}")
                
        sys.exit(1)

if __name__ == "__main__":
    main()