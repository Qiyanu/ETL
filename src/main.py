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
        
        # Process the date range
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
        logger.info(f"Processing completed in {elapsed:.2f}s: {successful_months} months successful, {failed_months} months failed")
        logger.info(f"Job summary: {json.dumps(summary)}")
        
        
        
        # Exit with appropriate code
        if failed_months > 0:
            sys.exit(1)  # Partial failure
        else:
            sys.exit(0)  # Success
            
    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt received")
        # Request shutdown to allow graceful termination
        request_shutdown()
        sys.exit(130)
        
    except Exception as e:
        # Calculate elapsed time even for failures
        elapsed = time.time() - start_time
        
        # Log the error with traceback
        logger.error(f"Critical error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
            
        sys.exit(1)

if __name__ == "__main__":
    main()