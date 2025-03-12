import time
import uuid
import threading
from datetime import date, datetime, timezone
from typing import Dict, List, Tuple, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError

from google.cloud import bigquery
from dateutil.relativedelta import relativedelta

from src.logging_utils import logger
from src.metrics import metrics
from src.query_templates import QUERY_GENERATORS
from src.shutdown_utils import is_shutdown_requested

def create_customer_data_table_if_not_exists(bq_ops):
    """
    Creates destination table with optimized schema.
    
    Args:
        bq_ops: BigQuery operations instance
    """
    # Define the schema with robust typing
    schema = [
        bigquery.SchemaField("Mois", "DATE", description="Month of data"),
        bigquery.SchemaField("Country", "STRING", description="Country code"),
        bigquery.SchemaField("Enseigne", "STRING", description="Store chain type"),
        bigquery.SchemaField("CA_Tous_Clients", "FLOAT64", description="Revenue all clients (€)"),
        bigquery.SchemaField("CA_Tous_Clients_local", "FLOAT64", description="Revenue all clients (local currency)"),
        bigquery.SchemaField("CA_Porteurs", "FLOAT64", description="Revenue cardholders (€)"),
        bigquery.SchemaField("CA_Porteurs_local", "FLOAT64", description="Revenue cardholders (local currency)"),
        bigquery.SchemaField("Taux_CA_encarte", "FLOAT64", description="Cardholder revenue percentage"),
        bigquery.SchemaField("Nb_transactions_Tous_Clients", "INT64", description="Transaction count all clients"),
        bigquery.SchemaField("Nb_transactions_porteurs", "INT64", description="Transaction count cardholders"),
        bigquery.SchemaField("Taux_transactions_encartees", "FLOAT64", description="Cardholder transaction percentage"),
        bigquery.SchemaField("Nb_foyers", "INT64", description="Number of households"),
        bigquery.SchemaField("Frequence_porteurs", "FLOAT64", description="Cardholder frequency"),
        bigquery.SchemaField("Panier_moyen_Porteurs", "FLOAT64", description="Average basket cardholders"),
        bigquery.SchemaField("nb_articles", "INT64", description="Number of articles"),
        bigquery.SchemaField("nb_articles_porteur", "INT64", description="Number of articles cardholders"),
        bigquery.SchemaField("Families", "INT64", description="Number of families"),
        bigquery.SchemaField("Seniors", "INT64", description="Number of seniors"),
        bigquery.SchemaField("CA_promo", "FLOAT64", description="Promotional revenue (€)"),
        bigquery.SchemaField("CA_promo_local", "FLOAT64", description="Promotional revenue (local currency)"),
        bigquery.SchemaField("nb_foyers_constants", "INT64", description="Stable households"),
        bigquery.SchemaField("nb_foyers_gagnes", "INT64", description="New households"),
        bigquery.SchemaField("nb_foyers_perdus", "INT64", description="Lost households")
    ]
    
    # Ensure dataset exists
    dataset_id = f"{bq_ops.config['DEST_PROJECT']}.{bq_ops.config['DEST_DATASET']}"
    bq_ops.create_dataset_if_not_exists(dataset_id)
    
    # Create table with partitioning and clustering
    bq_ops.create_table_with_schema(
        bq_ops.config["DEST_TABLE"],
        schema,
        partitioning_field="Mois",
        clustering_fields=["Country", "Enseigne"]
    )

def create_temp_table_for_month(bq_ops, the_month, temp_table_id, country, request_id=None, 
                               max_query_time=600):
    """
    Creates a temporary table with aggregated data for the month and country.
    
    Args:
        bq_ops: BigQuery operations instance
        the_month: Month to process
        temp_table_id: ID for the temporary table
        country: Country code to process
        request_id: Unique request identifier
        max_query_time: Maximum time in seconds to wait for the query
    
    Returns:
        Number of rows in the created temporary table
    """
    request_id = request_id or str(uuid.uuid4())[:8]
    logger.set_correlation_id(f"temp_{country}_{the_month.strftime('%Y%m')}_{request_id}")
    logger.info(f"Creating temp table for {country}: {temp_table_id}")
    
    # Start timer
    metrics.start_timer("create_temp_table")
    
    try:
        # Get the appropriate query generator for this country
        query_generator = QUERY_GENERATORS.get(country)
        if not query_generator:
            raise ValueError(f"Unsupported country: {country}. Available: {', '.join(QUERY_GENERATORS.keys())}")
        
        # Get the query and parameters
        result = query_generator(bq_ops, temp_table_id)
        
        # Support both return formats (query+params or just query)
        if isinstance(result, tuple) and len(result) == 2:
            create_temp_table_query, params = result
        else:
            create_temp_table_query = result
            params = [bigquery.ScalarQueryParameter("theMonth", "DATE", the_month)]
        
        # Set theMonth parameter value if needed
        for param in params:
            if param.name == "theMonth" and param.value is None:
                param._value = the_month
        
        # Execute query to create temporary table
        logger.info(f"Executing query to create temp table with timeout {max_query_time}s")
        bq_ops.execute_query(create_temp_table_query, params, timeout=max_query_time)
        
        # Set expiration if not already set
        try:
            table_ref = bq_ops.get_table(temp_table_id)
            if not table_ref.expires:
                expiration_hours = bq_ops.config.get("TEMP_TABLE_EXPIRATION_HOURS", 24)
                # Fix: Use timezone-aware datetime
                table_ref.expires = datetime.now(timezone.utc) + relativedelta(hours=expiration_hours)
                table_ref.description = f"Temp table for {country} data for {the_month}, job ID: {request_id}"
                bq_ops.update_table(table_ref, ["expires", "description"])
        except Exception as e:
            logger.warning(f"Unable to update table expiration: {e}")
        
        # Check row count with a short timeout
        logger.info("Checking row count in created table")
        query_result = bq_ops.execute_query(
            f"SELECT COUNT(*) as row_count FROM `{temp_table_id}`", 
            timeout=30
        )
        rows = list(query_result)
        row_count = rows[0].row_count if rows else 0
        metrics.record_value("temp_table_row_count", row_count)
        
        if row_count < 1:
            logger.warning(f"Temp table {temp_table_id} has suspiciously low row count: {row_count}")
        
        metrics.stop_timer("create_temp_table")
        return row_count
    except Exception as e:
        metrics.stop_timer("create_temp_table")
        metrics.increment_counter("temp_table_creation_failures")
        logger.error(f"Failed to create temp table: {e}")
        raise

def insert_from_temp_to_final(bq_ops, temp_table_id, the_month, max_query_time=300):
    """
    Insert data from temporary table to final table.
    
    Args:
        bq_ops: BigQuery operations instance
        temp_table_id: ID of the temporary table
        the_month: Month of the data
        max_query_time: Maximum time in seconds to wait for query execution
    
    Returns:
        Number of rows inserted
    """
    metrics.start_timer("insert_operation")
    
    try:
        # Get distinct countries in temp table
        logger.info("Querying for distinct countries in temp table")
        countries_query = f"SELECT DISTINCT Country FROM `{temp_table_id}`"
        countries_result = bq_ops.execute_query(countries_query, timeout=30)
        countries = [row.Country for row in countries_result]
        
        if not countries:
            logger.warning(f"No countries found in temp table {temp_table_id}")
            metrics.stop_timer("insert_operation")
            return 0
        
        # Use parameters for the countries
        countries_params = [bigquery.ScalarQueryParameter(f"country_{i}", "STRING", country) 
                           for i, country in enumerate(countries)]
        countries_str = ', '.join([f"@country_{i}" for i in range(len(countries))])
        
        # Delete existing data for this month and these countries
        delete_query = f"""
        DELETE FROM `{bq_ops.config["DEST_TABLE"]}`
        WHERE Mois = @theMonth
        AND Country IN ({countries_str})
        """
        
        delete_params = [bigquery.ScalarQueryParameter("theMonth", "DATE", the_month)] + countries_params
        logger.info(f"Deleting existing data for {the_month} and countries: {', '.join(countries)}")
        bq_ops.execute_query(delete_query, delete_params, timeout=max_query_time)
        
        # Insert from temp table
        insert_query = f"""
        INSERT INTO `{bq_ops.config["DEST_TABLE"]}` 
        SELECT * FROM `{temp_table_id}`
        """
        
        logger.info(f"Inserting new data from temp table {temp_table_id}")
        bq_ops.execute_query(insert_query, timeout=max_query_time)
        
        # Count inserted rows
        count_query = f"""
        SELECT COUNT(*) AS row_count 
        FROM `{bq_ops.config["DEST_TABLE"]}` 
        WHERE Mois = @theMonth
        AND Country IN ({countries_str})
        """
        logger.info("Counting inserted rows")
        count_result = bq_ops.execute_query(count_query, delete_params, timeout=30)
        rows = list(count_result)
        rows_affected = rows[0].row_count if rows else 0
        
        metrics.stop_timer("insert_operation")
        metrics.record_value("rows_inserted", rows_affected)
        
        logger.info(f"Inserted {rows_affected} rows for {the_month}")
        return rows_affected
    except Exception as e:
        metrics.stop_timer("insert_operation")
        metrics.increment_counter("insert_failures")
        logger.error(f"Failed to insert from temp to final: {e}")
        raise

def delete_temp_table(bq_ops, temp_table_id):
    """
    Delete a temporary table with simple retry.
    
    Args:
        bq_ops: BigQuery operations instance
        temp_table_id: ID of the temporary table to delete
    
    Returns:
        Whether deletion was successful
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Deleting temp table: {temp_table_id} (attempt {attempt+1}/{max_retries})")
            bq_ops.delete_table(temp_table_id, not_found_ok=True)
            logger.info(f"Deleted temp table: {temp_table_id}")
            return True
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to delete temp table {temp_table_id}: {e}")
                return False
            time.sleep(2 ** attempt)  # Simple exponential backoff
    return False

def process_month_for_country(bq_ops, the_month, country, request_id, max_query_time=600):
    """
    Process data for a specific month and country.
    
    Args:
        bq_ops: BigQuery operations instance
        the_month: Month to process
        country: Country code to process
        request_id: Unique request identifier
        max_query_time: Maximum time in seconds to wait for main queries
    
    Returns:
        Success status and number of rows inserted
    """
    logger.set_correlation_id(f"month_{the_month.strftime('%Y%m')}_{country}_{request_id}")
    logger.info(f"Processing month {the_month} for country {country}")
    
    metrics.start_timer(f"process_month_{the_month.strftime('%Y%m')}_{country}")
    
    # Create temp table ID with unique component to prevent collisions
    unique_id = str(uuid.uuid4())[:8]
    temp_table_id = (f"{bq_ops.config['DEST_PROJECT']}.{bq_ops.config['DEST_DATASET']}."
                   f"temp_data_{country}_{the_month.strftime('%Y%m')}_{request_id}_{unique_id}")
    
    try:
        # Create temp table
        create_temp_table_for_month(bq_ops, the_month, temp_table_id, country, 
                                   request_id, max_query_time=max_query_time)
        
        # Insert into final table
        rows_inserted = insert_from_temp_to_final(bq_ops, temp_table_id, the_month, 
                                               max_query_time=max_query_time)
        
        logger.info(f"Successfully processed {the_month} for {country}: {rows_inserted} rows")
        return True, rows_inserted
    except Exception as e:
        metrics.increment_counter("month_processing_failures")
        logger.error(f"Failed to process month {the_month} for country {country}: {e}")
        return False, 0
    finally:
        # Always clean up temp table
        metrics.stop_timer(f"process_month_{the_month.strftime('%Y%m')}_{country}")
        try:
            delete_temp_table(bq_ops, temp_table_id)
        except Exception as e:
            logger.warning(f"Failed to clean up temporary table {temp_table_id}: {e}")

def process_all_country_month_combinations(bq_ops, months, countries, request_id, 
                                          max_query_time=600,
                                          worker_timeout=1800):
    """
    Process all country/month combinations with a thread pool.
    
    Args:
        bq_ops: BigQuery operations instance
        months: List of months to process
        countries: List of countries to process
        request_id: Unique request identifier
        max_query_time: Maximum time in seconds to wait for main queries
        worker_timeout: Maximum time in seconds to wait for a worker
    
    Returns:
        Success counts, failure counts, total rows processed, failed combinations
    """
    logger.info(f"Processing {len(months)} months × {len(countries)} countries with thread pool")
    
    # Generate all combinations
    combinations = [(month, country) for month in months for country in countries]
    
    # Record metrics
    metrics.record_value("total_combinations", len(combinations))
    metrics.start_timer("process_all_combinations")
    
    # Tracking variables
    successful = 0
    failed = 0
    total_rows = 0
    failed_pairs = set()
    
    # Fixed thread pool size from config
    max_workers = min(bq_ops.config.get("MAX_WORKERS", 8), len(combinations))
    logger.info(f"Using thread pool with {max_workers} workers for {len(combinations)} tasks")
    
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all jobs
            future_to_combo = {}
            for month, country in combinations:
                if is_shutdown_requested():
                    logger.warning("Shutdown requested, stopping submissions")
                    break
                
                combo_id = f"{request_id}_{month.strftime('%Y%m')}_{country}"
                # Pass max_query_time to the worker function
                future = executor.submit(process_month_for_country, 
                                         bq_ops, month, country, combo_id, max_query_time)
                future_to_combo[future] = (month, country)
            
            # Process results as they complete, with timeout
            for future in as_completed(future_to_combo):
                month, country = future_to_combo[future]
                
                try:
                    # Add timeout to future.result() to prevent hanging
                    logger.info(f"Waiting for result of {month}/{country} (timeout: {worker_timeout}s)")
                    success, rows = future.result(timeout=worker_timeout)
                    
                    if success:
                        successful += 1
                        total_rows += rows
                        logger.info(f"Completed {month}/{country}: {rows} rows")
                    else:
                        failed += 1
                        failed_pairs.add((month, country))
                        logger.warning(f"Failed processing {month}/{country}")
                        
                except FutureTimeoutError:
                    # Handle worker timeout
                    failed += 1
                    failed_pairs.add((month, country))
                    logger.error(f"Processing {month}/{country} timed out after {worker_timeout}s")
                    
                except Exception as e:
                    failed += 1
                    failed_pairs.add((month, country))
                    logger.error(f"Error processing {month}/{country}: {e}")
                
                # Check for shutdown
                if is_shutdown_requested():
                    logger.warning("Shutdown requested, waiting for in-progress tasks")
                    # Add timeout to shutdown to prevent hanging
                    try:
                        executor.shutdown(wait=True, cancel_futures=True)
                    except Exception as e:
                        logger.error(f"Error during executor shutdown: {e}")
                    break
        
        return successful, failed, total_rows, failed_pairs
    finally:
        metrics.stop_timer("process_all_combinations")

def process_month(bq_ops, the_month, request_id, max_query_time=600):
    """
    Process data for a specific month for all countries.
    
    Args:
        bq_ops: BigQuery operations instance
        the_month: Month to process
        request_id: Unique request identifier
        max_query_time: Maximum time in seconds to wait for main queries
    
    Returns:
        Success status and total rows processed
    """
    logger.set_correlation_id(f"month_{the_month.strftime('%Y%m')}_{request_id}")
    logger.info(f"Processing month: {the_month}")
    
    metrics.start_timer(f"process_month_{the_month.strftime('%Y%m')}")
    
    try:
        # Process each country in sequence
        successful_countries = 0
        failed_countries = 0
        total_rows = 0
        
        for country in bq_ops.config["ALLOWED_COUNTRIES"]:
            if is_shutdown_requested():
                logger.warning("Shutdown requested, stopping further country processing")
                break
                
            country_request_id = f"{request_id}_{country}"
            logger.info(f"Starting sequential processing for {the_month}/{country}")
            success, rows = process_month_for_country(
                bq_ops, the_month, country, country_request_id, max_query_time
            )
            
            if success:
                successful_countries += 1
                total_rows += rows
                logger.info(f"Successfully processed {the_month}/{country}: {rows} rows")
            else:
                failed_countries += 1
                logger.error(f"Failed to process {the_month}/{country}")
        
        logger.info(f"Completed processing month {the_month}: {successful_countries} countries successful, {failed_countries} countries failed, {total_rows} total rows")
        
        # Return success only if all countries were processed successfully
        return failed_countries == 0, total_rows
    finally:
        # Record metrics
        metrics.stop_timer(f"process_month_{the_month.strftime('%Y%m')}")

def process_month_range(bq_ops, start_month, end_month, parallel=True, request_id=None, 
                       max_query_time=600, worker_timeout=1800):
    """
    Process a range of months.
    
    Args:
        bq_ops: BigQuery operations instance
        start_month: First month to process
        end_month: Last month to process
        parallel: Whether to process months in parallel
        request_id: Unique request identifier
        max_query_time: Maximum time in seconds to wait for main queries
        worker_timeout: Maximum time in seconds to wait for a worker
    
    Returns:
        Number of successful and failed months
    """
    request_id = request_id or str(uuid.uuid4())[:8]
    logger.set_correlation_id(request_id)
    
    # Generate months list
    months = []
    current_month = start_month
    while current_month <= end_month:
        months.append(current_month)
        current_month = (current_month.replace(day=1) + relativedelta(months=1))
    
    # Get countries to process
    countries = bq_ops.config["ALLOWED_COUNTRIES"]
    
    # Log parameters and create a timeout monitor
    logger.info(f"Processing {len(months)} months for {len(countries)} countries")
    logger.info(f"Query timeout: {max_query_time}s, Worker timeout: {worker_timeout}s")
    metrics.start_timer("process_month_range")
    
    # Create a timeout monitor thread to detect hangs
    timeout_flag = threading.Event()
    
    def timeout_monitor():
        max_time = 3600  # 1 hour timeout for the whole operation
        logger.info(f"Timeout monitor started, max time: {max_time}s")
        timeout_flag.wait(timeout=max_time)
        if not timeout_flag.is_set():
            logger.error(f"CRITICAL: process_month_range timed out after {max_time}s")
            # Request a shutdown
            from src.shutdown_utils import request_shutdown
            request_shutdown()
    
    timeout_thread = threading.Thread(target=timeout_monitor)
    timeout_thread.daemon = True
    timeout_thread.start()
    
    try:
        total_rows = 0
        
        if parallel and (len(months) > 1 or len(countries) > 1):
            # Process all combinations in parallel
            logger.info("Using parallel processing mode")
            successful, failed, rows_processed, failed_pairs = process_all_country_month_combinations(
                bq_ops, months, countries, request_id, max_query_time, worker_timeout
            )
            
            total_rows = rows_processed
            
            # Map success to month level
            month_success = {}
            for month in months:
                # A month is successful only if all its countries succeed
                month_success[month] = all((month, country) not in failed_pairs 
                                         for country in countries)
            
            successful_months = sum(1 for success in month_success.values() if success)
            failed_months = len(months) - successful_months
        else:
            # Process months sequentially
            logger.info("Using sequential processing mode")
            successful_months = 0
            failed_months = 0
            
            for month in months:
                if is_shutdown_requested():
                    logger.warning("Shutdown requested, stopping further processing")
                    break
                
                month_request_id = f"{request_id}_{month.strftime('%Y%m')}"
                logger.info(f"Starting sequential processing for month {month}")
                success, rows = process_month(bq_ops, month, month_request_id, max_query_time)
                total_rows += rows
                
                if success:
                    successful_months += 1
                    logger.info(f"Month {month} processed successfully")
                else:
                    failed_months += 1
                    logger.warning(f"Month {month} processing failed")
        
        logger.info(f"Processed {len(months)} months: {successful_months} successful, {failed_months} failed, {total_rows} total rows")
        return successful_months, failed_months
    except Exception as e:
        logger.error(f"Error in process_month_range: {e}")
        return 0, len(months)
    finally:
        # Signal the timeout thread to exit
        timeout_flag.set()
        metrics.stop_timer("process_month_range")
        metrics.record_value("total_rows_processed", total_rows)