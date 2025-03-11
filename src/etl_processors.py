import os
import sys
import time
import uuid
import json
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import bigquery
from dateutil.relativedelta import relativedelta

from src.logging_utils import logger
from src.metrics import metrics
from src.worker_scaling import AdaptiveWorkerScaler, AdaptiveSemaphore
from src.query_templates import QUERY_GENERATORS
from src.bigquery_utils import BigQueryConnectionPool
from src.shutdown_utils import is_shutdown_requested

def create_customer_data_table_if_not_exists(bq_ops):
    """
    Creates destination table with optimized schema.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
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

def create_temp_table_for_month(bq_ops, the_month, temp_table_id, country, request_id=None):
    """
    Creates a temporary table with aggregated data for the month and specific country.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
        the_month (date): Month to process
        temp_table_id (str): ID for the temporary table
        country (str): Country code to process
        request_id (str, optional): Unique request identifier
    
    Returns:
        int: Number of rows in the created temporary table
    """
    request_id = request_id or str(uuid.uuid4())[:8]
    logger.info(f"Creating temp table for {country}: {temp_table_id}")
    
    # Start timer for this operation
    metrics.start_timer("create_temp_table")
    
    try:
        # Create the appropriate query based on the country
        query_generator = QUERY_GENERATORS.get(country)
        if not query_generator:
            raise ValueError(f"Unsupported country: {country}")
        
        create_temp_table_query = query_generator(bq_ops, temp_table_id)
        
        # Set up query parameters
        params = [
            bigquery.ScalarQueryParameter("theMonth", "DATE", the_month),
        ]
        
        # Execute query to create temporary table
        bq_ops.execute_query(create_temp_table_query, params)
        
        # Explicitly update expiration if not set in SQL
        try:
            # Use the get_table method from bq_ops instead of client directly
            table_ref = bq_ops.get_table(temp_table_id)
            if not table_ref.expires:
                expiration_hours = bq_ops.config.get("TEMP_TABLE_EXPIRATION_HOURS", 24)
                table_ref.expires = datetime.now() + timedelta(hours=expiration_hours)
                if bq_ops.config.get("TEMP_TABLE_ADD_DESCRIPTIONS", True):
                    table_ref.description = f"Temporary table for {country} data for {the_month}, job ID: {request_id}"
                # Use the update_table method from bq_ops
                bq_ops.update_table(table_ref, ["expires", "description"])
                logger.info(f"Set expiration for table {temp_table_id} to {expiration_hours} hours from now")
        except Exception as e:
            logger.warning(f"Unable to update table expiration: {e}")
        
        # Check row count
        query_result = bq_ops.execute_query(f"SELECT COUNT(*) as row_count FROM `{temp_table_id}`")
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

def insert_from_temp_to_final(bq_ops, temp_table_id, the_month):
    """
    Insert data from temporary table to final table.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
        temp_table_id (str): ID of the temporary table
        the_month (date): Month of the data
    
    Returns:
        int: Number of rows inserted
    """
    metrics.start_timer("insert_operation")
    
    try:
        # First, get the list of countries in the temp table
        countries_query = f"""
        SELECT DISTINCT Country 
        FROM `{temp_table_id}`
        """
        countries_result = bq_ops.execute_query(countries_query)
        countries = [row.Country for row in countries_result]
        
        if not countries:
            logger.warning(f"No countries found in temp table {temp_table_id}, skipping delete/insert operations")
            metrics.stop_timer("insert_operation")
            return 0
            
        countries_str = ', '.join([f"'{country}'" for country in countries])
        logger.info(f"Found countries in temp data: {countries_str}")
        
        # Delete existing data only for the specific countries and month
        logger.info(f"Deleting existing data for month {the_month} and countries: {countries_str}")
        delete_query = f"""
        DELETE FROM `{bq_ops.config["DEST_TABLE"]}`
        WHERE Mois = @theMonth
        AND Country IN ({countries_str})
        """
        
        bq_ops.execute_query(
            delete_query,
            [bigquery.ScalarQueryParameter("theMonth", "DATE", the_month)]
        )
        
        # Now insert from temp table
        insert_query = f"""
        INSERT INTO `{bq_ops.config["DEST_TABLE"]}` (
            Mois, Country, Enseigne,
            CA_Tous_Clients, CA_Tous_Clients_local,
            CA_Porteurs, CA_Porteurs_local, Taux_CA_encarte,
            Nb_transactions_Tous_Clients, Nb_transactions_porteurs, Taux_transactions_encartees,
            Nb_foyers, Frequence_porteurs, Panier_moyen_Porteurs,
            nb_articles, nb_articles_porteur,
            Families, Seniors,
            CA_promo, CA_promo_local,
            nb_foyers_constants, nb_foyers_gagnes, nb_foyers_perdus
        )
        SELECT * FROM `{temp_table_id}`
        """
        
        # Execute the insert
        bq_ops.execute_query(insert_query)
        
        # Query to get the actual inserted rows
        count_query = f"""
        SELECT COUNT(*) AS row_count 
        FROM `{bq_ops.config["DEST_TABLE"]}` 
        WHERE Mois = @theMonth
        AND Country IN ({countries_str})
        """
        count_result = bq_ops.execute_query(
            count_query,
            [bigquery.ScalarQueryParameter("theMonth", "DATE", the_month)]
        )
        rows = list(count_result)
        rows_affected = rows[0].row_count if rows else 0
        
        # Record metrics
        metrics.stop_timer("insert_operation")
        metrics.record_value("rows_inserted", rows_affected)
        
        logger.info(f"Inserted {rows_affected} rows for {the_month} and countries: {countries_str}")
        return rows_affected
    except Exception as e:
        metrics.stop_timer("insert_operation")
        metrics.increment_counter("insert_failures")
        logger.error(f"Failed to insert from temp to final: {e}")
        raise

def delete_temp_table(bq_ops, temp_table_id):
    """
    Delete a temporary table with retries.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
        temp_table_id (str): ID of the temporary table to delete
    
    Returns:
        bool: Whether deletion was successful
    """
    def retry_delete(max_retries=5, initial_backoff=2, max_backoff=30):
        """
        Inner function to retry table deletion with exponential backoff.
        
        Args:
            max_retries (int): Maximum number of retry attempts
            initial_backoff (int): Initial delay between retries
            max_backoff (int): Maximum delay between retries
        
        Returns:
            bool: Whether deletion was successful
        """
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                bq_ops.delete_table(temp_table_id, not_found_ok=True)
                logger.info(f"Deleted temp table: {temp_table_id}")
                return True
            except Exception as e:
                retry_count += 1
                wait_time = min(initial_backoff * (2 ** (retry_count - 1)), max_backoff)
                logger.warning(
                    f"Failed to delete temp table {temp_table_id} (attempt {retry_count}/{max_retries}): {e}. "
                    f"Retrying in {wait_time}s"
                )
                time.sleep(wait_time)
        
        # If all retries fail
        logger.error(f"All deletion attempts failed for temp table {temp_table_id}")
        metrics.increment_counter("temp_table_deletion_failures")
        return False
    
    return retry_delete()

def process_month_for_country(bq_ops, the_month, country, request_id):
    """
    Process data for a specific month and country.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
        the_month (date): Month to process
        country (str): Country code to process
        request_id (str): Unique request identifier
    
    Returns:
        Tuple[bool, int]: Success status and number of rows inserted
    """
    logger.correlation_id = f"month_{the_month.strftime('%Y%m')}_{country}_{request_id}"
    logger.info(f"Processing month {the_month} for country {country}")
    
    metrics.start_timer(f"process_month_{the_month.strftime('%Y%m')}_{country}")
    temp_table_id = f"{bq_ops.config['DEST_PROJECT']}.{bq_ops.config['DEST_DATASET']}.temp_data_{country}_{the_month.strftime('%Y%m')}_{request_id}"
    
    try:
        # Create a temp table with the aggregated data for this country
        create_temp_table_for_month(bq_ops, the_month, temp_table_id, country, request_id)
        
        # Insert the data into the final table
        rows_inserted = insert_from_temp_to_final(bq_ops, temp_table_id, the_month)
        
        # Record metrics
        metrics.stop_timer(f"process_month_{the_month.strftime('%Y%m')}_{country}")
        
        # Clean up temp table with robust deletion
        delete_temp_table(bq_ops, temp_table_id)
        
        logger.info(f"Successfully processed month {the_month} for country {country}, inserted {rows_inserted} rows")
        return True, rows_inserted
    except Exception as e:
        metrics.stop_timer(f"process_month_{the_month.strftime('%Y%m')}_{country}")
        metrics.increment_counter("month_processing_failures")
        logger.error(f"Failed to process month {the_month} for country {country}: {e}")
        
        # Try to clean up temp table even on failure
        delete_temp_table(bq_ops, temp_table_id)
            
        return False, 0

def process_month(bq_ops, the_month, request_id):
    """
    Process data for a specific month for all countries.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
        the_month (date): Month to process
        request_id (str): Unique request identifier
    
    Returns:
        Tuple[bool, int]: Success status and total rows processed
    """
    logger.correlation_id = f"month_{the_month.strftime('%Y%m')}_{request_id}"
    logger.info(f"Processing month: {the_month}")
    
    metrics.start_timer(f"process_month_{the_month.strftime('%Y%m')}")
    
    # Process each country in sequence
    successful_countries = 0
    failed_countries = 0
    total_rows = 0
    
    for country in bq_ops.config["ALLOWED_COUNTRIES"]:
        if is_shutdown_requested():
            logger.warning("Shutdown requested, stopping further country processing")
            break
            
        country_request_id = f"{request_id}_{country}"
        success, rows = process_month_for_country(bq_ops, the_month, country, country_request_id)
        
        if success:
            successful_countries += 1
            total_rows += rows
        else:
            failed_countries += 1
    
    # Record metrics
    metrics.stop_timer(f"process_month_{the_month.strftime('%Y%m')}")
    
    logger.info(f"Completed processing month {the_month}: {successful_countries} countries successful, {failed_countries} countries failed, {total_rows} total rows")
    
    # Return success only if all countries were processed successfully
    return failed_countries == 0, total_rows

def process_month_range(bq_ops, start_month, end_month, parallel=True, request_id=None):
    """
    Process a range of months with optional parallelism and adaptive scaling.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
        start_month (date): First month to process
        end_month (date): Last month to process
        parallel (bool, optional): Whether to process months in parallel. Defaults to True.
        request_id (str, optional): Unique request identifier
    
    Returns:
        Tuple[int, int]: Number of successful and failed months
    """
    # Generate a unique request ID if not provided
    request_id = request_id or str(uuid.uuid4())[:8]
    
    # Set context for logging
    logger.correlation_id = request_id
    
    # Generate a list of months to process
    months = []
    current_month = start_month
    while current_month <= end_month:
        months.append(current_month)
        current_month = (current_month.replace(day=1) + relativedelta(months=1))
    
    # Get countries from config
    countries = bq_ops.config["ALLOWED_COUNTRIES"]
    
    logger.info(f"Processing {len(months)} months from {start_month} to {end_month} for {len(countries)} countries")
    
    # Start timer for whole range
    metrics.start_timer("process_month_range")
    
    try:
        if parallel and (len(months) > 1 or len(countries) > 1):
            # Process all combinations in parallel with adaptive scaling
            successful_combinations, failed_combinations, total_rows, failed_pairs = process_all_country_month_combinations(
                bq_ops, months, countries, request_id
            )
            
            # Map success to the month level
            month_success = {}
            for month in months:
                month_success[month] = True
                for country in countries:
                    if (month, country) in failed_pairs:
                        month_success[month] = False
                        break
            
            successful_months = sum(1 for month, success in month_success.items() if success)
            failed_months = len(months) - successful_months
            
        else:
            # Process months sequentially
            logger.info("Using sequential processing")
            successful_months = 0
            failed_months = 0
            total_rows = 0
            
            for i, month in enumerate(months):
                if is_shutdown_requested():
                    logger.warning("Shutdown requested, stopping further processing")
                    break
                
                month_request_id = f"{request_id}_{i}"
                success, rows = process_month(bq_ops, month, month_request_id)
                total_rows += rows
                
                if success:
                    successful_months += 1
                else:
                    failed_months += 1
                    
                logger.info(f"Month {month}: {'Success' if success else 'Failed'}, Rows: {rows}")
    finally:
        # Record metrics
        metrics.stop_timer("process_month_range")
        metrics.record_value("months_processed_successfully", successful_months)
        metrics.record_value("months_processed_with_failure", failed_months)
        metrics.record_value("total_rows_processed", total_rows)
    
    return successful_months, failed_months

def process_all_country_month_combinations(bq_ops, months, countries, request_id):
    """
    Process all country/month combinations with adaptive worker scaling.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
        months (List[date]): Months to process
        countries (List[str]): Countries to process
        request_id (str): Unique request identifier
    
    Returns:
        Tuple[int, int, int, List[Tuple[date, str]]]: 
        - Successful combinations count
        - Failed combinations count
        - Total rows processed
        - List of failed (month, country) pairs
    """
    logger.info(f"Processing {len(months)} months × {len(countries)} countries in parallel")
    
    # Generate all combinations
    combinations = [(month, country) for month in months for country in countries]
    total_combinations = len(combinations)
    
    # Record total work
    metrics.record_value("total_combinations", total_combinations)
    
    # Start timer for whole batch
    metrics.start_timer("process_all_combinations")
    
    # Initialize worker scaler
    worker_scaler = AdaptiveWorkerScaler(bq_ops.config)
    
    # Initialize tracking variables
    successful_combinations = 0
    failed_combinations = 0
    failed_combination_pairs = []
    total_rows = 0
    
    # Track jobs
    pending_futures = {}
    completed_futures = {}
    
    # Start an executor with dynamic worker count
    initial_workers = worker_scaler.get_worker_count()
    logger.info(f"Starting with {initial_workers} workers for {total_combinations} combinations")
    
    # Create a dynamic worker pool with adaptive semaphore
    max_possible_workers = bq_ops.config.get("MAX_WORKERS", 8)
    worker_semaphore = AdaptiveSemaphore(initial_workers)
    
    # Create a fixed-size thread pool with the maximum possible workers
    with ThreadPoolExecutor(max_workers=max_possible_workers) as executor:
        def submit_with_semaphore(fn, *args, **kwargs):
            """Submit a task that acquires and releases the worker semaphore."""
            worker_semaphore.acquire()
            try:
                future = executor.submit(fn, *args, **kwargs)
                # Add callback to release semaphore when done
                future.add_done_callback(lambda f: worker_semaphore.release())
                return future
            except Exception as e:
                worker_semaphore.release()
                raise e
        
        # Submit initial batch of jobs
        for idx, (month, country) in enumerate(combinations):
            if is_shutdown_requested():
                logger.warning("Shutdown requested, stopping submissions")
                break
                
            combo_id = f"{request_id}_{month.strftime('%Y%m')}_{country}"
            
            # Submit task with semaphore control
            future = submit_with_semaphore(
                process_month_for_country, bq_ops, month, country, combo_id
            )
            
            pending_futures[(month, country)] = future
        
        # Process results as they complete
        while pending_futures:
            # Check if we need to adjust worker count
            new_worker_count = worker_scaler.get_worker_count()
            
            # Update semaphore count if worker count changed
            if new_worker_count != worker_semaphore.current_value:
                worker_semaphore.adjust_value(new_worker_count)
            
            # Check for completed futures
            done_futures = [f for f in as_completed(list(pending_futures.values()), timeout=5) 
                            if f in list(pending_futures.values())]
            
            # Process completed futures
            for future in done_futures:
                # Find which combination this future belongs to
                for combo, f in list(pending_futures.items()):
                    if f == future:
                        month, country = combo
                        del pending_futures[combo]
                        completed_futures[combo] = future
                        
                        try:
                            if is_shutdown_requested():
                                continue
                                    
                            success, rows = future.result()
                            
                            if success:
                                successful_combinations += 1
                                total_rows += rows
                            else:
                                failed_combinations += 1
                                failed_combination_pairs.append((month, country))
                            
                            logger.info(f"Combination {month}/{country}: {'Success' if success else 'Failed'}, Rows: {rows}")
                        except Exception as e:
                            failed_combinations += 1
                            failed_combination_pairs.append((month, country))
                            logger.error(f"Error processing {month}/{country}: {e}")
                        
                        break
            
            # Check for shutdown
            if is_shutdown_requested():
                logger.warning("Shutdown requested, cancelling pending jobs")
                for future in pending_futures.values():
                    if not future.done():
                        future.cancel()
                break
    
    # Record metrics
    metrics.stop_timer("process_all_combinations")
    metrics.record_value("successful_combinations", successful_combinations)
    metrics.record_value("failed_combinations", failed_combinations)
    metrics.record_value("total_rows_processed", total_rows)
    
    # Record worker scaling history
    scaling_history = worker_scaler.get_adjustment_history()
    if scaling_history:
        logger.info(f"Worker scaling adjustments: {len(scaling_history)}")
    
    return successful_combinations, failed_combinations, total_rows, failed_combination_pairs
    info(f"Processing month: {the_month}")
    
    metrics.start_timer(f"process_month_{the_month.strftime('%Y%m')}")
    
    # Process each country in sequence
    successful_countries = 0
    failed_countries = 0
    total_rows = 0
    
    for country in bq_ops.config["ALLOWED_COUNTRIES"]:
        if is_shutdown_requested():
            logger.warning("Shutdown requested, stopping further country processing")
            break
            
        country_request_id = f"{request_id}_{country}"
        success, rows = process_month_for_country(bq_ops, the_month, country, country_request_id)
        
        if success:
            successful_countries += 1
            total_rows += rows
        else:
            failed_countries += 1
    
    # Record metrics
    metrics.stop_timer(f"process_month_{the_month.strftime('%Y%m')}")
    
    logger.info(f"Completed processing month {the_month}: {successful_countries} countries successful, {failed_countries} countries failed, {total_rows} total rows")
    
    # Return success only if all countries were processed successfully
    return failed_countries == 0, total_rows

def process_month_range(bq_ops, start_month, end_month, parallel=True, request_id=None):
    """
    Process a range of months with optional parallelism and adaptive scaling.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
        start_month (date): First month to process
        end_month (date): Last month to process
        parallel (bool, optional): Whether to process months in parallel. Defaults to True.
        request_id (str, optional): Unique request identifier
    
    Returns:
        Tuple[int, int]: Number of successful and failed months
    """
    # Generate a unique request ID if not provided
    request_id = request_id or str(uuid.uuid4())[:8]
    
    # Set context for logging
    logger.correlation_id = request_id
    
    # Generate a list of months to process
    months = []
    current_month = start_month
    while current_month <= end_month:
        months.append(current_month)
        current_month = (current_month.replace(day=1) + relativedelta(months=1))
    
    # Get countries from config
    countries = bq_ops.config["ALLOWED_COUNTRIES"]
    
    logger.info(f"Processing {len(months)} months from {start_month} to {end_month} for {len(countries)} countries")
    
    # Start timer for whole range
    metrics.start_timer("process_month_range")
    
    try:
        if parallel and (len(months) > 1 or len(countries) > 1):
            # Process all combinations in parallel with adaptive scaling
            successful_combinations, failed_combinations, total_rows, failed_pairs = process_all_country_month_combinations(
                bq_ops, months, countries, request_id
            )
            
            # Map success to the month level
            month_success = {}
            for month in months:
                month_success[month] = True
                for country in countries:
                    if (month, country) in failed_pairs:
                        month_success[month] = False
                        break
            
            successful_months = sum(1 for month, success in month_success.items() if success)
            failed_months = len(months) - successful_months
            
        else:
            # Process months sequentially
            logger.info("Using sequential processing")
            successful_months = 0
            failed_months = 0
            total_rows = 0
            
            for i, month in enumerate(months):
                if is_shutdown_requested():
                    logger.warning("Shutdown requested, stopping further processing")
                    break
                
                month_request_id = f"{request_id}_{i}"
                success, rows = process_month(bq_ops, month, month_request_id)
                total_rows += rows
                
                if success:
                    successful_months += 1
                else:
                    failed_months += 1
                    
                logger.info(f"Month {month}: {'Success' if success else 'Failed'}, Rows: {rows}")
    finally:
        # Record metrics
        metrics.stop_timer("process_month_range")
        metrics.record_value("months_processed_successfully", successful_months)
        metrics.record_value("months_processed_with_failure", failed_months)
        metrics.record_value("total_rows_processed", total_rows)
    
    return successful_months, failed_months

def process_all_country_month_combinations(bq_ops, months, countries, request_id):
    """
    Process all country/month combinations with adaptive worker scaling.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
        months (List[date]): Months to process
        countries (List[str]): Countries to process
        request_id (str): Unique request identifier
    
    Returns:
        Tuple[int, int, int, List[Tuple[date, str]]]: 
        - Successful combinations count
        - Failed combinations count
        - Total rows processed
        - List of failed (month, country) pairs
    """
    logger.info(f"Processing {len(months)} months × {len(countries)} countries in parallel")
    
    # Generate all combinations
    combinations = [(month, country) for month in months for country in countries]
    total_combinations = len(combinations)
    
    # Record total work
    metrics.record_value("total_combinations", total_combinations)
    
    # Start timer for whole batch
    metrics.start_timer("process_all_combinations")
    
    # Initialize worker scaler
    worker_scaler = AdaptiveWorkerScaler(bq_ops.config)
    
    # Initialize tracking variables
    successful_combinations = 0
    failed_combinations = 0
    failed_combination_pairs = []
    total_rows = 0
    
    # Track jobs
    pending_futures = {}
    completed_futures = {}
    
    # Start an executor with dynamic worker count
    initial_workers = worker_scaler.get_worker_count()
    logger.info(f"Starting with {initial_workers} workers for {total_combinations} combinations")
    
    # Create a dynamic worker pool with adaptive semaphore
    max_possible_workers = bq_ops.config.get("MAX_WORKERS", 8)
    worker_semaphore = AdaptiveSemaphore(initial_workers)
    
    # Create a fixed-size thread pool with the maximum possible workers
    with ThreadPoolExecutor(max_workers=max_possible_workers) as executor:
        def submit_with_semaphore(fn, *args, **kwargs):
            """Submit a task that acquires and releases the worker semaphore."""
            worker_semaphore.acquire()
            try:
                future = executor.submit(fn, *args, **kwargs)
                # Add callback to release semaphore when done
                future.add_done_callback(lambda f: worker_semaphore.release())
                return future
            except Exception as e:
                worker_semaphore.release()
                raise e
        
        # Submit initial batch of jobs
        for idx, (month, country) in enumerate(combinations):
            if is_shutdown_requested():
                logger.warning("Shutdown requested, stopping submissions")
                break
                
            combo_id = f"{request_id}_{month.strftime('%Y%m')}_{country}"
            
            # Submit task with semaphore control
            future = submit_with_semaphore(
                process_month_for_country, bq_ops, month, country, combo_id
            )
            
            pending_futures[(month, country)] = future
        
        # Process results as they complete
        while pending_futures:
            # Check if we need to adjust worker count
            new_worker_count = worker_scaler.get_worker_count()
            
            # Update semaphore count if worker count changed
            if new_worker_count != worker_semaphore.current_value:
                worker_semaphore.adjust_value(new_worker_count)
            
            # Check for completed futures
            done_futures = [f for f in as_completed(list(pending_futures.values()), timeout=5) 
                            if f in list(pending_futures.values())]
            
            # Process completed futures
            for future in done_futures:
                # Find which combination this future belongs to
                for combo, f in list(pending_futures.items()):
                    if f == future:
                        month, country = combo
                        del pending_futures[combo]
                        completed_futures[combo] = future
                        
                        try:
                            if is_shutdown_requested():
                                continue
                                    
                            success, rows = future.result()
                            
                            if success:
                                successful_combinations += 1
                                total_rows += rows
                            else:
                                failed_combinations += 1
                                failed_combination_pairs.append((month, country))
                            
                            logger.info(f"Combination {month}/{country}: {'Success' if success else 'Failed'}, Rows: {rows}")
                        except Exception as e:
                            failed_combinations += 1
                            failed_combination_pairs.append((month, country))
                            logger.error(f"Error processing {month}/{country}: {e}")
                        
                        break
            
            # Check for shutdown
            if is_shutdown_requested():
                logger.warning("Shutdown requested, cancelling pending jobs")
                for future in pending_futures.values():
                    if not future.done():
                        future.cancel()
                break
    
    # Record metrics
    metrics.stop_timer("process_all_combinations")
    metrics.record_value("successful_combinations", successful_combinations)
    metrics.record_value("failed_combinations", failed_combinations)
    metrics.record_value("total_rows_processed", total_rows)
    
    # Record worker scaling history
    scaling_history = worker_scaler.get_adjustment_history()
    if scaling_history:
        logger.info(f"Worker scaling adjustments: {len(scaling_history)}")
    
    return successful_combinations, failed_combinations, total_rows, failed_combination_pairs
    info(f"Processing month: {the_month}")
    
    metrics.start_timer(f"process_month_{the_month.strftime('%Y%m')}")
    
    # Process each country in sequence
    successful_countries = 0
    failed_countries = 0
    total_rows = 0
    
    for country in bq_ops.config["ALLOWED_COUNTRIES"]:
        if is_shutdown_requested():
            logger.warning("Shutdown requested, stopping further country processing")
            break
            
        country_request_id = f"{request_id}_{country}"
        success, rows = process_month_for_country(bq_ops, the_month, country, country_request_id)
        
        if success:
            successful_countries += 1
            total_rows += rows
        else:
            failed_countries += 1
    
    # Record metrics
    metrics.stop_timer(f"process_month_{the_month.strftime('%Y%m')}")
    
    logger.info(f"Completed processing month {the_month}: {successful_countries} countries successful, {failed_countries} countries failed, {total_rows} total rows")
    
    # Return success only if all countries were processed successfully
    return failed_countries == 0, total_rows

def process_month_range(bq_ops, start_month, end_month, parallel=True, request_id=None):
    """
    Process a range of months with optional parallelism and adaptive scaling.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
        start_month (date): First month to process
        end_month (date): Last month to process
        parallel (bool, optional): Whether to process months in parallel. Defaults to True.
        request_id (str, optional): Unique request identifier
    
    Returns:
        Tuple[int, int]: Number of successful and failed months
    """
    # Generate a unique request ID if not provided
    request_id = request_id or str(uuid.uuid4())[:8]
    
    # Set context for logging
    logger.correlation_id = request_id
    
    # Generate a list of months to process
    months = []
    current_month = start_month
    while current_month <= end_month:
        months.append(current_month)
        current_month = (current_month.replace(day=1) + relativedelta(months=1))
    
    # Get countries from config
    countries = bq_ops.config["ALLOWED_COUNTRIES"]
    
    logger.info(f"Processing {len(months)} months from {start_month} to {end_month} for {len(countries)} countries")
    
    # Start timer for whole range
    metrics.start_timer("process_month_range")
    
    try:
        if parallel and (len(months) > 1 or len(countries) > 1):
            # Process all combinations in parallel with adaptive scaling
            successful_combinations, failed_combinations, total_rows, failed_pairs = process_all_country_month_combinations(
                bq_ops, months, countries, request_id
            )
            
            # Map success to the month level
            month_success = {}
            for month in months:
                month_success[month] = True
                for country in countries:
                    if (month, country) in failed_pairs:
                        month_success[month] = False
                        break
            
            successful_months = sum(1 for month, success in month_success.items() if success)
            failed_months = len(months) - successful_months
            
        else:
            # Process months sequentially
            logger.info("Using sequential processing")
            successful_months = 0
            failed_months = 0
            total_rows = 0
            
            for i, month in enumerate(months):
                if is_shutdown_requested():
                    logger.warning("Shutdown requested, stopping further processing")
                    break
                
                month_request_id = f"{request_id}_{i}"
                success, rows = process_month(bq_ops, month, month_request_id)
                total_rows += rows
                
                if success:
                    successful_months += 1
                else:
                    failed_months += 1
                    
                logger.info(f"Month {month}: {'Success' if success else 'Failed'}, Rows: {rows}")
    finally:
        # Record metrics
        metrics.stop_timer("process_month_range")
        metrics.record_value("months_processed_successfully", successful_months)
        metrics.record_value("months_processed_with_failure", failed_months)
        metrics.record_value("total_rows_processed", total_rows)
    
    return successful_months, failed_months

def process_all_country_month_combinations(bq_ops, months, countries, request_id):
    """
    Process all country/month combinations with adaptive worker scaling.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
        months (List[date]): Months to process
        countries (List[str]): Countries to process
        request_id (str): Unique request identifier
    
    Returns:
        Tuple[int, int, int, List[Tuple[date, str]]]: 
        - Successful combinations count
        - Failed combinations count
        - Total rows processed
        - List of failed (month, country) pairs
    """
    logger.info(f"Processing {len(months)} months × {len(countries)} countries in parallel")
    
    # Generate all combinations
    combinations = [(month, country) for month in months for country in countries]
    total_combinations = len(combinations)
    
    # Record total work
    metrics.record_value("total_combinations", total_combinations)
    
    # Start timer for whole batch
    metrics.start_timer("process_all_combinations")
    
    # Initialize worker scaler
    worker_scaler = AdaptiveWorkerScaler(bq_ops.config)
    
    # Initialize tracking variables
    successful_combinations = 0
    failed_combinations = 0
    failed_combination_pairs = []
    total_rows = 0
    
    # Track jobs
    pending_futures = {}
    completed_futures = {}
    
    # Start an executor with dynamic worker count
    initial_workers = worker_scaler.get_worker_count()
    logger.info(f"Starting with {initial_workers} workers for {total_combinations} combinations")
    
    # Create a dynamic worker pool with adaptive semaphore
    max_possible_workers = bq_ops.config.get("MAX_WORKERS", 8)
    worker_semaphore = AdaptiveSemaphore(initial_workers)
    
    # Create a fixed-size thread pool with the maximum possible workers
    with ThreadPoolExecutor(max_workers=max_possible_workers) as executor:
        def submit_with_semaphore(fn, *args, **kwargs):
            """Submit a task that acquires and releases the worker semaphore."""
            worker_semaphore.acquire()
            try:
                future = executor.submit(fn, *args, **kwargs)
                # Add callback to release semaphore when done
                future.add_done_callback(lambda f: worker_semaphore.release())
                return future
            except Exception as e:
                worker_semaphore.release()
                raise e
        
        # Submit initial batch of jobs
        for idx, (month, country) in enumerate(combinations):
            if is_shutdown_requested():
                logger.warning("Shutdown requested, stopping submissions")
                break
                
            combo_id = f"{request_id}_{month.strftime('%Y%m')}_{country}"
            
            # Submit task with semaphore control
            future = submit_with_semaphore(
                process_month_for_country, bq_ops, month, country, combo_id
            )
            
            pending_futures[(month, country)] = future
        
        # Process results as they complete
        while pending_futures:
            # Check if we need to adjust worker count
            new_worker_count = worker_scaler.get_worker_count()
            
            # Update semaphore count if worker count changed
            if new_worker_count != worker_semaphore.current_value:
                worker_semaphore.adjust_value(new_worker_count)
            
            # Check for completed futures
            done_futures = [f for f in as_completed(list(pending_futures.values()), timeout=5) 
                            if f in list(pending_futures.values())]
            
            # Process completed futures
            for future in done_futures:
                # Find which combination this future belongs to
                for combo, f in list(pending_futures.items()):
                    if f == future:
                        month, country = combo
                        del pending_futures[combo]
                        completed_futures[combo] = future
                        
                        try:
                            if is_shutdown_requested():
                                continue
                                    
                            success, rows = future.result()
                            
                            if success:
                                successful_combinations += 1
                                total_rows += rows
                            else:
                                failed_combinations += 1
                                failed_combination_pairs.append((month, country))
                            
                            logger.info(f"Combination {month}/{country}: {'Success' if success else 'Failed'}, Rows: {rows}")
                        except Exception as e:
                            failed_combinations += 1
                            failed_combination_pairs.append((month, country))
                            logger.error(f"Error processing {month}/{country}: {e}")
                        
                        break
            
            # Check for shutdown
            if is_shutdown_requested():
                logger.warning("Shutdown requested, cancelling pending jobs")
                for future in pending_futures.values():
                    if not future.done():
                        future.cancel()
                break
    
    # Record metrics
    metrics.stop_timer("process_all_combinations")
    metrics.record_value("successful_combinations", successful_combinations)
    metrics.record_value("failed_combinations", failed_combinations)
    metrics.record_value("total_rows_processed", total_rows)
    
    # Record worker scaling history
    scaling_history = worker_scaler.get_adjustment_history()
    if scaling_history:
        logger.info(f"Worker scaling adjustments: {len(scaling_history)}")
    
    return successful_combinations, failed_combinations, total_rows, failed_combination_pairs
    """
    Process a range of months with optional parallelism and adaptive scaling.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
        start_month (date): First month to process
        end_month (date): Last month to process
        parallel (bool, optional): Whether to process months in parallel. Defaults to True.
        request_id (str, optional): Unique request identifier
    
    Returns:
        Tuple[int, int]: Number of successful and failed months
    """
    # Generate a unique request ID if not provided
    request_id = request_id or str(uuid.uuid4())[:8]
    
    # Set context for logging
    logger.correlation_id = request_id
    
    # Generate a list of months to process
    months = []
    current_month = start_month
    while current_month <= end_month:
        months.append(current_month)
        current_month = (current_month.replace(day=1) + relativedelta(months=1))
    
    # Get countries from config
    countries = bq_ops.config["ALLOWED_COUNTRIES"]
    
    logger.info(f"Processing {len(months)} months from {start_month} to {end_month} for {len(countries)} countries")
    
    # Start timer for whole range
    metrics.start_timer("process_month_range")
    
    try:
        if parallel and (len(months) > 1 or len(countries) > 1):
            # Process all combinations in parallel with adaptive scaling
            successful_combinations, failed_combinations, total_rows, failed_pairs = process_all_country_month_combinations(
                bq_ops, months, countries, request_id
            )
            
            # Map success to the month level
            month_success = {}
            for month in months:
                month_success[month] = True
                for country in countries:
                    if (month, country) in failed_pairs:
                        month_success[month] = False
                        break
            
            successful_months = sum(1 for month, success in month_success.items() if success)
            failed_months = len(months) - successful_months
            
        else:
            # Process months sequentially
            logger.info("Using sequential processing")
            successful_months = 0
            failed_months = 0
            total_rows = 0
            
            for i, month in enumerate(months):
                if is_shutdown_requested():
                    logger.warning("Shutdown requested, stopping further processing")
                    break
                
                month_request_id = f"{request_id}_{i}"
                success, rows = process_month(bq_ops, month, month_request_id)
                total_rows += rows
                
                if success:
                    successful_months += 1
                else:
                    failed_months += 1
                    
                logger.info(f"Month {month}: {'Success' if success else 'Failed'}, Rows: {rows}")
    finally:
        # Record metrics
        metrics.stop_timer("process_month_range")
        metrics.record_value("months_processed_successfully", successful_months)
        metrics.record_value("months_processed_with_failure", failed_months)
        metrics.record_value("total_rows_processed", total_rows)
    
    return successful_months, failed_months

def process_all_country_month_combinations(bq_ops, months, countries, request_id):
    """
    Process all country/month combinations with adaptive worker scaling.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
        months (List[date]): Months to process
        countries (List[str]): Countries to process
        request_id (str): Unique request identifier
    
    Returns:
        Tuple[int, int, int, List[Tuple[date, str]]]: 
        - Successful combinations count
        - Failed combinations count
        - Total rows processed
        - List of failed (month, country) pairs
    """
    logger.info(f"Processing {len(months)} months × {len(countries)} countries in parallel")
    
    # Generate all combinations
    combinations = [(month, country) for month in months for country in countries]
    total_combinations = len(combinations)
    
    # Record total work
    metrics.record_value("total_combinations", total_combinations)
    
    # Start timer for whole batch
    metrics.start_timer("process_all_combinations")
    
    # Initialize worker scaler
    worker_scaler = AdaptiveWorkerScaler(bq_ops.config)
    
    # Initialize tracking variables
    successful_combinations = 0
    failed_combinations = 0
    failed_combination_pairs = []
    total_rows = 0
    
    # Track jobs
    pending_futures = {}
    completed_futures = {}
    
    # Start an executor with dynamic worker count
    initial_workers = worker_scaler.get_worker_count()
    logger.info(f"Starting with {initial_workers} workers for {total_combinations} combinations")
    
    # Create a dynamic worker pool with adaptive semaphore
    max_possible_workers = bq_ops.config.get("MAX_WORKERS", 8)
    worker_semaphore = AdaptiveSemaphore(initial_workers)
    
    # Create a fixed-size thread pool with the maximum possible workers
    with ThreadPoolExecutor(max_workers=max_possible_workers) as executor:
        def submit_with_semaphore(fn, *args, **kwargs):
            """Submit a task that acquires and releases the worker semaphore."""
            worker_semaphore.acquire()
            try:
                future = executor.submit(fn, *args, **kwargs)
                # Add callback to release semaphore when done
                future.add_done_callback(lambda f: worker_semaphore.release())
                return future
            except Exception as e:
                worker_semaphore.release()
                raise e
        
        # Submit initial batch of jobs
        for idx, (month, country) in enumerate(combinations):
            if is_shutdown_requested():
                logger.warning("Shutdown requested, stopping submissions")
                break
                
            combo_id = f"{request_id}_{month.strftime('%Y%m')}_{country}"
            
            # Submit task with semaphore control
            future = submit_with_semaphore(
                process_month_for_country, bq_ops, month, country, combo_id
            )
            
            pending_futures[(month, country)] = future
        
        # Process results as they complete
        while pending_futures:
            # Check if we need to adjust worker count
            new_worker_count = worker_scaler.get_worker_count()
            
            # Update semaphore count if worker count changed
            if new_worker_count != worker_semaphore.current_value:
                worker_semaphore.adjust_value(new_worker_count)
            
            # Check for completed futures
            done_futures = [f for f in as_completed(list(pending_futures.values()), timeout=5) 
                            if f in list(pending_futures.values())]
            
            # Process completed futures
            for future in done_futures:
                # Find which combination this future belongs to
                for combo, f in list(pending_futures.items()):
                    if f == future:
                        month, country = combo
                        del pending_futures[combo]
                        completed_futures[combo] = future
                        
                        try:
                            if is_shutdown_requested():
                                continue
                                    
                            success, rows = future.result()
                            
                            if success:
                                successful_combinations += 1
                                total_rows += rows
                            else:
                                failed_combinations += 1
                                failed_combination_pairs.append((month, country))
                            
                            logger.info(f"Combination {month}/{country}: {'Success' if success else 'Failed'}, Rows: {rows}")
                        except Exception as e:
                            failed_combinations += 1
                            failed_combination_pairs.append((month, country))
                            logger.error(f"Error processing {month}/{country}: {e}")
                        
                        break
            
            # Check for shutdown
            if is_shutdown_requested():
                logger.warning("Shutdown requested, cancelling pending jobs")
                for future in pending_futures.values():
                    if not future.done():
                        future.cancel()
                break
    
    # Record metrics
    metrics.stop_timer("process_all_combinations")
    metrics.record_value("successful_combinations", successful_combinations)
    metrics.record_value("failed_combinations", failed_combinations)
    metrics.record_value("total_rows_processed", total_rows)
    
    # Record worker scaling history
    scaling_history = worker_scaler.get_adjustment_history()
    if scaling_history:
        logger.info(f"Worker scaling adjustments: {len(scaling_history)}")
    
    return successful_combinations, failed_combinations, total_rows, failed_combination_pairs

def delete_temp_table(bq_ops, temp_table_id):
    """
    Delete a temporary table with retries.
    
    Args:
        bq_ops (BigQueryOperations): BigQuery operations instance
        temp_table_id (str): ID of the temporary table to delete
    
    Returns:
        bool: Whether deletion was successful
    """
    def retry_delete(max_retries=5, initial_backoff=2, max_backoff=30):
        """
        Inner function to retry table deletion with exponential backoff.
        
        Args:
            max_retries (int): Maximum number of retry attempts
            initial_backoff (int): Initial delay between retries
            max_backoff (int): Maximum delay between retries
        
        Returns:
            bool: Whether deletion was successful
        """
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                bq_ops.delete_table(temp_table_id, not_found_ok=True)
                logger.info(f"Deleted temp table: {temp_table_id}")
                return True
            except Exception as e:
                retry_count += 1
                wait_time = min(initial_backoff * (2 ** (retry_count - 1)), max_backoff)
                logger.warning(
                    f"Failed to delete temp table {temp_table_id} (attempt {retry_count}/{max_retries}): {e}. "
                    f"Retrying in {wait_time}s"
                )
                time.sleep(wait_time)
        
        # If all retries fail
        logger.error(f"All deletion attempts failed for temp table {temp_table_id}")
        metrics.increment_counter("temp_table_deletion_failures")
        return False
