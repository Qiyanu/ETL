import os
import yaml
import logging
from typing import Dict, Any, Tuple, List, Union

def _sanitize_table_reference(table_id: str) -> str:
    """
    Sanitize a BigQuery table reference to prevent injection attacks.
    
    Args:
        table_id: Table ID to sanitize
        
    Returns:
        str: Sanitized table ID
    
    Raises:
        ValueError: If table ID is invalid
    """
    # Validate table reference format
    parts = table_id.strip().split('.')
    if len(parts) != 3:
        raise ValueError(f"Invalid table reference: '{table_id}'. Expected format: 'project.dataset.table'")
    
    # Check each part against allowed patterns
    for i, part in enumerate(parts):
        # BigQuery naming rules: letters, numbers, and underscores
        if not all(c.isalnum() or c == '_' or c == '-' for c in part):
            part_name = ["project", "dataset", "table"][i]
            raise ValueError(f"Invalid {part_name} name '{part}' in table reference")
    
    return '.'.join(parts)

def validate_config(config: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate the ETL configuration for required values and proper types.
    Enhanced with additional security checks and validations.
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        Tuple containing validation result (bool) and list of errors (if any)
    """
    errors = []
    
    # Required configuration keys with expected types
    required_keys = {
        "DEST_TABLE": str,
        "SOURCE_LINE_TABLE": str,
        "SOURCE_HEADER_TABLE": str,
        "SOURCE_CARD_TABLE": str,
        "SOURCE_SITE_TABLE": str,
        "SOURCE_STORE_TABLE": str,
        "MAX_WORKERS": int,
        "QUERY_TIMEOUT": int,
        "ALLOWED_COUNTRIES": list,
        "COUNTRY_MAPPING": dict,
        "EXCLUDED_CHAIN_TYPES": list,
        "ENABLE_RETRIES": bool,
        "CIRCUIT_BREAKER_THRESHOLD": int,
        "CIRCUIT_BREAKER_TIMEOUT": int,
        "MAX_RETRY_ATTEMPTS": int,
        "JOB_MAX_RUNTIME": int,
        "JOB_TIMEOUT_SAFETY_MARGIN": int
    }
    
    # Validate presence and types of required keys
    for key, expected_type in required_keys.items():
        if key not in config:
            errors.append(f"Missing required configuration key: {key}")
        elif not isinstance(config[key], expected_type):
            errors.append(f"Invalid type for {key}: expected {expected_type.__name__}, got {type(config[key]).__name__}")
    
    # Validate ALLOWED_COUNTRIES is not empty if present
    if "ALLOWED_COUNTRIES" in config and isinstance(config["ALLOWED_COUNTRIES"], list):
        if len(config["ALLOWED_COUNTRIES"]) == 0:
            errors.append("ALLOWED_COUNTRIES list cannot be empty")
        
        # Ensure all country codes are supported by query templates
        for country in config["ALLOWED_COUNTRIES"]:
            if not isinstance(country, str):
                errors.append(f"Country code must be a string, got: {type(country).__name__}")
                continue
                
            if country not in ["ITA", "ESP"]:
                errors.append(f"Unsupported country code: {country}. Only ITA and ESP are supported.")
            
    # Validate MAX_WORKERS is reasonable
    if "MAX_WORKERS" in config and isinstance(config["MAX_WORKERS"], int):
        if config["MAX_WORKERS"] < 1:
            errors.append("MAX_WORKERS must be at least 1")
        elif config["MAX_WORKERS"] > 32:
            errors.append("MAX_WORKERS is suspiciously high (>32), please verify")
    
    # Validate QUERY_TIMEOUT is reasonable
    if "QUERY_TIMEOUT" in config and isinstance(config["QUERY_TIMEOUT"], int):
        if config["QUERY_TIMEOUT"] < 10:
            errors.append("QUERY_TIMEOUT must be at least 10 seconds")
        elif config["QUERY_TIMEOUT"] > 86400:
            errors.append("QUERY_TIMEOUT is too large (>24 hours), please verify")
    
    # Validate table names have the proper format and sanitize them
    table_keys = ["DEST_TABLE", "SOURCE_LINE_TABLE", "SOURCE_HEADER_TABLE", 
                 "SOURCE_CARD_TABLE", "SOURCE_SITE_TABLE", "SOURCE_STORE_TABLE"]
                 
    for key in table_keys:
        if key in config and isinstance(config[key], str):
            try:
                # Sanitize and validate table references
                config[key] = _sanitize_table_reference(config[key])
            except ValueError as e:
                errors.append(f"Invalid {key}: {str(e)}")
    
    # Validate memory settings if present
    if "MEMORY_PER_WORKER_MB" in config:
        if not isinstance(config["MEMORY_PER_WORKER_MB"], (int, float)):
            errors.append("MEMORY_PER_WORKER_MB must be a number")
        elif config["MEMORY_PER_WORKER_MB"] < 64:
            errors.append("MEMORY_PER_WORKER_MB is too small (<64MB)")
        elif config["MEMORY_PER_WORKER_MB"] > 16384:
            errors.append("MEMORY_PER_WORKER_MB is suspiciously high (>16GB), please verify")
    
    # Validate circuit breaker settings
    if "CIRCUIT_BREAKER_THRESHOLD" in config:
        if not isinstance(config["CIRCUIT_BREAKER_THRESHOLD"], int):
            errors.append("CIRCUIT_BREAKER_THRESHOLD must be an integer")
        elif config["CIRCUIT_BREAKER_THRESHOLD"] < 1:
            errors.append("CIRCUIT_BREAKER_THRESHOLD must be at least 1")
    
    if "CIRCUIT_BREAKER_TIMEOUT" in config:
        if not isinstance(config["CIRCUIT_BREAKER_TIMEOUT"], int):
            errors.append("CIRCUIT_BREAKER_TIMEOUT must be an integer")
        elif config["CIRCUIT_BREAKER_TIMEOUT"] < 5:
            errors.append("CIRCUIT_BREAKER_TIMEOUT must be at least 5 seconds")
            
    # Validate country mapping
    if ("COUNTRY_MAPPING" in config and 
        isinstance(config["COUNTRY_MAPPING"], dict) and 
        "ALLOWED_COUNTRIES" in config and 
        isinstance(config["ALLOWED_COUNTRIES"], list)):
        
        # Validate country mapping keys and values
        for country, mapping in config["COUNTRY_MAPPING"].items():
            if not isinstance(country, str):
                errors.append(f"Country mapping key must be a string, got: {type(country).__name__}")
            if not isinstance(mapping, str):
                errors.append(f"Country mapping value for '{country}' must be a string, got: {type(mapping).__name__}")
        
        # Check for missing mappings
        for country in config["ALLOWED_COUNTRIES"]:
            if isinstance(country, str) and country not in config["COUNTRY_MAPPING"]:
                errors.append(f"Missing country mapping for {country}")
                
    # Validate date settings with improved error messages
    if "START_MONTH" in config and config["START_MONTH"]:
        try:
            from datetime import datetime
            datetime.strptime(config["START_MONTH"], '%Y-%m-%d')
        except ValueError as e:
            errors.append(f"START_MONTH must be in YYYY-MM-DD format: {str(e)}")
        except TypeError:
            errors.append(f"START_MONTH must be a string, got: {type(config['START_MONTH']).__name__}")
            
    if "END_MONTH" in config and config["END_MONTH"]:
        try:
            from datetime import datetime
            datetime.strptime(config["END_MONTH"], '%Y-%m-%d')
        except ValueError as e:
            errors.append(f"END_MONTH must be in YYYY-MM-DD format: {str(e)}")
        except TypeError:
            errors.append(f"END_MONTH must be a string, got: {type(config['END_MONTH']).__name__}")
            
    if "LAST_N_MONTHS" in config and config["LAST_N_MONTHS"]:
        try:
            last_n = int(config["LAST_N_MONTHS"])
            if last_n < 1:
                errors.append("LAST_N_MONTHS must be at least 1")
            elif last_n > 36:
                errors.append("LAST_N_MONTHS is suspiciously large (>36), please verify")
        except (ValueError, TypeError) as e:
            errors.append(f"LAST_N_MONTHS must be an integer: {str(e)}")
    
    # Check for potentially dangerous settings
    if "MAXIMUM_BYTES_BILLED" in config:
        try:
            max_bytes = int(config["MAXIMUM_BYTES_BILLED"])
            # Check if unusually high (> 10TB)
            if max_bytes > 10 * 1024**4:  
                errors.append(f"MAXIMUM_BYTES_BILLED is suspiciously high: {max_bytes} bytes")
        except (ValueError, TypeError):
            errors.append("MAXIMUM_BYTES_BILLED must be a number")
    
    # Validate worker history limits are set
    if "MAX_HISTORY_SIZE" not in config:
        config["MAX_HISTORY_SIZE"] = 100  # Set a default
        
    # Return validation result
    return len(errors) == 0, errors

def _convert_env_value(default_value: Any, env_value: str) -> Any:
    """
    Convert environment variable to appropriate type based on default value.
    With improved error handling and type safety.
    
    Args:
        default_value: Original default value defining the expected type
        env_value: Environment variable value as string
        
    Returns:
        Converted value matching the type of default_value
    """
    try:
        if default_value is None:
            return env_value
        
        default_type = type(default_value)
        
        if default_type == list:
            return env_value.split(",")
        elif default_type == bool:
            return env_value.lower() in ('true', 'yes', '1', 't', 'y')
        elif default_type == int:
            return int(env_value)
        elif default_type == float:
            return float(env_value)
        elif default_type == dict:
            # For dictionaries, use JSON format in environment variable
            import json
            try:
                result = json.loads(env_value)
                if not isinstance(result, dict):
                    logging.warning(f"JSON parsed from environment variable is not a dictionary: {env_value}")
                    return default_value
                return result
            except json.JSONDecodeError as e:
                logging.warning(f"Invalid JSON in environment variable: {env_value}. Error: {e}")
                return default_value
        else:
            return env_value
    except (ValueError, TypeError) as e:
        logging.warning(f"Invalid env value for key: {env_value}, using default. Error: {e}")
        return default_value

def load_config(config_file=None) -> Dict[str, Any]:
    """
    Load configuration with validation.
    
    Args:
        config_file: Path to the configuration file
        
    Returns:
        Validated configuration dictionary
        
    Raises:
        ValueError: If configuration contains critical errors
    """
    logger = logging.getLogger()
    
    # Default configuration
    config = {
        "DEST_TABLE": "c4-marketing-dev-347012.customer_data.customer_data_test",
        # Source tables (shared by all countries)
        "SOURCE_LINE_TABLE": "c4-united-datasharing-prd.datasharing_marketing_group_dashboard.a_ww_sales_trx_line",
        "SOURCE_HEADER_TABLE": "c4-united-datasharing-prd.datasharing_marketing_group_dashboard.a_ww_sales_trx_header",
        "SOURCE_CARD_TABLE": "c4-united-datasharing-prd.datasharing_marketing_group_dashboard.d_card",
        "SOURCE_SITE_TABLE": "c4-united-datasharing-prd.datasharing_marketing_group_dashboard.d_ww_oc_site",
        "SOURCE_STORE_TABLE": "c4-united-datasharing-prd.datasharing_marketing_group_dashboard.d_ww_store",
        "MAX_WORKERS": 8,  # Increased from 4 to 8 for better parallelism
        "QUERY_TIMEOUT": 3600,
        "CHUNK_SIZE": 20,
        "ALLOWED_COUNTRIES": ["ITA", "ESP"],
        "COUNTRY_MAPPING": {"ITA": "IT", "ESP": "SP"},
        "EXCLUDED_CHAIN_TYPES": ["SUPECO", "Galerie", "AUTRES"],
        "SPAIN_FILTERS": {
            "excluded_business_brands": ["supeco"],
            "excluded_ecm_combinations": [
                {"delivery_channel": "PICKUP_STORE"},
                {"business_service": "Home Delivery Non Food"}
            ]
        },
        "ENABLE_RETRIES": True,
        "MAX_RETRY_ATTEMPTS": 5,
        "CIRCUIT_BREAKER_THRESHOLD": 5,
        "CIRCUIT_BREAKER_TIMEOUT": 300,
        "LOCATION": "EU",
        "JOB_MAX_RUNTIME": 86400,  # Maximum job runtime in seconds (24 hours)
        "JOB_TIMEOUT_SAFETY_MARGIN": 1800,  # 30 minutes safety margin before job timeout
        "LOG_LEVEL": "INFO",
        # Temp table management
        "TEMP_TABLE_EXPIRATION_HOURS": 24,
        "TEMP_TABLE_ADD_DESCRIPTIONS": True,
        # Job-specific parameters with environment variable overrides
        "START_MONTH": os.environ.get("START_MONTH", None),  # Format: YYYY-MM-DD
        "END_MONTH": os.environ.get("END_MONTH", None),  # Format: YYYY-MM-DD
        "LAST_N_MONTHS": os.environ.get("LAST_N_MONTHS", None),  # Number of months to process
        "PARALLEL": os.environ.get("PARALLEL", "true").lower() in ('true', 'yes', '1'),
        
        # Additional default config values for new features
        "MAX_CONNECTIONS": 10,
        "CONNECTION_POOL_SHUTDOWN_TIMEOUT": 60,  # 60 seconds to wait for active connections
        "SLOW_QUERY_THRESHOLD_SECONDS": 420,  # 7 minutes
        "LARGE_QUERY_THRESHOLD_BYTES": 20 * 1024 * 1024 * 1024,  # 20GB
        "MIN_WORKERS": 1,
        "INITIAL_WORKERS": 4,
        "TARGET_CPU_USAGE": 0.7,  # 70%
        "TARGET_MEMORY_USAGE": 0.6,  # 60%
        "MEMORY_PER_WORKER_MB": 512,  # 512MB per worker
        "MEMORY_SAFETY_FACTOR": 1.2,  # 20% safety margin for memory allocation
        "SERVICE_NAME": "customer-data-etl",
        # Maximum history size for worker scaling adjustments
        "MAX_HISTORY_SIZE": 100,
        # Retry parameters
        "RETRY_INITIAL_DELAY_MS": 1000,  # 1 second
        "RETRY_MAX_DELAY_MS": 60000,  # 60 seconds
        "RETRY_MULTIPLIER": 2.0,
    }
    
    # Try to load from YAML file if provided
    loaded_config = {}
    if config_file and os.path.exists(config_file):
        try:
            with open(config_file, 'r') as f:
                loaded_config = yaml.safe_load(f)
                
                if not loaded_config:
                    logger.warning(f"Empty configuration file: {config_file}")
                else:
                    logger.info(f"Loaded configuration from {config_file}")
                    
        except Exception as e:
            logger.error(f"Error loading config file {config_file}: {e}")
    
    # Handle configuration transformations from YAML structure to our format
    if loaded_config:
        # Handle table section
        if 'tables' in loaded_config:
            if 'destination' in loaded_config['tables']:
                config["DEST_TABLE"] = loaded_config['tables']['destination']
                
            if 'sources' in loaded_config['tables']:
                sources = loaded_config['tables']['sources']
                if 'line_table' in sources:
                    config["SOURCE_LINE_TABLE"] = sources['line_table']
                if 'header_table' in sources:
                    config["SOURCE_HEADER_TABLE"] = sources['header_table']
                if 'card_table' in sources:
                    config["SOURCE_CARD_TABLE"] = sources['card_table']
                if 'site_table' in sources:
                    config["SOURCE_SITE_TABLE"] = sources['site_table']
                if 'store_table' in sources:
                    config["SOURCE_STORE_TABLE"] = sources['store_table']
        
        # Handle processing section
        if 'processing' in loaded_config:
            if 'max_workers' in loaded_config['processing']:
                config["MAX_WORKERS"] = loaded_config['processing']['max_workers']
            if 'query_timeout' in loaded_config['processing']:
                config["QUERY_TIMEOUT"] = loaded_config['processing']['query_timeout']
            if 'location' in loaded_config['processing']:
                config["LOCATION"] = loaded_config['processing']['location']
            if 'max_connections' in loaded_config['processing']:
                config["MAX_CONNECTIONS"] = loaded_config['processing']['max_connections']
            if 'memory_per_worker_mb' in loaded_config['processing']:
                config["MEMORY_PER_WORKER_MB"] = loaded_config['processing']['memory_per_worker_mb']
            if 'target_memory_usage' in loaded_config['processing']:
                config["TARGET_MEMORY_USAGE"] = loaded_config['processing']['target_memory_usage']
            if 'connection_pool_shutdown_timeout' in loaded_config['processing']:
                config["CONNECTION_POOL_SHUTDOWN_TIMEOUT"] = loaded_config['processing']['connection_pool_shutdown_timeout']
            if 'memory_scale_up_threshold' in loaded_config['processing']:
                config["MEMORY_SCALE_UP_THRESHOLD"] = loaded_config['processing']['memory_scale_up_threshold']
            if 'memory_scale_down_threshold' in loaded_config['processing']:
                config["MEMORY_SCALE_DOWN_THRESHOLD"] = loaded_config['processing']['memory_scale_down_threshold']
        
        # Handle BigQuery section
        if 'bigquery' in loaded_config:
            if 'use_query_cache' in loaded_config['bigquery']:
                config["USE_QUERY_CACHE"] = loaded_config['bigquery']['use_query_cache']
            if 'create_disposition' in loaded_config['bigquery']:
                config["CREATE_DISPOSITION"] = loaded_config['bigquery']['create_disposition']
            if 'write_disposition' in loaded_config['bigquery']:
                config["WRITE_DISPOSITION"] = loaded_config['bigquery']['write_disposition']
            if 'priority' in loaded_config['bigquery']:
                config["PRIORITY"] = loaded_config['bigquery']['priority']
            if 'maximum_bytes_billed' in loaded_config['bigquery']:
                config["MAXIMUM_BYTES_BILLED"] = loaded_config['bigquery']['maximum_bytes_billed']
            if 'query_batch_size' in loaded_config['bigquery']:
                config["QUERY_BATCH_SIZE"] = loaded_config['bigquery']['query_batch_size']
        
        # Handle filters section
        if 'filters' in loaded_config:
            if 'allowed_countries' in loaded_config['filters']:
                config["ALLOWED_COUNTRIES"] = loaded_config['filters']['allowed_countries']
            if 'country_mapping' in loaded_config['filters']:
                config["COUNTRY_MAPPING"] = loaded_config['filters']['country_mapping']
            if 'excluded_chain_types' in loaded_config['filters']:
                config["EXCLUDED_CHAIN_TYPES"] = loaded_config['filters']['excluded_chain_types']
            if 'spain_filters' in loaded_config['filters']:
                config["SPAIN_FILTERS"] = loaded_config['filters']['spain_filters']
        
        # Handle resilience section
        if 'resilience' in loaded_config:
            if 'enable_retries' in loaded_config['resilience']:
                config["ENABLE_RETRIES"] = loaded_config['resilience']['enable_retries']
            if 'max_retry_attempts' in loaded_config['resilience']:
                config["MAX_RETRY_ATTEMPTS"] = loaded_config['resilience']['max_retry_attempts']
            if 'circuit_breaker_threshold' in loaded_config['resilience']:
                config["CIRCUIT_BREAKER_THRESHOLD"] = loaded_config['resilience']['circuit_breaker_threshold']
            if 'circuit_breaker_timeout' in loaded_config['resilience']:
                config["CIRCUIT_BREAKER_TIMEOUT"] = loaded_config['resilience']['circuit_breaker_timeout']
            if 'retry_initial_delay_ms' in loaded_config['resilience']:
                config["RETRY_INITIAL_DELAY_MS"] = loaded_config['resilience']['retry_initial_delay_ms']
            if 'retry_max_delay_ms' in loaded_config['resilience']:
                config["RETRY_MAX_DELAY_MS"] = loaded_config['resilience']['retry_max_delay_ms']
            if 'retry_multiplier' in loaded_config['resilience']:
                config["RETRY_MULTIPLIER"] = loaded_config['resilience']['retry_multiplier']
        
        # Handle temp tables section
        if 'temp_tables' in loaded_config:
            if 'expiration_hours' in loaded_config['temp_tables']:
                config["TEMP_TABLE_EXPIRATION_HOURS"] = loaded_config['temp_tables']['expiration_hours']
            if 'add_descriptions' in loaded_config['temp_tables']:
                config["TEMP_TABLE_ADD_DESCRIPTIONS"] = loaded_config['temp_tables']['add_descriptions']
            if 'dedicated_dataset' in loaded_config['temp_tables']:
                config["TEMP_DEDICATED_DATASET"] = loaded_config['temp_tables']['dedicated_dataset']
                
        # Handle logging section
        if 'logging' in loaded_config:
            if 'level' in loaded_config['logging']:
                config["LOG_LEVEL"] = loaded_config['logging']['level']
            if 'enable_structured_logging' in loaded_config['logging']:
                config["ENABLE_STRUCTURED_LOGGING"] = loaded_config['logging']['enable_structured_logging']
            if 'include_query_stats' in loaded_config['logging']:
                config["INCLUDE_QUERY_STATS"] = loaded_config['logging']['include_query_stats']
        
        # Handle job section
        if 'job' in loaded_config:
            if 'max_runtime' in loaded_config['job']:
                config["JOB_MAX_RUNTIME"] = loaded_config['job']['max_runtime']
            if 'timeout_safety_margin' in loaded_config['job']:
                config["JOB_TIMEOUT_SAFETY_MARGIN"] = loaded_config['job']['timeout_safety_margin']
            if 'start_month' in loaded_config['job']:
                config["START_MONTH"] = loaded_config['job']['start_month']
            if 'end_month' in loaded_config['job']:
                config["END_MONTH"] = loaded_config['job']['end_month']
            if 'last_n_months' in loaded_config['job']:
                config["LAST_N_MONTHS"] = loaded_config['job']['last_n_months']
            if 'parallel' in loaded_config['job']:
                config["PARALLEL"] = loaded_config['job']['parallel']
            if 'memory_limit_gb' in loaded_config['job']:
                # Convert to MB for consistency
                config["MEMORY_LIMIT_MB"] = loaded_config['job']['memory_limit_gb'] * 1024
            if 'cpu_limit' in loaded_config['job']:
                config["CPU_LIMIT"] = loaded_config['job']['cpu_limit']
    
    # Override with environment variables
    for key in config.copy():
        env_value = os.environ.get(key)
        if env_value is not None:
            config[key] = _convert_env_value(config[key], env_value)
    
    # Validate configuration
    is_valid, errors = validate_config(config)
    if not is_valid:
        error_message = "\n  - ".join(["Configuration validation errors:"] + errors)
        logger.error(error_message)
        
        # Determine if errors are fatal
        fatal_errors = [e for e in errors if "Missing required" in e]
        if fatal_errors:
            raise ValueError(error_message)
        else:
            logger.warning("Using configuration despite warnings")
    
    # Parse destination table into components
    parts = config["DEST_TABLE"].split('.')
    if len(parts) == 3:
        config["DEST_PROJECT"] = parts[0]
        config["DEST_DATASET"] = parts[1]
        config["DEST_TABLE_NAME"] = parts[2]
    
    return config