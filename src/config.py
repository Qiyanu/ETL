import os
import yaml
import logging
from typing import Dict, Any, Tuple, List, Union

def get_config_path(config_file=None):
    """
    Determine the absolute path to the configuration file.
    
    Args:
        config_file (str, optional): Filename or path to the config file
    
    Returns:
        str: Absolute path to the configuration file
    """
    # Check if a specific config file is provided via environment variable
    if config_file is None:
        config_file = os.environ.get("CONFIG_FILE", "config.yaml")
    
    # If an absolute path is provided, return it
    if os.path.isabs(config_file):
        return config_file
    
    # When main.py is in src folder, project root is one directory up
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(project_root, config_file)
    
    if os.path.exists(config_path):
        return config_path
    
    # Check relative to current working directory
    current_dir_path = os.path.join(os.getcwd(), config_file)
    if os.path.exists(current_dir_path):
        return current_dir_path
    
    # Raise an error with detailed information
    raise FileNotFoundError(f"""
    Configuration file not found. 
    Searched locations:
    1. {os.environ.get('CONFIG_FILE')}
    2. {config_path}
    3. {current_dir_path}
    
    Please ensure the config.yaml file is in the correct location.
    """)

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
        "tables.destination": str,
        "tables.sources.line_table": str,
        "tables.sources.header_table": str,
        "tables.sources.card_table": str,
        "tables.sources.site_table": str,
        "tables.sources.store_table": str,
        "processing.max_workers": int,
        "processing.query_timeout": int,
        "countries.enabled": list,
        "countries.mapping": dict,
        "resilience.enable_retries": bool,
        "resilience.circuit_breaker_threshold": int,
        "resilience.circuit_breaker_timeout": int,
        "resilience.max_retry_attempts": int,
        "job.max_runtime": int,
        "job.timeout_safety_margin": int
    }
    
    # Validate presence and types of required keys
    for key, expected_type in required_keys.items():
        # Use helper function to access nested keys
        value = _get_nested_config_value(config, key)

        if value is None:
            errors.append(f"Missing required configuration key: {key}")
        elif not isinstance(value, expected_type):
            errors.append(f"Invalid type for {key}: expected {expected_type.__name__}, got {type(value).__name__}")
    
    # Validate enabled countries
    enabled_countries = _get_nested_config_value(config, "countries.enabled")
    if isinstance(enabled_countries, list):
        if len(enabled_countries) == 0:
            errors.append("countries.enabled list cannot be empty")
        
        # Ensure all country codes are supported by query templates
        for country in enabled_countries:
            if not isinstance(country, str):
                errors.append(f"Country code must be a string, got: {type(country).__name__}")
                continue
                
            # Check for country filters
            country_filters = _get_nested_config_value(config, f"countries.filters.{country}")
            if country_filters is None:
                errors.append(f"Missing filters for enabled country: {country}")
    
    # Validate country mappings
    country_mapping = _get_nested_config_value(config, "countries.mapping")
    if isinstance(country_mapping, dict) and isinstance(enabled_countries, list):
        # Validate country mapping keys and values
        for country, mapping in country_mapping.items():
            if not isinstance(country, str):
                errors.append(f"Country mapping key must be a string, got: {type(country).__name__}")
            if not isinstance(mapping, str):
                errors.append(f"Country mapping value for '{country}' must be a string, got: {type(mapping).__name__}")
        
        # Check for missing mappings
        for country in enabled_countries:
            if isinstance(country, str) and country not in country_mapping:
                errors.append(f"Missing country mapping for {country}")
    
    # Validate temp_tables settings if present
    temp_tables = _get_nested_config_value(config, "temp_tables")
    if temp_tables is not None:
        if not isinstance(temp_tables, dict):
            errors.append(f"temp_tables must be a dictionary, got: {type(temp_tables).__name__}")
        else:
            if "cleanup_age_hours" in temp_tables and not isinstance(temp_tables["cleanup_age_hours"], (int, float)):
                errors.append(f"temp_tables.cleanup_age_hours must be a number, got: {type(temp_tables['cleanup_age_hours']).__name__}")
            if "expiration_hours" in temp_tables and not isinstance(temp_tables["expiration_hours"], (int, float)):
                errors.append(f"temp_tables.expiration_hours must be a number, got: {type(temp_tables['expiration_hours']).__name__}")
    
    # Validate connection_pool settings if present
    conn_pool = _get_nested_config_value(config, "connection_pool")
    if conn_pool is not None:
        if not isinstance(conn_pool, dict):
            errors.append(f"connection_pool must be a dictionary, got: {type(conn_pool).__name__}")
        else:
            if "shutdown_timeout" in conn_pool and not isinstance(conn_pool["shutdown_timeout"], int):
                errors.append(f"connection_pool.shutdown_timeout must be an integer, got: {type(conn_pool['shutdown_timeout']).__name__}")
            if "max_connections" in conn_pool and not isinstance(conn_pool["max_connections"], int):
                errors.append(f"connection_pool.max_connections must be an integer, got: {type(conn_pool['max_connections']).__name__}")
            
    # Validate query_profiling settings if present
    query_profiling = _get_nested_config_value(config, "query_profiling")
    if query_profiling is not None:
        if not isinstance(query_profiling, dict):
            errors.append(f"query_profiling must be a dictionary, got: {type(query_profiling).__name__}")
        else:
            if "max_profiles" in query_profiling and not isinstance(query_profiling["max_profiles"], int):
                errors.append(f"query_profiling.max_profiles must be an integer, got: {type(query_profiling['max_profiles']).__name__}")
    
    # Return validation result
    return len(errors) == 0, errors

def _get_nested_config_value(config, key_path):
    """
    Access a nested configuration value using dot notation.
    
    Args:
        config: Configuration dictionary
        key_path: Dot-separated path to the config value
        
    Returns:
        The value or None if not found
    """
    keys = key_path.split('.')
    value = config
    
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return None
            
    return value

def _set_nested_config_value(config, key_path, new_value):
    """
    Set a nested configuration value using dot notation.
    
    Args:
        config: Configuration dictionary
        key_path: Dot-separated path to the config value
        new_value: Value to set
    """
    keys = key_path.split('.')
    current = config
    
    # Navigate to the parent of the target key
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    
    # Set the value
    current[keys[-1]] = new_value

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

def _flatten_config(config, prefix=''):
    """
    Convert a nested config dictionary to flat format with dot notation keys.
    
    Args:
        config: Nested configuration dictionary
        prefix: Key prefix for recursion
        
    Returns:
        Flattened configuration dictionary
    """
    result = {}
    for key, value in config.items():
        new_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            # Recursively flatten nested dictionaries
            nested_result = _flatten_config(value, new_key)
            result.update(nested_result)
        else:
            # Add leaf values
            result[new_key] = value
    return result

def _handle_env_overrides(config):
    """
    Apply environment variable overrides to the configuration.
    
    Environment variables override config values using either:
    1. Direct key match (ETL_MAX_WORKERS overrides processing.max_workers)
    2. Dot notation (ETL_PROCESSING_MAX_WORKERS overrides processing.max_workers)
    
    Args:
        config: Configuration dictionary
    """
    # Create flattened version of config for easier lookup
    flat_config = _flatten_config(config)
    
    # Check environment variables for overrides
    for flat_key, value in flat_config.items():
        # Try direct match (e.g., ETL_MAX_WORKERS)
        env_key = f"ETL_{flat_key.replace('.', '_').upper()}"
        env_value = os.environ.get(env_key)
        
        if env_value is not None:
            new_value = _convert_env_value(value, env_value)
            _set_nested_config_value(config, flat_key, new_value)
            logging.info(f"Environment override applied: {env_key} -> {flat_key}")

def load_config(config_file=None) -> Dict[str, Any]:
    """
    Load configuration from YAML file with environment overrides and validation.
    
    Args:
        config_file: Path to the configuration file
        
    Returns:
        Validated configuration dictionary
        
    Raises:
        ValueError: If configuration contains critical errors
    """
    logger = logging.getLogger()
    
    # Start with minimal default configuration
    # Only defaults that are absolutely necessary for startup
    config = {
        "job": {
            "max_runtime": 86400,  # 24 hours
            "timeout_safety_margin": 1800  # 30 minutes
        },
        "logging": {
            "level": "INFO"
        }
    }
    
    # Try to load from YAML file if provided
    if config_file and os.path.exists(config_file):
        try:
            with open(config_file, 'r') as f:
                loaded_config = yaml.safe_load(f)
                
                if not loaded_config:
                    logger.warning(f"Empty configuration file: {config_file}")
                else:
                    logger.info(f"Loaded configuration from {config_file}")
                    config = loaded_config
        except Exception as e:
            logger.error(f"Error loading config file {config_file}: {e}")
            # Fall back to default configuration
    else:
        logger.warning(f"Configuration file not found: {config_file}. Using minimal defaults.")

    # Apply environment variable overrides
    _handle_env_overrides(config)
    
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
    
    # Convert nested config to flat for easy access
    flat_config = {}
    
    def flatten_dict(nested_dict, prefix=""):
        for key, value in nested_dict.items():
            new_key = f"{prefix}_{key}".upper() if prefix else key.upper()
            if isinstance(value, dict):
                flatten_dict(value, new_key)
            else:
                flat_config[new_key] = value
    
    # Flatten to uppercase keys for compatibility with existing code
    flatten_dict(config)
    
    # Add a few convenience keys
    if "TABLES_DESTINATION" in flat_config:
        # Parse destination table into components
        parts = flat_config["TABLES_DESTINATION"].split('.')
        if len(parts) == 3:
            flat_config["DEST_PROJECT"] = parts[0]
            flat_config["DEST_DATASET"] = parts[1]
            flat_config["DEST_TABLE_NAME"] = parts[2]
            flat_config["DEST_TABLE"] = flat_config["TABLES_DESTINATION"]
            
    # Map source tables
    if "TABLES_SOURCES_LINE_TABLE" in flat_config:
        flat_config["SOURCE_LINE_TABLE"] = flat_config["TABLES_SOURCES_LINE_TABLE"]
    if "TABLES_SOURCES_HEADER_TABLE" in flat_config:
        flat_config["SOURCE_HEADER_TABLE"] = flat_config["TABLES_SOURCES_HEADER_TABLE"]
    if "TABLES_SOURCES_CARD_TABLE" in flat_config:
        flat_config["SOURCE_CARD_TABLE"] = flat_config["TABLES_SOURCES_CARD_TABLE"]
    if "TABLES_SOURCES_SITE_TABLE" in flat_config:
        flat_config["SOURCE_SITE_TABLE"] = flat_config["TABLES_SOURCES_SITE_TABLE"]
    if "TABLES_SOURCES_STORE_TABLE" in flat_config:
        flat_config["SOURCE_STORE_TABLE"] = flat_config["TABLES_SOURCES_STORE_TABLE"]
        
    # Map required top-level keys from the nested structure
    # This is key to make BigQueryOperations work with our nested config structure
    if "PROCESSING_QUERY_TIMEOUT" in flat_config:
        flat_config["QUERY_TIMEOUT"] = flat_config["PROCESSING_QUERY_TIMEOUT"]
    if "PROCESSING_MAX_WORKERS" in flat_config:
        flat_config["MAX_WORKERS"] = flat_config["PROCESSING_MAX_WORKERS"]
    if "FILTERS_ALLOWED_COUNTRIES" in flat_config:
        flat_config["ALLOWED_COUNTRIES"] = flat_config["FILTERS_ALLOWED_COUNTRIES"]
    if "RESILIENCE_CIRCUIT_BREAKER_THRESHOLD" in flat_config:
        flat_config["CIRCUIT_BREAKER_THRESHOLD"] = flat_config["RESILIENCE_CIRCUIT_BREAKER_THRESHOLD"]
    if "RESILIENCE_CIRCUIT_BREAKER_TIMEOUT" in flat_config:
        flat_config["CIRCUIT_BREAKER_TIMEOUT"] = flat_config["RESILIENCE_CIRCUIT_BREAKER_TIMEOUT"]
    if "RESILIENCE_MAX_RETRY_ATTEMPTS" in flat_config:
        flat_config["MAX_RETRY_ATTEMPTS"] = flat_config["RESILIENCE_MAX_RETRY_ATTEMPTS"]
    if "COUNTRIES_ENABLED" in flat_config:
        flat_config["ALLOWED_COUNTRIES"] = flat_config["COUNTRIES_ENABLED"]
    if "COUNTRIES_MAPPING" in flat_config:
        flat_config["COUNTRY_MAPPING"] = flat_config["COUNTRIES_MAPPING"]
    if "FILTERS_COUNTRY_MAPPING" in flat_config:
        flat_config["COUNTRY_MAPPING"] = flat_config["FILTERS_COUNTRY_MAPPING"]
    if "FILTERS_EXCLUDED_CHAIN_TYPES" in flat_config:
        flat_config["EXCLUDED_CHAIN_TYPES"] = flat_config["FILTERS_EXCLUDED_CHAIN_TYPES"]
    
    # Map temporary table settings
    if "TEMP_TABLES_CLEANUP_AGE_HOURS" in flat_config:
        flat_config["TEMP_TABLE_CLEANUP_AGE_HOURS"] = flat_config["TEMP_TABLES_CLEANUP_AGE_HOURS"]
    if "TEMP_TABLES_EXPIRATION_HOURS" in flat_config:
        flat_config["TEMP_TABLE_EXPIRATION_HOURS"] = flat_config["TEMP_TABLES_EXPIRATION_HOURS"]
    if "TEMP_TABLES_ADD_DESCRIPTIONS" in flat_config:
        flat_config["TEMP_TABLE_ADD_DESCRIPTIONS"] = flat_config["TEMP_TABLES_ADD_DESCRIPTIONS"]

    # Map query profiling settings
    if "QUERY_PROFILING_MAX_PROFILES" in flat_config:
        flat_config["MAX_QUERY_PROFILES"] = flat_config["QUERY_PROFILING_MAX_PROFILES"]
    if "QUERY_PROFILING_ENABLE_QUERY_PLAN_ANALYSIS" in flat_config:
        flat_config["ENABLE_QUERY_PLAN_ANALYSIS"] = flat_config["QUERY_PROFILING_ENABLE_QUERY_PLAN_ANALYSIS"]
    if "QUERY_PROFILING_SLOW_QUERY_THRESHOLD_SECONDS" in flat_config:
        flat_config["SLOW_QUERY_THRESHOLD_SECONDS"] = flat_config["QUERY_PROFILING_SLOW_QUERY_THRESHOLD_SECONDS"]
    if "QUERY_PROFILING_LARGE_QUERY_THRESHOLD_BYTES" in flat_config:
        flat_config["LARGE_QUERY_THRESHOLD_BYTES"] = flat_config["QUERY_PROFILING_LARGE_QUERY_THRESHOLD_BYTES"]

    # Map connection pool settings
    if "CONNECTION_POOL_SHUTDOWN_TIMEOUT" in flat_config:
        flat_config["CONNECTION_POOL_SHUTDOWN_TIMEOUT"] = flat_config["CONNECTION_POOL_SHUTDOWN_TIMEOUT"]
    if "CONNECTION_POOL_MAX_CONNECTIONS" in flat_config:
        flat_config["MAX_CONNECTIONS"] = flat_config["CONNECTION_POOL_MAX_CONNECTIONS"]
    if "CONNECTION_POOL_MAX_TOTAL_CONNECTIONS" in flat_config:
        flat_config["MAX_TOTAL_CONNECTIONS"] = flat_config["CONNECTION_POOL_MAX_TOTAL_CONNECTIONS"]

    # Map metrics settings
    if "METRICS_MAX_HISTORY_PER_METRIC" in flat_config:
        flat_config["METRICS_MAX_HISTORY"] = flat_config["METRICS_MAX_HISTORY_PER_METRIC"]
        
    # Special handling for country mapping (mapped differently in the flattening)
    if "FILTERS_COUNTRY_MAPPING_ITA" in flat_config and "FILTERS_COUNTRY_MAPPING_ESP" in flat_config:
        flat_config["COUNTRY_MAPPING"] = {
            "ITA": flat_config["FILTERS_COUNTRY_MAPPING_ITA"],
            "ESP": flat_config["FILTERS_COUNTRY_MAPPING_ESP"]
        }
        
    # Add the nested config
    flat_config["_NESTED_CONFIG"] = config
    return flat_config