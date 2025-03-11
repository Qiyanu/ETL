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
        "tables.destination": str,
        "tables.sources.line_table": str,
        "tables.sources.header_table": str,
        "tables.sources.card_table": str,
        "tables.sources.site_table": str,
        "tables.sources.store_table": str,
        "processing.max_workers": int,
        "processing.query_timeout": int,
        "filters.allowed_countries": list,
        "filters.country_mapping": dict,
        "filters.excluded_chain_types": list,
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
    
    # Validate ALLOWED_COUNTRIES is not empty if present
    allowed_countries = _get_nested_config_value(config, "filters.allowed_countries")
    if isinstance(allowed_countries, list):
        if len(allowed_countries) == 0:
            errors.append("filters.allowed_countries list cannot be empty")
        
        # Ensure all country codes are supported by query templates
        for country in allowed_countries:
            if not isinstance(country, str):
                errors.append(f"Country code must be a string, got: {type(country).__name__}")
                continue
                
            if country not in ["ITA", "ESP"]:
                errors.append(f"Unsupported country code: {country}. Only ITA and ESP are supported.")
    
    # Validate MAX_WORKERS is reasonable
    max_workers = _get_nested_config_value(config, "processing.max_workers")
    if isinstance(max_workers, int):
        if max_workers < 1:
            errors.append("processing.max_workers must be at least 1")
        elif max_workers > 32:
            errors.append("processing.max_workers is suspiciously high (>32), please verify")
    
    # Validate QUERY_TIMEOUT is reasonable
    query_timeout = _get_nested_config_value(config, "processing.query_timeout")
    if isinstance(query_timeout, int):
        if query_timeout < 10:
            errors.append("processing.query_timeout must be at least 10 seconds")
        elif query_timeout > 86400:
            errors.append("processing.query_timeout is too large (>24 hours), please verify")
    
    # Validate table names have the proper format and sanitize them
    table_keys = [
        "tables.destination", 
        "tables.sources.line_table", 
        "tables.sources.header_table", 
        "tables.sources.card_table", 
        "tables.sources.site_table", 
        "tables.sources.store_table"
    ]
                 
    for key in table_keys:
        value = _get_nested_config_value(config, key)
        if isinstance(value, str):
            try:
                # Sanitize and validate table references
                sanitized_value = _sanitize_table_reference(value)
                # Update the value in the config
                _set_nested_config_value(config, key, sanitized_value)
            except ValueError as e:
                errors.append(f"Invalid {key}: {str(e)}")
    
    # Validate memory settings if present
    memory_per_worker = _get_nested_config_value(config, "processing.memory_per_worker_mb")
    if memory_per_worker is not None:
        if not isinstance(memory_per_worker, (int, float)):
            errors.append("processing.memory_per_worker_mb must be a number")
        elif memory_per_worker < 64:
            errors.append("processing.memory_per_worker_mb is too small (<64MB)")
        elif memory_per_worker > 16384:
            errors.append("processing.memory_per_worker_mb is suspiciously high (>16GB), please verify")
    
    # Validate circuit breaker settings
    cb_threshold = _get_nested_config_value(config, "resilience.circuit_breaker_threshold")
    if cb_threshold is not None:
        if not isinstance(cb_threshold, int):
            errors.append("resilience.circuit_breaker_threshold must be an integer")
        elif cb_threshold < 1:
            errors.append("resilience.circuit_breaker_threshold must be at least 1")
    
    cb_timeout = _get_nested_config_value(config, "resilience.circuit_breaker_timeout")
    if cb_timeout is not None:
        if not isinstance(cb_timeout, int):
            errors.append("resilience.circuit_breaker_timeout must be an integer")
        elif cb_timeout < 5:
            errors.append("resilience.circuit_breaker_timeout must be at least 5 seconds")
            
    # Validate country mapping
    country_mapping = _get_nested_config_value(config, "filters.country_mapping")
    if isinstance(country_mapping, dict) and isinstance(allowed_countries, list):
        # Validate country mapping keys and values
        for country, mapping in country_mapping.items():
            if not isinstance(country, str):
                errors.append(f"Country mapping key must be a string, got: {type(country).__name__}")
            if not isinstance(mapping, str):
                errors.append(f"Country mapping value for '{country}' must be a string, got: {type(mapping).__name__}")
        
        # Check for missing mappings
        for country in allowed_countries:
            if isinstance(country, str) and country not in country_mapping:
                errors.append(f"Missing country mapping for {country}")
                
    # Validate date settings with improved error messages
    start_month = _get_nested_config_value(config, "job.start_month")
    if start_month:
        try:
            from datetime import datetime
            datetime.strptime(start_month, '%Y-%m-%d')
        except ValueError as e:
            errors.append(f"job.start_month must be in YYYY-MM-DD format: {str(e)}")
        except TypeError:
            errors.append(f"job.start_month must be a string, got: {type(start_month).__name__}")
            
    end_month = _get_nested_config_value(config, "job.end_month")
    if end_month:
        try:
            from datetime import datetime
            datetime.strptime(end_month, '%Y-%m-%d')
        except ValueError as e:
            errors.append(f"job.end_month must be in YYYY-MM-DD format: {str(e)}")
        except TypeError:
            errors.append(f"job.end_month must be a string, got: {type(end_month).__name__}")
            
    last_n_months = _get_nested_config_value(config, "job.last_n_months")
    if last_n_months:
        try:
            last_n = int(last_n_months)
            if last_n < 1:
                errors.append("job.last_n_months must be at least 1")
            elif last_n > 36:
                errors.append("job.last_n_months is suspiciously large (>36), please verify")
        except (ValueError, TypeError) as e:
            errors.append(f"job.last_n_months must be an integer: {str(e)}")
    
    # Check for potentially dangerous settings
    max_bytes = _get_nested_config_value(config, "bigquery.maximum_bytes_billed")
    if max_bytes is not None:
        try:
            max_bytes_val = int(max_bytes)
            # Check if unusually high (> 10TB)
            if max_bytes_val > 10 * 1024**4:  
                errors.append(f"bigquery.maximum_bytes_billed is suspiciously high: {max_bytes_val} bytes")
        except (ValueError, TypeError):
            errors.append("bigquery.maximum_bytes_billed must be a number")
    
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
    
    # Add the nested config
    flat_config["_NESTED_CONFIG"] = config
    
    return flat_config