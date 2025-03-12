import os
import yaml
import logging
from typing import Dict, Any, Tuple, List

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

def _convert_env_value(default_value: Any, env_value: str) -> Any:
    """
    Convert environment variable to appropriate type based on default value.
    
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
        logging.warning(f"Invalid env value: {env_value}, using default. Error: {e}")
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
            # Update corresponding values in the original (nested) config
            keys = flat_key.split('.')
            target = config
            for k in keys[:-1]:
                if k not in target:
                    target[k] = {}
                target = target[k]
            
            # Convert and set the value
            target[keys[-1]] = _convert_env_value(value, env_value)
            logging.info(f"Environment override applied: {env_key} -> {flat_key}")

def load_config(config_file=None) -> Dict[str, Any]:
    """
    Load configuration from YAML file with environment overrides.
    
    Args:
        config_file: Path to the configuration file
        
    Returns:
        Validated configuration dictionary
    """
    logger = logging.getLogger()
    
    # Start with minimal default configuration
    config = {
        "tables": {
            "destination": "c4-marketing-dev-347012.customer_data.customer_data_test",
            "sources": {
                "line_table": "c4-united-datasharing-prd.datasharing_marketing_group_dashboard.a_ww_sales_trx_line",
                "header_table": "c4-united-datasharing-prd.datasharing_marketing_group_dashboard.a_ww_sales_trx_header",
                "card_table": "c4-united-datasharing-prd.datasharing_marketing_group_dashboard.d_card",
                "site_table": "c4-united-datasharing-prd.datasharing_marketing_group_dashboard.d_ww_oc_site",
                "store_table": "c4-united-datasharing-prd.datasharing_marketing_group_dashboard.d_ww_store"
            }
        },
        "processing": {
            "query_timeout": 3600,
            "max_workers": 8,
            "location": "EU"
        },
        "countries": {
            "enabled": ["ITA", "ESP"],
            "mapping": {"ITA": "IT", "ESP": "SP"},
            "filters": {
                "ITA": {"excluded_chain_types": ["SUPECO", "Galerie", "AUTRES"]},
                "ESP": {
                    "excluded_business_brands": ["supeco"],
                    "excluded_ecm_combinations": [{"business_brand": "alcampo", "activity_type": "ECM"}]
                }
            }
        },
        "job": {
            "last_n_months": 3,
            "parallel": True
        },
        "resilience": {
            "max_retry_attempts": 3
        },
        "temp_tables": {
            "cleanup_age_hours": 24,
            "expiration_hours": 24
        }
    }
    
    # Find configuration file
    if config_file is None:
        try:
            config_file = get_config_path()
        except FileNotFoundError as e:
            logger.warning(str(e))
    
    # Try to load from YAML file if provided
    if config_file and os.path.exists(config_file):
        try:
            with open(config_file, 'r') as f:
                loaded_config = yaml.safe_load(f)
                
                if not loaded_config:
                    logger.warning(f"Empty configuration file: {config_file}")
                else:
                    logger.info(f"Loaded configuration from {config_file}")
                    # Merge with defaults
                    config.update(loaded_config)
        except Exception as e:
            logger.error(f"Error loading config file {config_file}: {e}")
    else:
        logger.warning(f"Configuration file not found, using defaults")

    # Apply environment variable overrides
    _handle_env_overrides(config)
    
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
    
    # Add convenient direct access keys
    tables_dest = config.get('tables', {}).get('destination')
    if tables_dest:
        flat_config["DEST_TABLE"] = tables_dest
        parts = tables_dest.split('.')
        if len(parts) == 3:
            flat_config["DEST_PROJECT"] = parts[0]
            flat_config["DEST_DATASET"] = parts[1]
            flat_config["DEST_TABLE_NAME"] = parts[2]
    
    # Add source tables for direct access
    sources = config.get('tables', {}).get('sources', {})
    if sources:
        if 'line_table' in sources:
            flat_config["SOURCE_LINE_TABLE"] = sources['line_table']
        if 'header_table' in sources:
            flat_config["SOURCE_HEADER_TABLE"] = sources['header_table']
        if 'card_table' in sources:
            flat_config["SOURCE_CARD_TABLE"] = sources['card_table']
        if 'site_table' in sources:
            flat_config["SOURCE_SITE_TABLE"] = sources['site_table']
        if 'store_table' in sources:
            flat_config["SOURCE_STORE_TABLE"] = sources['store_table']
    
    # Add countries for direct access
    countries = config.get('countries', {})
    if countries:
        if 'enabled' in countries:
            flat_config["ALLOWED_COUNTRIES"] = countries['enabled']
        if 'mapping' in countries:
            flat_config["COUNTRY_MAPPING"] = countries['mapping']
        if 'filters' in countries:
            for country_code, filters in countries['filters'].items():
                flat_config[f"COUNTRIES_FILTERS_{country_code}"] = filters
    
    # Add processing params
    processing = config.get('processing', {})
    if processing:
        if 'query_timeout' in processing:
            flat_config["QUERY_TIMEOUT"] = processing['query_timeout']
        if 'max_workers' in processing:
            flat_config["MAX_WORKERS"] = processing['max_workers']
        if 'location' in processing:
            flat_config["LOCATION"] = processing['location']
    
    # Add job params
    job = config.get('job', {})
    if job:
        if 'last_n_months' in job:
            flat_config["JOB_LAST_N_MONTHS"] = job['last_n_months']
        if 'parallel' in job:
            flat_config["JOB_PARALLEL"] = job['parallel']
        if 'start_month' in job:
            flat_config["JOB_START_MONTH"] = job['start_month']
        if 'end_month' in job:
            flat_config["JOB_END_MONTH"] = job['end_month']
    
    # Add temp tables params
    temp_tables = config.get('temp_tables', {})
    if temp_tables:
        if 'cleanup_age_hours' in temp_tables:
            flat_config["TEMP_TABLE_CLEANUP_AGE_HOURS"] = temp_tables['cleanup_age_hours']
        if 'expiration_hours' in temp_tables:
            flat_config["TEMP_TABLE_EXPIRATION_HOURS"] = temp_tables['expiration_hours']
    
    # Add resilience params
    resilience = config.get('resilience', {})
    if resilience:
        if 'max_retry_attempts' in resilience:
            flat_config["MAX_RETRY_ATTEMPTS"] = resilience['max_retry_attempts']
    
    # Store the original nested config
    flat_config["_NESTED_CONFIG"] = config
    
    return flat_config