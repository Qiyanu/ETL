import os
import yaml
import logging

def load_config(config_file=None):
    """Load configuration with sensible defaults."""
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
        "MAX_RETRY_ATTEMPTS": 5,  # Increased from 3 for better reliability
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
    }
    
    # Try to load from YAML file if provided
    if config_file and os.path.exists(config_file):
        try:
            with open(config_file, 'r') as f:
                yaml_config = yaml.safe_load(f)
                
                # Deep merge YAML config with default config
                _deep_merge(config, yaml_config)
                
                logging.info(f"Loaded configuration from {config_file}")
        except Exception as e:
            logging.error(f"Error loading config file {config_file}: {e}")
    
    # Override with environment variables (type conversion included)
    for key in config.copy():
        env_value = os.environ.get(key)
        if env_value is not None:
            config[key] = _convert_env_value(config[key], env_value)
    
    # Parse destination table into components
    parts = config["DEST_TABLE"].split('.')
    if len(parts) == 3:
        config["DEST_PROJECT"] = parts[0]
        config["DEST_DATASET"] = parts[1]
        config["DEST_TABLE_NAME"] = parts[2]
    
    return config

def _deep_merge(original, update):
    """Recursively merge two dictionaries."""
    for key, value in update.items():
        if isinstance(value, dict):
            # If the key exists and it's a dict, recursively merge
            original[key] = _deep_merge(original.get(key, {}), value)
        else:
            # Otherwise, update or add the key
            original[key] = value
    return original

def _convert_env_value(default_value, env_value):
    """Convert environment variable to appropriate type."""
    default_type = type(default_value)
    try:
        if default_type == list:
            return env_value.split(",")
        elif default_type == bool:
            return env_value.lower() in ('true', 'yes', '1')
        elif default_type == int:
            return int(env_value)
        elif default_type == float:
            return float(env_value)
        else:
            return env_value
    except (ValueError, TypeError) as e:
        logging.warning(f"Invalid env value for key: {env_value}, using default. Error: {e}")
        return default_value