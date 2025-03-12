# query_templates/__init__.py
# This file ensures that Python treats the directory as a package
# and provides easy access to query generation functions

# Import specific query generation functions
from .italy_query import create_italy_query
from .spain_query import create_spain_query
# When adding a new country, import its query generator here
# from .france_query import create_france_query

# Optional: Add version information specific to query templates
__version__ = "0.1.0"

# Create a dictionary of available query generators for easy access
QUERY_GENERATORS = {
    "ITA": create_italy_query,
    "ESP": create_spain_query,
    # When adding a new country, add its entry here
    # "FRA": create_france_query,
}

# Template for implementing a new country query generator
"""
To add a new country, create a new file named [country_code_lowercase]_query.py with 
a function named create_[country_code_lowercase]_query(bq_ops, temp_table_id) that returns either:
1. A tuple of (query_string, parameters_list)
2. Just the query_string

Example structure:
```python
from google.cloud import bigquery

def create_newcountry_query(bq_ops, temp_table_id):
    # Get country-specific filters
    country_code = "NEW"
    country_filters = bq_ops.config.get(f"COUNTRIES_FILTERS_{country_code}", {})
    
    # Create your query using the filters
    query = f'''CREATE OR REPLACE TABLE {temp_table_id} AS ...'''
    
    # Create parameters
    params = [bigquery.ScalarQueryParameter("theMonth", "DATE", None)]
    
    # Add other parameters based on filters
    
    return query, params
```

Then update this __init__.py file to:
1. Import the new function:
   from .newcountry_query import create_newcountry_query
2. Add it to the QUERY_GENERATORS dictionary:
   "NEW": create_newcountry_query
"""

# You can add any package-level imports or configurations here