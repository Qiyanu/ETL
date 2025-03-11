# query_templates/__init__.py
# This file ensures that Python treats the directory as a package
# and provides easy access to query generation functions

# Import specific query generation functions
from .italy_query import create_italy_query
from .spain_query import create_spain_query

# Optional: Add version information specific to query templates
__version__ = "0.1.0"

# Create a dictionary of available query generators for easy access
QUERY_GENERATORS = {
    "ITA": create_italy_query,
    "ESP": create_spain_query
}

# You can add any package-level imports or configurations here