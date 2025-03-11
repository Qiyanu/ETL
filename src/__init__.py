# src/__init__.py
# This file ensures that Python treats the directory as a package
# and can import modules from this directory

# Optional: Add version information
__version__ = "0.1.0"

# Optional: Import key components for easier access
from .config import load_config
from .logging_utils import logger
from .metrics import metrics

# You can add any package-level imports or configurations here