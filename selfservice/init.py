# src/mypackage/__init__.py

# Package version
__version__ = "1.0.0"

# Public API exposure
from .module1 import important_function, ImportantClass
from .module2 import useful_utility

# Define what gets imported with `from mypackage import *`
__all__ = [
    "important_function",
    "ImportantClass",
    "useful_utility",
]

# Optional: Package-wide logger (non-intrusive)
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
