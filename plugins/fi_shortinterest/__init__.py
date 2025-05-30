"""
FI Short Interest Plugin - Entry point registration.
"""

from .fetcher import FiFetcher
from .parser import FiAggParser, FiActParser
from .diff_parser import DiffParser

__all__ = ["FiFetcher", "FiAggParser", "FiActParser", "DiffParser"]
