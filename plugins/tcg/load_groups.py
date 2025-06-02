# filepath: /Users/lejoon/Projects/modular-scraping-platform/plugins/tcg/load_groups.py
"""
Legacy module for backward compatibility.

This module now imports from the new organized modules:
- data_manager: for data loading and management
- data_collector: for API data fetching
- database: for database operations
"""

# Import all functions from new modules for backward compatibility
from data_manager import load_groups, load_pokemon_sets, load_all, get_pokemon_sets_summary, analyze_price_trends
from data_collector import (
    fetch_and_save_price_history, 
    fetch_all_pokemon_price_history,
    fetch_pokemon_sets_price_history,
    fetch_pokemon_sets_price_history_incremental
)
from database import save_price_history_data, get_latest_bucket_date

# Keep original constants for compatibility
import os
DB_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(DB_DIR, "tcg.db")

if __name__ == "__main__":
    load_all()