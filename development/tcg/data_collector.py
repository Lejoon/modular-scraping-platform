"""
Data collection module for TCG price data.

This module handles fetching data from external APIs like TCGPlayer and TCGcsv.
"""

import os
import time
import requests
from typing import Optional, Dict, Any

# Database path - inherit from original structure
DB_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(DB_DIR, "tcg.db")


class DataCollector:
    """Handles data collection from external APIs."""
    
    def __init__(self, default_headers: Optional[Dict[str, str]] = None):
        """Initialize the data collector with optional default headers."""
        self.default_headers = default_headers or {
            "accept": "application/json, text/plain, */*",
            "origin": "https://www.tcgplayer.com", 
            "referer": "https://www.tcgplayer.com/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        }
    
    def fetch_tcg_groups(self, category_id: int) -> Dict[str, Any]:
        """
        Fetch TCG groups from tcgcsv.com API.
        
        Args:
            category_id: Category ID (79 = Star Wars: Unlimited, 3 = Pokemon)
            
        Returns:
            JSON response containing group data
            
        Raises:
            requests.RequestException: If the API request fails
        """
        url = f"https://tcgcsv.com/tcgplayer/{category_id}/groups"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    def fetch_price_history(self, product_id: int, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Fetch price history data from TCGPlayer API.
        
        Args:
            product_id: The TCGPlayer product ID
            headers: Optional custom headers for the request
            
        Returns:
            JSON response containing price history data
            
        Raises:
            requests.RequestException: If the API request fails
        """
        if headers is None:
            headers = self.default_headers
        
        url = f"https://infinite-api.tcgplayer.com/price/history/{product_id}/detailed?range=annual"
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    
    def fetch_pokemon_sets_price_history(self, delay_seconds: float = 1, skip_existing: bool = True) -> None:
        """
        Fetch price history for all Pokemon sets with comprehensive error handling.
        
        Args:
            delay_seconds: Delay between API calls to avoid rate limiting
            skip_existing: If True, skip products that already have recent data
        """
        from database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Get all Pokemon sets with their product IDs
        sets = db_manager.execute_query("""
            SELECT id, set_name, booster_product_id, booster_box_product_id
            FROM pokemon_sets
            WHERE booster_product_id IS NOT NULL OR booster_box_product_id IS NOT NULL
            ORDER BY id
        """)
        
        total_sets = len(sets)
        processed_count = 0
        success_count = 0
        error_count = 0
        
        print(f"Starting price history fetch for {total_sets} Pokemon sets")
        print(f"Delay between requests: {delay_seconds}s")
        print(f"Skip existing data: {skip_existing}")
        print("=" * 80)
        
        for set_id, set_name, booster_id, booster_box_id in sets:
            processed_count += 1
            print(f"\n[{processed_count}/{total_sets}] Processing: {set_name}")
            
            # Process booster product
            if booster_id:
                try:
                    if skip_existing:
                        latest_date = db_manager.get_latest_bucket_date(booster_id)
                        if latest_date and latest_date >= '2025-06-01':  # Has recent data
                            print(f"  ✓ Booster (ID: {booster_id}) - Skipping, has recent data: {latest_date}")
                        else:
                            print(f"  → Fetching booster price history (ID: {booster_id})")
                            self.fetch_and_save_price_history(booster_id)
                            success_count += 1
                            time.sleep(delay_seconds)
                    else:
                        print(f"  → Fetching booster price history (ID: {booster_id})")
                        self.fetch_and_save_price_history(booster_id)
                        success_count += 1
                        time.sleep(delay_seconds)
                        
                except Exception as e:
                    print(f"  ✗ Error fetching booster (ID: {booster_id}): {e}")
                    error_count += 1
                    
            # Process booster box product
            if booster_box_id:
                try:
                    if skip_existing:
                        latest_date = db_manager.get_latest_bucket_date(booster_box_id)
                        if latest_date and latest_date >= '2025-06-01':  # Has recent data
                            print(f"  ✓ Booster Box (ID: {booster_box_id}) - Skipping, has recent data: {latest_date}")
                        else:
                            print(f"  → Fetching booster box price history (ID: {booster_box_id})")
                            self.fetch_and_save_price_history(booster_box_id)
                            success_count += 1
                            time.sleep(delay_seconds)
                    else:
                        print(f"  → Fetching booster box price history (ID: {booster_box_id})")
                        self.fetch_and_save_price_history(booster_box_id)
                        success_count += 1
                        time.sleep(delay_seconds)
                        
                except Exception as e:
                    print(f"  ✗ Error fetching booster box (ID: {booster_box_id}): {e}")
                    error_count += 1
        
        print("\n" + "=" * 80)
        print(f"Completed processing {processed_count} Pokemon sets")
        print(f"Successful API calls: {success_count}")
        print(f"Errors encountered: {error_count}")
    
    def fetch_and_save_price_history(self, product_id: int, headers: Optional[Dict[str, str]] = None) -> None:
        """
        Fetch price history data from TCGPlayer API and save to database.
        
        Args:
            product_id: The TCGPlayer product ID
            headers: Optional custom headers for the request
        """
        from database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        try:
            price_data = self.fetch_price_history(product_id, headers)
            db_manager.save_price_history_data(price_data, product_id)
            print(f"Successfully fetched and saved price history for product {product_id}")
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching price history for product {product_id}: {e}")
            raise
        except Exception as e:
            print(f"Error processing price history data for product {product_id}: {e}")
            raise


# Backward compatibility functions
def fetch_and_save_price_history(product_id: int, headers: Optional[Dict[str, str]] = None) -> None:
    """Legacy function for backward compatibility."""
    collector = DataCollector()
    collector.fetch_and_save_price_history(product_id, headers)


def fetch_all_pokemon_price_history(delay_seconds: float = 2, skip_existing: bool = True) -> None:
    """Legacy function for backward compatibility."""
    collector = DataCollector()
    collector.fetch_pokemon_sets_price_history(delay_seconds, skip_existing)


def fetch_pokemon_sets_price_history(delay_seconds: float = 1) -> None:
    """Legacy function for backward compatibility."""
    collector = DataCollector()
    collector.fetch_pokemon_sets_price_history(delay_seconds, skip_existing=False)


def fetch_pokemon_sets_price_history_incremental(delay_seconds: float = 1) -> None:
    """Legacy function for backward compatibility."""
    collector = DataCollector()
    collector.fetch_pokemon_sets_price_history(delay_seconds, skip_existing=True)
