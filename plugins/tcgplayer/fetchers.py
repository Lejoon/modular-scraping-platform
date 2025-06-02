"""
Fetchers for TCGPlayer plugin.
"""

import os
import asyncio
import sqlite3
from typing import AsyncIterator, List, Optional

from core.interfaces import Fetcher
from core.models import RawItem
from core.infra.http import HttpClient


class PokemonSetsCsvFetcher(Fetcher):
    """Fetches Pokemon sets data from CSV file."""
    
    def __init__(self, csv_path: str = None):
        """Initialize with CSV file path."""
        if csv_path is None:
            # Default to development CSV location
            csv_path = "/Users/lejoon/Projects/modular-scraping-platform/development/tcg/pokemon_sets.csv"
        self.csv_path = csv_path
    
    @property
    def name(self) -> str:
        return "PokemonSetsCsvFetcher"
    
    async def fetch(self) -> AsyncIterator[RawItem]:
        """Fetch CSV data as raw bytes."""
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")
        
        with open(self.csv_path, 'rb') as f:
            csv_data = f.read()
        
        yield RawItem(
            source="pokemon_sets.csv",
            payload=csv_data
        )


class TcgPlayerPriceHistoryFetcher(Fetcher):
    """Fetch price history data from TCGPlayer API for multiple products."""
    
    def __init__(self, db_path: str = "tcg.db", product_ids: Optional[List[int]] = None, delay_seconds: float = 1.0):
        """
        Initialize with database path to read product IDs, or explicit product IDs.
        
        Args:
            db_path: Path to SQLite database to read product IDs from
            product_ids: Optional list of explicit product IDs (overrides database lookup)
            delay_seconds: Delay between API requests for rate limiting
        """
        self.db_path = db_path
        self.explicit_product_ids = product_ids
        self.delay_seconds = delay_seconds
        self.http_client = None
        
        # Default headers for TCGPlayer API
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "origin": "https://www.tcgplayer.com", 
            "referer": "https://www.tcgplayer.com/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        }
    
    @property
    def name(self) -> str:
        return "TcgPlayerPriceHistoryFetcher"
    
    def _get_product_ids_from_db(self) -> List[int]:
        """Read product IDs from the database."""
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Database file not found: {self.db_path}")
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            # Query unique product IDs from pokemon_sets table
            # Get both booster and booster box product IDs
            cursor.execute("""
                SELECT DISTINCT booster_product_id FROM pokemon_sets 
                WHERE booster_product_id IS NOT NULL
                UNION
                SELECT DISTINCT booster_box_product_id FROM pokemon_sets 
                WHERE booster_box_product_id IS NOT NULL
            """)
            rows = cursor.fetchall()
            product_ids = [row[0] for row in rows if row[0] is not None]
            return product_ids
        finally:
            conn.close()
    
    def _get_product_ids(self) -> List[int]:
        """Get product IDs either from explicit list or from database."""
        if self.explicit_product_ids:
            return self.explicit_product_ids
        else:
            return self._get_product_ids_from_db()
    
    async def fetch(self) -> AsyncIterator[RawItem]:
        """Fetch price history data for all products."""
        if self.http_client is None:
            self.http_client = HttpClient()
        
        product_ids = self._get_product_ids()
        
        if not product_ids:
            print("No product IDs found to fetch price history for")
            return
        
        print(f"Fetching price history for {len(product_ids)} products")
        
        for i, product_id in enumerate(product_ids):
            try:
                print(f"Fetching price history for product {product_id} ({i+1}/{len(product_ids)})")
                
                # TCGPlayer API endpoint for price history
                url = f"https://infinite-api.tcgplayer.com/price/history/{product_id}/detailed?range=annual"
                
                # Fetch JSON data as bytes
                response_bytes = await self.http_client.get_bytes(url, headers=self.headers)
                
                yield RawItem(
                    source=f"tcgplayer.price_history.{product_id}",
                    payload=response_bytes
                )
                
                # Rate limiting - wait between requests
                if i < len(product_ids) - 1:  # Don't sleep after the last request
                    await asyncio.sleep(self.delay_seconds)
                    
            except Exception as e:
                print(f"Error fetching price history for product {product_id}: {e}")
                continue
