"""
Database sink for TCGPlayer plugin.
"""

from typing import Any
from datetime import datetime

from core.interfaces import Sink
from core.models import ParsedItem
from core.infra.db import Database


class TcgDatabaseSink(Sink):
    """Database sink for TCG data persistence."""
    
    def __init__(self, db_path: str = "tcg.db"):
        """Initialize with database path."""
        self.db_path = db_path
        self.db = Database(db_path)
        
        # Table configurations for different topics
        self._table_configs = {
            "tcg.pokemon_sets": {
                "table": "pokemon_sets",
                "primary_key": ["set_name"],  # Use set_name as unique identifier
                "schema": """
                    CREATE TABLE IF NOT EXISTS pokemon_sets (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        set_name TEXT NOT NULL UNIQUE,
                        release_date TEXT,
                        booster_product_id INTEGER,
                        booster_box_product_id INTEGER,
                        group_id INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
            },
            "tcg.price_history": {
                "table": "price_history",
                "primary_key": ["sku_id", "bucket_start_date"],  # Composite key for uniqueness
                "schema": """
                    CREATE TABLE IF NOT EXISTS price_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        product_id INTEGER NOT NULL,
                        sku_id TEXT NOT NULL,
                        variant TEXT,
                        language TEXT,
                        condition TEXT,
                        market_price DECIMAL(10,2),
                        quantity_sold INTEGER,
                        low_sale_price DECIMAL(10,2),
                        high_sale_price DECIMAL(10,2),
                        bucket_start_date DATE NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(sku_id, bucket_start_date)
                    )
                """
            }
        }
    
    @property
    def name(self) -> str:
        return "TcgDatabaseSink"
    
    async def __aenter__(self):
        """Initialize database connection and create tables."""
        # Connect to database first
        await self.db.connect()
        
        # Create all tables
        for topic_config in self._table_configs.values():
            await self.db.execute(topic_config["schema"])
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up database connection."""
        await self.db.close()
    
    async def handle(self, item: ParsedItem) -> None:
        """Handle a ParsedItem by persisting to database."""
        if not isinstance(item, ParsedItem):
            return
        
        config = self._table_configs.get(item.topic)
        if not config:
            print(f"Warning: No table config found for topic '{item.topic}'")
            return
        
        table_name = config["table"]
        primary_key = config["primary_key"]
        data = dict(item.content)  # Make a copy
        
        # Add timestamp if not present
        if item.discovered_at:
            data["updated_at"] = item.discovered_at.isoformat()
        else:
            data["updated_at"] = datetime.now().isoformat()
        
        # Use the database upsert method which handles commits
        try:
            await self.db.upsert(table_name, data, primary_key)
        except Exception as e:
            print(f"Error upserting to {table_name}: {e}")
