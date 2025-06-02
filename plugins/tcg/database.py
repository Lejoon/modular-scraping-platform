"""
Database management module for TCG price history system.

Handles database initialization, schema creation, and basic database operations.
"""

import sqlite3
import os
from typing import Optional, Tuple, List, Dict, Any

DB_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(DB_DIR, "tcg.db")

class DatabaseManager:
    """Manages database connections and operations for TCG price data."""
    
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or DB_PATH
    
    def init_database(self) -> None:
        """Initialize the database with required tables and schemas."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        # Create TCG sets table
        c.execute("""
            CREATE TABLE IF NOT EXISTS tcg_sets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                set_name TEXT NOT NULL,
                tcgplayer_id INTEGER UNIQUE,
                mkm_id INTEGER UNIQUE,
                game TEXT NOT NULL,
                release_date TEXT
            );
        """)
        
        # Create TCGPlayer groups table
        c.execute("""
            CREATE TABLE IF NOT EXISTS tcgplayer_groups (
                tcgplayer_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                abbreviation TEXT,
                is_supplemental BOOLEAN,
                published_on TEXT,
                modified_on TEXT,
                category_id INTEGER NOT NULL
            );
        """)
        
        # Create Pokemon sets table
        c.execute("""
            CREATE TABLE IF NOT EXISTS pokemon_sets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                set_name TEXT NOT NULL,
                release_date TEXT,
                booster_product_id INTEGER,
                booster_box_product_id INTEGER,
                group_id INTEGER,
                FOREIGN KEY (group_id) REFERENCES tcgplayer_groups(tcgplayer_id)
            );
        """)
        
        # Create price history table
        c.execute("""
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
                UNIQUE(sku_id, bucket_start_date)
            );
        """)
        
        conn.commit()
        conn.close()
        print(f"Initialized database at {self.db_path}")
    
    def get_connection(self) -> sqlite3.Connection:
        """Get a database connection."""
        return sqlite3.connect(self.db_path)
    
    def execute_query(self, query: str, params: Tuple = ()) -> List[Tuple]:
        """Execute a SELECT query and return results."""
        conn = self.get_connection()
        c = conn.cursor()
        c.execute(query, params)
        results = c.fetchall()
        conn.close()
        return results
    
    def execute_insert(self, query: str, params: Tuple = ()) -> int:
        """Execute an INSERT query and return the last row ID."""
        conn = self.get_connection()
        c = conn.cursor()
        c.execute(query, params)
        last_id = c.lastrowid
        conn.commit()
        conn.close()
        return last_id
    
    def execute_many(self, query: str, params_list: List[Tuple]) -> None:
        """Execute multiple queries with parameter lists."""
        conn = self.get_connection()
        c = conn.cursor()
        c.executemany(query, params_list)
        conn.commit()
        conn.close()
    
    def save_price_history_data(self, price_history_response: Dict[str, Any], product_id: int) -> None:
        """Save price history data from TCGPlayer API response to database."""
        if not price_history_response.get("result"):
            print("No price history data in response")
            return
        
        conn = self.get_connection()
        c = conn.cursor()
        
        total_saved = 0
        total_duplicates = 0
        
        for result in price_history_response["result"]:
            sku_id = result.get("skuId")
            variant = result.get("variant")
            language = result.get("language")
            condition = result.get("condition")
            
            for bucket in result.get("buckets", []):
                try:
                    c.execute("""
                        INSERT INTO price_history 
                        (product_id, sku_id, variant, language, condition, market_price, 
                         quantity_sold, low_sale_price, high_sale_price, bucket_start_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        product_id,
                        sku_id,
                        variant,
                        language,
                        condition,
                        float(bucket.get("marketPrice", 0)) if bucket.get("marketPrice") else None,
                        int(bucket.get("quantitySold", 0)) if bucket.get("quantitySold") else None,
                        float(bucket.get("lowSalePrice", 0)) if bucket.get("lowSalePrice") else None,
                        float(bucket.get("highSalePrice", 0)) if bucket.get("highSalePrice") else None,
                        bucket.get("bucketStartDate")
                    ))
                    total_saved += 1
                except sqlite3.IntegrityError:
                    # Duplicate entry (sku_id, bucket_start_date already exists)
                    total_duplicates += 1
                    continue
        
        conn.commit()
        conn.close()
        
        print(f"Saved {total_saved} price history records for product {product_id}")
        if total_duplicates > 0:
            print(f"Skipped {total_duplicates} duplicate records")
    
    def get_latest_bucket_date(self, product_id: int) -> Optional[str]:
        """Get the latest bucket date for a product ID."""
        results = self.execute_query("""
            SELECT MAX(bucket_start_date) 
            FROM price_history 
            WHERE product_id = ?
        """, (product_id,))
        
        return results[0][0] if results and results[0][0] else None

# Global instance for backward compatibility
db_manager = DatabaseManager()

# Backward compatibility functions
def init_db() -> None:
    """Initialize the database (backward compatibility)."""
    db_manager.init_database()

def save_price_history_data(price_history_response: Dict[str, Any], product_id: int) -> None:
    """Save price history data (backward compatibility)."""
    db_manager.save_price_history_data(price_history_response, product_id)

def get_latest_bucket_date(product_id: int) -> Optional[str]:
    """Get latest bucket date (backward compatibility)."""
    return db_manager.get_latest_bucket_date(product_id)

if __name__ == "__main__":
    init_db()
