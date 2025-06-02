"""
Data management module for TCG data.

This module handles data loading, CSV processing, and summary operations.
"""

import os
import csv
import sqlite3
from typing import List, Dict, Any, Optional

# Database path - inherit from original structure
DB_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(DB_DIR, "tcg.db")


class DataManager:
    """Handles data loading and management operations."""
    
    def __init__(self):
        """Initialize the data manager."""
        pass
    
    def load_groups(self) -> None:
        """
        Load TCG groups from API for Star Wars Unlimited and Pokemon categories.
        """
        from database import DatabaseManager
        from data_collector import DataCollector
        
        db_manager = DatabaseManager()
        collector = DataCollector()
        
        # Clear existing data
        db_manager.execute_insert("DELETE FROM tcgplayer_groups")
        
        # Fetch categories: 79 = Star Wars: Unlimited, 3 = Pokemon
        total = 0
        for category_id in (79, 3):
            response = collector.fetch_tcg_groups(category_id)
            groups = response.get("results", [])
            
            for group in groups:
                db_manager.execute_insert("""
                    INSERT OR REPLACE INTO tcgplayer_groups
                    (tcgplayer_id, name, abbreviation, is_supplemental, published_on, modified_on, category_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    group["groupId"],
                    group["name"],
                    group.get("abbreviation"),
                    int(group.get("isSupplemental", False)),
                    group.get("publishedOn"),
                    group.get("modifiedOn"),
                    group.get("categoryId")
                ))
                total += 1
        
        print(f"Loaded {total} groups for categories 79 and 3")
    
    def load_pokemon_sets(self) -> None:
        """
        Load Pokemon sets from CSV file.
        """
        from database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Clear existing data
        db_manager.execute_insert("DELETE FROM pokemon_sets")
        
        csv_path = os.path.join(DB_DIR, "pokemon_sets.csv")
        loaded_count = 0
        
        with open(csv_path, 'r', encoding='utf-8-sig') as file:  # utf-8-sig handles BOM
            # Use semicolon as delimiter since the CSV uses semicolons
            reader = csv.DictReader(file, delimiter=';')
            
            for row in reader:
                # Skip empty rows
                if not row.get('Set Name') or not row['Set Name'].strip():
                    continue
                    
                # Extract data from CSV row
                set_name = row['Set Name'].strip()
                release_date = row['Release Date'].strip() if row['Release Date'] else None
                booster_product_id = int(row['TCGPlayer Booster Product ID']) if row['TCGPlayer Booster Product ID'].strip() else None
                booster_box_product_id = int(row['TCGPlayer Booster Box Product ID']) if row['TCGPlayer Booster Box Product ID'].strip() else None
                group_id = int(row['TCGPlayer Group ID']) if row['TCGPlayer Group ID'].strip() else None
                
                db_manager.execute_insert("""
                    INSERT INTO pokemon_sets 
                    (set_name, release_date, booster_product_id, booster_box_product_id, group_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (set_name, release_date, booster_product_id, booster_box_product_id, group_id))
                
                loaded_count += 1
        
        print(f"Loaded {loaded_count} Pokemon sets from CSV")
    
    def load_all(self) -> None:
        """
        Load all data (groups and Pokemon sets).
        """
        self.load_groups()
        self.load_pokemon_sets()
    
    def get_pokemon_sets_summary(self) -> None:
        """
        Get a summary of Pokemon sets and their price history data status.
        """
        from database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        results = db_manager.execute_query("""
            SELECT 
                ps.set_name,
                ps.booster_product_id,
                ps.booster_box_product_id,
                COUNT(CASE WHEN ph.product_id = ps.booster_product_id THEN 1 END) as booster_records,
                COUNT(CASE WHEN ph.product_id = ps.booster_box_product_id THEN 1 END) as booster_box_records,
                MAX(CASE WHEN ph.product_id = ps.booster_product_id THEN ph.bucket_start_date END) as booster_latest_date,
                MAX(CASE WHEN ph.product_id = ps.booster_box_product_id THEN ph.bucket_start_date END) as booster_box_latest_date
            FROM pokemon_sets ps
            LEFT JOIN price_history ph ON (
                ph.product_id = ps.booster_product_id OR 
                ph.product_id = ps.booster_box_product_id
            )
            GROUP BY ps.id, ps.set_name, ps.booster_product_id, ps.booster_box_product_id
            ORDER BY ps.id
        """)
        
        print("Pokemon Sets Price History Summary:")
        print("=" * 120)
        print(f"{'Set Name':<35} {'Booster ID':<12} {'Box ID':<12} {'Boost Rec':<10} {'Box Rec':<10} {'Boost Latest':<12} {'Box Latest':<12}")
        print("-" * 120)
        
        for row in results:
            set_name, booster_id, box_id, booster_recs, box_recs, booster_date, box_date = row
            print(f"{set_name[:34]:<35} {booster_id or 'N/A':<12} {box_id or 'N/A':<12} {booster_recs:<10} {box_recs:<10} {booster_date or 'N/A':<12} {box_date or 'N/A':<12}")
    
    def analyze_price_trends(self, product_id: Optional[int] = None, days: int = 30) -> None:
        """
        Analyze price trends for Pokemon products.
        
        Args:
            product_id: Specific product ID to analyze, or None for all products
            days: Number of recent days to analyze
        """
        from database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        if product_id:
            query = """
                SELECT 
                    ps.set_name,
                    ph.product_id,
                    CASE 
                        WHEN ph.product_id = ps.booster_product_id THEN 'Booster'
                        WHEN ph.product_id = ps.booster_box_product_id THEN 'Booster Box'
                        ELSE 'Unknown'
                    END as product_type,
                    ph.market_price,
                    ph.quantity_sold,
                    ph.bucket_start_date,
                    ph.condition
                FROM price_history ph
                JOIN pokemon_sets ps ON (ph.product_id = ps.booster_product_id OR ph.product_id = ps.booster_box_product_id)
                WHERE ph.product_id = ? AND DATE(ph.bucket_start_date) >= DATE('now', '-' || ? || ' days')
                ORDER BY ph.bucket_start_date DESC
            """
            results = db_manager.execute_query(query, (product_id, days))
        else:
            query = """
                SELECT 
                    ps.set_name,
                    ph.product_id,
                    CASE 
                        WHEN ph.product_id = ps.booster_product_id THEN 'Booster'
                        WHEN ph.product_id = ps.booster_box_product_id THEN 'Booster Box'
                        ELSE 'Unknown'
                    END as product_type,
                    AVG(ph.market_price) as avg_price,
                    SUM(ph.quantity_sold) as total_sold,
                    MIN(ph.bucket_start_date) as first_date,
                    MAX(ph.bucket_start_date) as last_date,
                    COUNT(*) as data_points
                FROM price_history ph
                JOIN pokemon_sets ps ON (ph.product_id = ps.booster_product_id OR ph.product_id = ps.booster_box_product_id)
                WHERE DATE(ph.bucket_start_date) >= DATE('now', '-' || ? || ' days')
                GROUP BY ps.set_name, ph.product_id, product_type
                ORDER BY ps.set_name, product_type
            """
            results = db_manager.execute_query(query, (days,))
        
        if product_id:
            print(f"Price History for Product {product_id} (Last {days} days):")
            print("=" * 100)
            print(f"{'Set':<35} {'Type':<12} {'Price':<8} {'Sold':<6} {'Date':<12} {'Condition':<10}")
            print("-" * 100)
            for row in results:
                set_name, pid, ptype, price, sold, date, condition = row
                print(f"{set_name[:34]:<35} {ptype:<12} ${price:<7.2f} {sold:<6} {date:<12} {condition:<10}")
        else:
            print(f"Price Summary (Last {days} days):")
            print("=" * 120)
            print(f"{'Set':<35} {'Type':<12} {'Avg Price':<10} {'Total Sold':<12} {'Data Points':<12} {'Date Range':<20}")
            print("-" * 120)
            for row in results:
                set_name, pid, ptype, avg_price, total_sold, first_date, last_date, data_points = row
                date_range = f"{first_date} to {last_date}"
                print(f"{set_name[:34]:<35} {ptype:<12} ${avg_price:<9.2f} {total_sold:<12} {data_points:<12} {date_range:<20}")


# Backward compatibility functions
def load_groups() -> None:
    """Legacy function for backward compatibility."""
    manager = DataManager()
    manager.load_groups()


def load_pokemon_sets() -> None:
    """Legacy function for backward compatibility."""
    manager = DataManager()
    manager.load_pokemon_sets()


def load_all() -> None:
    """Legacy function for backward compatibility."""
    manager = DataManager()
    manager.load_all()


def get_pokemon_sets_summary() -> None:
    """Legacy function for backward compatibility."""
    manager = DataManager()
    manager.get_pokemon_sets_summary()


def analyze_price_trends(product_id: Optional[int] = None, days: int = 30) -> None:
    """Legacy function for backward compatibility."""
    manager = DataManager()
    manager.analyze_price_trends(product_id, days)
