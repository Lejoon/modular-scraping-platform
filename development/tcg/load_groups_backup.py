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

def load_pokemon_sets():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Clear existing data
    c.execute("DELETE FROM pokemon_sets")
    
    csv_path = os.path.join(DB_DIR, "pokemon_sets.csv")
    
    with open(csv_path, 'r', encoding='utf-8-sig') as file:  # utf-8-sig handles BOM
        # Use semicolon as delimiter since the CSV uses semicolons
        reader = csv.DictReader(file, delimiter=';')
        loaded_count = 0
        
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
            
            c.execute("""
                INSERT INTO pokemon_sets 
                (set_name, release_date, booster_product_id, booster_box_product_id, group_id)
                VALUES (?, ?, ?, ?, ?)
            """, (set_name, release_date, booster_product_id, booster_box_product_id, group_id))
            
            loaded_count += 1
    
    conn.commit()
    conn.close()
    print(f"Loaded {loaded_count} Pokemon sets from CSV")

def load_all():
    load_groups()
    load_pokemon_sets()

def save_price_history_data(price_history_response, product_id):
    """
    Save price history data from the TCGPlayer API response to the database.
    Expected response format from https://infinite-api.tcgplayer.com/price/history/{product_id}/detailed?range=annual
    
    Args:
        price_history_response: The JSON response from the price history API
        product_id: The product ID that was queried (our booster/booster_box ID)
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    saved_count = 0
    skipped_count = 0
    
    for result in price_history_response.get('result', []):
        sku_id = result.get('skuId')
        variant = result.get('variant')
        language = result.get('language')
        condition = result.get('condition')
        
        for bucket in result.get('buckets', []):
            try:
                # Extract bucket data
                market_price = float(bucket.get('marketPrice', 0)) if bucket.get('marketPrice') else None
                quantity_sold = int(bucket.get('quantitySold', 0)) if bucket.get('quantitySold') else None
                low_sale_price = float(bucket.get('lowSalePrice', 0)) if bucket.get('lowSalePrice') else None
                high_sale_price = float(bucket.get('highSalePrice', 0)) if bucket.get('highSalePrice') else None
                bucket_start_date = bucket.get('bucketStartDate')
                
                # Insert or ignore (due to UNIQUE constraint on sku_id + bucket_start_date)
                c.execute("""
                    INSERT OR IGNORE INTO price_history 
                    (product_id, sku_id, variant, language, condition, market_price, quantity_sold, 
                     low_sale_price, high_sale_price, bucket_start_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (product_id, sku_id, variant, language, condition, market_price, quantity_sold,
                      low_sale_price, high_sale_price, bucket_start_date))
                
                if c.rowcount > 0:
                    saved_count += 1
                else:
                    skipped_count += 1
                    
            except (ValueError, TypeError) as e:
                print(f"Error processing bucket data: {e}")
                continue
    
    conn.commit()
    conn.close()
    print(f"Price history: {saved_count} new records saved, {skipped_count} duplicates skipped")

def fetch_and_save_price_history(product_id, headers=None):
    """
    Fetch price history data from TCGPlayer API and save to database.
    
    Args:
        product_id: The TCGPlayer product ID
        headers: Optional custom headers for the request
    """
    if headers is None:
        headers = {
            "accept": "application/json, text/plain, */*",
            "origin": "https://www.tcgplayer.com", 
            "referer": "https://www.tcgplayer.com/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        }
    
    url = f"https://infinite-api.tcgplayer.com/price/history/{product_id}/detailed?range=annual"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        price_data = response.json()
        
        save_price_history_data(price_data, product_id)
        print(f"Successfully fetched and saved price history for product {product_id}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching price history for product {product_id}: {e}")
    except Exception as e:
        print(f"Error processing price history data for product {product_id}: {e}")

def get_latest_bucket_date(product_id):
    """
    Get the latest bucket date for a given product_id to determine what new data to fetch.
    
    Args:
        product_id: The product ID to check
        
    Returns:
        The latest bucket_start_date as a string, or None if no data exists
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("""
        SELECT MAX(bucket_start_date) 
        FROM price_history 
        WHERE product_id = ?
    """, (product_id,))
    
    result = c.fetchone()
    conn.close()
    
    return result[0] if result and result[0] else None

def fetch_pokemon_sets_price_history(delay_seconds=1):
    """
    Fetch price history for all booster and booster box products in the pokemon_sets table.
    
    Args:
        delay_seconds: Delay between API calls to avoid rate limiting
    """
    import time
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Get all Pokemon sets with their product IDs
    c.execute("""
        SELECT id, set_name, booster_product_id, booster_box_product_id
        FROM pokemon_sets
        WHERE booster_product_id IS NOT NULL OR booster_box_product_id IS NOT NULL
        ORDER BY id
    """)
    
    sets = c.fetchall()
    conn.close()
    
    total_sets = len(sets)
    processed_count = 0
    
    print(f"Found {total_sets} Pokemon sets to process")
    
    for set_id, set_name, booster_id, booster_box_id in sets:
        processed_count += 1
        print(f"\n[{processed_count}/{total_sets}] Processing: {set_name}")
        
        # Fetch booster product price history
        if booster_id:
            print(f"  Fetching booster price history (ID: {booster_id})")
            fetch_and_save_price_history(booster_id)
            time.sleep(delay_seconds)
            
        # Fetch booster box product price history  
        if booster_box_id:
            print(f"  Fetching booster box price history (ID: {booster_box_id})")
            fetch_and_save_price_history(booster_box_id)
            time.sleep(delay_seconds)
    
    print(f"\nCompleted processing {processed_count} Pokemon sets")

def fetch_pokemon_sets_price_history_incremental(delay_seconds=1):
    """
    Fetch only new price history data for Pokemon sets by checking existing data first.
    This is more efficient for regular updates.
    
    Args:
        delay_seconds: Delay between API calls to avoid rate limiting
    """
    import time
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Get all Pokemon sets with their product IDs
    c.execute("""
        SELECT id, set_name, booster_product_id, booster_box_product_id
        FROM pokemon_sets
        WHERE booster_product_id IS NOT NULL OR booster_box_product_id IS NOT NULL
        ORDER BY id
    """)
    
    sets = c.fetchall()
    conn.close()
    
    total_sets = len(sets)
    processed_count = 0
    
    print(f"Found {total_sets} Pokemon sets for incremental update")
    
    for set_id, set_name, booster_id, booster_box_id in sets:
        processed_count += 1
        print(f"\n[{processed_count}/{total_sets}] Processing: {set_name}")
        
        # Check booster product
        if booster_id:
            latest_date = get_latest_bucket_date(booster_id)
            if latest_date:
                print(f"  Booster (ID: {booster_id}) - Latest data: {latest_date}")
            else:
                print(f"  Booster (ID: {booster_id}) - No existing data, fetching all")
            
            fetch_and_save_price_history(booster_id)
            time.sleep(delay_seconds)
            
        # Check booster box product
        if booster_box_id:
            latest_date = get_latest_bucket_date(booster_box_id)
            if latest_date:
                print(f"  Booster Box (ID: {booster_box_id}) - Latest data: {latest_date}")
            else:
                print(f"  Booster Box (ID: {booster_box_id}) - No existing data, fetching all")
                
            fetch_and_save_price_history(booster_box_id)
            time.sleep(delay_seconds)
    
    print(f"\nCompleted incremental update for {processed_count} Pokemon sets")

def get_pokemon_sets_summary():
    """
    Get a summary of Pokemon sets and their price history data status.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("""
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
    
    results = c.fetchall()
    conn.close()
    
    print("Pokemon Sets Price History Summary:")
    print("=" * 120)
    print(f"{'Set Name':<35} {'Booster ID':<12} {'Box ID':<12} {'Boost Rec':<10} {'Box Rec':<10} {'Boost Latest':<12} {'Box Latest':<12}")
    print("-" * 120)
    
    for row in results:
        set_name, booster_id, box_id, booster_recs, box_recs, booster_date, box_date = row
        print(f"{set_name[:34]:<35} {booster_id or 'N/A':<12} {box_id or 'N/A':<12} {booster_recs:<10} {box_recs:<10} {booster_date or 'N/A':<12} {box_date or 'N/A':<12}")

def fetch_all_pokemon_price_history(delay_seconds=2, skip_existing=True):
    """
    Fetch price history for all Pokemon sets with comprehensive error handling.
    
    Args:
        delay_seconds: Delay between API calls to avoid rate limiting
        skip_existing: If True, skip products that already have recent data
    """
    import time
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Get all Pokemon sets with their product IDs
    c.execute("""
        SELECT id, set_name, booster_product_id, booster_box_product_id
        FROM pokemon_sets
        WHERE booster_product_id IS NOT NULL OR booster_box_product_id IS NOT NULL
        ORDER BY id
    """)
    
    sets = c.fetchall()
    conn.close()
    
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
                    latest_date = get_latest_bucket_date(booster_id)
                    if latest_date and latest_date >= '2025-06-01':  # Has recent data
                        print(f"  ✓ Booster (ID: {booster_id}) - Skipping, has recent data: {latest_date}")
                    else:
                        print(f"  → Fetching booster price history (ID: {booster_id})")
                        fetch_and_save_price_history(booster_id)
                        success_count += 1
                        time.sleep(delay_seconds)
                else:
                    print(f"  → Fetching booster price history (ID: {booster_id})")
                    fetch_and_save_price_history(booster_id)
                    success_count += 1
                    time.sleep(delay_seconds)
                    
            except Exception as e:
                print(f"  ✗ Error fetching booster (ID: {booster_id}): {e}")
                error_count += 1
                
        # Process booster box product
        if booster_box_id:
            try:
                if skip_existing:
                    latest_date = get_latest_bucket_date(booster_box_id)
                    if latest_date and latest_date >= '2025-06-01':  # Has recent data
                        print(f"  ✓ Booster Box (ID: {booster_box_id}) - Skipping, has recent data: {latest_date}")
                    else:
                        print(f"  → Fetching booster box price history (ID: {booster_box_id})")
                        fetch_and_save_price_history(booster_box_id)
                        success_count += 1
                        time.sleep(delay_seconds)
                else:
                    print(f"  → Fetching booster box price history (ID: {booster_box_id})")
                    fetch_and_save_price_history(booster_box_id)
                    success_count += 1
                    time.sleep(delay_seconds)
                    
            except Exception as e:
                print(f"  ✗ Error fetching booster box (ID: {booster_box_id}): {e}")
                error_count += 1
    
    print("\n" + "=" * 80)
    print(f"Completed processing {processed_count} Pokemon sets")
    print(f"Successful API calls: {success_count}")
    print(f"Errors encountered: {error_count}")
    
    # Show final summary
    get_pokemon_sets_summary()

def analyze_price_trends(product_id=None, days=30):
    """
    Analyze price trends for Pokemon products.
    
    Args:
        product_id: Specific product ID to analyze, or None for all products
        days: Number of recent days to analyze
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
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
        c.execute(query, (product_id, days))
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
        c.execute(query, (days,))
    
    results = c.fetchall()
    conn.close()
    
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
 
if __name__ == "__main__":
    load_all()
