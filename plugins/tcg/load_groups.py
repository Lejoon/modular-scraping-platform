import sqlite3
import os
import requests
import csv

DB_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(DB_DIR, "tcg.db")

def load_groups():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # replace existing data on each load
    c.execute("DELETE FROM tcgplayer_groups")
    # fetch these categories: 79 = Star Wars: Unlimited, 3 = Pokemon
    total = 0
    for category_id in (79, 3):
        url = f"https://tcgcsv.com/tcgplayer/{category_id}/groups"
        resp = requests.get(url)
        resp.raise_for_status()
        rows = resp.json().get("results", [])
        for g in rows:
            c.execute("""
                INSERT OR REPLACE INTO tcgplayer_groups
                (tcgplayer_id, name, abbreviation, is_supplemental, published_on, modified_on, category_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                g["groupId"],
                g["name"],
                g.get("abbreviation"),
                int(g.get("isSupplemental", False)),
                g.get("publishedOn"),
                g.get("modifiedOn"),
                g.get("categoryId")
            ))
            total += 1
    conn.commit()
    conn.close()
    print(f"Loaded {total} groups for categories 79 and 3")

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

def save_price_history_data(price_history_response):
    """
    Save price history data from the TCGPlayer API response to the database.
    Expected response format from https://infinite-api.tcgplayer.com/price/history/{product_id}/detailed?range=annual
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
                    (sku_id, variant, language, condition, market_price, quantity_sold, 
                     low_sale_price, high_sale_price, bucket_start_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (sku_id, variant, language, condition, market_price, quantity_sold,
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
        
        save_price_history_data(price_data)
        print(f"Successfully fetched and saved price history for product {product_id}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching price history for product {product_id}: {e}")
    except Exception as e:
        print(f"Error processing price history data for product {product_id}: {e}")

def get_latest_bucket_date(sku_id):
    """
    Get the latest bucket date for a given SKU to determine what new data to fetch.
    
    Args:
        sku_id: The SKU ID to check
        
    Returns:
        The latest bucket_start_date as a string, or None if no data exists
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("""
        SELECT MAX(bucket_start_date) 
        FROM price_history 
        WHERE sku_id = ?
    """, (sku_id,))
    
    result = c.fetchone()
    conn.close()
    
    return result[0] if result and result[0] else None

if __name__ == "__main__":
    load_all()
