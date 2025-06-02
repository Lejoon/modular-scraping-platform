# filepath: /Users/lejoon/Projects/modular-scraping-platform/plugins/tcg/testing.py
"""
Legacy testing module for API functionality.

Main CLI functionality has been moved to cli.py.
This module now contains only API testing functions and imports the CLI.
"""

import requests
import json
from database import save_price_history_data


def main():
    """Test the latest sales API endpoint."""
    url = "https://mpapi.tcgplayer.com/v2/product/624679/latestsales?mpfev=3726"
    payload = {
        "conditions": [],
        "languages": [1],
        "variants": [],
        "listingType": "All",
        "offset": 0,
        "limit": 25  # max 25
    }
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "origin": "https://www.tcgplayer.com",
        "referer": "https://www.tcgplayer.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }
    response = requests.post(url, json=payload, headers=headers)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Request failed: {e}")
        print("Response body:", response.text)
        return
    print(json.dumps(response.json(), indent=2))


def test_price_history_api(product_id=None):
    """
    Test function to fetch price history data and save it to the database.
    Uses example response structure for testing.
    """
    if product_id:
        # This would be the actual API call (currently commented out as it may require authentication)
        # url = f"https://infinite-api.tcgplayer.com/price/history/{product_id}/detailed?range=annual"
        # headers = {
        #     "accept": "application/json, text/plain, */*",
        #     "origin": "https://www.tcgplayer.com",
        #     "referer": "https://www.tcgplayer.com/",
        #     "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        # }
        # response = requests.get(url, headers=headers)
        # response.raise_for_status()
        # price_data = response.json()
        pass
    else:
        # Use the example data for testing
        price_data = {
            "count": 1,
            "result": [
                {
                    "skuId": "8637794",
                    "variant": "Normal",
                    "language": "English",
                    "condition": "Unopened",
                    "averageDailyQuantitySold": "113",
                    "averageDailyTransactionCount": "88",
                    "totalQuantitySold": "3406",
                    "totalTransactionCount": "2662",
                    "trendingMarketPricePercentages": {},
                    "buckets": [
                        {
                            "marketPrice": "255.55",
                            "quantitySold": "17",
                            "lowSalePrice": "248.97",
                            "lowSalePriceWithShipping": "257.99",
                            "highSalePrice": "259.95",
                            "highSalePriceWithShipping": "259.95",
                            "transactionCount": "12",
                            "bucketStartDate": "2025-06-02"
                        },
                        {
                            "marketPrice": "255.76",
                            "quantitySold": "135",
                            "lowSalePrice": "235",
                            "lowSalePriceWithShipping": "235",
                            "highSalePrice": "261.8",
                            "highSalePriceWithShipping": "289.94",
                            "transactionCount": "109",
                            "bucketStartDate": "2025-06-01"
                        },
                        {
                            "marketPrice": "255.72",
                            "quantitySold": "177",
                            "lowSalePrice": "235.01",
                            "lowSalePriceWithShipping": "249",
                            "highSalePrice": "264.95",
                            "highSalePriceWithShipping": "350",
                            "transactionCount": "154",
                            "bucketStartDate": "2025-05-31"
                        }
                    ]
                }
            ]
        }
    
        # Save the price history data to database
        save_price_history_data(price_data, 624679)  # Using the example product ID
        print("Price history data saved to database")


def query_price_history_example():
    """
    Example function showing how to query price history data from the database.
    """
    import sqlite3
    import os
    
    DB_DIR = os.path.dirname(__file__)
    DB_PATH = os.path.join(DB_DIR, "tcg.db")
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Get recent price history
    c.execute("""
        SELECT sku_id, variant, condition, market_price, quantity_sold, 
               low_sale_price, high_sale_price, bucket_start_date
        FROM price_history 
        ORDER BY bucket_start_date DESC 
        LIMIT 10
    """)
    
    print("Recent Price History:")
    print("SKU ID | Variant | Condition | Market Price | Qty Sold | Low Price | High Price | Date")
    print("-" * 90)
    
    for row in c.fetchall():
        print(f"{row[0]} | {row[1]} | {row[2]} | ${row[3]} | {row[4]} | ${row[5]} | ${row[6]} | {row[7]}")
    
    conn.close()


if __name__ == "__main__":
    # Delegate to the new CLI module
    from cli import main as cli_main
    cli_main()
