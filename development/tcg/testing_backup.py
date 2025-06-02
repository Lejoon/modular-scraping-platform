import requests
import json
from load_groups import save_price_history_data

# links for groups that can be saved as json files
# https://tcgcsv.com/tcgplayer/79/groups # starwars unlimited
# https://tcgcsv.com/tcgplayer/3/groups # pokemon
# example contents

'''
{"totalItems": 16, "success": true, "errors": [], "results": [{"groupId": 24279, "name": "Legends of the Force", "abbreviation": "LOF", "isSupplemental": false, "publishedOn": "2025-07-11T00:00:00", "modifiedOn": "2025-05-15T19:06:41.827", "categoryId": 79}, {"groupId": 23956, "name": "Jump to Lightspeed", "abbreviation": "JTL", "isSupplemental": false, "publishedOn": "2025-03-14            print("Analysis commands:")
            print("  analyze <product_id> [window_days] - Detailed analysis of a product")
            print("  export <product_id> [filename]     - Export time series data to CSV")
            print("  sets                              - List all available Pokemon sets")
            print("  recent [days]                     - Show recent price activity")
            print("  demo                              - Run analysis demo")
            print("  volume_csv                        - Export volume data to CSV")
            print("  visualize                         - Create volume charts")00", "modifiedOn": "2025-04-08T14:04:54.053", "categoryId": 79}, {"groupId": 24171, "name": "Jump to Lightspeed - Weekly Play Promos", "abbreviation": "JTL-WPP", "isSupplemental": false, "publishedOn": "2025-03-14T00:00:00", "modifiedOn": "2025-03-17T18:17:34.873", "categoryId": 79}, {"groupId": 23597, "name": "Twilight of the Republic", "abbreviation": "TWI", "isSupplemental": false, "publishedOn": "2024-11-08T00:00:00", "modifiedOn": "2025-01-23T21:26:38.15", "categoryId": 79}, {"groupId": 23820, "name": "Twilight of the Republic: Weekly Play Promos", "abbreviation": "TWI-WPP", "isSupplemental": false, "publishedOn": "2024-11-08T00:00:00", "modifiedOn": "2024-12-10T16:00:15.833", "categoryId": 79}, {"groupId": 23572, "name": "2024 Convention Exclusive", "abbreviation": "CE2024", "isSupplemental": false, "publishedOn": "2024-07-25T00:00:00", "modifiedOn": "2025-01-20T15:59:34.78", "categoryId": 79}, {"groupId": 23488, "name": "Shadows of the Galaxy", "abbreviation": "SHD", "isSupplemental": false, "publishedOn": "2024-07-12T00:00:00", "modifiedOn": "2025-03-17T16:48:34.71", "categoryId": 79}, {"groupId": 23555, "name": "Shadows of the Galaxy: Weekly Play Promos", "abbreviation": "SHD-WPP", "isSupplemental": false, "publishedOn": "2024-07-04T00:00:00", "modifiedOn": "2024-10-11T15:39:38.523", "categoryId": 79}, {"groupId": 23454, "name": "Event Exclusive Promos", "abbreviation": "EEP", "isSupplemental": false, "publishedOn": "2024-03-08T00:00:00", "modifiedOn": "2025-05-16T18:16:54.43", "categoryId": 79}, {"groupId": 23453, "name": "Judge Promos", "abbreviation": "JDG", "isSupplemental": false, "publishedOn": "2024-03-08T00:00:00", "modifiedOn": "2025-05-16T18:31:53.06", "categoryId": 79}, {"groupId": 23455, "name": "Organized Play Promos", "abbreviation": "OPP", "isSupplemental": false, "publishedOn": "2024-03-08T00:00:00", "modifiedOn": "2025-05-21T20:41:58.38", "categoryId": 79}, {"groupId": 23405, "name": "Spark of Rebellion", "abbreviation": "SOR", "isSupplemental": false, "publishedOn": "2024-03-08T00:00:00", "modifiedOn": "2025-02-19T13:55:35.28", "categoryId": 79}, {"groupId": 23451, "name": "Spark of Rebellion: Weekly Play Promos", "abbreviation": "SOR-WPP", "isSupplemental": false, "publishedOn": "2024-03-08T00:00:00", "modifiedOn": "2024-07-12T14:31:13.46", "categoryId": 79}, {"groupId": 23452, "name": "Prerelease Promos", "abbreviation": "PRE", "isSupplemental": false, "publishedOn": "2024-03-01T00:00:00", "modifiedOn": "2024-11-15T14:13:29.703", "categoryId": 79}, {"groupId": 23406, "name": "GenCon 2023 Promos", "abbreviation": "", "isSupplemental": false, "publishedOn": "2023-08-03T00:00:00", "modifiedOn": "2024-01-18T16:13:13.217", "categoryId": 79}, {"groupId": 24272, "name": "Gamegenic Promos", "abbreviation": "", "isSupplemental": false, "publishedOn": "2025-06-01T20:00:06.2193042Z", "modifiedOn": "2025-04-04T19:57:18.827", "categoryId": 79}]}
'''

# Returns the latest sales data for a specific product from TCGPlayer API.
# This example uses a product ID of 624679, which corresponds to a specific Magic: The Gathering card.
# The request includes parameters for conditions, languages, variants, listing type, offset, and limit.
# Request:
# {"conditions":[],"languages":[1],"variants":[],"listingType":"All","offset":100,"limit":25,"time":1748802627610}

# POST: https://mpapi.tcgplayer.com/v2/product/624679/latestsales?mpfev=3726

def main():
    url = "https://mpapi.tcgplayer.com/v2/product/624679/latestsales?mpfev=3726"
    payload = {
        "conditions": [],
        "languages": [1],
        "variants": [],
        "listingType": "All",
        "offset": 0,
        "limit": 25 #max 25
        }
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "origin": "https://www.tcgplayer.com",
        "referer": "https://www.tcgplayer.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36" # Example UA

    }
    response = requests.post(url, json=payload, headers=headers)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Request failed: {e}")
        print("Response body:", response.text)
        return
    print(json.dumps(response.json(), indent=2))
    
# https://infinite-api.tcgplayer.com/price/history
# 23956	group_id
# Response:

'''
{
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
                },
                {
                    "marketPrice": "251.88",
                    "quantitySold": "448",
                    "lowSalePrice": "220",
                    "lowSalePriceWithShipping": "224",
                    "highSalePrice": "265",
                    "highSalePriceWithShipping": "288.94",
                    "transactionCount": "383",
                    "bucketStartDate": "2025-05-30"
                },
                {
                    "marketPrice": "255.23",
                    "quantitySold": "146",
                    "lowSalePrice": "243.75",
                    "lowSalePriceWithShipping": "248.74",
                    "highSalePrice": "265.49",
                    "highSalePriceWithShipping": "291.79",
                    "transactionCount": "122",
                    "bucketStartDate": "2025-05-29"
                },
                {
                    "marketPrice": "258.85",
                    "quantitySold": "152",
                    "lowSalePrice": "250",
                    "lowSalePriceWithShipping": "259.63",
                    "highSalePrice": "310.49",
                    "highSalePriceWithShipping": "335.44",
                    "transactionCount": "112",
                    "bucketStartDate": "2025-05-28"
                },
                {
                    "marketPrice": "259.56",
                    "quantitySold": "115",
                    "lowSalePrice": "249.99",
                    "lowSalePriceWithShipping": "259.66",
                    "highSalePrice": "310.49",
                    "highSalePriceWithShipping": "335.44",
                    "transactionCount": "100",
                    "bucketStartDate": "2025-05-27"
                },
                {
                    "marketPrice": "262.03",
                    "quantitySold": "129",
                    "lowSalePrice": "256.99",
                    "lowSalePriceWithShipping": "261.85",
                    "highSalePrice": "264.95",
                    "highSalePriceWithShipping": "301.96",
                    "transactionCount": "95",
                    "bucketStartDate": "2025-05-26"
                },
                {
                    "marketPrice": "263.32",
                    "quantitySold": "121",
                    "lowSalePrice": "253.92",
                    "lowSalePriceWithShipping": "263.67",
                    "highSalePrice": "264.8",
                    "highSalePriceWithShipping": "288.92",
                    "transactionCount": "87",
                    "bucketStartDate": "2025-05-25"
                },
                {
                    "marketPrice": "264",
                    "quantitySold": "125",
                    "lowSalePrice": "254.7",
                    "lowSalePriceWithShipping": "263.96",
                    "highSalePrice": "265.49",
                    "highSalePriceWithShipping": "289.71",
                    "transactionCount": "100",
                    "bucketStartDate": "2025-05-24"
                },
                {
                    "marketPrice": "264.16",
                    "quantitySold": "121",
                    "lowSalePrice": "256.99",
                    "lowSalePriceWithShipping": "264.72",
                    "highSalePrice": "264.88",
                    "highSalePriceWithShipping": "309.8",
                    "transactionCount": "92",
                    "bucketStartDate": "2025-05-23"
                },
                {
                    "marketPrice": "257.76",
                    "quantitySold": "101",
                    "lowSalePrice": "249.8",
                    "lowSalePriceWithShipping": "257.79",
                    "highSalePrice": "269",
                    "highSalePriceWithShipping": "309.8",
                    "transactionCount": "82",
                    "bucketStartDate": "2025-05-22"
                },
                {
                    "marketPrice": "263.79",
                    "quantitySold": "79",
                    "lowSalePrice": "256.99",
                    "lowSalePriceWithShipping": "264.79",
                    "highSalePrice": "265.49",
                    "highSalePriceWithShipping": "270.85",
                    "transactionCount": "64",
                    "bucketStartDate": "2025-05-21"
                },
                {
                    "marketPrice": "264.46",
                    "quantitySold": "88",
                    "lowSalePrice": "249.87",
                    "lowSalePriceWithShipping": "264.84",
                    "highSalePrice": "310.49",
                    "highSalePriceWithShipping": "360.49",
                    "transactionCount": "63",
                    "bucketStartDate": "2025-05-20"
                },
                {
                    "marketPrice": "262.32",
                    "quantitySold": "75",
                    "lowSalePrice": "249.93",
                    "lowSalePriceWithShipping": "264.89",
                    "highSalePrice": "266.19",
                    "highSalePriceWithShipping": "279.92",
                    "transactionCount": "56",
                    "bucketStartDate": "2025-05-19"
                },
                {
                    "marketPrice": "265.12",
                    "quantitySold": "122",
                    "lowSalePrice": "260.49",
                    "lowSalePriceWithShipping": "265.45",
                    "highSalePrice": "310.49",
                    "highSalePriceWithShipping": "360.49",
                    "transactionCount": "92",
                    "bucketStartDate": "2025-05-18"
                },
                {
                    "marketPrice": "267.3",
                    "quantitySold": "105",
                    "lowSalePrice": "265",
                    "lowSalePriceWithShipping": "265.49",
                    "highSalePrice": "310.49",
                    "highSalePriceWithShipping": "335.44",
                    "transactionCount": "83",
                    "bucketStartDate": "2025-05-17"
                },
                {
                    "marketPrice": "267.47",
                    "quantitySold": "380",
                    "lowSalePrice": "265.47",
                    "lowSalePriceWithShipping": "265.47",
                    "highSalePrice": "310.49",
                    "highSalePriceWithShipping": "365.89",
                    "transactionCount": "252",
                    "bucketStartDate": "2025-05-16"
                },
                {
                    "marketPrice": "265.43",
                    "quantitySold": "69",
                    "lowSalePrice": "261.1",
                    "lowSalePriceWithShipping": "265.46",
                    "highSalePrice": "266.68",
                    "highSalePriceWithShipping": "311.09",
                    "transactionCount": "56",
                    "bucketStartDate": "2025-05-15"
                },
                {
                    "marketPrice": "265.79",
                    "quantitySold": "48",
                    "lowSalePrice": "264.1",
                    "lowSalePriceWithShipping": "265.97",
                    "highSalePrice": "266.49",
                    "highSalePriceWithShipping": "274.1",
                    "transactionCount": "41",
                    "bucketStartDate": "2025-05-14"
                },
                {
                    "marketPrice": "266.15",
                    "quantitySold": "44",
                    "lowSalePrice": "266.09",
                    "lowSalePriceWithShipping": "266.09",
                    "highSalePrice": "269",
                    "highSalePriceWithShipping": "269",
                    "transactionCount": "36",
                    "bucketStartDate": "2025-05-13"
                },
                {
                    "marketPrice": "267.39",
                    "quantitySold": "54",
                    "lowSalePrice": "266.19",
                    "lowSalePriceWithShipping": "266.19",
                    "highSalePrice": "295",
                    "highSalePriceWithShipping": "295",
                    "transactionCount": "37",
                    "bucketStartDate": "2025-05-12"
                },
                {
                    "marketPrice": "268.57",
                    "quantitySold": "37",
                    "lowSalePrice": "266.68",
                    "lowSalePriceWithShipping": "266.68",
                    "highSalePrice": "310.49",
                    "highSalePriceWithShipping": "360.49",
                    "transactionCount": "27",
                    "bucketStartDate": "2025-05-11"
                },
                {
                    "marketPrice": "265.89",
                    "quantitySold": "61",
                    "lowSalePrice": "265",
                    "lowSalePriceWithShipping": "265",
                    "highSalePrice": "266.92",
                    "highSalePriceWithShipping": "276.92",
                    "transactionCount": "48",
                    "bucketStartDate": "2025-05-10"
                },
                {
                    "marketPrice": "266.48",
                    "quantitySold": "66",
                    "lowSalePrice": "260.71",
                    "lowSalePriceWithShipping": "266.7",
                    "highSalePrice": "295",
                    "highSalePriceWithShipping": "295",
                    "transactionCount": "50",
                    "bucketStartDate": "2025-05-09"
                },
                {
                    "marketPrice": "268.48",
                    "quantitySold": "61",
                    "lowSalePrice": "261.88",
                    "lowSalePriceWithShipping": "265.64",
                    "highSalePrice": "302.72",
                    "highSalePriceWithShipping": "304.71",
                    "transactionCount": "51",
                    "bucketStartDate": "2025-05-08"
                },
                {
                    "marketPrice": "266.89",
                    "quantitySold": "59",
                    "lowSalePrice": "265",
                    "lowSalePriceWithShipping": "266.86",
                    "highSalePrice": "269.9",
                    "highSalePriceWithShipping": "276.92",
                    "transactionCount": "52",
                    "bucketStartDate": "2025-05-07"
                },
                {
                    "marketPrice": "269.06",
                    "quantitySold": "56",
                    "lowSalePrice": "264.95",
                    "lowSalePriceWithShipping": "268.92",
                    "highSalePrice": "271.8",
                    "highSalePriceWithShipping": "279.91",
                    "transactionCount": "46",
                    "bucketStartDate": "2025-05-06"
                },
                {
                    "marketPrice": "270",
                    "quantitySold": "62",
                    "lowSalePrice": "269.91",
                    "lowSalePriceWithShipping": "269.91",
                    "highSalePrice": "271.84",
                    "highSalePriceWithShipping": "279.91",
                    "transactionCount": "47",
                    "bucketStartDate": "2025-05-05"
                },
                {
                    "marketPrice": "269.95",
                    "quantitySold": "73",
                    "lowSalePrice": "269.91",
                    "lowSalePriceWithShipping": "269.95",
                    "highSalePrice": "269.99",
                    "highSalePriceWithShipping": "299.98",
                    "transactionCount": "58",
                    "bucketStartDate": "2025-05-04"
                }
            ]
        }
    ]
}
'''



def test_price_history_api(product_id=None):
    """
    Test function to fetch price history data and save it to the database.
    Uses the example response structure shown in the comments above.
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
        # Use the example data from the comments above for testing
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
    Example function showing how to query price history data from the database
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
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "summary":
            from load_groups import get_pokemon_sets_summary
            get_pokemon_sets_summary()
            
        elif command == "trends":
            from load_groups import analyze_price_trends
            product_id = int(sys.argv[2]) if len(sys.argv) > 2 else None
            days = int(sys.argv[3]) if len(sys.argv) > 3 else 30
            analyze_price_trends(product_id, days)
            
        elif command == "fetch_one":
            if len(sys.argv) < 3:
                print("Usage: python testing.py fetch_one <product_id>")
                sys.exit(1)
            from load_groups import fetch_and_save_price_history
            product_id = int(sys.argv[2])
            fetch_and_save_price_history(product_id)
            
        elif command == "fetch_all":
            from load_groups import fetch_all_pokemon_price_history
            fetch_all_pokemon_price_history()
            
        elif command == "test_api":
            test_price_history_api()
            
        elif command == "query":
            query_price_history_example()
            
        elif command == "latest_sales":
            main()
            
        elif command == "analyze":
            if len(sys.argv) < 3:
                print("Usage: python testing.py analyze <product_id> [window_days]")
                sys.exit(1)
            import analysis
            product_id = int(sys.argv[2])
            window_days = int(sys.argv[3]) if len(sys.argv) > 3 else 7
            
            print(f"=== Analysis for Product {product_id} ===")
            
            # Get time series data
            time_series = analysis.get_time_series_by_product_id(product_id)
            print(f"Data points: {len(time_series)}")
            
            if time_series:
                print(f"Date range: {time_series[0].date} to {time_series[-1].date}")
                print(f"Latest price: ${time_series[-1].market_price}")
                print(f"Latest volume: {time_series[-1].quantity_sold} sold")
                
                # Price summary
                summary = analysis.get_price_summary_by_product(product_id)
                print(f"\nPrice Summary:")
                print(f"  Min: ${summary['market_price']['min']:.2f}")
                print(f"  Max: ${summary['market_price']['max']:.2f}")
                print(f"  Avg: ${summary['market_price']['avg']:.2f}")
                print(f"  Total volume: {summary['sales_volume']['total']:,}")
                
                # Trend analysis
                trends = analysis.analyze_price_trends(product_id, window_days)
                if 'error' not in trends:
                    print(f"\nTrend Analysis ({window_days}-day window):")
                    print(f"  Moving avg: ${trends['moving_avg_price']:.2f}")
                    print(f"  Recent trend: {trends['recent_trend']['direction']} (${trends['recent_trend']['change']:.2f})")
                    print(f"  Overall trend: {trends['overall_trend']['direction']} (${trends['overall_trend']['change']:.2f})")
                else:
                    print(f"\nTrend Analysis: {trends['error']}")
            else:
                print("No data found for this product ID")
                
        elif command == "export":
            if len(sys.argv) < 3:
                print("Usage: python testing.py export <product_id> [filename]")
                sys.exit(1)
            import analysis
            product_id = int(sys.argv[2])
            filename = sys.argv[3] if len(sys.argv) > 3 else f"product_{product_id}_data.csv"
            
            time_series = analysis.get_time_series_by_product_id(product_id)
            if time_series:
                analysis.export_to_csv(time_series, filename)
            else:
                print(f"No data found for product {product_id}")
                
        elif command == "sets":
            import analysis
            sets = analysis.get_all_available_sets()
            print("=== Available Pokemon Sets ===")
            for i, set_info in enumerate(sets, 1):
                print(f"{i:2d}. {set_info['set_name']}")
                if set_info['booster_product_id']:
                    print(f"     Booster Pack ID: {set_info['booster_product_id']} (Data: {set_info['has_booster_data']})")
                if set_info['booster_box_product_id']:
                    print(f"     Booster Box ID: {set_info['booster_box_product_id']} (Data: {set_info['has_box_data']})")
                print(f"     Release: {set_info['release_date']}")
                print()
                
        elif command == "recent":
            import analysis
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 30
            activity = analysis.get_recent_activity(days)
            print(f"=== Recent Activity (Last {days} days) ===")
            
            if activity:
                for item in activity[:20]:  # Show first 20
                    print(f"{item['date']}: {item['set_name']} - ${item['market_price']:.2f} ({item['quantity_sold']} sold)")
            else:
                print("No recent activity found")
                
        elif command == "demo":
            import analysis
            analysis.demo_analysis()
            
        elif command == "visualize":
            import subprocess
            result = subprocess.run([sys.executable, "visualize_volume.py"], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                print(result.stdout)
            else:
                print("Error running visualization:")
                print(result.stderr)
            
        elif command == "volume":
            import analysis
            filename = sys.argv[2] if len(sys.argv) > 2 else "pokemon_sets_volume_sold.csv"
            include_product_type = "--include-type" in sys.argv
            
            print("Generating volume sold data for all Pokemon sets...")
            analysis.export_volume_sold_csv(filename, include_product_type)
            
        elif command == "volume_summary":
            import analysis
            summary = analysis.get_volume_summary_by_set()
            
            print("=== Volume Sold Summary by Pokemon Set ===")
            print(f"{'Set Name':<40} {'Total Volume ($)':<15} {'Qty Sold':<10} {'Avg Price':<10} {'Data Points':<12}")
            print("-" * 90)
            
            for set_data in summary:
                print(f"{set_data['set_name']:<40} ${set_data['total_volume']:>13,.2f} {set_data['total_quantity']:>9,} ${set_data['avg_price']:>8.2f} {set_data['data_points']:>11}")
            
            print(f"\nTotal sets with sales data: {len(summary)}")
            if summary:
                total_volume = sum(s['total_volume'] for s in summary)
                total_quantity = sum(s['total_quantity'] for s in summary)
                print(f"Overall total volume: ${total_volume:,.2f}")
                print(f"Overall total quantity: {total_quantity:,} units")
            
        else:
            print("Available commands:")
            print("  summary              - Show Pokemon sets summary")
            print("  trends [product_id] [days] - Show price trends")
            print("  fetch_one <product_id> - Fetch price history for one product")
            print("  fetch_all            - Fetch price history for all Pokemon sets")
            print("  test_api             - Test price history API with sample data")
            print("  query                - Query and display price history example")
            print("  latest_sales         - Test latest sales API")
            print()
            print("Analysis commands:")
            print("  analyze <product_id> [window_days] - Detailed analysis of a product")
            print("  export <product_id> [filename]     - Export time series data to CSV")
            print("  sets                              - List all available Pokemon sets")
            print("  recent [days]                     - Show recent price activity")
            print("  demo                              - Run analysis demo")
            print("  volume [filename]                 - Export volume sold data for all sets to CSV")
            print("  volume_summary                    - Show volume sold summary by set")
            print("                                      Use --include-type to include product type in volume export")
    else:
        # Default behavior - test price history
        test_price_history_api()
        query_price_history_example()

