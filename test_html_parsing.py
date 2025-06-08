#!/usr/bin/env python3

import asyncio
import json
import logging
import sys
import os
from bs4 import BeautifulSoup
import re

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from plugins.appmagic.fetcher import AppMagicFetcher
from plugins.appmagic.parser import parse_appmagic_metric, _handler_publisher_html

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_html_parsing():
    companies = [{'name': 'Mag Interactive', 'store': 1, 'store_publisher_id': '6558349509091194327'}]
    
    print("=== Testing JavaScript Rendering (Default) ===")
    async with AppMagicFetcher(companies=companies, use_javascript_renderer=True) as fetcher:
        from plugins.appmagic.fetcher import construct_html_url
        url = construct_html_url("MAG Interactive", 1, "6558349509091194327")
        print(f"Fetching HTML with JS rendering from: {url}")
        
        # Use the new JavaScript-aware method
        wait_selector = ".publisher-info, .app-card, .stats-card, [data-testid='publisher-stats']"
        html_js = await fetcher._safe_get_html_with_js(url, wait_selector)
        print(f"JS-rendered HTML content length: {len(html_js) if html_js else 0}")
        
        if html_js:
            # Save JS-rendered HTML to file for inspection
            with open("publisher_page_js.html", "w", encoding="utf-8") as f:
                f.write(html_js)
            print("JS-rendered HTML saved to publisher_page_js.html")
    
    print("\n=== Testing Simple HTTP Fetching (Fallback) ===")
    async with AppMagicFetcher(companies=companies, use_javascript_renderer=False) as fetcher:
        url = construct_html_url("MAG Interactive", 1, "6558349509091194327")
        print(f"Fetching HTML with simple HTTP from: {url}")
        
        html_simple = await fetcher._safe_get_text(url)
        print(f"Simple HTTP HTML content length: {len(html_simple) if html_simple else 0}")
        
        if html_simple:
            # Save simple HTML to file for comparison
            with open("publisher_page_simple.html", "w", encoding="utf-8") as f:
                f.write(html_simple)
            print("Simple HTTP HTML saved to publisher_page_simple.html")
    
    # Use the JS-rendered content for further analysis
    html = html_js if 'html_js' in locals() and html_js else (html_simple if 'html_simple' in locals() else None)
    
    if html:
        print(f"\n=== Using {'JS-rendered' if html == html_js else 'simple HTTP'} content for analysis ===")
        
        # Compare content lengths if both are available
        if 'html_js' in locals() and 'html_simple' in locals() and html_js and html_simple:
            print(f"Content length comparison:")
            print(f"  JS-rendered: {len(html_js):,} characters")
            print(f"  Simple HTTP: {len(html_simple):,} characters")
            print(f"  Difference: {len(html_js) - len(html_simple):+,} characters")
            
            # Check for dynamic content indicators
            js_unique = set(html_js.split()) - set(html_simple.split())
            if js_unique:
                print(f"  Found {len(js_unique)} unique words in JS version (first 5): {list(js_unique)[:5]}")
        
        # Save the selected HTML for inspection
        with open("publisher_page.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("Selected HTML saved to publisher_page.html")
        
        # Test the improved HTML parser
        print("\n=== Testing Enhanced HTML Parser ===")
        test_payload = {
            "up_id": 901,
            "store": 1,
            "store_publisher_id": "6558349509091194327",
            "html_url": url,
            "html": html
        }
        
        parsed_items = _handler_publisher_html(test_payload)
        print(f"Enhanced parser found {len(parsed_items)} items")
        
        # Separate apps from publisher info
        app_items = [item for item in parsed_items if item.topic == "appmagic.publisher_html"]
        publisher_items = [item for item in parsed_items if item.topic == "appmagic.publisher_info"]
        
        print(f"  - {len(app_items)} app items")
        print(f"  - {len(publisher_items)} publisher info items")
        
        # Display publisher info
        if publisher_items:
            pub_info = publisher_items[0].content
            print(f"\nPublisher Info:")
            print(f"  Name: {pub_info.get('name')}")
            print(f"  Total Apps: {pub_info.get('total_apps', 0)}")
            print(f"  Apps Parsed: {pub_info.get('apps_count', 0)}")
        
        # Display first few apps
        print(f"\nFirst {min(3, len(app_items))} Apps:")
        for i, item in enumerate(app_items[:3]):
            app = item.content
            print(f"  App {i+1}: {app.get('name')}")
            print(f"    Genres: {app.get('genres', [])}")
            print(f"    Countries: {app.get('countries', 0)}")
            print(f"    Release: {app.get('first_release')}")
            print(f"    Downloads: {app.get('lifetime_downloads_str')} ({app.get('lifetime_downloads_val'):,})")
            print(f"    Revenue: {app.get('lifetime_revenue_str')} ({app.get('lifetime_revenue_val'):,})")
            print(f"    Store ID: {app.get('store_specific_id')}")
            print()
        
        # Manual inspection of HTML structure
        print("\n=== Manual HTML Inspection ===")
        soup = BeautifulSoup(html, "html.parser")
        
        # Look for common app listing patterns
        print("Looking for app-related elements...")
        
        # Search for tables, rows, or other structures that might contain app data
        tables = soup.find_all("table")
        print(f"Found {len(tables)} tables")
        
        for i, table in enumerate(tables[:3]):  # Check first 3 tables
            rows = table.find_all("tr")
            print(f"Table {i+1}: {len(rows)} rows")
            if rows:
                # Look at the first few rows to understand structure
                for j, row in enumerate(rows[:3]):
                    cells = row.find_all(["td", "th"])
                    print(f"  Row {j+1}: {len(cells)} cells - {[cell.get_text(strip=True)[:50] for cell in cells[:3]]}")
        
        # Look for div structures
        app_divs = soup.find_all("div", class_=lambda x: x and ("app" in x.lower() or "item" in x.lower()))
        print(f"Found {len(app_divs)} divs with 'app' or 'item' in class")
        
        # Look for download/revenue indicators
        download_text = soup.find_all(text=re.compile(r'\d+[KMB]?\s*(download|DL)', re.I))
        revenue_text = soup.find_all(text=re.compile(r'\$\d+[KMB]?', re.I))
        
        print(f"Found {len(download_text)} download indicators")
        print(f"Found {len(revenue_text)} revenue indicators")
        
        if download_text:
            print("Sample download text:", download_text[:3])
        if revenue_text:
            print("Sample revenue text:", revenue_text[:3])

if __name__ == "__main__":
    asyncio.run(test_html_parsing())
