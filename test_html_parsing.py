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
    
    async with AppMagicFetcher(companies=companies) as fetcher:
        from plugins.appmagic.fetcher import construct_html_url
        url = construct_html_url("MAG Interactive", 1, "6558349509091194327")
        print(f"Fetching HTML from: {url}")
        
        html = await fetcher._safe_get_text(url)
        print(f"HTML content length: {len(html) if html else 0}")
        
        if html:
            # Save HTML to file for inspection
            with open("publisher_page.html", "w", encoding="utf-8") as f:
                f.write(html)
            print("HTML saved to publisher_page.html")
            
            # Test the HTML parser
            print("\n=== Testing HTML Parser ===")
            test_payload = {
                "up_id": 901,
                "store": 1,
                "store_publisher_id": "6558349509091194327",
                "html_url": url,
                "html": html
            }
            
            parsed_items = _handler_publisher_html(test_payload)
            print(f"Parser found {len(parsed_items)} items")
            
            for item in parsed_items:
                print(f"App: {item.content.get('name')}")
                print(f"  Lifetime downloads: {item.content.get('lifetime_downloads_val')}")
                print(f"  Lifetime revenue: {item.content.get('lifetime_revenue_val')}")
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
