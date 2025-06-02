#!/usr/bin/env python3
"""
Simple test script to debug TCGPlayer pipeline issues.
"""

import asyncio
import logging
import os
import sys

# Add project root to PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from plugins.tcgplayer.fetchers import PokemonSetsCsvFetcher
from plugins.tcgplayer.parsers import PokemonSetsParser
from plugins.tcgplayer.sinks import TcgDatabaseSink

async def test_pokemon_sets_pipeline():
    """Test the Pokemon sets pipeline step by step."""
    try:
        print("Testing Pokemon sets pipeline...")
        
        # Step 1: Test fetcher
        print("1. Testing fetcher...")
        fetcher = PokemonSetsCsvFetcher(csv_path="development/tcg/pokemon_sets.csv")
        
        raw_items = []
        async for item in fetcher.fetch():
            raw_items.append(item)
            print(f"   Fetched: {item.source}, payload size: {len(item.payload)} bytes")
        
        if not raw_items:
            print("   ERROR: No items fetched!")
            return
        
        # Step 2: Test parser
        print("2. Testing parser...")
        parser = PokemonSetsParser()
        
        parsed_items = []
        for raw_item in raw_items:
            items = await parser.parse_csv(raw_item)
            parsed_items.extend(items)
            print(f"   Parsed {len(items)} items from {raw_item.source}")
            
            # Show first few items
            for i, item in enumerate(items[:3]):
                print(f"   Item {i+1}: {item.content}")
        
        if not parsed_items:
            print("   ERROR: No items parsed!")
            return
        
        # Step 3: Test sink
        print("3. Testing sink...")
        sink = TcgDatabaseSink(db_path="tcg_test_debug.db")
        
        # Initialize the sink using context manager
        async with sink:
            # Handle items
            for item in parsed_items:
                await sink.handle(item)
                print(f"   Handled: {item.topic}")
        
        print("4. Checking database...")
        import sqlite3
        conn = sqlite3.connect("tcg_test_debug.db")
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM pokemon_sets")
        count = cursor.fetchone()[0]
        print(f"   Total records in database: {count}")
        
        cursor.execute("SELECT set_name, booster_product_id FROM pokemon_sets LIMIT 3")
        rows = cursor.fetchall()
        for row in rows:
            print(f"   Record: {row}")
        
        conn.close()
        print("Pipeline test complete!")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_pokemon_sets_pipeline())
