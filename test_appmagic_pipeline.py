#!/usr/bin/env python3
"""
Test script for the updated AppMagic plugin pipeline.
"""
import asyncio
import json
from pathlib import Path

from plugins.appmagic.fetcher import AppMagicFetcher
from plugins.appmagic.parser import AppMagicParser
from plugins.appmagic.sinks import AppMagicSink
from core.models import RawItem, ParsedItem


async def test_appmagic_pipeline():
    """Test the complete AppMagic pipeline with mock data."""
    print("🧪 Testing AppMagic Pipeline...")
    
    # Test 1: Initialize components
    print("\n1. Initializing components...")
    fetcher = AppMagicFetcher(
        api_key="test_key",
        include_apps=True,
        include_country_split=False
    )
    parser = AppMagicParser()
    sink = AppMagicSink(db_path="test_appmagic.db")
    
    print(f"   ✅ Fetcher: {fetcher.name}")
    print(f"   ✅ Parser: {parser.name}")
    print(f"   ✅ Sink: {sink.name}")
    
    # Test 2: Test parser with mock data
    print("\n2. Testing parser with mock API data...")
    
    # Mock API response data
    mock_api_data = {
        "united_publisher_id": 12345,
        "applications": [
            {
                "united_application_id": 98765,
                "name": "Test App",
                "icon_url": "https://example.com/icon.png",
                "release_date": "2023-01-15",
                "contains_ads": True,
                "has_in_app_purchases": False,
                "snapshot": {
                    "downloads_30d": 1000000,
                    "revenue_30d": 50000.0,
                    "downloads_lifetime": 5000000,
                    "revenue_lifetime": 250000.0
                },
                "store_applications": [
                    {
                        "store_id": 1,
                        "store_app_id": "com.test.app",
                        "name": "Test App",
                        "url": "https://play.google.com/store/apps/details?id=com.test.app"
                    }
                ]
            }
        ]
    }
    
    # Mock HTML data
    mock_html_data = {
        "up_id": 12345,
        "html": '''
        <table>
            <tr class="app-row">
                <td class="name"><a href="/app/1/com.test.app" class="app-name">Test App</a></td>
                <td><img src="https://example.com/icon.png" /></td>
                <td>5M downloads</td>
                <td>$250K</td>
                <td>Jan 15, 2023</td>
            </tr>
        </table>
        '''
    }
    
    # Create mock RawItems
    raw_items = [
        RawItem(source="appmagic.publisher_apps_api", payload=json.dumps(mock_api_data)),
        RawItem(source="appmagic.publisher_html", payload=json.dumps(mock_html_data))
    ]
    
    # Test parser
    async def mock_item_stream():
        for item in raw_items:
            yield item
    
    parsed_items = []
    async for item in parser(mock_item_stream()):
        if isinstance(item, ParsedItem):
            parsed_items.append(item)
            print(f"   ✅ Parsed item: {item.topic}")
    
    print(f"   📊 Total parsed items: {len(parsed_items)}")
    
    # Test 3: Test sink with parsed data
    print("\n3. Testing sink with parsed data...")
    
    # Clean up test database if it exists
    test_db_path = Path("test_appmagic.db")
    if test_db_path.exists():
        test_db_path.unlink()
    
    sink_count = 0
    for item in parsed_items:
        await sink.handle(item)
        sink_count += 1
        print(f"   ✅ Sunk item: {item.topic}")
    
    print(f"   📊 Total items sunk: {sink_count}")
    
    # Test 4: Verify database content
    print("\n4. Verifying database content...")
    
    # Check if tables were created
    tables_query = "SELECT name FROM sqlite_master WHERE type='table'"
    tables = await sink.db.fetch_all(tables_query)
    table_names = [row['name'] for row in tables]
    
    print(f"   📋 Tables created: {len(table_names)}")
    for table in sorted(table_names):
        # Count rows in each table
        count_query = f"SELECT COUNT(*) as count FROM {table}"
        result = await sink.db.fetch_one(count_query)
        count = result['count'] if result else 0
        print(f"      - {table}: {count} rows")
    
    # Test 5: Test fetcher configuration
    print("\n5. Testing fetcher configuration...")
    print(f"   ✅ API Key set: {'***' + fetcher._api_key[-4:] if len(fetcher._api_key) > 4 else '***'}")
    print(f"   ✅ Include apps: {fetcher._include_apps}")
    print(f"   ✅ Include country split: {fetcher._include_country_split}")
    print(f"   ✅ Publisher apps URL: {fetcher._URL_PUBLISHER_APPS}")
    
    print("\n🎉 All tests completed successfully!")
    
    # Clean up
    if test_db_path.exists():
        test_db_path.unlink()
        print("   🧹 Test database cleaned up")


if __name__ == "__main__":
    asyncio.run(test_appmagic_pipeline())
