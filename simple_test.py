#!/usr/bin/env python3
import asyncio
import json
from plugins.appmagic.parser import AppMagicParser
from core.models import RawItem

async def simple_test():
    print("Testing AppMagic Parser...")
    
    parser = AppMagicParser()
    print(f"Parser name: {parser.name}")
    
    # Mock data
    mock_data = {
        "united_publisher_id": 12345,
        "applications": [{
            "united_application_id": 98765,
            "name": "Test App",
            "snapshot": {"downloads_30d": 1000}
        }]
    }
    
    raw_item = RawItem(source="appmagic.publisher_apps_api", payload=json.dumps(mock_data))
    
    async def mock_stream():
        yield raw_item
    
    count = 0
    async for item in parser(mock_stream()):
        count += 1
        print(f"Parsed item {count}: {item.topic if hasattr(item, 'topic') else item}")
    
    print(f"Total items processed: {count}")

if __name__ == "__main__":
    asyncio.run(simple_test())
