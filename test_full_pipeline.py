#!/usr/bin/env python3
"""
Test script to verify the full AppMagic pipeline with JavaScript rendering.
This simulates how the fetcher actually works in the real pipeline.
"""
import asyncio
import logging
from plugins.appmagic.fetcher import AppMagicFetcher
from plugins.appmagic.parser import AppMagicParser

# Set up logging to see what's happening
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_full_pipeline():
    """Test the complete AppMagic pipeline with JavaScript rendering."""
    print("🧪 Testing Full AppMagic Pipeline with JavaScript Rendering...")
    
    # Mock company data for testing
    test_companies = [
        {
            "name": "MAG Interactive",
            "store": 1,  # iOS App Store
            "store_publisher_id": "6558349509091194327"
        }
    ]
    
    # Test with JavaScript rendering enabled (production-like scenario)
    print("\n1. Testing full pipeline with JavaScript rendering...")
    async with AppMagicFetcher(
        companies=test_companies,
        use_javascript_renderer=True,
        include_apps=True,  # Enable apps fetching like real pipeline
        include_country_split=False,  # Disable for faster testing
        rate_limit_s=0.5  # Faster for testing
    ) as fetcher:
        print(f"   ✅ Fetcher initialized: {fetcher.name}")
        print(f"   ✅ JavaScript rendering enabled: {fetcher._use_js_renderer}")
        print(f"   ✅ Playwright client available: {fetcher._playwright_client is not None}")
        
        # Initialize parser
        parser = AppMagicParser()
        
        # Collect all raw items
        raw_items = []
        item_count = 0
        
        print("\n   🔄 Fetching raw items...")
        async for raw_item in fetcher.fetch():
            raw_items.append(raw_item)
            item_count += 1
            print(f"   📦 Item {item_count}: {raw_item.source} ({len(raw_item.payload)} bytes)")
            
            # Stop after a reasonable number for testing
            if item_count >= 10:  # Limit for testing
                break
        
        print(f"\n   ✅ Collected {len(raw_items)} raw items")
        
        # Parse the raw items
        print("\n   🔄 Parsing raw items...")
        parsed_items = []
        
        async def raw_item_generator():
            for item in raw_items:
                yield item
        
        async for parsed_item in parser(raw_item_generator()):
            parsed_items.append(parsed_item)
            print(f"   📋 Parsed item: {parsed_item.topic}")
        
        print(f"\n   ✅ Generated {len(parsed_items)} parsed items")
        
        # Analyze results
        html_items = [item for item in raw_items if item.source == "appmagic.publisher_html"]
        api_items = [item for item in raw_items if item.source == "appmagic.publisher_apps_api"]
        
        print(f"\n   📊 Raw Items Summary:")
        print(f"      - HTML items: {len(html_items)}")
        print(f"      - API items: {len(api_items)}")
        
        # Show HTML content size if available
        if html_items:
            html_payload = html_items[0].payload.decode('utf-8')
            import json
            html_data = json.loads(html_payload)
            html_content = html_data.get('html', '')
            print(f"      - HTML content size: {len(html_content):,} characters")
            
            # Quick check for JavaScript content
            has_angular = 'ng-' in html_content or 'angular' in html_content.lower()
            has_react = 'react' in html_content.lower()
            print(f"      - Contains Angular: {has_angular}")
            print(f"      - Contains React: {has_react}")
        
        # Show parsed app items
        app_parsed_items = [item for item in parsed_items if item.topic == "appmagic.publisher_html"]
        publisher_info_items = [item for item in parsed_items if item.topic == "appmagic.publisher_info"]
        
        print(f"\n   📱 Parsed Items Summary:")
        print(f"      - App items: {len(app_parsed_items)}")
        print(f"      - Publisher info items: {len(publisher_info_items)}")
        
        if publisher_info_items:
            pub_info = publisher_info_items[0].content
            print(f"      - Publisher: {pub_info.get('name')} ({pub_info.get('apps_count', 0)} apps)")
        
        if app_parsed_items:
            print(f"\n   🎮 Sample Apps from Pipeline:")
            for i, item in enumerate(app_parsed_items[:3]):
                app = item.content
                print(f"      {i+1}. {app.get('name')}")
                print(f"         📊 Downloads: {app.get('lifetime_downloads_str')} ({app.get('lifetime_downloads_val'):,})")
                print(f"         💰 Revenue: {app.get('lifetime_revenue_str')} ({app.get('lifetime_revenue_val'):,})")
        
    print("\n✅ Full pipeline test completed!")


async def test_fallback_behavior():
    """Test fallback behavior when JavaScript rendering fails."""
    print("\n🧪 Testing Fallback Behavior...")
    
    test_companies = [
        {
            "name": "MAG Interactive",
            "store": 1,
            "store_publisher_id": "6558349509091194327"
        }
    ]
    
    # Test with JavaScript rendering disabled
    print("\n1. Testing with JavaScript rendering disabled...")
    async with AppMagicFetcher(
        companies=test_companies,
        use_javascript_renderer=False,
        include_apps=False,
        include_country_split=False
    ) as fetcher:
        print(f"   ✅ Fetcher initialized without JS rendering")
        print(f"   ✅ Playwright client disabled: {fetcher._playwright_client is None}")
        
        # Collect HTML items only
        html_items = []
        async for raw_item in fetcher.fetch():
            if raw_item.source == "appmagic.publisher_html":
                html_items.append(raw_item)
                break  # Just get one for comparison
        
        if html_items:
            html_payload = html_items[0].payload.decode('utf-8')
            import json
            html_data = json.loads(html_payload)
            html_content = html_data.get('html', '')
            print(f"   📄 HTTP-only content size: {len(html_content):,} characters")
        else:
            print("   ❌ No HTML items collected")
    
    print("\n✅ Fallback test completed!")


if __name__ == "__main__":
    asyncio.run(test_full_pipeline())
    asyncio.run(test_fallback_behavior())
