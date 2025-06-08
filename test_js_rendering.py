#!/usr/bin/env python3
"""
Test script to verify JavaScript rendering functionality in AppMagic fetcher.
"""
import asyncio
import logging
from plugins.appmagic.fetcher import AppMagicFetcher
from plugins.appmagic.parser import _handler_publisher_html

# Set up logging to see what's happening
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_js_rendering():
    """Test JavaScript rendering vs regular HTTP fetching."""
    print("🧪 Testing JavaScript Rendering vs Regular HTTP...")
    
    # Mock company data for testing
    test_company = {
        "name": "MAG Interactive",
        "store": 1,  # iOS App Store
        "store_publisher_id": "6558349509091194327"
    }
    
    # Test with JavaScript rendering enabled
    print("\n1. Testing with JavaScript rendering (stealth mode)...")
    async with AppMagicFetcher(
        companies=[test_company],
        use_javascript_renderer=True,
        include_apps=False,
        include_country_split=False
    ) as fetcher_js:
        print(f"   ✅ JavaScript fetcher initialized: {fetcher_js.name}")
        print(f"   ✅ Playwright client available: {fetcher_js._playwright_client is not None}")
        
        # Test the HTML fetching method directly
        test_url = "https://appmagic.rocks/publisher/mag-interactive/1_6558349509091194327"
        print(f"   🔄 Testing URL: {test_url}")
        
        try:
            html_js = await fetcher_js._safe_get_html_with_js(
                test_url, 
                wait_for_selector=".publisher-info, .app-card, .stats-card, div.table.ng-star-inserted"
            )
            print(f"   ✅ JavaScript rendering result: {len(html_js)} characters")
            if html_js:
                # Check for common SPA indicators
                has_react = "react" in html_js.lower() or "React" in html_js
                has_vue = "vue" in html_js.lower() or "Vue" in html_js
                has_angular = "angular" in html_js.lower() or "ng-" in html_js
                has_js_framework = has_react or has_vue or has_angular
                print(f"   📊 JavaScript framework detected: {has_js_framework}")
                print(f"   📄 First 200 chars: {html_js[:200]}...")
                
                # Test the enhanced HTML parser
                print("\n   🔍 Testing enhanced HTML parser on JS-rendered content...")
                test_payload = {
                    "up_id": 901,
                    "store": 1,
                    "store_publisher_id": "6558349509091194327",
                    "html_url": test_url,
                    "html": html_js
                }
                
                parsed_items = _handler_publisher_html(test_payload)
                app_items = [item for item in parsed_items if item.topic == "appmagic.publisher_html"]
                publisher_items = [item for item in parsed_items if item.topic == "appmagic.publisher_info"]
                
                print(f"   📋 Enhanced parser found {len(parsed_items)} total items:")
                print(f"      - {len(app_items)} app items")
                print(f"      - {len(publisher_items)} publisher info items")
                
                if publisher_items:
                    pub_info = publisher_items[0].content
                    print(f"   🏢 Publisher: {pub_info.get('name')} ({pub_info.get('apps_count', 0)} apps)")
                
                if app_items:
                    print(f"   📱 Sample apps:")
                    for i, item in enumerate(app_items[:3]):
                        app = item.content
                        print(f"      {i+1}. {app.get('name')}")
                        print(f"         📊 Downloads: {app.get('lifetime_downloads_str')} ({app.get('lifetime_downloads_val'):,})")
                        print(f"         💰 Revenue: {app.get('lifetime_revenue_str')} ({app.get('lifetime_revenue_val'):,})")
                        print(f"         🎯 Genres: {app.get('genres', [])}")
                        
        except Exception as e:
            print(f"   ❌ JavaScript rendering failed: {e}")
    
    # Test with JavaScript rendering disabled
    print("\n2. Testing with regular HTTP fetching...")
    async with AppMagicFetcher(
        companies=[test_company],
        use_javascript_renderer=False,
        include_apps=False,
        include_country_split=False
    ) as fetcher_http:
        print(f"   ✅ HTTP fetcher initialized: {fetcher_http.name}")
        print(f"   ✅ Playwright client disabled: {fetcher_http._playwright_client is None}")
        
        try:
            html_http = await fetcher_http._safe_get_text(test_url)
            print(f"   ✅ HTTP fetching result: {len(html_http)} characters")
            if html_http:
                print(f"   📄 First 200 chars: {html_http[:200]}...")
                
                # Test parser on simple HTTP content
                print("\n   🔍 Testing enhanced HTML parser on HTTP content...")
                test_payload = {
                    "up_id": 902,
                    "store": 1,
                    "store_publisher_id": "6558349509091194327",
                    "html_url": test_url,
                    "html": html_http
                }
                
                parsed_items = _handler_publisher_html(test_payload)
                print(f"   📋 Parser found {len(parsed_items)} items from HTTP content")
                
        except Exception as e:
            print(f"   ❌ HTTP fetching failed: {e}")
    
    print("\n✅ Test completed!")


async def test_appmagic_url_construction():
    """Test the URL construction function."""
    print("\n🔗 Testing AppMagic URL construction...")
    
    from plugins.appmagic.fetcher import construct_html_url
    
    test_cases = [
        ("Test Publisher", 1, "12345"),
        ("My Game Studio", 2, "abcdef"),
        ("Publisher With Spaces", 1, "xyz789"),
        ("MAG Interactive", 1, "6558349509091194327"),
    ]
    
    for name, store_id, publisher_id in test_cases:
        url = construct_html_url(name, store_id, publisher_id)
        print(f"   📎 {name} -> {url}")
    
    print("   ✅ URL construction test completed!")


if __name__ == "__main__":
    asyncio.run(test_js_rendering())
    asyncio.run(test_appmagic_url_construction())
