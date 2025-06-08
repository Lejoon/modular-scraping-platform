#!/usr/bin/env python3

import asyncio
import json
import logging
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from plugins.appmagic.fetcher import AppMagicFetcher

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def test_publisher_data():
    companies = [{'name': 'Mag Interactive', 'store': 1, 'store_publisher_id': '6558349509091194327'}]
    
    async with AppMagicFetcher(companies=companies) as fetcher:
        # Test the united-publishers search to see what accounts data looks like
        ids = [{'store': 1, 'store_publisher_id': '6558349509091194327'}]
        search_payload = await fetcher._safe_post_json(fetcher._URL_SEARCH, {'ids': ids})
        publishers = fetcher._extract_publishers(search_payload)
        
        print('Number of publishers:', len(publishers))
        if publishers:
            pub = publishers[0]
            print('Publisher keys:', list(pub.keys()))
            print('Publisher accounts:', pub.get('accounts', []))
            print('\nPublisher full data:')
            print(json.dumps(pub, indent=2, default=str))
            
            # Test HTML URL construction
            if pub.get('accounts'):
                for acc in pub.get('accounts', []):
                    store_id = acc.get('storeId') or acc.get('store') or 1
                    publisher_id = acc.get('publisherId') or acc.get('store_publisher_id')
                    print(f"\nAccount: store={store_id}, publisher_id={publisher_id}")
                    
                    if publisher_id:
                        from plugins.appmagic.fetcher import construct_html_url
                        url = construct_html_url(pub.get('name', ''), store_id, publisher_id)
                        print(f"HTML URL: {url}")
            else:
                print("\nNo accounts found - this is why HTML isn't being fetched!")
                print("Let's try to construct URL from main publisher data:")
                store_id = 1
                publisher_id = '6558349509091194327'
                from plugins.appmagic.fetcher import construct_html_url
                url = construct_html_url(pub.get('name', ''), store_id, publisher_id)
                print(f"Constructed HTML URL: {url}")
                
                # Test if we can fetch HTML from this URL
                print("\nTesting HTML fetch...")
                html = await fetcher._safe_get_text(url)
                print(f"HTML content length: {len(html) if html else 0}")
                if html:
                    print("First 200 chars of HTML:")
                    print(html[:200])

if __name__ == "__main__":
    asyncio.run(test_publisher_data())
