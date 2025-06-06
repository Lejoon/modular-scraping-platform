#!/usr/bin/env python3
"""Test script to inspect AppMagic API response structure."""

import asyncio
import json
import sys
import os

# Add project root to PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.infra.http import HttpClient

async def test_appmagic_api():
    """Test the AppMagic API to see response structure."""
    print("Starting API test...")
    http = HttpClient()
    print("HTTP client created")
    
    # Test the publisher applications API
    url = "https://appmagic.rocks/api/v2/search/publisher-applications"
    params = {
        "sort": "downloads",
        "united_publisher_id": 5605118827,  # Big Blue Bubble
        "from": 0,
    }
    
    print(f"Making request to: {url}")
    print(f"Params: {params}")
    
    try:
        result = await http.get_json(url, params=params)
        print(f"\nResponse type: {type(result)}")
        
        if isinstance(result, list):
            print(f"Response is a list with {len(result)} items")
            if result:
                print(f"First item type: {type(result[0])}")
                if isinstance(result[0], dict):
                    print(f"First item keys: {list(result[0].keys())}")
                    print(f"First item sample: {json.dumps(result[0], indent=2)[:500]}...")
        elif isinstance(result, dict):
            print(f"Response is a dict with keys: {list(result.keys())}")
            if result:
                for key, value in list(result.items())[:3]:
                    print(f"  {key}: {type(value)} {'- length '+str(len(value)) if hasattr(value, '__len__') else ''}")
                    
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if hasattr(http, 'close'):
            await http.close()

if __name__ == "__main__":
    asyncio.run(test_appmagic_api())
