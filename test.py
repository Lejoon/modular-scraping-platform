#!/usr/bin/env python3
"""
marketcap.py – look up a stock’s market-cap (numeric, no currency) from its ISIN
using Avanza’s public endpoints.

Usage:
    python marketcap.py SE0017615644
"""

import sys
import requests

SEARCH_URL = "https://www.avanza.se/_api/search/filtered-search"
HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    # Any sensible UA will do; Avanza blocks empty/default UA strings
    "User-Agent": "Mozilla/5.0 (market-cap lookup script)",
}

def _get_orderbook_id(isin: str) -> str:
    """POST the search endpoint and return the first hit’s orderBookId."""
    payload = {
        "query": isin,
        "searchFilter": {"types": []},
        "screenSize": "DESKTOP",
        "originPath": "/start",
        "originPlatform": "PWA",
        "searchSessionId": "python-script",
        "pagination": {"from": 0, "size": 30},
    }
    resp = requests.post(SEARCH_URL, json=payload, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if data.get("totalNumberOfHits", 0) == 0:
        raise ValueError(f"No hits for ISIN {isin!r}")

    return data["hits"][0]["orderBookId"]

def get_market_cap(isin: str) -> int:
    """
    Return the market-capitalisation *value* (int) for the given ISIN.
    Raises ValueError if the ISIN is not found or the expected JSON path is missing.
    """
    orderbook_id = _get_orderbook_id(isin)
    info_url = f"https://www.avanza.se/_api/market-guide/stock/{orderbook_id}"
    resp = requests.get(info_url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    info = resp.json()

    try:
        return int(info["keyIndicators"]["marketCapital"]["value"])
    except (KeyError, TypeError) as exc:
        raise ValueError(f"Market-cap not available for orderBookId {orderbook_id}") from exc

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python marketcap.py <ISIN>\nExample: python marketcap.py SE0017615644")
    try:
        print(get_market_cap(sys.argv[1].strip()))
    except Exception as e:
        sys.exit(f"Error: {e}")
