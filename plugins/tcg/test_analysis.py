#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(__file__))

import analysis

print("=== Testing Time Series Analysis Module ===")

# Test 1: Get time series data for a known product
print("\n1. Testing time series extraction for product 624679...")
time_series = analysis.get_time_series_by_product_id(624679)
print(f"Found {len(time_series)} data points")

if time_series:
    print("\nFirst data point:")
    ts = time_series[0]
    print(f"  Date: {ts.date}")
    print(f"  Market Price: ${ts.market_price}")
    print(f"  Quantity Sold: {ts.quantity_sold}")
    print(f"  Low Price: ${ts.low_price}")
    print(f"  High Price: ${ts.high_price}")
    print(f"  Variant: {ts.variant}")
    print(f"  Condition: {ts.condition}")
    
    print("\nLast data point:")
    ts = time_series[-1]
    print(f"  Date: {ts.date}")
    print(f"  Market Price: ${ts.market_price}")
    print(f"  Quantity Sold: {ts.quantity_sold}")
    print(f"  Low Price: ${ts.low_price}")
    print(f"  High Price: ${ts.high_price}")

# Test 2: Get available sets
print("\n2. Testing available sets...")
sets = analysis.get_all_available_sets()
print(f"Found {len(sets)} Pokemon sets")

for s in sets[:3]:
    print(f"  {s['set_name']}")
    print(f"    Booster data: {s['has_booster_data']}")
    print(f"    Box data: {s['has_box_data']}")

# Test 3: Price summary
print("\n3. Testing price summary...")
summary = analysis.get_price_summary_by_product(624679)
if summary:
    print(f"Data points: {summary['data_points']}")
    print(f"Date range: {summary['date_range']['start']} to {summary['date_range']['end']}")
    print(f"Current price: ${summary['market_price']['current']}")
    print(f"Price range: ${summary['market_price']['min']} - ${summary['market_price']['max']}")
    print(f"Total sales volume: {summary['sales_volume']['total']}")

# Test 4: Time series by set name
print("\n4. Testing time series by set name...")
ts_by_set = analysis.get_time_series_by_set_name("Scarlet & Violet: Destined Rivals", "booster_box")
print(f"Found {len(ts_by_set)} data points for Destined Rivals booster box")

print("\n=== Tests completed ===")
