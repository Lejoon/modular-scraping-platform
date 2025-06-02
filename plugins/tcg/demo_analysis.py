#!/usr/bin/env python3
"""
Demo script showing how to use the analysis.py module for time series data extraction.

This script demonstrates various ways to extract and analyze TCG price data.
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

import analysis
from datetime import datetime, timedelta

def main():
    print("=== TCG Price Data Analysis Demo ===\n")
    
    # 1. Show available Pokemon sets
    print("1. Available Pokemon Sets with Price Data:")
    sets = analysis.get_all_available_sets()
    sets_with_data = [s for s in sets if s['has_booster_data'] or s['has_box_data']]
    
    for i, set_info in enumerate(sets_with_data[:5], 1):
        print(f"   {i}. {set_info['set_name']}")
        print(f"      Booster data: {set_info['has_booster_data']}")
        print(f"      Box data: {set_info['has_box_data']}")
    
    if not sets_with_data:
        print("   No sets with price data found.")
        return
    
    # 2. Extract time series for a specific product
    sample_set = sets_with_data[0]
    product_id = sample_set['booster_box_product_id'] if sample_set['has_box_data'] else sample_set['booster_product_id']
    product_type = "Booster Box" if sample_set['has_box_data'] else "Booster Pack"
    
    print(f"\n2. Time Series Data for {sample_set['set_name']} - {product_type} (ID: {product_id}):")
    
    time_series = analysis.get_time_series_by_product_id(product_id)
    print(f"   Total data points: {len(time_series)}")
    
    if time_series:
        print(f"   Date range: {time_series[0].date} to {time_series[-1].date}")
        print(f"   Latest price: ${time_series[-1].market_price}")
        print(f"   Latest sales: {time_series[-1].quantity_sold} units")
        
        # Show first few and last few data points
        print(f"\n   First 3 data points:")
        for i, ts in enumerate(time_series[:3]):
            print(f"     {ts.date}: ${ts.market_price:6.2f} market, {ts.quantity_sold:4d} sold, ${ts.low_price:6.2f}-${ts.high_price:6.2f} range")
        
        print(f"\n   Last 3 data points:")
        for ts in time_series[-3:]:
            print(f"     {ts.date}: ${ts.market_price:6.2f} market, {ts.quantity_sold:4d} sold, ${ts.low_price:6.2f}-${ts.high_price:6.2f} range")
    
    # 3. Price summary analysis
    print(f"\n3. Price Summary Analysis:")
    summary = analysis.get_price_summary_by_product(product_id)
    if summary:
        print(f"   Current Price: ${summary['market_price']['current']:.2f}")
        print(f"   Min Price: ${summary['market_price']['min']:.2f}")
        print(f"   Max Price: ${summary['market_price']['max']:.2f}")
        print(f"   Average Price: ${summary['market_price']['avg']:.2f}")
        print(f"   Total Volume Sold: {summary['sales_volume']['total']:,} units")
        print(f"   Average Daily Volume: {summary['sales_volume']['avg_daily']:.1f} units")
        print(f"   Highest Single Day Sales: {summary['sales_volume']['max_daily']} units")
    
    # 4. Trend analysis
    print(f"\n4. Price Trend Analysis:")
    trends = analysis.analyze_price_trends(product_id, window_days=7)
    if 'error' not in trends:
        print(f"   Current Price: ${trends['current_price']:.2f}")
        print(f"   7-day Moving Average: ${trends['moving_avg_price']:.2f}")
        print(f"   Recent Trend: {trends['recent_trend']['direction']} (${trends['recent_trend']['change']:.2f})")
        print(f"   Overall Trend: {trends['overall_trend']['direction']} (${trends['overall_trend']['change']:.2f})")
        print(f"   Price Volatility (Std Dev): ${trends['volatility']['price_std']:.2f}")
    else:
        print(f"   {trends['error']}")
    
    # 5. Extract data by set name
    print(f"\n5. Extract Data by Set Name:")
    set_name = sample_set['set_name']
    ts_by_name = analysis.get_time_series_by_set_name(set_name, "booster_box" if sample_set['has_box_data'] else "booster")
    print(f"   Data for '{set_name}': {len(ts_by_name)} points")
    
    # 6. Filter by date range (last 30 days)
    print(f"\n6. Recent Activity (Last 30 days):")
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    recent_data = analysis.get_time_series_by_product_id(product_id, date_from=thirty_days_ago)
    print(f"   Data points in last 30 days: {len(recent_data)}")
    
    if recent_data:
        total_recent_sales = sum(ts.quantity_sold for ts in recent_data)
        avg_recent_price = sum(ts.market_price for ts in recent_data) / len(recent_data)
        print(f"   Total sales in period: {total_recent_sales} units")
        print(f"   Average price in period: ${avg_recent_price:.2f}")
    
    # 7. Export to CSV demonstration
    print(f"\n7. Export Data to CSV:")
    if len(time_series) > 0:
        export_filename = f"{set_name.replace(' ', '_').replace(':', '').replace('&', 'and')}_price_data.csv"
        analysis.export_to_csv(time_series[:10], export_filename)  # Export first 10 points as demo
    
    # 8. Show data structure
    print(f"\n8. Data Structure Example:")
    if time_series:
        sample_data = time_series[-1].to_dict()
        print("   TimeSeriesData fields:")
        for key, value in sample_data.items():
            print(f"     {key}: {value}")
    
    print(f"\n=== Demo completed ===")
    print(f"\nUsage Tips:")
    print(f"- Use get_time_series_by_product_id() for direct product ID extraction")
    print(f"- Use get_time_series_by_set_name() for easier set-based extraction")
    print(f"- Filter by variant, condition, and date range as needed")
    print(f"- Use get_price_summary_by_product() for quick statistics")
    print(f"- Use analyze_price_trends() for trend analysis with moving averages")
    print(f"- Export data with export_to_csv() for external analysis")

if __name__ == "__main__":
    main()
