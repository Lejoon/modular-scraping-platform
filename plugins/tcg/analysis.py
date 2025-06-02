"""
Time Series Analysis Module for TCG Price Data

This module provides functions to extract and analyze time series data from the price history database.
It returns structured data including date, number sold, low, high, and market price information.
"""

import sqlite3
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

DB_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(DB_DIR, "tcg.db")

def _calculate_std(values: List[float]) -> float:
    """Calculate standard deviation manually if pandas not available"""
    if len(values) <= 1:
        return 0.0
    
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
    return variance ** 0.5

class TimeSeriesData:
    """Class to represent time series data point"""
    def __init__(self, date: str, quantity_sold: int, low_price: float, 
                 high_price: float, market_price: float, variant: str = None, 
                 condition: str = None):
        self.date = date
        self.quantity_sold = quantity_sold
        self.low_price = low_price
        self.high_price = high_price
        self.market_price = market_price
        self.variant = variant
        self.condition = condition
    
    def to_dict(self) -> Dict:
        """Convert to dictionary format"""
        return {
            'date': self.date,
            'quantity_sold': self.quantity_sold,
            'low_price': self.low_price,
            'high_price': self.high_price,
            'market_price': self.market_price,
            'variant': self.variant,
            'condition': self.condition
        }

def get_time_series_by_product_id(product_id: int, 
                                  variant: Optional[str] = None,
                                  condition: Optional[str] = None,
                                  date_from: Optional[str] = None,
                                  date_to: Optional[str] = None) -> List[TimeSeriesData]:
    """
    Extract time series data for a specific product ID
    
    Args:
        product_id: The product ID to get data for
        variant: Optional variant filter (e.g., 'Normal')
        condition: Optional condition filter (e.g., 'Unopened', 'Near Mint')
        date_from: Optional start date filter (YYYY-MM-DD format)
        date_to: Optional end date filter (YYYY-MM-DD format)
    
    Returns:
        List of TimeSeriesData objects sorted by date
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Build the query with optional filters
    query = """
        SELECT bucket_start_date, quantity_sold, low_sale_price, 
               high_sale_price, market_price, variant, condition
        FROM price_history 
        WHERE product_id = ?
    """
    params = [product_id]
    
    if variant:
        query += " AND variant = ?"
        params.append(variant)
    
    if condition:
        query += " AND condition = ?"
        params.append(condition)
    
    if date_from:
        query += " AND bucket_start_date >= ?"
        params.append(date_from)
    
    if date_to:
        query += " AND bucket_start_date <= ?"
        params.append(date_to)
    
    query += " ORDER BY bucket_start_date ASC"
    
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()
    
    time_series = []
    for row in rows:
        date, qty_sold, low_price, high_price, market_price, var, cond = row
        time_series.append(TimeSeriesData(
            date=date,
            quantity_sold=qty_sold or 0,
            low_price=float(low_price) if low_price else 0.0,
            high_price=float(high_price) if high_price else 0.0,
            market_price=float(market_price) if market_price else 0.0,
            variant=var,
            condition=cond
        ))
    
    return time_series

def get_time_series_by_set_name(set_name: str, 
                                product_type: str = 'booster',
                                variant: Optional[str] = None,
                                condition: Optional[str] = None,
                                date_from: Optional[str] = None,
                                date_to: Optional[str] = None) -> List[TimeSeriesData]:
    """
    Extract time series data for a Pokemon set by name
    
    Args:
        set_name: The name of the Pokemon set
        product_type: 'booster' or 'booster_box'
        variant: Optional variant filter
        condition: Optional condition filter
        date_from: Optional start date filter (YYYY-MM-DD format)
        date_to: Optional end date filter (YYYY-MM-DD format)
    
    Returns:
        List of TimeSeriesData objects sorted by date
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Get the product ID for the set
    if product_type == 'booster':
        c.execute("SELECT booster_product_id FROM pokemon_sets WHERE set_name = ?", (set_name,))
    else:
        c.execute("SELECT booster_box_product_id FROM pokemon_sets WHERE set_name = ?", (set_name,))
    
    result = c.fetchone()
    conn.close()
    
    if not result or not result[0]:
        return []
    
    product_id = result[0]
    return get_time_series_by_product_id(product_id, variant, condition, date_from, date_to)

def get_all_available_sets() -> List[Dict]:
    """
    Get all available Pokemon sets with their product IDs
    
    Returns:
        List of dictionaries containing set information
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("""
        SELECT set_name, release_date, booster_product_id, booster_box_product_id
        FROM pokemon_sets
        ORDER BY release_date DESC
    """)
    
    rows = c.fetchall()
    conn.close()
    
    sets = []
    for row in rows:
        set_name, release_date, booster_id, box_id = row
        sets.append({
            'set_name': set_name,
            'release_date': release_date,
            'booster_product_id': booster_id,
            'booster_box_product_id': box_id,
            'has_booster_data': has_price_data(booster_id) if booster_id else False,
            'has_box_data': has_price_data(box_id) if box_id else False
        })
    
    return sets

def has_price_data(product_id: int) -> bool:
    """Check if we have price data for a product ID"""
    if not product_id:
        return False
        
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM price_history WHERE product_id = ?", (product_id,))
    count = c.fetchone()[0]
    conn.close()
    return count > 0

def get_price_summary_by_product(product_id: int) -> Dict:
    """
    Get price summary statistics for a product
    
    Args:
        product_id: The product ID to analyze
    
    Returns:
        Dictionary containing summary statistics
    """
    time_series = get_time_series_by_product_id(product_id)
    
    if not time_series:
        return {}
    
    market_prices = [ts.market_price for ts in time_series if ts.market_price > 0]
    quantities = [ts.quantity_sold for ts in time_series]
    low_prices = [ts.low_price for ts in time_series if ts.low_price > 0]
    high_prices = [ts.high_price for ts in time_series if ts.high_price > 0]
    
    return {
        'product_id': product_id,
        'data_points': len(time_series),
        'date_range': {
            'start': time_series[0].date,
            'end': time_series[-1].date
        },
        'market_price': {
            'current': time_series[-1].market_price,
            'min': min(market_prices) if market_prices else 0,
            'max': max(market_prices) if market_prices else 0,
            'avg': sum(market_prices) / len(market_prices) if market_prices else 0
        },
        'sales_volume': {
            'total': sum(quantities),
            'avg_daily': sum(quantities) / len(quantities) if quantities else 0,
            'max_daily': max(quantities) if quantities else 0
        },
        'price_range': {
            'lowest_sale': min(low_prices) if low_prices else 0,
            'highest_sale': max(high_prices) if high_prices else 0
        }
    }

def export_to_csv(time_series: List[TimeSeriesData], filename: str) -> None:
    """
    Export time series data to CSV file
    
    Args:
        time_series: List of TimeSeriesData objects
        filename: Output filename
    """
    if not time_series:
        print("No data to export")
        return
    
    if not PANDAS_AVAILABLE:
        # Manual CSV export if pandas not available
        filepath = os.path.join(DB_DIR, filename)
        with open(filepath, 'w') as f:
            # Write header
            f.write("date,quantity_sold,low_price,high_price,market_price,variant,condition\n")
            # Write data
            for ts in time_series:
                f.write(f"{ts.date},{ts.quantity_sold},{ts.low_price},{ts.high_price},{ts.market_price},{ts.variant},{ts.condition}\n")
        print(f"Exported {len(time_series)} data points to {filepath}")
    else:
        data = [ts.to_dict() for ts in time_series]
        df = pd.DataFrame(data)
        filepath = os.path.join(DB_DIR, filename)
        df.to_csv(filepath, index=False)
        print(f"Exported {len(time_series)} data points to {filepath}")

def get_recent_activity(days: int = 30) -> List[Dict]:
    """
    Get recent price activity across all products
    
    Args:
        days: Number of days to look back
    
    Returns:
        List of recent activity data
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    date_threshold = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    c.execute("""
        SELECT ph.product_id, ps.set_name, ph.bucket_start_date, 
               ph.market_price, ph.quantity_sold, ph.variant, ph.condition
        FROM price_history ph
        JOIN pokemon_sets ps ON (ph.product_id = ps.booster_product_id OR 
                                ph.product_id = ps.booster_box_product_id)
        WHERE ph.bucket_start_date >= ?
        ORDER BY ph.bucket_start_date DESC
    """, (date_threshold,))
    
    rows = c.fetchall()
    conn.close()
    
    activity = []
    for row in rows:
        product_id, set_name, date, market_price, qty_sold, variant, condition = row
        activity.append({
            'product_id': product_id,
            'set_name': set_name,
            'date': date,
            'market_price': float(market_price) if market_price else 0,
            'quantity_sold': qty_sold or 0,
            'variant': variant,
            'condition': condition
        })
    
    return activity

def analyze_price_trends(product_id: int, window_days: int = 7) -> Dict:
    """
    Analyze price trends for a product using moving averages
    
    Args:
        product_id: Product ID to analyze
        window_days: Number of days for moving average calculation
    
    Returns:
        Dictionary containing trend analysis
    """
    time_series = get_time_series_by_product_id(product_id)
    
    if len(time_series) < window_days:
        return {'error': f'Insufficient data. Need at least {window_days} data points.'}
    
    # Calculate moving averages
    prices = [ts.market_price for ts in time_series]
    volumes = [ts.quantity_sold for ts in time_series]
    
    moving_avg_prices = []
    moving_avg_volumes = []
    
    for i in range(len(prices) - window_days + 1):
        window_prices = prices[i:i + window_days]
        window_volumes = volumes[i:i + window_days]
        
        moving_avg_prices.append(sum(window_prices) / len(window_prices))
        moving_avg_volumes.append(sum(window_volumes) / len(window_volumes))
    
    # Calculate trend direction
    if len(moving_avg_prices) >= 2:
        recent_trend = moving_avg_prices[-1] - moving_avg_prices[-2]
        overall_trend = moving_avg_prices[-1] - moving_avg_prices[0]
    else:
        recent_trend = 0
        overall_trend = 0
    
    return {
        'product_id': product_id,
        'analysis_window_days': window_days,
        'current_price': time_series[-1].market_price,
        'moving_avg_price': moving_avg_prices[-1] if moving_avg_prices else 0,
        'recent_trend': {
            'direction': 'up' if recent_trend > 0 else 'down' if recent_trend < 0 else 'stable',
            'change': recent_trend
        },
        'overall_trend': {
            'direction': 'up' if overall_trend > 0 else 'down' if overall_trend < 0 else 'stable',
            'change': overall_trend
        },
        'volatility': {
            'price_std': _calculate_std(prices) if len(prices) > 1 else 0,
            'volume_std': _calculate_std(volumes) if len(volumes) > 1 else 0
        }
    }

def get_volume_sold_data_all_sets() -> List[Dict]:
    """
    Get volume sold data (quantity * market price) for all Pokemon sets
    
    Returns:
        List of dictionaries containing set_name, date, and volume_sold
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Query to get all price history data with set names
    c.execute("""
        SELECT ps.set_name, ph.bucket_start_date, ph.quantity_sold, ph.market_price,
               CASE 
                   WHEN ph.product_id = ps.booster_product_id THEN 'Booster Pack'
                   WHEN ph.product_id = ps.booster_box_product_id THEN 'Booster Box'
                   ELSE 'Unknown'
               END as product_type
        FROM price_history ph
        JOIN pokemon_sets ps ON (ph.product_id = ps.booster_product_id OR 
                                ph.product_id = ps.booster_box_product_id)
        WHERE ph.quantity_sold > 0 AND ph.market_price > 0
        ORDER BY ps.set_name, ph.bucket_start_date
    """)
    
    rows = c.fetchall()
    conn.close()
    
    volume_data = []
    for row in rows:
        set_name, date, quantity_sold, market_price, product_type = row
        volume_sold = quantity_sold * float(market_price)
        
        volume_data.append({
            'set_name': set_name,
            'date': date,
            'quantity_sold': quantity_sold,
            'market_price': float(market_price),
            'volume_sold': volume_sold,
            'product_type': product_type
        })
    
    return volume_data

def export_volume_sold_csv(filename: str = "pokemon_sets_volume_sold.csv", 
                          include_product_type: bool = False) -> None:
    """
    Export volume sold data for all Pokemon sets to CSV
    
    Args:
        filename: Output CSV filename
        include_product_type: Whether to include product type column (Booster Pack/Box)
    """
    volume_data = get_volume_sold_data_all_sets()
    
    if not volume_data:
        print("No volume data found")
        return
    
    filepath = os.path.join(DB_DIR, filename)
    
    with open(filepath, 'w') as f:
        # Write header
        if include_product_type:
            f.write("set_name,date,volume_sold,product_type\n")
        else:
            f.write("set_name,date,volume_sold\n")
        
        # Write data
        for item in volume_data:
            if include_product_type:
                f.write(f"{item['set_name']},{item['date']},{item['volume_sold']:.2f},{item['product_type']}\n")
            else:
                f.write(f"{item['set_name']},{item['date']},{item['volume_sold']:.2f}\n")
    
    print(f"Exported {len(volume_data)} volume data points to {filepath}")

def get_volume_summary_by_set() -> List[Dict]:
    """
    Get volume sold summary statistics by Pokemon set
    
    Returns:
        List of dictionaries with set-level volume statistics
    """
    volume_data = get_volume_sold_data_all_sets()
    
    # Group by set name
    set_volumes = {}
    for item in volume_data:
        set_name = item['set_name']
        if set_name not in set_volumes:
            set_volumes[set_name] = {
                'set_name': set_name,
                'total_volume': 0,
                'total_quantity': 0,
                'data_points': 0,
                'first_date': item['date'],
                'last_date': item['date'],
                'avg_price': 0,
                'product_types': set()
            }
        
        set_data = set_volumes[set_name]
        set_data['total_volume'] += item['volume_sold']
        set_data['total_quantity'] += item['quantity_sold']
        set_data['data_points'] += 1
        set_data['product_types'].add(item['product_type'])
        
        # Update date range
        if item['date'] < set_data['first_date']:
            set_data['first_date'] = item['date']
        if item['date'] > set_data['last_date']:
            set_data['last_date'] = item['date']
    
    # Calculate averages and convert to list
    summary = []
    for set_name, data in set_volumes.items():
        if data['total_quantity'] > 0:
            data['avg_price'] = data['total_volume'] / data['total_quantity']
        data['product_types'] = list(data['product_types'])
        summary.append(data)
    
    # Sort by total volume (highest first)
    summary.sort(key=lambda x: x['total_volume'], reverse=True)
    
    return summary

# Example usage functions
def demo_analysis():
    """Demonstrate the analysis capabilities"""
    print("=== TCG Price Analysis Demo ===\n")
    
    # Get available sets
    print("Available Pokemon Sets:")
    sets = get_all_available_sets()
    for set_info in sets[:5]:  # Show first 5
        print(f"- {set_info['set_name']} (Booster data: {set_info['has_booster_data']}, Box data: {set_info['has_box_data']})")
    
    # Find a set with data
    sets_with_data = [s for s in sets if s['has_booster_data'] or s['has_box_data']]
    if not sets_with_data:
        print("\nNo sets with price data found.")
        return
    
    sample_set = sets_with_data[0]
    print(f"\n=== Analysis for {sample_set['set_name']} ===")
    
    # Get booster data if available
    if sample_set['has_booster_data']:
        product_id = sample_set['booster_product_id']
        print(f"\nBooster Pack Analysis (Product ID: {product_id}):")
        
        # Get time series
        time_series = get_time_series_by_product_id(product_id)
        print(f"Data points: {len(time_series)}")
        
        if time_series:
            print(f"Date range: {time_series[0].date} to {time_series[-1].date}")
            print(f"Latest price: ${time_series[-1].market_price}")
            print(f"Latest volume: {time_series[-1].quantity_sold} sold")
            
            # Get summary
            summary = get_price_summary_by_product(product_id)
            print(f"Price range: ${summary['market_price']['min']:.2f} - ${summary['market_price']['max']:.2f}")
            print(f"Total volume: {summary['sales_volume']['total']} units")
            
            # Get trends
            trends = analyze_price_trends(product_id)
            if 'error' not in trends:
                print(f"Recent trend: {trends['recent_trend']['direction']} (${trends['recent_trend']['change']:.2f})")
                print(f"Overall trend: {trends['overall_trend']['direction']} (${trends['overall_trend']['change']:.2f})")

if __name__ == "__main__":
    demo_analysis()
