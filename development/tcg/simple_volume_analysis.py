#!/usr/bin/env python3
"""
TCG Dollar Volume Analysis - Simplified Version

This script queries the TCG price history database and visualizes 
the total dollar volume (market_price * quantity_sold) over time for different sets.
"""

import sqlite3
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np

# Database path
DB_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(DB_DIR, "tcg.db")

def main():
    """Main function to analyze and visualize TCG dollar volume"""
    print("TCG Dollar Volume Analysis")
    print(f"Database path: {DB_PATH}")
    
    # Check if database exists
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        return
    
    try:
        # Connect to the database
        conn = sqlite3.connect(DB_PATH)
        
        # Get all product IDs from price history
        product_query = """
            SELECT DISTINCT product_id 
            FROM price_history
            ORDER BY product_id
        """
        
        product_ids = pd.read_sql_query(product_query, conn)['product_id'].tolist()
        print(f"Found {len(product_ids)} unique product IDs in price history table")
        
        if not product_ids:
            print("No product IDs found in the database.")
            return
        
        # Query price history data
        data_query = """
            SELECT 
                product_id,
                bucket_start_date as date,
                market_price,
                quantity_sold,
                (market_price * quantity_sold) as dollar_volume
            FROM 
                price_history
            WHERE 
                product_id IN ({})
                AND market_price IS NOT NULL
                AND quantity_sold IS NOT NULL
            ORDER BY
                product_id, bucket_start_date
        """.format(','.join('?' * len(product_ids)))
        
        # Load data
        df = pd.read_sql_query(data_query, conn, params=product_ids)
        
        conn.close()
        
        if df.empty:
            print("No data found in the database.")
            return
        
        print(f"Loaded {len(df)} price history records")
        
        # Convert date to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Print sample data
        print("\nSample data:")
        print(df.head())
        
        # Create product_id to label mapping (using product_id as the set name for now)
        product_labels = {pid: f"Product {pid}" for pid in product_ids}
        
        # Add a column with readable labels
        df['set_name'] = df['product_id'].map(product_labels)
        
        # Aggregate dollar volume by product and date
        df_agg = df.groupby(['set_name', 'date'])['dollar_volume'].sum().reset_index()
        
        # Pivot for visualization
        df_pivot = df_agg.pivot(index='date', columns='set_name', values='dollar_volume')
        df_pivot = df_pivot.fillna(0)
        df_pivot = df_pivot.sort_index()
        
        # Plot data
        plt.figure(figsize=(16, 9))
        
        # Create a colormap with distinct colors
        num_sets = len(df_pivot.columns)
        cmap = plt.cm.get_cmap('viridis', num_sets)
        colors = [cmap(i) for i in range(num_sets)]
        
        # Plot stacked area chart
        ax = df_pivot.plot.area(stacked=True, alpha=0.75, figsize=(16, 9), color=colors)
        
        # Formatting
        plt.title("TCG Dollar Volume by Product Over Time", fontsize=16)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Dollar Volume (Price × Quantity Sold)', fontsize=12)
        
        # Format x-axis as dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.xticks(rotation=45)
        
        # Add legend outside of plot
        plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5), fontsize=10)
        
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        # Save figure
        output_file = "tcg_dollar_volume.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"\nPlot saved to {output_file}")
        
        # Save data to CSV
        csv_file = "tcg_dollar_volume_data.csv"
        df.to_csv(csv_file, index=False)
        print(f"Data saved to {csv_file}")
        
        # Print summary statistics
        print("\nSummary statistics by product:")
        summary = df.groupby('product_id')['dollar_volume'].agg(['sum', 'mean', 'min', 'max'])
        summary = summary.sort_values('sum', ascending=False)
        print(summary.head(10))  # Show top 10 products by total volume
        
    except Exception as e:
        print(f"Error analyzing data: {e}")

if __name__ == "__main__":
    main()
