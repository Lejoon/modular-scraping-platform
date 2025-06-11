#!/usr/bin/env python3
"""
TCG Dollar Volume Analysis

This script queries the TCG price history database, creates time series data for different sets,
and visualizes the total dollar volume (market_price * quantity_sold) over time.
Supports filtering for Pokémon and Star Wars sets.
"""

import sqlite3
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np
from typing import Dict, List, Tuple, Optional, Set
import argparse

# Database path
DB_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(DB_DIR, "../.."))  # Go up two directories to reach root
DB_PATH = os.path.join(ROOT_DIR, "tcg.db")  # Database in root folder
SETS_CSV = os.path.join(DB_DIR, "pokemon_sets.csv")

# Star Wars set product IDs
STAR_WARS_PRODUCT_IDS = {
    # Booster product IDs
    533898,  # Spark of Rebellion
    549700,  # Shadows of the Galaxy
    578940,  # Twilight of the Republic
    610306,  # Jump to Lightspeed
    626542,  # Legends of the Force
    
    # Booster Box product IDs
    533897,  # Spark of Rebellion Box
    549696,  # Shadows of the Galaxy Box
    578939,  # Twilight of the Republic Box
    610308,  # Jump to Lightspeed Box
    626543,  # Legends of the Force Box
}

def get_latest_data_date() -> str:
    """
    Get the most recent data update date from the database
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT MAX(created_at) FROM price_history
    """)
    latest_date = cursor.fetchone()[0]
    
    conn.close()
    return latest_date

def get_set_info() -> pd.DataFrame:
    """
    Load set information from the pokemon_sets.csv file
    """
    try:
        # Check if file exists
        if not os.path.exists(SETS_CSV):
            print(f"Warning: Set information file not found at {SETS_CSV}")
            return pd.DataFrame()
        
        # Read the CSV with semicolon separator
        df_sets = pd.read_csv(SETS_CSV, sep=';', header=0)  # Skip the first comment row
        print(df_sets.head())  # Display first few rows for debugging
        # Verify that required columns exist
        required_columns = ['Set Name', 'TCGPlayer Booster Product ID', 'TCGPlayer Booster Box Product ID']
        for col in required_columns:
            if col not in df_sets.columns:
                print(f"Warning: Required column '{col}' not found in {SETS_CSV}")
                return pd.DataFrame()
        
        return df_sets
    except Exception as e:
        print(f"Error loading set info: {e}")
        return pd.DataFrame()

def get_product_ids_by_set(include_star_wars: bool = True, include_pokemon: bool = True) -> Dict[str, List[int]]:
    """
    Create a dictionary mapping set names to their product IDs
    
    Args:
        include_star_wars: Whether to include Star Wars sets
        include_pokemon: Whether to include Pokémon sets
        
    Returns:
        Dictionary mapping set names to their product IDs
    """
    product_ids_by_set = {}
    
    # Hardcoded Star Wars sets for reliability
    star_wars_sets = {
        "Spark of Rebellion": [533898, 533897],
        "Shadows of the Galaxy": [549700, 549696],
        "Twilight of the Republic": [578940, 578939],
        "Jump to Lightspeed": [610306, 610308],
        "Legends of the Force": [626542, 626543]
    }
    
    # Add Star Wars sets if requested
    if include_star_wars:
        product_ids_by_set.update(star_wars_sets)
    
    # If we don't need Pokemon sets, return just the Star Wars sets
    if not include_pokemon:
        return product_ids_by_set
    
    # Try to load Pokemon sets from CSV
    df_sets = get_set_info()
    
    if df_sets.empty:
        # Fallback - get all product IDs from the database
        conn = sqlite3.connect(DB_PATH)
        product_query = "SELECT DISTINCT product_id FROM price_history"
        product_ids = pd.read_sql_query(product_query, conn)['product_id'].tolist()
        conn.close()
        
        if product_ids:
            # Filter out known Star Wars IDs to avoid duplication
            star_wars_ids = set()
            for ids in star_wars_sets.values():
                star_wars_ids.update(ids)
            
            # Use product IDs as set names for non-Star Wars products
            for pid in product_ids:
                if pid not in star_wars_ids:
                    product_ids_by_set[f"Product {pid}"] = [pid]
        
        return product_ids_by_set
    
    # Process Pokemon sets from CSVƒ
    for _, row in df_sets.iterrows():
        set_name = row['Set Name']
        product_ids = []
        is_star_wars = False
        
        # Skip if this is already in our Star Wars sets
        if set_name in star_wars_sets:
            continue
        
        # Add booster product ID if available
        if not pd.isna(row['TCGPlayer Booster Product ID']):
            try:
                booster_id = int(row['TCGPlayer Booster Product ID'])
                if booster_id in STAR_WARS_PRODUCT_IDS:
                    is_star_wars = True
                product_ids.append(booster_id)
            except (ValueError, TypeError):
                pass
        
        # Add booster box product ID if available
        if not pd.isna(row['TCGPlayer Booster Box Product ID']):
            try:
                box_id = int(row['TCGPlayer Booster Box Product ID'])
                if box_id in STAR_WARS_PRODUCT_IDS:
                    is_star_wars = True
                product_ids.append(box_id)
            except (ValueError, TypeError):
                pass
        
        # Only add non-Star Wars sets since we've already added Star Wars sets
        if product_ids and not is_star_wars:
            product_ids_by_set[set_name] = product_ids
    
    return product_ids_by_set

def query_dollar_volume_by_set(latest_update_date: Optional[str] = None, 
                             include_star_wars: bool = True, 
                             include_pokemon: bool = True) -> pd.DataFrame:
    """
    Query the database for dollar volume data
    
    Args:
        latest_update_date: Optional filter to get only the most recent data
        include_star_wars: Whether to include Star Wars sets
        include_pokemon: Whether to include Pokémon sets
        
    Returns:
        DataFrame with dollar volume data by set and date
    """
    # Get product IDs for each set
    product_ids_by_set = get_product_ids_by_set(include_star_wars, include_pokemon)
    if not product_ids_by_set:
        print("No set information found. Unable to query data.")
        return pd.DataFrame()
    
    conn = sqlite3.connect(DB_PATH)
    
    # Build a master dataframe with all results
    all_data = []
    
    for set_name, product_ids in product_ids_by_set.items():
        for product_id in product_ids:
            query = """
                SELECT 
                    ? as set_name,
                    ? as product_id,
                    bucket_start_date as date,
                    market_price,
                    quantity_sold,
                    (market_price * quantity_sold) as dollar_volume,
                    created_at
                FROM 
                    price_history
                WHERE 
                    product_id = ?
                    AND market_price IS NOT NULL
                    AND quantity_sold IS NOT NULL
            """
            params = [set_name, product_id, product_id]
            
            # Add date filter if provided
            if latest_update_date:
                query += " AND created_at = ?"
                params.append(latest_update_date)
            
            # Execute query
            try:
                df = pd.read_sql_query(query, conn, params=params)
                if not df.empty:
                    all_data.append(df)
            except Exception as e:
                print(f"Error querying data for product {product_id}: {e}")
                continue
    
    conn.close()
    
    # Combine all results
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        # Convert date to datetime
        combined_df['date'] = pd.to_datetime(combined_df['date'])
        return combined_df
    else:
        print("No data found for the specified products.")
        return pd.DataFrame()

def aggregate_data_by_set_and_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate dollar volume by set and date
    """
    if df.empty:
        return df
    
    # Group by set_name and date, summing the dollar_volume
    df_agg = df.groupby(['set_name', 'date'])['dollar_volume'].sum().reset_index()
    
    # Pivot to have sets as columns and dates as index
    df_pivot = df_agg.pivot(index='date', columns='set_name', values='dollar_volume')
    
    # Fill NaN values with 0 (no sales on that date)
    df_pivot = df_pivot.fillna(0)
    
    # Sort by date
    df_pivot = df_pivot.sort_index()
    
    return df_pivot

def plot_dollar_volume(df_pivot: pd.DataFrame, output_file: str = "tcg_dollar_volume.png", 
                      title: str = "TCG Dollar Volume by Set Over Time",
                      min_volume_threshold: float = 0) -> None:
    """
    Create a stacked area chart of dollar volume over time
    
    Args:
        df_pivot: Pivoted DataFrame with dates as index and sets as columns
        output_file: Path to save the output image
        title: Title for the plot
        min_volume_threshold: Minimum total dollar volume for a set to be included
    """
    if df_pivot.empty:
        print("No data to plot.")
        return
    
    # Filter out sets with low total volume if threshold is provided
    if min_volume_threshold > 0:
        total_volumes = df_pivot.sum()
        sets_to_keep = total_volumes[total_volumes >= min_volume_threshold].index
        
        if len(sets_to_keep) < len(df_pivot.columns):
            df_filtered = df_pivot[sets_to_keep]
            print(f"Filtered out {len(df_pivot.columns) - len(sets_to_keep)} sets with volume < {min_volume_threshold}")
            df_pivot = df_filtered
    
    # Sort columns by total volume (descending)
    total_volumes = df_pivot.sum().sort_values(ascending=False)
    sorted_columns = total_volumes.index
    df_pivot = df_pivot[sorted_columns]
    
    plt.figure(figsize=(16, 9))
    
    # Create a colormap with distinct colors
    num_sets = len(df_pivot.columns)
    if num_sets > 0:
        # Use viridis for many sets, tab10 for fewer sets
        colormap_name = 'tab10' if num_sets <= 10 else 'viridis'
        try:
            colors = plt.cm.get_cmap(colormap_name, num_sets)
            color_list = [colors(i) for i in range(num_sets)]
        except Exception:
            # Fallback to default colors
            color_list = None
        
        # Plot stacked area chart
        ax = df_pivot.plot.area(stacked=True, alpha=0.75, figsize=(16, 9), color=color_list)
        
        # Formatting
        plt.title(title, fontsize=16)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Dollar Volume (Price × Quantity Sold)', fontsize=12)
        
        # Format x-axis as dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.xticks(rotation=45)
        
        # Add legend outside of plot with volume info
        handles, labels = ax.get_legend_handles_labels()
        
        # Add total volume to labels
        new_labels = []
        for i, label in enumerate(labels):
            total_vol = total_volumes[label]
            new_labels.append(f"{label} (${total_vol:,.2f})")
        
        plt.legend(handles, new_labels, loc='center left', bbox_to_anchor=(1.0, 0.5), 
                  fontsize=10, title="Set (Total Dollar Volume)")
        
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        # Save figure
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Plot saved to {output_file}")
        
        # Optional: also save the data to CSV
        csv_file = output_file.replace('.png', '.csv')
        df_pivot.to_csv(csv_file)
        print(f"Data saved to {csv_file}")
    else:
        print("No sets to plot after filtering.")

def export_data_to_csv(df: pd.DataFrame, output_file: str = "tcg_dollar_volume_data.csv") -> None:
    """
    Export the raw data to CSV for further analysis
    """
    if df.empty:
        print("No data to export.")
        return
    
    df.to_csv(output_file, index=False)
    print(f"Data exported to {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Analyze TCG dollar volume by set over time")
    parser.add_argument("--latest", action="store_true", help="Use only the latest data update")
    parser.add_argument("--output", default="tcg_dollar_volume.png", help="Output file name")
    parser.add_argument("--csv", default="tcg_dollar_volume_data.csv", help="CSV export file name")
    parser.add_argument("--min-volume", type=float, default=0, help="Minimum dollar volume threshold to include a set")
    parser.add_argument("--no-star-wars", action="store_true", help="Exclude Star Wars sets")
    parser.add_argument("--no-pokemon", action="store_true", help="Exclude Pokémon sets")
    parser.add_argument("--only-star-wars", action="store_true", help="Include only Star Wars sets")
    args = parser.parse_args()
    
    # Handle exclusive flags
    include_star_wars = True
    include_pokemon = True
    
    if args.only_star_wars:
        include_pokemon = False
    elif args.no_star_wars:
        include_star_wars = False
    
    if args.no_pokemon:
        include_pokemon = False
    
    # Create appropriate title
    if include_star_wars and include_pokemon:
        title = "TCG Dollar Volume by Set Over Time (Pokémon & Star Wars)"
    elif include_star_wars:
        title = "TCG Dollar Volume by Set Over Time (Star Wars Only)"
    elif include_pokemon:
        title = "TCG Dollar Volume by Set Over Time (Pokémon Only)"
    else:
        print("Error: You cannot exclude both Pokémon and Star Wars sets.")
        return
    
    # Get the latest data date if requested
    latest_date = get_latest_data_date() if args.latest else None
    if args.latest and latest_date:
        print(f"Using data from latest update: {latest_date}")
    
    # Query the data
    df = query_dollar_volume_by_set(latest_date, include_star_wars, include_pokemon)
    
    if df.empty:
        print("No data found in the database.")
        return
    
    # Export raw data to CSV
    export_data_to_csv(df, args.csv)
    
    # Aggregate and plot
    df_pivot = aggregate_data_by_set_and_date(df)
    plot_dollar_volume(df_pivot, args.output, title, args.min_volume)
    
    # Print summary statistics
    print("\nSummary statistics by set:")
    summary = df.groupby('set_name')['dollar_volume'].agg(['sum', 'mean', 'min', 'max'])
    summary = summary.sort_values('sum', ascending=False)
    print(summary)

if __name__ == "__main__":
    main()
