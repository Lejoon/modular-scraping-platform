#!/usr/bin/env python3
"""
Pokemon TCG Volume Sold Visualization

Creates a stacked area chart showing volume sold over time for each Pokemon set.
"""

import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np
import os

def load_and_process_data(csv_file):
    """Load the CSV and aggregate volume by date and set"""
    
    # Read the CSV
    df = pd.read_csv(csv_file)
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Group by set_name and date, summing the volume_sold
    df_agg = df.groupby(['set_name', 'date'])['volume_sold'].sum().reset_index()
    
    # Pivot to have sets as columns and dates as index
    df_pivot = df_agg.pivot(index='date', columns='set_name', values='volume_sold')
    
    # Fill NaN values with 0 (no sales on that date)
    df_pivot = df_pivot.fillna(0)
    
    # Sort by date
    df_pivot = df_pivot.sort_index()
    
    return df_pivot

def create_stacked_area_chart(df_pivot, output_file="pokemon_volume_chart.png"):
    """Create a stacked area chart"""
    
    # Set up the figure
    plt.figure(figsize=(16, 10))
    
    # Create the stacked area plot
    ax = plt.gca()
    
    # Get colors for each set - use a colormap
    colors = plt.cm.Set3(np.linspace(0, 1, len(df_pivot.columns)))
    
    # Create the stacked area plot
    ax.stackplot(df_pivot.index, 
                *[df_pivot[col] for col in df_pivot.columns],
                labels=df_pivot.columns,
                colors=colors,
                alpha=0.8)
    
    # Customize the plot
    plt.title('Pokemon TCG Volume Sold Over Time by Set', fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Volume Sold ($)', fontsize=12)
    
    # Format the x-axis dates
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    plt.xticks(rotation=45)
    
    # Format y-axis to show currency
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1000:.0f}K'))
    
    # Add grid
    plt.grid(True, alpha=0.3)
    
    # Add legend
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    
    # Adjust layout to prevent legend cutoff
    plt.tight_layout()
    
    # Save the plot
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Chart saved as {output_file}")
    
    # Don't show plot in non-interactive mode
    # plt.show()
    
    return ax

def create_summary_stats(df_pivot):
    """Create summary statistics"""
    
    print("=== Pokemon TCG Volume Analysis ===\\n")
    
    # Total volume by set
    total_by_set = df_pivot.sum().sort_values(ascending=False)
    print("Total Volume by Set:")
    for set_name, total in total_by_set.items():
        print(f"  {set_name}: ${total:,.0f}")
    
    print(f"\\nOverall Total Volume: ${total_by_set.sum():,.0f}")
    
    # Date range
    print(f"\\nDate Range: {df_pivot.index.min().strftime('%Y-%m-%d')} to {df_pivot.index.max().strftime('%Y-%m-%d')}")
    
    # Peak volume day
    daily_totals = df_pivot.sum(axis=1)
    peak_day = daily_totals.idxmax()
    peak_volume = daily_totals.max()
    print(f"Peak Volume Day: {peak_day.strftime('%Y-%m-%d')} (${peak_volume:,.0f})")
    
    # Average daily volume
    avg_daily = daily_totals.mean()
    print(f"Average Daily Volume: ${avg_daily:,.0f}")
    
    print("\\n" + "="*50)

def create_individual_set_chart(df_pivot, output_file="pokemon_individual_sets.png"):
    """Create individual line charts for each set"""
    
    # Set up subplot grid
    n_sets = len(df_pivot.columns)
    n_cols = 3
    n_rows = (n_sets + n_cols - 1) // n_cols
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, 5*n_rows))
    axes = axes.flatten() if n_rows > 1 else [axes] if n_cols == 1 else axes
    
    colors = plt.cm.Set3(np.linspace(0, 1, n_sets))
    
    for i, (set_name, color) in enumerate(zip(df_pivot.columns, colors)):
        ax = axes[i]
        
        # Plot the data for this set
        ax.fill_between(df_pivot.index, df_pivot[set_name], alpha=0.7, color=color)
        ax.plot(df_pivot.index, df_pivot[set_name], color=color, linewidth=2)
        
        # Customize each subplot
        ax.set_title(set_name, fontsize=10, fontweight='bold')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.tick_params(axis='x', rotation=45, labelsize=8)
        ax.tick_params(axis='y', labelsize=8)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1000:.0f}K'))
        ax.grid(True, alpha=0.3)
    
    # Hide unused subplots
    for i in range(n_sets, len(axes)):
        axes[i].set_visible(False)
    
    plt.suptitle('Individual Pokemon Set Volume Over Time', fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Individual sets chart saved as {output_file}")
    # plt.show()  # Don't show in non-interactive mode

def main():
    """Main function"""
    
    csv_file = "pokemon_sets_volume_sold.csv"
    
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} not found!")
        print("Please run the volume export command first:")
        print("python testing.py volume_csv")
        return
    
    print("Loading and processing data...")
    df_pivot = load_and_process_data(csv_file)
    
    print(f"Data loaded: {len(df_pivot)} dates, {len(df_pivot.columns)} sets")
    
    # Create summary statistics
    create_summary_stats(df_pivot)
    
    # Create the main stacked area chart
    print("\\nCreating stacked area chart...")
    create_stacked_area_chart(df_pivot)
    
    # Create individual set charts
    print("\\nCreating individual set charts...")
    create_individual_set_chart(df_pivot)
    
    print("\\nVisualization complete!")

if __name__ == "__main__":
    main()
