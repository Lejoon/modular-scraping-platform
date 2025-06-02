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
import matplotlib.cm as cm

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

def load_set_release_dates(sets_csv="pokemon_sets.csv"):
    """Load and parse Pokemon set release dates"""
    
    # Check if the file exists
    if not os.path.exists(sets_csv):
        print(f"Warning: {sets_csv} not found. Using column order instead of release dates.")
        return None, None
    
    try:
        # Read the CSV with semicolon separator
        df_sets = pd.read_csv(sets_csv, sep=';', skiprows=1)  # Skip the first comment row
        
        # Parse release dates
        df_sets['Release Date'] = pd.to_datetime(df_sets['Release Date'], format='%B %d, %Y')
        
        # Create a dictionary mapping set names to release dates
        set_dates = dict(zip(df_sets['Set Name'], df_sets['Release Date']))
        
        # Sort sets by release date (newest to oldest)
        sorted_sets = sorted(set_dates.keys(), key=lambda x: set_dates[x], reverse=True)
        
        return sorted_sets, set_dates
    except Exception as e:
        print(f"Error loading set release dates: {e}")
        return None, None

def create_stacked_area_chart(df_pivot, ordered_sets=None, output_file="pokemon_volume_chart.png"):
    """Create a stacked area chart"""
    
    # Set up the figure
    plt.figure(figsize=(16, 10))
    
    # Create the stacked area plot
    ax = plt.gca()
    
    # Use ordered sets if provided, otherwise use df_pivot columns
    if ordered_sets is None:
        ordered_sets = df_pivot.columns.tolist()
    else:
        # Filter out any sets not in the data
        ordered_sets = [s for s in ordered_sets if s in df_pivot.columns]
    
    # Create progressive color scheme - earliest sets lightest, latest sets darkest
    n_sets = len(ordered_sets)
    
    # Use a sequential colormap (Blues) for progressive darkening
    # Map 0.3 (light) to 0.9 (dark) for better visibility while maintaining progression
    lightness_values = np.linspace(0.3, 0.9, n_sets)
    colors = plt.cm.Blues(lightness_values)
    
    # Create the stacked area plot with ordered sets
    ax.stackplot(df_pivot.index, 
                *[df_pivot[col] for col in ordered_sets],
                labels=ordered_sets,
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

def create_individual_set_chart(df_pivot, ordered_sets=None, output_file="pokemon_individual_sets.png"):
    """Create individual line charts for each set"""
    
    # Use ordered sets if provided, otherwise use df_pivot columns
    if ordered_sets is None:
        ordered_sets = df_pivot.columns.tolist()
    else:
        # Filter out any sets not in the data
        ordered_sets = [s for s in ordered_sets if s in df_pivot.columns]
    
    # Set up subplot grid
    n_sets = len(ordered_sets)
    n_cols = 3
    n_rows = (n_sets + n_cols - 1) // n_cols
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, 5*n_rows))
    axes = axes.flatten() if n_rows > 1 else [axes] if n_cols == 1 else axes
    
    # Create progressive color scheme - earliest sets lightest, latest sets darkest
    lightness_values = np.linspace(0.3, 0.9, n_sets)
    colors = plt.cm.Blues(lightness_values)
    
    # Calculate global max y value for consistent scaling across all charts
    global_max_y = df_pivot.max().max()
    
    for i, set_name in enumerate(ordered_sets):
        ax = axes[i]
        color = colors[i]
        
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
        
        # Set the same y-axis limit for all charts for better comparability
        ax.set_ylim(0, global_max_y * 1.05)  # Add 5% padding above max value
    
    # Hide unused subplots
    for i in range(n_sets, len(axes)):
        axes[i].set_visible(False)
    
    plt.suptitle('Individual Pokemon Set Volume Over Time', fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Individual sets chart saved as {output_file}")
    # plt.show()  # Don't show in non-interactive mode

def create_monthly_stacked_chart(df_pivot, ordered_sets=None, output_file="pokemon_monthly_volume_chart.png"):
    """Create a monthly aggregated stacked column chart"""
    
    # Use ordered sets if provided, otherwise use df_pivot columns
    if ordered_sets is None:
        ordered_sets = df_pivot.columns.tolist()
    else:
        # Filter out any sets not in the data
        ordered_sets = [s for s in ordered_sets if s in df_pivot.columns]
    
    # Get the latest date in the data
    latest_date = df_pivot.index.max()
    current_month = pd.Timestamp(latest_date.year, latest_date.month, 1)
    
    # Determine if the current month is complete or in progress
    now = pd.Timestamp.now()
    is_current_month_complete = (latest_date.month != now.month or latest_date.year != now.year)
    
    if is_current_month_complete:
        # Include the current month if it's complete
        df_monthly = df_pivot.resample('ME').sum()
        chart_title = 'Monthly Pokemon TCG Volume Sold by Set'
    else:
        # Exclude the current month if it's incomplete
        last_complete_month = current_month - pd.DateOffset(days=1)
        df_monthly = df_pivot[df_pivot.index < current_month].resample('ME').sum()
        chart_title = 'Monthly Pokemon TCG Volume Sold by Set (Complete Months Only)'
    
    # Set up the figure
    plt.figure(figsize=(16, 10))
    
    # Create the stacked column plot
    ax = plt.gca()
    
    # Create progressive color scheme - earliest sets lightest, latest sets darkest
    n_sets = len(ordered_sets)
    
    # Use a sequential colormap (Blues) for progressive darkening
    # Map 0.3 (light) to 0.9 (dark) for better visibility while maintaining progression
    lightness_values = np.linspace(0.3, 0.9, n_sets)
    colors = plt.cm.Blues(lightness_values)
    
    # Create the stacked column plot
    bottom = np.zeros(len(df_monthly.index))
    for i, col in enumerate(ordered_sets):
        if col in df_monthly.columns:  # Check if this set has data in the monthly aggregation
            ax.bar(df_monthly.index, df_monthly[col], bottom=bottom, 
                  label=col, color=colors[i], width=25, alpha=0.8)
            bottom += df_monthly[col].values
    
    # Customize the plot
    plt.title(chart_title, fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Month', fontsize=12)
    plt.ylabel('Volume Sold ($)', fontsize=12)
    
    # Format the x-axis dates to show month and year
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.xticks(rotation=45)
    
    # Set x-axis limits to show only the range of actual data (no future months)
    ax.set_xlim(df_monthly.index.min() - pd.Timedelta(days=15), 
                df_monthly.index.max() + pd.Timedelta(days=15))
    
    # Format y-axis to show currency
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1000:.0f}K'))
    
    # Add grid
    plt.grid(True, alpha=0.3, axis='y')  # Only show horizontal grid lines for bar charts
    
    # Add legend
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    
    # Adjust layout to prevent legend cutoff
    plt.tight_layout()
    
    # Add note about current month
    if not is_current_month_complete:
        current_month_name = latest_date.strftime('%B %Y')
        plt.figtext(0.5, 0.01, f"Note: {current_month_name} is excluded as it contains incomplete data", 
                   ha='center', fontsize=10, fontstyle='italic')
    
    # Save the plot
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Monthly column chart saved as {output_file}")
    
    return ax
    
    # Add note about current month
    if not is_current_month_complete:
        current_month_name = latest_date.strftime('%B %Y')
        plt.figtext(0.5, 0.01, f"Note: {current_month_name} is excluded as it contains incomplete data", 
                   ha='center', fontsize=10, fontstyle='italic')
    
    # Save the plot
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Monthly column chart saved as {output_file}")
    
    return ax

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
    
    # Load set release dates from pokemon_sets.csv
    print("Loading set release dates...")
    ordered_sets, set_dates = load_set_release_dates()
    
    if ordered_sets is None:
        print("Warning: Could not load set release dates. Using default ordering.")
        ordered_sets = df_pivot.columns.tolist()
    else:
        print(f"Found {len(ordered_sets)} sets with release dates")
        # Filter to only include sets that are in our data
        ordered_sets = [s for s in ordered_sets if s in df_pivot.columns]
        print(f"Using {len(ordered_sets)} sets for progressive coloring (newest darkest, oldest lightest)")
    
    print(f"Data loaded: {len(df_pivot)} dates, {len(df_pivot.columns)} sets")
    
    # Create summary statistics
    create_summary_stats(df_pivot)
    
    # Create the main stacked area chart
    print("\nCreating stacked area chart...")
    create_stacked_area_chart(df_pivot, ordered_sets)
    
    # Create monthly aggregated chart
    print("\nCreating monthly aggregated stacked chart...")
    create_monthly_stacked_chart(df_pivot, ordered_sets)
    
    # Create individual set charts
    print("\nCreating individual set charts...")
    create_individual_set_chart(df_pivot, ordered_sets)
    
    print("\nVisualization complete!")

if __name__ == "__main__":
    main()
