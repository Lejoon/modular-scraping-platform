#!/usr/bin/env python3
"""
Interactive Pokemon TCG Volume Visualization using Plotly

Creates an interactive HTML chart showing volume sold over time for each Pokemon set.
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import os

def load_and_process_data(csv_file):
    """Load the CSV and aggregate volume by date and set"""
    
    # Read the CSV
    df = pd.read_csv(csv_file)
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Group by set_name and date, summing the volume_sold
    df_agg = df.groupby(['set_name', 'date'])['volume_sold'].sum().reset_index()
    
    return df_agg

def create_interactive_stacked_area(df_agg, output_file="pokemon_volume_interactive.html"):
    """Create an interactive stacked area chart"""
    
    # Create the figure
    fig = go.Figure()
    
    # Get unique sets and colors
    sets = df_agg['set_name'].unique()
    colors = px.colors.qualitative.Set3[:len(sets)]
    
    # Add traces for each set
    for i, set_name in enumerate(sets):
        set_data = df_agg[df_agg['set_name'] == set_name].sort_values('date')
        
        fig.add_trace(go.Scatter(
            x=set_data['date'],
            y=set_data['volume_sold'],
            mode='lines',
            stackgroup='one',
            name=set_name,
            line=dict(color=colors[i % len(colors)]),
            hovertemplate='<b>%{fullData.name}</b><br>' +
                         'Date: %{x}<br>' +
                         'Volume: $%{y:,.0f}<br>' +
                         '<extra></extra>'
        ))
    
    # Update layout
    fig.update_layout(
        title=dict(
            text='Pokemon TCG Volume Sold Over Time by Set',
            x=0.5,
            font=dict(size=20)
        ),
        xaxis=dict(
            title='Date',
            showgrid=True,
            gridcolor='lightgray'
        ),
        yaxis=dict(
            title='Volume Sold ($)',
            showgrid=True,
            gridcolor='lightgray',
            tickformat='$,.0f'
        ),
        hovermode='x unified',
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02
        ),
        width=1200,
        height=700,
        plot_bgcolor='white'
    )
    
    # Save as HTML
    fig.write_html(output_file)
    print(f"Interactive chart saved as {output_file}")
    
    return fig

def create_individual_line_charts(df_agg, output_file="pokemon_individual_interactive.html"):
    """Create individual line charts for each set"""
    
    sets = df_agg['set_name'].unique()
    n_sets = len(sets)
    
    # Create subplots
    rows = (n_sets + 2) // 3  # 3 columns
    fig = make_subplots(
        rows=rows, 
        cols=3,
        subplot_titles=sets,
        vertical_spacing=0.08,
        horizontal_spacing=0.05
    )
    
    colors = px.colors.qualitative.Set3
    
    for i, set_name in enumerate(sets):
        row = (i // 3) + 1
        col = (i % 3) + 1
        
        set_data = df_agg[df_agg['set_name'] == set_name].sort_values('date')
        
        fig.add_trace(
            go.Scatter(
                x=set_data['date'],
                y=set_data['volume_sold'],
                mode='lines+markers',
                name=set_name,
                line=dict(color=colors[i % len(colors)]),
                fill='tonexty' if i > 0 else 'tozeroy',
                showlegend=False,
                hovertemplate='Date: %{x}<br>Volume: $%{y:,.0f}<extra></extra>'
            ),
            row=row, col=col
        )
    
    # Update layout
    fig.update_layout(
        title=dict(
            text='Individual Pokemon Set Volume Over Time',
            x=0.5,
            font=dict(size=18)
        ),
        height=300 * rows,
        showlegend=False,
        plot_bgcolor='white'
    )
    
    # Update axes
    for i in range(1, rows + 1):
        for j in range(1, 4):
            fig.update_xaxes(showgrid=True, gridcolor='lightgray', row=i, col=j)
            fig.update_yaxes(showgrid=True, gridcolor='lightgray', tickformat='$,.0f', row=i, col=j)
    
    # Save as HTML
    fig.write_html(output_file)
    print(f"Individual charts saved as {output_file}")
    
    return fig

def create_summary_dashboard(df_agg, output_file="pokemon_dashboard.html"):
    """Create a comprehensive dashboard"""
    
    # Calculate summary statistics
    total_by_set = df_agg.groupby('set_name')['volume_sold'].sum().sort_values(ascending=False)
    daily_totals = df_agg.groupby('date')['volume_sold'].sum()
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=[
            'Volume by Set (Total)',
            'Daily Total Volume Over Time',
            'Volume Distribution by Set',
            'Weekly Volume Trends'
        ],
        specs=[[{"type": "bar"}, {"type": "scatter"}],
               [{"type": "box"}, {"type": "scatter"}]],
        vertical_spacing=0.12,
        horizontal_spacing=0.1
    )
    
    # 1. Bar chart of total volume by set
    fig.add_trace(
        go.Bar(
            x=total_by_set.index,
            y=total_by_set.values,
            name="Total Volume",
            marker_color=px.colors.qualitative.Set3[0],
            showlegend=False,
            hovertemplate='Set: %{x}<br>Total Volume: $%{y:,.0f}<extra></extra>'
        ),
        row=1, col=1
    )
    
    # 2. Daily total volume line chart
    fig.add_trace(
        go.Scatter(
            x=daily_totals.index,
            y=daily_totals.values,
            mode='lines+markers',
            name="Daily Total",
            line=dict(color=px.colors.qualitative.Set3[1]),
            showlegend=False,
            hovertemplate='Date: %{x}<br>Total Volume: $%{y:,.0f}<extra></extra>'
        ),
        row=1, col=2
    )
    
    # 3. Box plot of volume distribution
    sets = df_agg['set_name'].unique()
    for i, set_name in enumerate(sets):
        set_data = df_agg[df_agg['set_name'] == set_name]
        fig.add_trace(
            go.Box(
                y=set_data['volume_sold'],
                name=set_name,
                showlegend=False,
                marker_color=px.colors.qualitative.Set3[i % len(px.colors.qualitative.Set3)]
            ),
            row=2, col=1
        )
    
    # 4. Weekly aggregated trends
    df_agg['week'] = df_agg['date'].dt.to_period('W').dt.start_time
    weekly_data = df_agg.groupby(['set_name', 'week'])['volume_sold'].sum().reset_index()
    
    top_sets = total_by_set.head(5).index  # Show only top 5 sets for clarity
    for i, set_name in enumerate(top_sets):
        set_weekly = weekly_data[weekly_data['set_name'] == set_name]
        fig.add_trace(
            go.Scatter(
                x=set_weekly['week'],
                y=set_weekly['volume_sold'],
                mode='lines+markers',
                name=set_name,
                line=dict(color=px.colors.qualitative.Set3[i]),
                showlegend=True
            ),
            row=2, col=2
        )
    
    # Update layout
    fig.update_layout(
        title=dict(
            text='Pokemon TCG Volume Analysis Dashboard',
            x=0.5,
            font=dict(size=20)
        ),
        height=800,
        plot_bgcolor='white'
    )
    
    # Update specific axes
    fig.update_xaxes(tickangle=45, row=1, col=1)
    fig.update_yaxes(tickformat='$,.0f', row=1, col=1)
    fig.update_yaxes(tickformat='$,.0f', row=1, col=2)
    fig.update_xaxes(title_text="Date", row=1, col=2)
    fig.update_yaxes(title_text="Volume ($)", row=1, col=2)
    fig.update_yaxes(tickformat='$,.0f', row=2, col=1)
    fig.update_xaxes(tickangle=45, row=2, col=1)
    fig.update_yaxes(tickformat='$,.0f', row=2, col=2)
    
    # Save as HTML
    fig.write_html(output_file)
    print(f"Dashboard saved as {output_file}")
    
    return fig

def main():
    """Main function"""
    
    csv_file = "pokemon_sets_volume_sold.csv"
    
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} not found!")
        print("Please run the volume export command first:")
        print("python testing.py volume_csv")
        return
    
    print("Loading and processing data...")
    df_agg = load_and_process_data(csv_file)
    
    print(f"Data loaded: {len(df_agg)} records, {df_agg['set_name'].nunique()} sets")
    
    # Create the main stacked area chart
    print("\nCreating interactive stacked area chart...")
    create_interactive_stacked_area(df_agg)
    
    # Create individual set charts
    print("Creating individual set charts...")
    create_individual_line_charts(df_agg)
    
    # Create comprehensive dashboard
    print("Creating analysis dashboard...")
    create_summary_dashboard(df_agg)
    
    print("\nInteractive visualizations complete!")
    print("\nGenerated files:")
    print("- pokemon_volume_interactive.html (Main stacked area chart)")
    print("- pokemon_individual_interactive.html (Individual set charts)")
    print("- pokemon_dashboard.html (Comprehensive dashboard)")
    print("\nOpen these HTML files in your browser to view the interactive charts.")

if __name__ == "__main__":
    main()
