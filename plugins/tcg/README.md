# TCG (Trading Card Game) Price Analytics Module

A comprehensive Pokemon TCG price history scraper and analytics platform that fetches data from TCGPlayer's API and provides detailed time series analysis, visualization, and trend monitoring capabilities.

## Overview

This module provides end-to-end functionality for:
- Scraping Pokemon TCG price history from TCGPlayer API
- Storing price data in a structured SQLite database  
- Performing time series analysis with trend detection
- Generating interactive visualizations and charts
- Exporting data for external analysis

## Core Files Description

### Modular Architecture (New)
The system has been refactored into a clean, modular architecture with clear separation of concerns:

- **`database.py`** - Database operations and connection management
  - DatabaseManager class for all database interactions
  - Connection pooling and transaction management
  - Database initialization and schema creation

- **`data_collector.py`** - API data fetching and collection
  - DataCollector class for TCGPlayer API interactions
  - Rate-limited API calls with configurable delays
  - Incremental data updates and duplicate prevention

- **`data_manager.py`** - Data loading and management
  - DataManager class for CSV loading and data processing
  - Summary functions for data status reporting
  - Data export and formatting utilities

- **`cli.py`** - Command-line interface
  - TCGCommandLineInterface class for user interactions
  - All CLI commands and argument parsing
  - Clean separation of interface from business logic

### Legacy Files (Refactored for Compatibility)
- **`init_db.py`** - Database schema initialization (uses new DatabaseManager)
  - Maintains backward compatibility while using modular architecture
  - Creates SQLite database with tables for sets, price history, and TCGPlayer groups

- **`load_groups.py`** - Main entry point for data operations (imports from new modules)
  - Preserves original function signatures for backward compatibility
  - Delegates to new modular components

- **`testing.py`** - API testing utilities and legacy CLI support
  - Keeps API testing functions and sample data
  - Delegates CLI operations to new cli.py module

### Analysis & Time Series
- **`analysis.py`** - Comprehensive time series analysis toolkit
  - TimeSeriesData class for structured price data representation
  - Functions for extracting data by product ID or set name
  - Price trend analysis with moving averages and volatility metrics
  - Statistical summaries and volume analysis
  - CSV export capabilities for external tools

### Visualization
- **`visualize_volume.py`** - Static chart generation using matplotlib
  - Creates stacked area charts showing volume trends over time
  - Individual set performance charts
  - Summary statistics and trend analysis

- **`interactive_charts.py`** - Interactive web-based visualizations using Plotly
  - Dynamic HTML charts with zoom, filter, and hover capabilities
  - Comprehensive dashboard with multiple chart types
  - Weekly trend analysis and distribution plots

### Demonstration & Examples
- **`demo_analysis.py`** - Example usage and capabilities showcase
  - Demonstrates all major analysis functions
  - Shows data extraction patterns and best practices

- **`test_analysis.py`** - Unit testing for analysis functions
  - Validates time series extraction and summary calculations
  - Tests data integrity and function reliability

## Key Features

### Data Collection
- **Rate-limited API scraping** (2-second delays by default)
- **Incremental updates** - Skip products with recent data
- **Duplicate prevention** - Unique constraints on SKU and date
- **Comprehensive coverage** - Both booster packs and booster boxes

### Analysis Capabilities
- **Time series extraction** by product ID or set name
- **Trend analysis** with configurable moving average windows
- **Price volatility metrics** using standard deviation
- **Volume analysis** calculating dollar volume (quantity × price)
- **Date range filtering** for focused analysis periods

### Visualization Options
- **Static charts** (PNG) for reports and documentation
- **Interactive dashboards** (HTML) for exploration
- **Stacked area charts** showing market composition over time
- **Individual set performance** tracking
- **Summary statistics** with key metrics

## Database Schema

- **`tcgplayer_groups`** - TCGPlayer product group metadata
- **`pokemon_sets`** - Pokemon set information with product IDs
- **`price_history`** - Daily price data with market prices and sales volumes

## Quick Start

### Initialize System
```bash
# Using new modular CLI
python cli.py init                   # Create database and load sets
python cli.py fetch_all             # Fetch all price history (with rate limiting)

# Using legacy interface (still supported)
python init_db.py                   # Create database
python load_groups.py               # Load Pokemon sets data
python testing.py fetch_all         # Fetch all price history
```

### Basic Analysis
```bash
# New CLI interface
python cli.py summary               # Show data status
python cli.py trends                # Price trends (last 30 days)
python cli.py analyze 624679        # Detailed analysis for specific product
python cli.py volume_summary        # Volume statistics by set

# Legacy interface (still supported)
python testing.py summary           # Show data status
python testing.py trends            # Price trends (last 30 days)
python testing.py analyze 624679    # Detailed analysis for specific product
```

### Generate Visualizations
```bash
python cli.py volume pokemon_volume.csv     # Export volume data
python visualize_volume.py                  # Create static charts
python interactive_charts.py                # Create interactive dashboard
```

### Advanced Analysis with New Modules
```python
from database import DatabaseManager
from data_collector import DataCollector
from data_manager import DataManager
import analysis

# Using new modular architecture
db = DatabaseManager()
collector = DataCollector(db)
manager = DataManager(db)

# Load and fetch data
manager.load_pokemon_sets_from_csv("pokemon_sets.csv")
collector.fetch_price_history_for_product(624679)

# Traditional analysis still works
data = analysis.get_time_series_by_product_id(624679)
trends = analysis.analyze_price_trends(624679, window_days=14)
analysis.export_to_csv(data, "my_analysis.csv")
```

## Command Line Interface

The system now provides two CLI options:

### New Modular CLI (`cli.py`)
The recommended interface with clean architecture:

**Setup:**
- `init` - Initialize database and load Pokemon sets
- `summary` - Show Pokemon sets and price history status

**Data Collection:**
- `fetch_one <id>` - Fetch price history for single product
- `fetch_all` - Fetch price history for all Pokemon sets
- `load_sets <csv_file>` - Load Pokemon sets from CSV

**Analysis:**
- `analyze <id> [window]` - Detailed product analysis
- `trends [id] [days]` - Show price trends
- `export <id> [file]` - Export time series to CSV
- `sets` - List all available Pokemon sets
- `recent [days]` - Show recent price activity

**Visualization:**
- `volume [file]` - Export volume data to CSV
- `volume_summary` - Show volume statistics by set

### Legacy CLI (`testing.py`)
Still supported for backward compatibility with identical commands:
- All the same commands as above
- Plus additional API testing functions
- Delegates to new modular system internally

Both interfaces provide the same functionality, with the new `cli.py` offering better organization and maintainability.

## Data Sources

- **TCGPlayer API**: `https://infinite-api.tcgplayer.com/price/history/{product_id}/detailed`
- **Pokemon Sets CSV**: Manually curated set information with product IDs
- **Rate Limiting**: 2-second delays between API requests (configurable)

## Output Files

- **`tcg.db`** - SQLite database with all price history
- **`pokemon_volume_chart.png`** - Static volume trend chart
- **`pokemon_volume_interactive.html`** - Interactive volume dashboard
- **`pokemon_dashboard.html`** - Comprehensive analysis dashboard
- **CSV exports** - Formatted data for external analysis tools

## Technical Notes

### Architecture
- **Modular Design**: Clean separation of concerns with dedicated modules for database, data collection, management, and CLI
- **Backward Compatibility**: All original function signatures preserved for existing code
- **Class-based Organization**: Each module uses classes for better encapsulation and reusability
- **Database**: SQLite with proper indexing and constraints via DatabaseManager
- **Rate Limiting**: Respectful API usage with configurable delays in DataCollector
- **Data Integrity**: Unique constraints prevent duplicate entries
- **Error Handling**: Graceful handling of API failures and missing data throughout all modules
- **Performance**: Optimized queries and batch processing for large datasets

### Module Dependencies
- `database.py` - Core database operations (no dependencies)
- `data_collector.py` - Depends on database.py
- `data_manager.py` - Depends on database.py  
- `cli.py` - Depends on all other modules
- Legacy files import from new modules for compatibility

This refactored architecture provides better maintainability, testability, and extensibility while preserving all existing functionality.
