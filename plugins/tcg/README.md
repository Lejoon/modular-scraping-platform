# TCGPlayer Price History Scraper

This module provides functionality to scrape and store Pokemon TCG price history data from TCGPlayer's API.

## Database Schema

The system maintains several tables:

1. **`tcgplayer_groups`** - TCGPlayer product groups/sets
2. **`pokemon_sets`** - Pokemon set information with booster and booster box product IDs
3. **`price_history`** - Historical price data with market prices, quantities sold, and date ranges

## Key Concepts

- **Product ID**: The main product identifier used by TCGPlayer (e.g., 624679)
- **SKU ID**: Specific variant identifier that includes language, condition, and variant (e.g., "8637794")
- **Buckets**: Daily price data points with market price, quantity sold, and price ranges

## Usage

### Command Line Interface

Use `testing.py` with various commands:

```bash
# Show summary of Pokemon sets and their price history status
python testing.py summary

# Show price trends for all products (last 30 days)
python testing.py trends

# Show detailed price history for a specific product (last 7 days)
python testing.py trends 624679 7

# Fetch price history for a single product
python testing.py fetch_one 624679

# Fetch price history for all Pokemon sets (with rate limiting)
python testing.py fetch_all

# Test with sample data
python testing.py test_api

# Query price history examples
python testing.py query

# Test latest sales API
python testing.py latest_sales
```

### Python Functions

#### Initialize Database
```python
from init_db import init_db
init_db()
```

#### Load Data
```python
from load_groups import load_all, load_groups, load_pokemon_sets
load_all()  # Load both groups and Pokemon sets
```

#### Fetch Price History
```python
from load_groups import fetch_and_save_price_history, fetch_all_pokemon_price_history

# Fetch for a single product
fetch_and_save_price_history(624679)

# Fetch for all Pokemon sets
fetch_all_pokemon_price_history(delay_seconds=2)
```

#### Analyze Data
```python
from load_groups import analyze_price_trends, get_pokemon_sets_summary

# Get summary of all sets
get_pokemon_sets_summary()

# Analyze price trends
analyze_price_trends()  # All products, last 30 days
analyze_price_trends(624679, 7)  # Specific product, last 7 days
```

#### Time Series Analysis
```python
import analysis

# Extract time series data
time_series = analysis.get_time_series_by_product_id(624679)
time_series = analysis.get_time_series_by_set_name("Scarlet & Violet: Destined Rivals", "booster_box")

# Get price summary
summary = analysis.get_price_summary_by_product(624679)

# Analyze trends with moving averages
trends = analysis.analyze_price_trends(624679, window_days=7)

# Export data to CSV
analysis.export_to_csv(time_series, "my_data.csv")

# Get available sets
sets = analysis.get_all_available_sets()

# Get recent activity
activity = analysis.get_recent_activity(days=30)
```

## Data Structure

### Pokemon Sets CSV Format
```
Set Name;Release Date;TCGPlayer Booster Product ID;TCGPlayer Booster Box Product ID;TCGPlayer Group ID
```

### Price History API Response
The system processes responses from:
```
https://infinite-api.tcgplayer.com/price/history/{product_id}/detailed?range=annual
```

Each response contains:
- Product metadata (SKU ID, variant, language, condition)
- Daily buckets with market price, quantity sold, low/high prices, and dates

## Database Relationships

- `pokemon_sets.group_id` → `tcgplayer_groups.tcgplayer_id`
- `price_history.product_id` → `pokemon_sets.booster_product_id` OR `pokemon_sets.booster_box_product_id`
- `price_history` has unique constraint on `(sku_id, bucket_start_date)` to prevent duplicates

## Rate Limiting

The system includes built-in rate limiting (default 2 seconds between requests) to be respectful to the TCGPlayer API. This can be adjusted in the fetch functions.

## Files

- `init_db.py` - Database initialization
- `load_groups.py` - Main functionality for loading and fetching data
- `testing.py` - Command-line interface and testing functions
- `pokemon_sets.csv` - Pokemon set data
- `tcg.db` - SQLite database file
