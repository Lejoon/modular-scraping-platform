# Modular Scraper Platform

A plugin-centric framework for building data processing pipelines with automatic plugin discovery and YAML-based configuration. Features a Transform-based architecture where plugins can be chained together without manual registration.

## Architecture

```
modular-scraping-platform/
│
├── core/                           # Core framework
│   ├── interfaces.py              # Transform abstract base class
│   ├── models.py                  # RawItem, ParsedItem, Event
│   ├── plugin_loader.py           # Automatic plugin discovery
│   ├── pipeline_orchestrator.py   # Pipeline execution engine
│   └── infra/                     # Infrastructure components
│       ├── http.py               # aiohttp wrapper with retry logic
│       ├── ws.py                 # websocket client with heartbeat / reconnect
│       ├── sel.py                # Playwright helpers for browser automation
│       ├── db.py                 # async SQLite wrapper with migrations
│       └── scheduler.py          # APScheduler wrapper
│
├── plugins/                        # Auto-discovered plugins
│   └── fi_shortinterest/
│       ├── fetcher.py             # FiFetcher (data fetching transform)
│       ├── parser.py              # FiAggParser / FiActParser (parsing transforms)
│       ├── sinks.py               # DatabaseSink (storage transform)
│       └── sinkmap.yaml           # plugin configuration
│
├── pipelines.yml                   # Declarative pipeline configuration
├── main.py                         # Pipeline entry point
└── integration_bridge.py           # Bridge for existing Discord bot
```

## Key Features

- **Plugin-Centric Architecture**: Auto-discovery of Transform classes from `plugins/` directory
- **Transform-Based Pipeline**: All components implement `async def __call__(items: AsyncIterator[Any]) -> AsyncIterator[Any]`
- **YAML Configuration**: Declarative pipeline definition without code changes
- **Drop-in Plugins**: Add new functionality by dropping folders in `plugins/`
- **Async Streaming**: Backpressure-aware processing with async iterators
- **Context Management**: Automatic resource cleanup for sinks and transforms
- **Type Safety**: Pydantic models throughout for validation
- **Robust Infrastructure**: HTTP retry logic, database migrations, WebSocket support

## Quick Start

### Standalone Usage

```bash
cd modular-scraping-platform
pip install -r requirements.txt
python main.py
```

The system will automatically discover plugins and run the pipelines defined in `pipelines.yml`.

### Example Pipeline Configuration

```yaml
pipelines:
  - name: "FI Short Interest Data"
    chain:
      - class: "fi_shortinterest.FiFetcher"
        kwargs:
          poll_interval: 300
      - class: "fi_shortinterest.FiAggParser"
      - class: "fi_shortinterest.FiActParser"  
      - class: "fi_shortinterest.DatabaseSink"
        kwargs:
          db_path: "fi_shortinterest.db"
```

### Integration with Existing Discord Bot

```python
from integration_bridge import start_fi_monitoring

# In your Discord bot setup
bridge = await start_fi_monitoring(
    discord_bot=bot,
    session=your_aiohttp_session,  # optional
    channel_id=1175019650963222599,
    error_channel_id=1162053416290361516,
)

# The system now runs continuously in the background
# No need for manual update calls
```

## Configuration

Edit `pipelines.yml` to define your data processing pipelines:

```yaml
pipelines:
  - name: "My Data Pipeline"
    chain:
      - class: "my_plugin.DataFetcher"
        kwargs:
          api_key: "your-api-key"
      - class: "my_plugin.DataParser"
      - class: "my_plugin.DatabaseSink"
        kwargs:
          db_path: "data.db"
```

Each stage in the chain must implement the `Transform` interface with an `async __call__` method.

## Database Schema

The system automatically creates and migrates SQLite tables:

- `short_positions`: Aggregate short interest data
- `position_holders`: Individual position holder data
- `migrations`: Schema version tracking

## Adding New Data Sources

1. **Create a plugin directory** under `plugins/your_plugin_name/`
2. **Implement Transform classes** that inherit from `core.interfaces.Transform`:
   ```python
   from core.interfaces import Transform
   from typing import AsyncIterator, Any
   
   class MyFetcher(Transform):
       async def __call__(self, items: AsyncIterator[Any]) -> AsyncIterator[Any]:
           # Your fetching logic here
           yield some_data
   ```
3. **Add to pipeline configuration** in `pipelines.yml`:
   ```yaml
   pipelines:
     - name: "My Pipeline"
       chain:
         - class: "your_plugin_name.MyFetcher"
         - class: "your_plugin_name.MyParser"
         - class: "your_plugin_name.MySink"
   ```
4. **Run the system** - plugins are auto-discovered on startup!

## Development

```bash
# Install development dependencies
pip install -r requirements.txt

# Run tests
pytest

# Format code
black .
isort .

# Type checking
mypy .
```

## Migration from Old System

The `integration_bridge.py` provides a drop-in replacement for the old system. The new architecture offers:

- **Zero Registration**: Plugins are auto-discovered
- **Declarative Configuration**: Pipeline chains defined in YAML
- **Transform Pattern**: Unified `async __call__` interface for all components  
- **Streaming Processing**: Async iterators enable backpressure and efficient memory usage
- **Hot-Swappable**: Drop new plugins in `plugins/` directory and restart