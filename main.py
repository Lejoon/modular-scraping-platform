"""
Main entry point for the scraper platform.
"""

import asyncio
import logging
import sys
import os
from pathlib import Path

# Add project root to PYTHONPATH so imports work when running this script directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.orchestrator import Orchestrator, Pipeline
from plugins.fi_shortinterest.fetcher import FiFetcher
from plugins.fi_shortinterest.parser import FiAggParser, FiActParser
from sinks.database_sink import DatabaseSink


# Make sure we run in the project root for relative paths (e.g. scraper.db)
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)


async def main():
    """Main entry point."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    # Create orchestrator
    orchestrator = Orchestrator("config.yaml")
    
    # Create and register FI short interest pipeline
    fetcher = FiFetcher()
    parsers = [FiAggParser(), FiActParser()]
    
    # Ensure we write to the project-root scraper.db
    project_root = Path(__file__).parent
    db_file = project_root / "scraper.db"
    sinks = [DatabaseSink(str(db_file))]
    
    pipeline = Pipeline(
        name="fi_shortinterest",
        fetcher=fetcher,
        parsers=parsers,
        sinks=sinks,
        use_diff=True
    )
    
    await orchestrator.register_pipeline(pipeline)
    
    # Run the orchestrator
    logger.info("Starting scraper platform...")
    try:
        await orchestrator.run_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await orchestrator.stop()
        
        # Close database connections
        for sink in sinks:
            if hasattr(sink, 'close'):
                await sink.close()


if __name__ == "__main__":
    asyncio.run(main())