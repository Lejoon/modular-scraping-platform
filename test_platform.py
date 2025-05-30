#!/usr/bin/env python3
"""
Test script to validate the new modular scraping platform setup.
"""

import asyncio
import logging
import sys
import os

# Add project root to PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.orchestrator import Orchestrator


async def test_system():
    """Test the complete system setup."""
    logging.basicConfig(level=logging.INFO, format="%(name)s | %(levelname)s | %(message)s")
    logger = logging.getLogger(__name__)
    
    logger.info("Testing modular scraping platform...")
    
    # Test entry point discovery
    orchestrator = Orchestrator("config.yaml")
    
    logger.info(f"Discovered {len(orchestrator.fetchers)} fetchers:")
    for name in orchestrator.fetchers:
        logger.info(f"  - {name}")
    
    logger.info(f"Discovered {len(orchestrator.parsers)} parsers:")
    for name in orchestrator.parsers:
        logger.info(f"  - {name}")
    
    logger.info(f"Discovered {len(orchestrator.sinks)} sinks:")
    for name in orchestrator.sinks:
        logger.info(f"  - {name}")
    
    # Test fetcher instantiation
    logger.info("Testing fetcher instantiation...")
    fetcher_class = orchestrator.fetchers["FiFetcher"]
    fetcher = fetcher_class()
    logger.info(f"✓ FiFetcher created: {fetcher.name}")
    
    # Test parser instantiation
    logger.info("Testing parser instantiation...")
    parser_class = orchestrator.parsers["FiAggParser"]
    parser = parser_class()
    logger.info(f"✓ FiAggParser created: {parser.name}")
    
    # Test sink instantiation
    logger.info("Testing sink instantiation...")
    sink_class = orchestrator.sinks["DatabaseSink"]
    sink = sink_class(db_url="test.db")
    logger.info(f"✓ DatabaseSink created: {sink.name}")
    
    # Test diff parser
    logger.info("Testing diff parser instantiation...")
    diff_parser_class = orchestrator.parsers["DiffParser"]
    diff_parser = diff_parser_class()
    logger.info(f"✓ DiffParser created: {diff_parser.name}")
    
    logger.info("✅ All components successfully instantiated!")
    logger.info("🎉 Platform ready for production!")


if __name__ == "__main__":
    asyncio.run(test_system())
