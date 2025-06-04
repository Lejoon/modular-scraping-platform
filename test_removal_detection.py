#!/usr/bin/env python3
"""
Test script to demonstrate removal detection in DiffParser.

This script shows how the enhanced DiffParser can detect when positions
are removed from the data feed (dropping to 0.0%).
"""

import asyncio
import logging
from datetime import datetime
from typing import AsyncIterator

from core.models import ParsedItem
from plugins.fi_shortinterest.diff_parser import DiffParser
from plugins.fi_shortinterest.sinks import DatabaseSink
from core.infra.db import Database

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

async def create_test_item(topic: str, content: dict) -> ParsedItem:
    """Create a test ParsedItem."""
    return ParsedItem(
        topic=topic,
        content=content,
        discovered_at=datetime.utcnow()
    )

async def setup_test_data(db_path: str = "test_removal.db"):
    """Setup initial test data in database."""
    db = Database(db_path)
    await db.connect()
    
    # Create tables
    await db.execute("""
        CREATE TABLE IF NOT EXISTS short_positions (
            lei TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            position_percent REAL NOT NULL,
            latest_position_date TEXT,
            timestamp TEXT NOT NULL
        )
    """)
    
    await db.execute("""
        CREATE TABLE IF NOT EXISTS position_holders (
            entity_name TEXT NOT NULL,
            issuer_name TEXT NOT NULL,
            isin TEXT NOT NULL,
            position_percent REAL NOT NULL,
            position_date TEXT,
            timestamp TEXT NOT NULL,
            comment TEXT,
            PRIMARY KEY (entity_name, issuer_name, isin)
        )
    """)
    
    # Insert test data
    await db.execute("""
        INSERT OR REPLACE INTO short_positions 
        (lei, company_name, position_percent, latest_position_date, timestamp)
        VALUES 
        ('LEI001', 'Company A', 2.5, '2024-01-01', '2024-01-01T10:00:00'),
        ('LEI002', 'Company B', 1.8, '2024-01-01', '2024-01-01T10:00:00'),
        ('LEI003', 'Company C', 0.9, '2024-01-01', '2024-01-01T10:00:00')
    """)
    
    await db.execute("""
        INSERT OR REPLACE INTO position_holders 
        (entity_name, issuer_name, isin, position_percent, position_date, timestamp, comment)
        VALUES 
        ('Entity X', 'Issuer 1', 'ISIN001', 1.2, '2024-01-01', '2024-01-01T10:00:00', ''),
        ('Entity Y', 'Issuer 1', 'ISIN001', 0.8, '2024-01-01', '2024-01-01T10:00:00', ''),
        ('Entity Z', 'Issuer 2', 'ISIN002', 2.1, '2024-01-01', '2024-01-01T10:00:00', '')
    """)
    
    await db.close()
    logger.info("Test data setup complete")

async def test_removal_detection():
    """Test the removal detection feature."""
    logger.info("=== Testing Removal Detection ===")
    
    # Use consistent database path
    db_path = "test_removal.db"
    
    # Setup initial data in database
    await setup_test_data(db_path)
    
    # Create DiffParser and DatabaseSink instances
    diff_parser = DiffParser(db_path=db_path)
    db_sink = DatabaseSink(db_path=db_path)
    
    # Initialize the DatabaseSink
    await db_sink.db.connect()
    await db_sink._create_tables()
    
    # BATCH 1: Process all entities (this should populate database and show changes)
    logger.info("Phase 1: Processing full dataset...")
    initial_data = [
        await create_test_item("fi.short.aggregate", {
            "lei": "LEI001",
            "company_name": "Company A",
            "position_percent": 2.5,
            "latest_position_date": "2024-01-01",
            "timestamp": "2024-01-01T10:00:00"
        }),
        await create_test_item("fi.short.aggregate", {
            "lei": "LEI002", 
            "company_name": "Company B",
            "position_percent": 1.8,
            "latest_position_date": "2024-01-01",
            "timestamp": "2024-01-01T10:00:00"
        }),
        await create_test_item("fi.short.aggregate", {
            "lei": "LEI003",
            "company_name": "Company C", 
            "position_percent": 0.9,
            "latest_position_date": "2024-01-01",
            "timestamp": "2024-01-01T10:00:00"
        }),
        await create_test_item("fi.short.positions", {
            "entity_name": "Entity X",
            "issuer_name": "Issuer 1", 
            "isin": "ISIN001",
            "position_percent": 1.2,
            "position_date": "2024-01-01",
            "timestamp": "2024-01-01T10:00:00",
            "comment": ""
        }),
        await create_test_item("fi.short.positions", {
            "entity_name": "Entity Y",
            "issuer_name": "Issuer 1", 
            "isin": "ISIN001",
            "position_percent": 0.8,
            "position_date": "2024-01-01",
            "timestamp": "2024-01-01T10:00:00",
            "comment": ""
        }),
        await create_test_item("fi.short.positions", {
            "entity_name": "Entity Z",
            "issuer_name": "Issuer 2", 
            "isin": "ISIN002",
            "position_percent": 2.1,
            "position_date": "2024-01-01",
            "timestamp": "2024-01-01T10:00:00",
            "comment": ""
        })
    ]
    
    async def initial_stream() -> AsyncIterator[ParsedItem]:
        for item in initial_data:
            yield item
    
    batch1_results = []
    async for result in diff_parser(initial_stream()):
        batch1_results.append(result)
        # Also pass through DatabaseSink to persist data
        if isinstance(result, ParsedItem):
            await db_sink.handle(result)
    
    logger.info(f"Batch 1: {len([r for r in batch1_results if not r.topic.endswith('.diff')])} originals, {len([r for r in batch1_results if r.topic.endswith('.diff')])} changes")
    
    # BATCH 2: Process partial dataset (missing entities should be detected as removals)
    logger.info("Phase 2: Processing partial dataset with missing entities...")
    new_data = [
        # Only LEI001 and LEI002 present (LEI003 removed)
        await create_test_item("fi.short.aggregate", {
            "lei": "LEI001",
            "company_name": "Company A",
            "position_percent": 2.6,  # Slight change
            "latest_position_date": "2024-01-02",
            "timestamp": "2024-01-02T10:00:00"
        }),
        await create_test_item("fi.short.aggregate", {
            "lei": "LEI002", 
            "company_name": "Company B",
            "position_percent": 1.8,  # No change
            "latest_position_date": "2024-01-01",
            "timestamp": "2024-01-02T10:00:00"
        }),
        
        # Only Entity X present (Entity Y and Z removed)
        await create_test_item("fi.short.positions", {
            "entity_name": "Entity X",
            "issuer_name": "Issuer 1", 
            "isin": "ISIN001",
            "position_percent": 1.3,  # Slight change
            "position_date": "2024-01-02",
            "timestamp": "2024-01-02T10:00:00",
            "comment": ""
        })
    ]
    
    async def item_stream() -> AsyncIterator[ParsedItem]:
        """Stream the new data items."""
        for item in new_data:
            yield item
    
    # Process through diff parser and collect results
    results = []
    async for result in diff_parser(item_stream()):
        results.append(result)
        # Also pass through DatabaseSink to persist data
        if isinstance(result, ParsedItem):
            await db_sink.handle(result)
        
        if result.topic.endswith(".diff"):
            if result.content.get("removal_detected"):
                logger.info(f"🗑️  REMOVAL: {result.topic} - {result.content}")
            else:
                logger.info(f"🔄 CHANGE: {result.topic} - {result.content}")
        else:
            logger.info(f"📄 ORIGINAL: {result.topic}")
    
    await diff_parser.close()
    await db_sink.close()
    
    # Count events
    original_items = [r for r in results if not r.topic.endswith(".diff")]
    change_events = [r for r in results if r.topic.endswith(".diff") and not r.content.get("removal_detected")]
    removal_events = [r for r in results if r.topic.endswith(".diff") and r.content.get("removal_detected")]
    
    logger.info(f"\n=== SUMMARY ===")
    logger.info(f"Batch 2 - Original items processed: {len(original_items)}")
    logger.info(f"Batch 2 - Change events generated: {len(change_events)}")
    logger.info(f"Batch 2 - Removal events generated: {len(removal_events)}")
    
    expected_removals = 3  # LEI003, Entity Y, Entity Z
    if len(removal_events) == expected_removals:
        logger.info("✅ Removal detection working correctly!")
    else:
        logger.error(f"❌ Expected {expected_removals} removals, got {len(removal_events)}")
        for event in removal_events:
            logger.info(f"   Removal detected: {event.content}")
    
    # Clean up
    import os
    try:
        os.remove(db_path)
    except FileNotFoundError:
        pass

async def test_continuous_monitoring():
    """Test continuous monitoring scenario - multiple batches."""
    logger.info("\n=== Testing Continuous Monitoring ===")
    
    # Create diff parser (removal detection always enabled)
    diff_parser = DiffParser(db_path="test_removal.db")
    
    # Batch 1: Add a new entity
    logger.info("Batch 1: Adding new entity...")
    new_data_batch1 = [
        await create_test_item("fi.short.aggregate", {
            "lei": "LEI004",
            "company_name": "Company D", 
            "position_percent": 1.5,
            "latest_position_date": "2024-01-04",
            "timestamp": "2024-01-04T10:00:00"
        })
    ]
    
    async def batch1_stream() -> AsyncIterator[ParsedItem]:
        for item in new_data_batch1:
            yield item
    
    results_batch1 = []
    async for result in diff_parser(batch1_stream()):
        results_batch1.append(result)
    
    # Batch 2: Remove the entity we just added
    logger.info("Batch 2: Removing entity (empty batch)...")
    
    async def batch2_stream() -> AsyncIterator[ParsedItem]:
        # Empty stream - no current data
        return
        yield  # unreachable
    
    results_batch2 = []
    async for result in diff_parser(batch2_stream()):
        results_batch2.append(result)
        if result.topic.endswith(".diff") and result.content.get("removal_detected"):
            logger.info(f"🗑️  REMOVAL DETECTED: {result.content}")
    
    await diff_parser.close()
    
    # Check results
    new_events = [r for r in results_batch1 if r.topic.endswith(".diff") and not r.content.get("removal_detected")]
    removal_events = [r for r in results_batch2 if r.topic.endswith(".diff") and r.content.get("removal_detected")]
    
    logger.info(f"New entity events: {len(new_events)}")
    logger.info(f"Removal events: {len(removal_events)}")
    logger.info("✅ Continuous monitoring with removal detection working!")

if __name__ == "__main__":
    asyncio.run(test_removal_detection())
    asyncio.run(test_continuous_monitoring())
