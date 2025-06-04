#!/usr/bin/env python3
"""
Test script to verify the deletion fix works.
"""

import asyncio
from plugins.fi_shortinterest.sinks import DatabaseSink
from core.models import ParsedItem
from datetime import datetime

async def test_deletion():
    # Create test item for removal
    removal_item = ParsedItem(
        topic='fi.short.aggregate.diff',
        content={
            'lei': '549300S5CCFESE4C6Y07',
            'company_name': 'Mycronic AB (publ)',
            'position_percent': 0.18,
            'latest_position_date': '2024-01-01',
            'event_timestamp': '2025-06-04T13:00:00',
            'old_pct': 0.18,
            'new_pct': 0.0,
            'removal_detected': True
        },
        discovered_at=datetime.now()
    )
    
    # Test the fix
    sink = DatabaseSink(db_path='fi_shortinterest.db')
    await sink.db.connect()
    await sink._create_tables()
    
    print('Before deletion:')
    result = await sink.db.fetch_one('SELECT COUNT(*) as count FROM short_positions WHERE lei = ?', ('549300S5CCFESE4C6Y07',))
    print(f'Records in main table: {result["count"]}')
    
    # Handle the removal
    await sink.handle(removal_item)
    
    print('\nAfter deletion:')
    result = await sink.db.fetch_one('SELECT COUNT(*) as count FROM short_positions WHERE lei = ?', ('549300S5CCFESE4C6Y07',))
    print(f'Records in main table: {result["count"]}')
    
    await sink.close()

if __name__ == "__main__":
    asyncio.run(test_deletion())
