"""
Database sink that implements the Transform interface.
"""

import logging
from typing import Dict, List, AsyncIterator, Any

from core.interfaces import Sink
from core.models import ParsedItem
from core.infra.db import Database


logger = logging.getLogger(__name__)


class DatabaseSink(Sink):
    """Sink that persists data to SQLite database."""
    
    name = "DatabaseSink"
    
    _TABLE_MAP = {
        "fi.short.aggregate": {
            "table": "short_positions",
            "pk": ["lei"],
            "cols": [
                "lei",
                "company_name",
                "position_percent",
                "latest_position_date",
                "timestamp",
            ],
        },
        "fi.short.aggregate.diff": {
            "table": "short_positions_history",
            "pk": ["lei", "event_timestamp"],
            "cols": [
                "lei",
                "company_name", 
                "position_percent",
                "latest_position_date",
                "event_timestamp",
                "old_pct",
                "new_pct",
            ],
        },
        "fi.short.positions": {
            "table": "position_holders",
            "pk": ["entity_name", "issuer_name", "isin"],
            "cols": [
                "entity_name",
                "issuer_name",
                "isin",
                "position_percent",
                "position_date",
                "timestamp",
                "comment",
            ],
        },
        "fi.short.positions.diff": {
            "table": "position_holders_history", 
            "pk": ["entity_name", "issuer_name", "isin", "event_timestamp"],
            "cols": [
                "entity_name",
                "issuer_name",
                "isin",
                "position_percent",
                "position_date",
                "event_timestamp",
                "old_pct",
                "new_pct",
            ],
        },
    }

    def __init__(self, db_path: str = "scraper.db", **kwargs):
        """Initialize DatabaseSink with configurable database path."""
        self.db = Database(db_path)

    async def _create_tables(self) -> None:
        """Create FI short interest tables."""
        # Create main tables
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS short_positions (
                lei TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                position_percent REAL NOT NULL,
                latest_position_date TEXT,
                timestamp TEXT NOT NULL
            )
        """)
        
        await self.db.execute("""
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
        
        # Create history tables
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS short_positions_history (
                lei TEXT NOT NULL,
                company_name TEXT NOT NULL,
                position_percent REAL NOT NULL,
                latest_position_date TEXT,
                event_timestamp TEXT NOT NULL,
                old_pct REAL,
                new_pct REAL,
                PRIMARY KEY (lei, event_timestamp)
            )
        """)
        
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS position_holders_history (
                entity_name TEXT NOT NULL,
                issuer_name TEXT NOT NULL,
                isin TEXT NOT NULL,
                position_percent REAL NOT NULL,
                position_date TEXT,
                event_timestamp TEXT NOT NULL,
                comment TEXT,
                old_pct REAL,
                new_pct REAL,
                PRIMARY KEY (entity_name, issuer_name, isin, event_timestamp)
            )
        """)
        
        # Create indexes for better performance
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_short_positions_company 
            ON short_positions(company_name)
        """)
        
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_position_holders_issuer 
            ON position_holders(issuer_name)
        """)
        
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_position_holders_timestamp 
            ON position_holders(timestamp)
        """)

    async def handle(self, item: ParsedItem) -> None:
        """Handle a parsed item by persisting to database."""
        if item.topic not in self._TABLE_MAP:
            logger.debug(f"No table mapping for topic: {item.topic}")
            return
        
        config = self._TABLE_MAP[item.topic]
        table = config["table"]
        pk_columns = config["pk"]
        columns = config["cols"]
        
        # Extract data for the configured columns
        data = {}
        for col in columns:
            value = item.content.get(col)
            if value is not None:
                data[col] = value
        
        if not data:
            logger.warning(f"No data to insert for topic: {item.topic}")
            return
        
        try:
            await self.db.upsert(table, data, pk_columns)
            logger.info(f"Upserted data to {table}: columns={list(data.keys())}")
        except Exception as e:
            logger.error(f"Failed to upsert data to {table}: {e}")

    async def close(self) -> None:
        """Close the database connection."""
        await self.db.close()

    async def __call__(self, items: AsyncIterator[Any]) -> AsyncIterator[None]:
        """Transform interface: handle ParsedItems and pass them through."""
        async for item in items:
            if isinstance(item, ParsedItem):
                await self.handle(item)
            # Sinks typically don't yield anything, but we yield None to complete the chain
            yield None

    async def __aenter__(self):
        """Async context manager entry."""
        await self.db.connect()
        await self._create_tables()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
