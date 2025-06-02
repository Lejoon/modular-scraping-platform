"""
Database infrastructure with SQLite and async support.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite


logger = logging.getLogger(__name__)


class Database:
    """Async SQLite database wrapper."""
    
    def __init__(self, db_path: str = "scraper.db"):
        # Handle SQLite URL format if provided
        if db_path.startswith("sqlite"):
            # Handle sqlite+aiosqlite:///path format
            if "///" in db_path:
                actual_path = db_path.split("///")[-1]
            else:
                actual_path = db_path.split("//")[-1]
            self.db_path = Path(actual_path)
        else:
            self.db_path = Path(db_path)
        self._connection: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        """Connect to the database and run migrations."""
        if self._connection:
            return
        
        # Open connection with a longer busy timeout
        self._connection = await aiosqlite.connect(self.db_path, timeout=30)
        self._connection.row_factory = aiosqlite.Row
        # Improve concurrency: use WAL journal mode and set busy timeout (ms)
        await self._connection.execute("PRAGMA journal_mode=WAL;")
        await self._connection.execute("PRAGMA busy_timeout=30000;")
        await self._run_migrations()

    async def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None

    @asynccontextmanager
    async def transaction(self):
        """Context manager for database transactions."""
        if not self._connection:
            await self.connect()
        
        try:
            await self._connection.execute("BEGIN")
            yield self._connection
            await self._connection.commit()
        except Exception:
            await self._connection.rollback()
            raise

    async def execute(self, sql: str, params: Tuple[Any, ...] = ()) -> aiosqlite.Cursor:
        """Execute a SQL statement."""
        if not self._connection:
            await self.connect()
        return await self._connection.execute(sql, params)

    async def fetch_one(self, sql: str, params: Tuple[Any, ...] = ()) -> Optional[aiosqlite.Row]:
        """Fetch one row."""
        cursor = await self.execute(sql, params)
        return await cursor.fetchone()

    async def fetch_all(self, sql: str, params: Tuple[Any, ...] = ()) -> List[aiosqlite.Row]:
        """Fetch all rows."""
        cursor = await self.execute(sql, params)
        return await cursor.fetchall()

    async def upsert(
        self,
        table: str,
        data: Dict[str, Any],
        pk_columns: List[str],
    ) -> None:
        """Upsert data into a table."""
        columns = list(data.keys())
        placeholders = ", ".join("?" * len(columns))
        values = list(data.values())
        
        # Build the conflict resolution clause
        update_columns = [col for col in columns if col not in pk_columns]
        if update_columns:
            update_clause = ", ".join(f"{col} = excluded.{col}" for col in update_columns)
            conflict_clause = f"ON CONFLICT({', '.join(pk_columns)}) DO UPDATE SET {update_clause}"
        else:
            conflict_clause = f"ON CONFLICT({', '.join(pk_columns)}) DO NOTHING"
        
        sql = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({placeholders})
            {conflict_clause}
        """
        
        # Execute and immediately commit to persist data
        await self.execute(sql, tuple(values))
        await self._connection.commit()

    async def _run_migrations(self) -> None:
        """Run database migrations."""
        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS migrations (
                version INTEGER PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # No hardcoded migrations - plugins should handle their own table creation
        # This ensures the core Database class is plugin-agnostic
