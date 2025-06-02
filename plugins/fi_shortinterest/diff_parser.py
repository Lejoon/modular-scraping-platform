"""
Diff parser for detecting changes in scraped data.
"""

import logging
from typing import List, Dict, Any, AsyncIterator

from core.interfaces import Transform
from core.models import ParsedItem
from core.infra.db import Database

logger = logging.getLogger(__name__)

class DiffParser(Transform):
    """Parser that compares ParsedItems against the last saved state and emits only changes."""
    
    name = "DiffParser"

    def __init__(self, **kwargs):
        # file‐backed DB is still used
        self.db = Database(kwargs.get("db_path", "scraper.db"))
        self._initialized = False

    async def _ensure_initialized(self):
        """Ensure database connection is initialized."""
        if not self._initialized:
            await self.db.connect()
            self._initialized = True

    async def parse(self, item: ParsedItem) -> List[ParsedItem]:
        """
        Entry point for core framework: receives ParsedItem from upstream parsers,
        compares to DB state, and returns either:
          - [ParsedItem(topic="fi.short.aggregate.diff", …)] or
          - [ParsedItem(topic="fi.short.positions.diff", …)]
        when there’s a change, or
          - [] if nothing changed, or
          - [item] for topics we don’t handle.
        """
        await self._ensure_initialized()
        
        if item.topic == "fi.short.aggregate":
            return await self._diff_aggregate(item)
        elif item.topic == "fi.short.positions":
            return await self._diff_positions(item)
        else:
            # unknown topics just pass through
            return [item]

    async def _diff_aggregate(self, item: ParsedItem) -> List[ParsedItem]:
        """Diff aggregate short interest data against DB."""
        lei = item.content.get("lei")
        if not lei:
            return []

        previous = await self.db.fetch_one(
            "SELECT position_percent, latest_position_date FROM short_positions WHERE lei = ?",
            (lei,)
        )
        current_percent = float(item.content.get("position_percent", 0))
        current_date = item.content.get("latest_position_date", "")

        # brand new
        if not previous:
            logger.info(f"New aggregate position detected: {lei}")
            diff_content = item.content.copy()
            diff_content.update({
                "event_timestamp": item.discovered_at.isoformat(),
                "old_pct": 0.0,
                "new_pct": current_percent,
            })
            return [ParsedItem(
                topic="fi.short.aggregate.diff",
                content=diff_content,
                discovered_at=item.discovered_at
            )]

        prev_percent = float(previous["position_percent"])
        prev_date = previous["latest_position_date"] or ""

        if (abs(current_percent - prev_percent) > 0.001 or
            current_date != prev_date):
            logger.info(f"Aggregate position changed for {lei}: {prev_percent:.3f}% -> {current_percent:.3f}%")
            diff_content = item.content.copy()
            diff_content.update({
                "event_timestamp": item.discovered_at.isoformat(),
                "old_pct": prev_percent,
                "new_pct": current_percent,
                "previous_percent": prev_percent,  # Keep for backward compatibility
                "percent_change": current_percent - prev_percent,
                "previous_date": prev_date
            })
            return [ParsedItem(
                topic="fi.short.aggregate.diff",
                content=diff_content,
                discovered_at=item.discovered_at
            )]

        return []

    async def _diff_positions(self, item: ParsedItem) -> List[ParsedItem]:
        """Diff individual position data against DB."""
        entity_name = item.content.get("entity_name", "")
        issuer_name = item.content.get("issuer_name", "")
        isin = item.content.get("isin", "")
        if not all([entity_name, issuer_name, isin]):
            return []

        previous = await self.db.fetch_one(
            """SELECT position_percent, position_date 
               FROM position_holders 
               WHERE entity_name = ? AND issuer_name = ? AND isin = ?""",
            (entity_name, issuer_name, isin)
        )
        current_percent = float(item.content.get("position_percent", 0))
        current_date = item.content.get("position_date", "")

        # brand new
        if not previous:
            logger.info(f"New position detected: {entity_name} -> {issuer_name}")
            diff_content = item.content.copy()
            diff_content.update({
                "event_timestamp": item.discovered_at.isoformat(),
                "old_pct": 0.0,
                "new_pct": current_percent,
            })
            return [ParsedItem(
                topic="fi.short.positions.diff",
                content=diff_content,
                discovered_at=item.discovered_at
            )]

        prev_percent = float(previous["position_percent"])
        prev_date = previous["position_date"] or ""

        if (abs(current_percent - prev_percent) > 0.001 or
            current_date != prev_date):
            logger.info(f"Position changed for {entity_name} -> {issuer_name}: {prev_percent:.3f}% -> {current_percent:.3f}%")
            diff_content = item.content.copy()
            diff_content.update({
                "event_timestamp": item.discovered_at.isoformat(),
                "old_pct": prev_percent,
                "new_pct": current_percent,
                "previous_percent": prev_percent,  # Keep for backward compatibility
                "percent_change": current_percent - prev_percent,
                "previous_date": prev_date
            })
            return [ParsedItem(
                topic="fi.short.positions.diff",
                content=diff_content,
                discovered_at=item.discovered_at
            )]

        return []

    async def close(self):
        """Close database connection."""
        if self._initialized:
            await self.db.close()

    async def __call__(self, items: AsyncIterator[Any]) -> AsyncIterator[ParsedItem]:
        """Transform interface: parse ParsedItems and emit diff results."""
        async for item in items:
            if isinstance(item, ParsedItem):
                diff_items = await self.parse(item)
                for diff_item in diff_items:
                    yield diff_item