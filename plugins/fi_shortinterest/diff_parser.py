"""
Diff parser for detecting changes in scraped data.
"""

import logging
from typing import List, Dict, Any

from core.interfaces import Parser
from core.models import RawItem, ParsedItem
from core.infra.db import Database


logger = logging.getLogger(__name__)


class DiffParser(Parser):
    """Parser that compares current data with previous state and emits only changes."""
    
    name = "DiffParser"

    def __init__(self, **kwargs):
        self.db = Database(kwargs.get("db_path", "scraper.db"))
        self._initialized = False

    async def _ensure_initialized(self):
        """Ensure database connection is initialized."""
        if not self._initialized:
            await self.db.connect()
            self._initialized = True

    async def parse(self, raw: RawItem) -> List[ParsedItem]:
        """
        Parse raw items by comparing with previous state in database.
        Only emits items that have changed since last time.
        """
        await self._ensure_initialized()
        
        # This parser doesn't directly parse raw items,
        # it processes already parsed items through diff()
        return []

    async def diff(self, parsed_item: ParsedItem) -> List[ParsedItem]:
        """
        Compare parsed item with previous state and return diff if changed.
        """
        await self._ensure_initialized()
        
        if parsed_item.topic == "fi.short.aggregate":
            return await self._diff_aggregate(parsed_item)
        elif parsed_item.topic == "fi.short.positions":
            return await self._diff_positions(parsed_item)
        else:
            # For unknown topics, just pass through
            return [parsed_item]

    async def _diff_aggregate(self, item: ParsedItem) -> List[ParsedItem]:
        """Diff aggregate short interest data."""
        lei = item.content.get("lei")
        if not lei:
            return []

        # Get previous data from database
        previous = await self.db.fetch_one(
            "SELECT position_percent, latest_position_date FROM short_positions WHERE lei = ?",
            (lei,)
        )

        current_percent = float(item.content.get("position_percent", 0))
        current_date = item.content.get("latest_position_date", "")

        # Check if this is new or changed
        if not previous:
            # New company, emit diff
            logger.info(f"New aggregate position detected: {lei}")
            return [item.copy(update={"topic": "fi.short.aggregate.diff"})]
        
        prev_percent = float(previous["position_percent"])
        prev_date = previous["latest_position_date"] or ""
        
        # Check for changes in percentage or date
        if (abs(current_percent - prev_percent) > 0.001 or 
            current_date != prev_date):
            logger.info(f"Aggregate position changed for {lei}: {prev_percent:.3f}% -> {current_percent:.3f}%")
            
            # Add change information to content
            diff_content = item.content.copy()
            diff_content.update({
                "previous_percent": prev_percent,
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
        """Diff individual position data."""
        entity_name = item.content.get("entity_name", "")
        issuer_name = item.content.get("issuer_name", "")
        isin = item.content.get("isin", "")
        
        if not all([entity_name, issuer_name, isin]):
            return []

        # Get previous data from database
        previous = await self.db.fetch_one(
            """SELECT position_percent, position_date 
               FROM position_holders 
               WHERE entity_name = ? AND issuer_name = ? AND isin = ?""",
            (entity_name, issuer_name, isin)
        )

        current_percent = float(item.content.get("position_percent", 0))
        current_date = item.content.get("position_date", "")

        # Check if this is new or changed
        if not previous:
            # New position, emit diff
            logger.info(f"New position detected: {entity_name} -> {issuer_name}")
            return [item.copy(update={"topic": "fi.short.positions.diff"})]
        
        prev_percent = float(previous["position_percent"])
        prev_date = previous["position_date"] or ""
        
        # Check for changes in percentage or date
        if (abs(current_percent - prev_percent) > 0.001 or 
            current_date != prev_date):
            logger.info(f"Position changed for {entity_name} -> {issuer_name}: {prev_percent:.3f}% -> {current_percent:.3f}%")
            
            # Add change information to content
            diff_content = item.content.copy()
            diff_content.update({
                "previous_percent": prev_percent,
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
