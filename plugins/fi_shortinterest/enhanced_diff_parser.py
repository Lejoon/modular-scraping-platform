"""
Enhanced diff parser that detects both changes and removals (positions dropping to 0).
"""

import logging
from typing import List, Dict, Any, AsyncIterator, Set
from datetime import datetime

from core.interfaces import Transform
from core.models import ParsedItem
from core.infra.db import Database

logger = logging.getLogger(__name__)

class EnhancedDiffParser(Transform):
    """
    Enhanced parser that detects:
    1. New positions (0.0% -> current%)
    2. Position changes (previous% -> current%)
    3. Position removals (previous% -> 0.0%) - NEW CAPABILITY
    
    Works by tracking processed entities in each batch and comparing
    against all entities currently in the database.
    """
    
    name = "EnhancedDiffParser"

    def __init__(self, **kwargs):
        self.db = Database(kwargs.get("db_path", "scraper.db"))
        self._initialized = False
        
        # Track entities processed in current batch
        self._current_batch_aggregates: Set[str] = set()
        self._current_batch_positions: Set[tuple] = set()
        self._batch_timestamp: datetime = None

    async def _ensure_initialized(self):
        """Ensure database connection is initialized."""
        if not self._initialized:
            await self.db.connect()
            self._initialized = True

    async def __call__(self, items: AsyncIterator[Any]) -> AsyncIterator[ParsedItem]:
        """
        Transform interface: Enhanced to handle batch reconciliation.
        
        Process flow:
        1. Reset batch tracking
        2. Process all items in stream (normal diff logic)
        3. Perform reconciliation to detect removals
        4. Yield all diff events
        """
        await self._ensure_initialized()
        
        # Reset batch tracking
        self._current_batch_aggregates.clear()
        self._current_batch_positions.clear()
        self._batch_timestamp = datetime.utcnow()
        
        # Collect all diff events during streaming
        diff_events: List[ParsedItem] = []
        
        # Phase 1: Stream processing (existing logic)
        async for item in items:
            if isinstance(item, ParsedItem):
                # Always yield the original item
                yield item
                
                # Process diffs and collect events
                item_diffs = await self.parse(item)
                for diff_item in item_diffs[1:]:  # Skip original item
                    diff_events.append(diff_item)
                    yield diff_item
        
        # Phase 2: Batch reconciliation (NEW)
        removal_events = await self._detect_removals()
        for removal_event in removal_events:
            diff_events.append(removal_event)
            yield removal_event
        
        logger.info(f"Batch complete: {len(diff_events)} diff events generated")

    async def parse(self, item: ParsedItem) -> List[ParsedItem]:
        """
        Enhanced parse method that tracks processed entities.
        Returns [original_item] + diff_items
        """
        if item.topic == "fi.short.aggregate":
            # Track this LEI as processed
            lei = item.content.get("lei")
            if lei:
                self._current_batch_aggregates.add(lei)
            
            diff_items = await self._diff_aggregate(item)
            return [item] + diff_items
            
        elif item.topic == "fi.short.positions":
            # Track this position as processed
            entity_name = item.content.get("entity_name", "")
            issuer_name = item.content.get("issuer_name", "")
            isin = item.content.get("isin", "")
            if all([entity_name, issuer_name, isin]):
                self._current_batch_positions.add((entity_name, issuer_name, isin))
            
            diff_items = await self._diff_positions(item)
            return [item] + diff_items
        else:
            return [item]

    async def _detect_removals(self) -> List[ParsedItem]:
        """
        NEW: Detect entities that were in database but missing from current batch.
        This indicates positions have dropped to 0.0%.
        """
        removal_events: List[ParsedItem] = []
        
        # Check for removed aggregate positions
        removed_aggregates = await self._find_removed_aggregates()
        for lei, prev_data in removed_aggregates:
            logger.info(f"Aggregate position removed: {lei} (was {prev_data['position_percent']:.3f}%)")
            
            removal_event = ParsedItem(
                topic="fi.short.aggregate.diff",
                content={
                    "lei": lei,
                    "company_name": prev_data["company_name"],
                    "position_percent": 0.0,
                    "latest_position_date": prev_data["latest_position_date"],
                    "event_timestamp": self._batch_timestamp.isoformat(),
                    "old_pct": float(prev_data["position_percent"]),
                    "new_pct": 0.0,
                    "previous_percent": float(prev_data["position_percent"]),
                    "percent_change": -float(prev_data["position_percent"]),
                    "removal_detected": True  # Flag to indicate this is a removal
                },
                discovered_at=self._batch_timestamp
            )
            removal_events.append(removal_event)
        
        # Check for removed individual positions
        removed_positions = await self._find_removed_positions()
        for (entity_name, issuer_name, isin), prev_data in removed_positions:
            logger.info(f"Position removed: {entity_name} -> {issuer_name} (was {prev_data['position_percent']:.3f}%)")
            
            removal_event = ParsedItem(
                topic="fi.short.positions.diff",
                content={
                    "entity_name": entity_name,
                    "issuer_name": issuer_name,
                    "isin": isin,
                    "position_percent": 0.0,
                    "position_date": prev_data["position_date"],
                    "comment": prev_data.get("comment", ""),
                    "event_timestamp": self._batch_timestamp.isoformat(),
                    "old_pct": float(prev_data["position_percent"]),
                    "new_pct": 0.0,
                    "previous_percent": float(prev_data["position_percent"]),
                    "percent_change": -float(prev_data["position_percent"]),
                    "removal_detected": True  # Flag to indicate this is a removal
                },
                discovered_at=self._batch_timestamp
            )
            removal_events.append(removal_event)
        
        return removal_events

    async def _find_removed_aggregates(self) -> List[tuple]:
        """Find LEIs in database but not in current batch."""
        db_leis = await self.db.fetch_all(
            "SELECT lei, company_name, position_percent, latest_position_date FROM short_positions"
        )
        
        removed = []
        for row in db_leis:
            lei = row["lei"]
            if lei not in self._current_batch_aggregates:
                removed.append((lei, dict(row)))
        
        return removed

    async def _find_removed_positions(self) -> List[tuple]:
        """Find positions in database but not in current batch."""
        db_positions = await self.db.fetch_all(
            """SELECT entity_name, issuer_name, isin, position_percent, position_date, comment 
               FROM position_holders"""
        )
        
        removed = []
        for row in db_positions:
            key = (row["entity_name"], row["issuer_name"], row["isin"])
            if key not in self._current_batch_positions:
                removed.append((key, dict(row)))
        
        return removed

    # Existing diff methods unchanged
    async def _diff_aggregate(self, item: ParsedItem) -> List[ParsedItem]:
        """Diff aggregate short interest data against DB (unchanged)."""
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
        """Diff individual position data against DB (unchanged)."""
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
