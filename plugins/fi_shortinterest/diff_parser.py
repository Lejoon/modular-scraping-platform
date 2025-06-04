
import logging
from datetime import datetime
from typing import List, Dict, Any, AsyncIterator, Set

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
        self._seen_keys: Set[tuple] = set()  # Track keys seen in current batch

    async def _ensure_initialized(self):
        """Ensure database connection is initialized."""
        if not self._initialized:
            await self.db.connect()
            self._initialized = True

    async def parse(self, item: ParsedItem) -> List[ParsedItem]:
        """
        Entry point for core framework: receives ParsedItem from upstream parsers,
        compares to DB state, and returns:
          - [original_item, diff_item] when there's a change
          - [original_item] when no change detected but item should be stored
          - [item] for topics we don't handle.
        """
        await self._ensure_initialized()
        
        if item.topic == "fi.short.aggregate":
            # Track this key as seen
            lei = item.content.get("lei")
            if lei:
                self._seen_keys.add(("aggregate", lei))
            
            diff_items = await self._diff_aggregate(item)
            # Always return the original item plus any diff items
            return [item] + diff_items
        elif item.topic == "fi.short.positions":
            # Track this key as seen
            entity_name = item.content.get("entity_name", "")
            issuer_name = item.content.get("issuer_name", "")
            isin = item.content.get("isin", "")
            if all([entity_name, issuer_name, isin]):
                self._seen_keys.add(("positions", entity_name, issuer_name, isin))
            
            diff_items = await self._diff_positions(item)
            # Always return the original item plus any diff items
            return [item] + diff_items
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
        # Reset seen keys for this batch
        self._seen_keys.clear()
        
        # Normal per-row diff processing
        async for item in items:
            if isinstance(item, ParsedItem):
                diff_items = await self.parse(item)
                for diff_item in diff_items:
                    yield diff_item
        
        # Removal detection at end of batch
        async for removal_item in self._emit_removals():
            yield removal_item

    async def _emit_removals(self) -> AsyncIterator[ParsedItem]:
        """Emit diff events for entities that were in DB but not in current batch."""
        await self._ensure_initialized()
        
        # Determine which data types were processed in this batch
        processed_aggregates = any(key[0] == "aggregate" for key in self._seen_keys)
        processed_positions = any(key[0] == "positions" for key in self._seen_keys)
        
        # Only check for removed aggregate positions if we processed aggregate data in this batch
        if processed_aggregates:
            agg_rows = await self.db.fetch_all(
                "SELECT lei, company_name, position_percent, latest_position_date FROM short_positions"
            )
            
            for row in agg_rows:
                lei = row["lei"]
                if ("aggregate", lei) not in self._seen_keys:
                    logger.info(f"Aggregate position removed: {lei} (was {row['position_percent']:.3f}%)")
                    yield ParsedItem(
                        topic="fi.short.aggregate.diff",
                        content={
                            "lei": lei,
                            "company_name": row["company_name"],
                            "position_percent": 0.0,
                            "latest_position_date": row["latest_position_date"],
                            "event_timestamp": datetime.utcnow().isoformat(),
                            "old_pct": float(row["position_percent"]),
                            "new_pct": 0.0,
                            "previous_percent": float(row["position_percent"]),
                            "percent_change": -float(row["position_percent"]),
                            "removal_detected": True
                        },
                        discovered_at=datetime.utcnow()
                    )
        
        # Only check for removed individual positions if we processed position data in this batch  
        if processed_positions:
            pos_rows = await self.db.fetch_all(
                "SELECT entity_name, issuer_name, isin, position_percent, position_date, comment FROM position_holders"
            )
            
            for row in pos_rows:
                key = ("positions", row["entity_name"], row["issuer_name"], row["isin"])
                if key not in self._seen_keys:
                    logger.info(f"Position removed: {row['entity_name']} -> {row['issuer_name']} (was {row['position_percent']:.3f}%)")
                    yield ParsedItem(
                        topic="fi.short.positions.diff",
                        content={
                            "entity_name": row["entity_name"],
                            "issuer_name": row["issuer_name"],
                            "isin": row["isin"],
                            "position_percent": 0.0,
                            "position_date": row["position_date"],
                            "comment": row["comment"] if "comment" in row.keys() else "",
                            "event_timestamp": datetime.utcnow().isoformat(),
                            "old_pct": float(row["position_percent"]),
                            "new_pct": 0.0,
                            "previous_percent": float(row["position_percent"]),
                            "percent_change": -float(row["position_percent"]),
                            "removal_detected": True
                        },
                        discovered_at=datetime.utcnow()
                    )