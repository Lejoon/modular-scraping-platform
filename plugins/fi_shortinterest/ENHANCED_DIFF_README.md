# Enhanced Diff Detection: Handling Position Removals

## Problem Statement

The original `DiffParser` in the FI Short Interest pipeline only detects:
- ✅ **New positions**: When an entity appears for the first time (0.0% → current%)
- ✅ **Position changes**: When an existing entity's position changes (previous% → current%)
- ❌ **Position removals**: When an entity disappears from the data feed (previous% → 0.0%)

This limitation exists because the original diff parser only processes entities present in the incoming data stream.

## Solution: Enhanced Batch Reconciliation

The `EnhancedDiffParser` maintains the existing Transform architecture while adding **batch reconciliation** to detect removals.

### How It Works

#### Phase 1: Stream Processing (Existing Logic)
```python
async for item in items:
    # Track which entities we've seen in this batch
    if item.topic == "fi.short.aggregate":
        self._current_batch_aggregates.add(lei)
    elif item.topic == "fi.short.positions":
        self._current_batch_positions.add((entity_name, issuer_name, isin))
    
    # Normal diff processing
    yield item
    diff_items = await self.parse(item)
    for diff_item in diff_items:
        yield diff_item
```

#### Phase 2: Batch Reconciliation (NEW)
```python
# After processing all items, check for removals
removal_events = await self._detect_removals()
for removal_event in removal_events:
    yield removal_event
```

### Architecture Compatibility

This solution fits perfectly within the Transform-based architecture:

1. **Maintains streaming**: Still processes items one-by-one during the stream
2. **Preserves async iterators**: Uses the existing `AsyncIterator` pattern
3. **Follows Transform interface**: Implements `async def __call__(items: AsyncIterator[Any]) -> AsyncIterator[ParsedItem]`
4. **Resource management**: Uses existing database connection patterns
5. **Zero registration**: Auto-discovered like other plugins

### Key Features

#### 1. Removal Detection
```python
# Example: Entity "BlackRock Inc" was holding 2.3% of AAPL but is no longer in the data
removal_event = ParsedItem(
    topic="fi.short.positions.diff",
    content={
        "entity_name": "BlackRock Inc",
        "issuer_name": "Apple Inc",
        "isin": "US0378331005",
        "old_pct": 2.3,
        "new_pct": 0.0,
        "percent_change": -2.3,
        "removal_detected": True  # Flag indicating this is a removal
    }
)
```

#### 2. Batch Tracking
- Tracks all LEIs and positions processed in current batch
- Compares against database to find missing entities
- Generates removal events with proper diff format

#### 3. Enhanced Logging
```
INFO: Position removed: BlackRock Inc -> Apple Inc (was 2.300%)
INFO: Aggregate position removed: SE0000123456 (was 1.500%)
INFO: Batch complete: 15 diff events generated (3 changes, 2 removals)
```

### Database Impact

The enhanced diff parser generates events that flow to the same database tables:

#### History Tables Get Removal Events
```sql
-- position_holders_history table
INSERT INTO position_holders_history (
    entity_name, issuer_name, isin, event_timestamp,
    old_pct, new_pct, removal_detected
) VALUES (
    'BlackRock Inc', 'Apple Inc', 'US0378331005', '2025-06-04T10:30:00',
    2.3, 0.0, 1
);
```

#### Main Tables Updated by DatabaseSink
The DatabaseSink can be enhanced to:
- Delete entries when `new_pct = 0.0` and `removal_detected = True`
- Or update them with `position_percent = 0.0` for audit trail

### Pipeline Configuration

```yaml
# Enhanced pipeline with removal detection
fi_shortinterest_pos_enhanced:
  chain:
    - class: fi_shortinterest.FiFetcher      # Fetch ODS files
    - class: fi_shortinterest.FiActParser    # Parse positions  
    - class: fi_shortinterest.EnhancedDiffParser  # Detect changes AND removals
      kwargs:
        db_path: "fi_shortinterest.db"
    - class: fi_shortinterest.DatabaseSink   # Persist to database
      kwargs:
        db_path: "fi_shortinterest.db"
```

### Benefits

1. **Complete change detection**: Captures all three types of position changes
2. **Maintains architecture**: Fits cleanly within Transform pattern
3. **Backward compatible**: Original DiffParser still works unchanged
4. **Auditable**: All removal events are logged and stored
5. **Configurable**: Can be enabled/disabled per pipeline
6. **Performance efficient**: Batch reconciliation happens once per run

### Usage Examples

#### Before Enhancement
```
# Only detects when positions change or appear
INFO: New position detected: Goldman Sachs -> Tesla Inc
INFO: Position changed for Morgan Stanley -> Apple Inc: 1.250% -> 1.445%
# Missing: BlackRock position dropped from 2.3% to 0.0% (not detected)
```

#### After Enhancement
```
# Detects all three types of changes
INFO: New position detected: Goldman Sachs -> Tesla Inc  
INFO: Position changed for Morgan Stanley -> Apple Inc: 1.250% -> 1.445%
INFO: Position removed: BlackRock Inc -> Apple Inc (was 2.300%)  # NEW!
INFO: Batch complete: 3 diff events generated
```

### Implementation Notes

The solution leverages the Transform architecture's batch nature:
- Each pipeline run processes a complete snapshot of data
- The `__call__` method receives the entire stream of items
- Batch reconciliation happens after stream processing completes
- All events flow through the same downstream components (DatabaseSink, Discord notifications, etc.)

This approach is both architecturally sound and provides complete position change detection for the FI Short Interest monitoring system.
