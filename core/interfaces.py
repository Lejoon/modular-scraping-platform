"""
Core interfaces for the scraper platform.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator, List, Any

from .models import RawItem, ParsedItem


class Transform(ABC):
    """Universal transform interface for plugin pipeline stages.
    
    This is the core abstraction that enables plugin chaining.
    Any stage in a pipeline implements this interface.
    """
    
    @abstractmethod
    async def __call__(
        self, items: AsyncIterator[Any]
    ) -> AsyncIterator[Any]:
        """Transform an async iterator of items to another async iterator."""
        ...


class Fetcher(Transform):
    """Abstract base class for data fetchers.
    
    Fetchers are specialized transforms that typically ignore input and yield RawItems.
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name for this fetcher."""
        pass

    @abstractmethod
    async def fetch(self) -> AsyncIterator[RawItem]:
        """Fetch raw data items."""
        pass

    async def __call__(self, items: AsyncIterator[Any]) -> AsyncIterator[RawItem]:
        """Transform interface: ignore input stream and yield fetched items."""
        async for item in items:
            # For fetchers, we ignore the input stream and start fresh
            async for raw_item in self.fetch():
                yield raw_item
            break  # Only process one input item to trigger fetching


class Sink(Transform):
    """Abstract base class for data sinks.
    
    Sinks are specialized transforms that consume items and yield them unchanged.
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name for this sink."""
        pass

    @abstractmethod
    async def handle(self, item: Any) -> None:
        """Handle an item."""
        pass

    async def __call__(self, items: AsyncIterator[Any]) -> AsyncIterator[Any]:
        """Transform interface: handle items and pass them through."""
        async for item in items:
            await self.handle(item)
            yield item
