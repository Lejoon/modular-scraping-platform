"""
Core interfaces for the scraper platform.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator, List, Any
from discord.ext.commands import Bot  # Assuming discord.py Bot object

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


class DiscordCommands(ABC):
    """Interface for plugin Discord command registration."""

    @abstractmethod
    def register(self, bot: Bot) -> None:
        """Register all commands for this plugin on the given bot.

        The bot instance can be used to access shared resources
        like a database connection or an HTTP client if they are
        attached to the bot object during its initialization.
        """
        pass

    async def setup(self, bot: Bot) -> None:
        """Optional asynchronous setup method for the plugin.

        This method is called once when the plugin is loaded.
        Plugins can use this to initialize resources like database connections
        and attach them to the bot instance if needed.
        """
        pass
