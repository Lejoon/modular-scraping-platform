"""
Discord sink for sending pipeline results to Discord channels.
"""

import json
import logging
from typing import Any, Optional

import discord

from ..interfaces import Sink
from ..models import ParsedItem

logger = logging.getLogger(__name__)


class DiscordSink(Sink):
    """Sink that sends pipeline results to a Discord channel."""
    
    def __init__(self, channel_id: int, client: Optional[discord.Client] = None, max_length: int = 1900):
        """
        Initialize Discord sink.
        
        Args:
            channel_id: Discord channel ID to send messages to
            client: Discord client instance (will be resolved from DI if None)
            max_length: Maximum message length before truncation
        """
        self.channel_id = channel_id
        self.client = client
        self.max_length = max_length
        self._client_resolved = False
        
    @property
    def name(self) -> str:
        return "DiscordSink"
    
    async def _resolve_client(self):
        """Resolve Discord client from global registry if not provided."""
        if not self.client and not self._client_resolved:
            # Try to get client from a global registry or singleton
            # This is a simple implementation - in production you might use DI
            import asyncio
            
            # Look for running event loop tasks that might have a Discord client
            try:
                tasks = asyncio.all_tasks()
                for task in tasks:
                    if hasattr(task, '_client') and isinstance(task._client, discord.Client):
                        self.client = task._client
                        break
            except Exception:
                pass
                
            self._client_resolved = True
    
    async def handle(self, item: Any) -> None:
        """Send item to Discord channel."""
        await self._resolve_client()
        
        if not self.client:
            logger.warning("Discord client not available, skipping message")
            return
            
        try:
            channel = self.client.get_channel(self.channel_id)
            if not channel:
                logger.error(f"Discord channel {self.channel_id} not found")
                return
                
            # Format the message based on item type
            if isinstance(item, ParsedItem):
                message = self._format_parsed_item(item)
            else:
                message = self._format_generic_item(item)
                
            # Truncate if too long
            if len(message) > self.max_length:
                message = message[:self.max_length - 3] + "..."
                
            await channel.send(message)
            logger.debug(f"Sent message to Discord channel {self.channel_id}")
            
        except Exception as e:
            logger.error(f"Failed to send Discord message: {e}")
    
    def _format_parsed_item(self, item: ParsedItem) -> str:
        """Format a ParsedItem for Discord."""
        content_preview = json.dumps(item.content, indent=2)
        
        # Limit content preview length
        if len(content_preview) > 800:
            content_preview = content_preview[:800] + "\n  ... (truncated)"
            
        return f"📊 **New item on {item.topic}**\n```json\n{content_preview}\n```"
    
    def _format_generic_item(self, item: Any) -> str:
        """Format a generic item for Discord."""
        try:
            if hasattr(item, '__dict__'):
                content = json.dumps(item.__dict__, indent=2, default=str)
            else:
                content = str(item)
                
            # Limit content length
            if len(content) > 800:
                content = content[:800] + "\n... (truncated)"
                
            return f"📨 **Pipeline Output**\n```\n{content}\n```"
            
        except Exception as e:
            return f"📨 **Pipeline Output** (format error: {e})\n```\n{str(item)[:800]}\n```"
