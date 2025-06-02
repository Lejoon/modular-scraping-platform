"""
FI Short Interest Fetcher - Downloads ODS files from Finansinspektionen.
"""

import logging
from datetime import datetime
from typing import AsyncIterator, Optional, Any

import aiohttp
from bs4 import BeautifulSoup

from core.interfaces import Fetcher
from core.models import RawItem
from core.infra.http import HttpClient


logger = logging.getLogger(__name__)


class FiFetcher(Fetcher):
    """Fetches short interest data from Finansinspektionen."""
    
    name = "FiFetcher"
    
    URL_TS = "https://www.fi.se/sv/vara-register/blankningsregistret/"
    URL_AGG = "https://www.fi.se/sv/vara-register/blankningsregistret/GetBlankningsregisterAggregat/"
    URL_ACT = "https://www.fi.se/sv/vara-register/blankningsregistret/GetAktuellFile/"

    def __init__(self, **kwargs):
        self.http = HttpClient(**kwargs)
        self._last_seen: Optional[str] = None

    async def fetch(self) -> AsyncIterator[RawItem]:
        """Fetch FI short interest data - single poll, no infinite loop."""
        try:
            # 1) Poll timestamp
            html = await self.http.get_text(self.URL_TS)
            soup = BeautifulSoup(html, "html.parser")
            tag = soup.find("p", string=lambda t: t and "Listan uppdaterades:" in t)
            ts = tag.text.split(": ", 1)[1].strip() if tag else None
            
            logger.debug(f"Found timestamp: {ts}")

            # 2) If new timestamp, download both files
            if ts and ts != "0001-01-01 00:00" and ts != self._last_seen:
                logger.info(f"New timestamp detected: {ts} (previous: {self._last_seen})")
                
                now = datetime.utcnow()
                
                # Download aggregate file
                agg_bytes = await self.http.get_bytes(self.URL_AGG)
                logger.info(f"Downloaded aggregate file: {len(agg_bytes)} bytes")
                
                # Download current positions file
                act_bytes = await self.http.get_bytes(self.URL_ACT)
                logger.info(f"Downloaded positions file: {len(act_bytes)} bytes")
                
                self._last_seen = ts

                yield RawItem(source="fi.short.agg", payload=agg_bytes, fetched_at=now)
                yield RawItem(source="fi.short.act", payload=act_bytes, fetched_at=now)
            else:
                logger.debug(f"No new data (timestamp: {ts})")

        except Exception as e:
            logger.error(f"Failed to fetch FI data: {e}")
            # Don't re-raise to allow scheduler to continue
        finally:
            await self.http.close()
