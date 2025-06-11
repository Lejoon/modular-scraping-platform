"""appmagic.fetcher_refactored – async *Fetcher* for AppMagic.

Changes in this revision (v2)
----------------------------
* **Dropped custom 429 back‑off** – we now *fully* rely on
  :class:`core.infra.http.HttpClient`’s smart retry logic.  No duplicated
  sleep‑handling.
* `_json_request()` is retained only as a thin logger; it no longer parses
  error messages or sleeps.
* `BACKOFF_429` constant removed.
* All public behaviour (yielded ``RawItem.source`` strings, constructor args)
  remains unchanged.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Optional, Set

from core.interfaces import Fetcher
from core.models import RawItem
from core.infra.http import HttpClient

logger = logging.getLogger(__name__)

__all__ = ["AppMagicFetcher"]


# --------------------------------------------------------------------------- #
_BASE = "https://appmagic.rocks/api/v2"
URL = {
    "groups": f"{_BASE}/publishers/groups",
    "search": f"{_BASE}/united-publishers/search-by-ids",
    "publisher_apps": f"{_BASE}/search/publisher-applications",
    "countries": f"{_BASE}/united-publishers/data-countries",
}

PAGE_SIZE = 100  # fixed by upstream API

slugify = lambda s: re.sub(r"\s+", "-", s.strip().lower())  # noqa: E731, S002


# --------------------------------------------------------------------------- #
class AppMagicFetcher(Fetcher):
    """Transform stage 1 / 3 – yields :class:`~core.models.RawItem`."""

    name = "AppMagicFetcher"

    # ------------------------------------------------------------------- #
    def __init__(
        self,
        *,
        companies: List[Dict[str, Any]],
        rate_limit_s: float = 1.5,
        max_retries: int = 5,
        include_apps: bool = True,
        include_country_split: bool = False,
        http: Optional[HttpClient] = None,
    ) -> None:
        self._companies = companies
        self._rl = rate_limit_s
        self._include_apps = include_apps
        self._include_country = include_country_split

        self._http = http or HttpClient(
            max_retries=max_retries,
            base_delay=15.0,
            max_delay=120.0,
        )

    # ------------------------------------------------------------------- #
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        if hasattr(self._http, "close"):
            await self._http.close()

    # ------------------------------------------------------------------- #
    async def _json_request(self, method: str, url: str, **kw) -> Dict[str, Any]:
        """Thin logging wrapper around :pymeth:`HttpClient.get_json` / `.post_json`."""
        verb = method.upper()
        fn = self._http.get_json if verb == "GET" else self._http.post_json
        try:
            logger.debug("%s %s – params=%s data=%s", verb, url, kw.get("params"), kw.get("data"))
            return await fn(url, **kw) or {}
        except Exception as exc:  # noqa: BLE001
            logger.warning("%s %s failed: %s", verb, url, exc)
            return {}

    # ------------------------------------------------------------------- #
    async def fetch(self) -> AsyncIterator[RawItem]:
        logger.info("AppMagicFetcher – %d companies", len(self._companies))
        for company in self._companies:
            async for itm in self._run_for_company(company):
                yield itm
                await asyncio.sleep(self._rl)

    # ------------------------------------------------------------------- #
    async def _run_for_company(self, company: Dict[str, Any]) -> AsyncIterator[RawItem]:
        fetched_at = datetime.now(tz=timezone.utc)
        comp_name = company.get("name", "Unknown")
        logger.info("Company – %s", comp_name)

        store_id = company.get("store", 1)
        store_pid = company["store_publisher_id"]

        # 1️⃣ Groups ---------------------------------------------------
        grp_payload = await self._json_request("GET", URL["groups"], params={"store": store_id, "store_publisher_id": store_pid})
        publishers_in_group = grp_payload.get("publishers", [])
        yield RawItem(
            source="appmagic.groups",
            payload=json.dumps({"company": company, "groups": publishers_in_group}).encode(),
            fetched_at=fetched_at,
        )

        ids = [
            {"store": p.get("store"), "store_publisher_id": p.get("store_publisher_id")}
            for p in publishers_in_group
            if p.get("store") and p.get("store_publisher_id")
        ] or [{"store": store_id, "store_publisher_id": store_pid}]

        # 2️⃣ Publisher search ----------------------------------------
        search = await self._json_request("POST", URL["search"], data={"ids": ids})
        publishers = self._extract_publishers(search)
        yield RawItem(
            source="appmagic.publishers",
            payload=json.dumps({"company": company, "publishers": publishers}).encode(),
            fetched_at=fetched_at,
        )

        # 3️⃣ Per‑publisher section -----------------------------------
        seen: Set[int] = set()
        for pub in publishers:
            up_id = pub.get("id")
            if not up_id or up_id in seen:
                continue
            seen.add(up_id)

            if self._include_apps:
                async for itm in self._fetch_publisher_apps(up_id, company, fetched_at):
                    yield itm

            if self._include_country:
                ctry = await self._json_request("GET", URL["countries"], params={"united_publisher_id": up_id})
                if ctry:
                    yield RawItem(
                        source="appmagic.publisher.country.metrics",
                        payload=json.dumps({"united_publisher_id": up_id, "countries": ctry}).encode(),
                        fetched_at=fetched_at,
                    )

    # ------------------------------------------------------------------- #
    async def _fetch_publisher_apps(self, up_id: int, company: Dict[str, Any], fetched_at: datetime) -> AsyncIterator[RawItem]:
        offset = 0
        while True:
            params = {"sort": "downloads", "united_publisher_id": up_id, "from": offset}
            apps = await self._json_request("GET", URL["publisher_apps"], params=params)
            if not apps:
                break

            yield RawItem(
                source="appmagic.publisher_apps_api",
                payload=json.dumps({
                    "united_publisher_id": up_id,
                    "company": company,
                    "applications": apps,
                    "from_offset": offset,
                }).encode(),
                fetched_at=fetched_at,
            )

            if len(apps) < PAGE_SIZE:
                break
            offset += len(apps)

    # ------------------------------------------------------------------- #
    @staticmethod
    def _extract_publishers(payload: Dict[str, Any]) -> List[dict]:
        for key in ("publishers", "data", "result"):
            if key in payload:
                return payload[key]
        return []
