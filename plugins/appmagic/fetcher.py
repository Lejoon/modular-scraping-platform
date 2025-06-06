# core/plugins/appmagic/fetcher.py
"""Async *Fetcher* for AppMagic.

‣ identical REST calls and request-payloads to `scrape-company.py`  
‣ shape-guard for `"publishers" | "data" | "result"` wrappers  
‣ publisher-country snapshot endpoint  
‣ HTML page download via `construct_html_url`  
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


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def construct_html_url(name: str, store_id: int, store_publisher_id: str) -> str:
    """Return the public AppMagic page for a publisher account."""
    slug = re.sub(r"\s+", "-", name.strip().lower())
    return f"https://appmagic.rocks/publisher/{slug}/{store_id}_{store_publisher_id}"


# --------------------------------------------------------------------------- #
class AppMagicFetcher(Fetcher):
    """Transform stage 1 / 3 – emits RawItems."""

    # abstract‐method override ------------------------------------------------
    @property
    def name(self) -> str:  # noqa: D401  (property, not a verb)
        return "AppMagicFetcher"

    # -------- static endpoint paths ----------------------------------------
    _BASE = "https://appmagic.rocks/api/v2"

    _URL_GROUPS = _BASE + "/publishers/groups"
    _URL_SEARCH = _BASE + "/united-publishers/search-by-ids"
    _URL_PUBLISHER_APPS = _BASE + "/search/publisher-applications"
    _URL_COUNTRIES = _BASE + "/united-publishers/data-countries"

    # ------------------------------------------------------------------- #
    def __init__(
        self,
        *,
        companies: List[Dict[str, Any]],
        rate_limit_s: float = 0.8,
        max_retries: int = 4,
        http: Optional[HttpClient] = None,
        include_apps: bool = True,
        include_country_split: bool = False,
    ) -> None:
        self._companies = companies
        self._rl = rate_limit_s
        self._http = http or HttpClient(max_retries=max_retries)
        self._include_apps = include_apps
        self._include_country_split = include_country_split

    # ------------------------------------------------------------------- #
    # resilient wrappers around HttpClient
    # ------------------------------------------------------------------- #
    async def _safe_get_json(self, url: str, **kw) -> Dict[str, Any]:
        try:
            return await self._http.get_json(url, **kw) or {}
        except Exception as exc:  # noqa: BLE001
            logger.debug("GET %s failed: %s", url, exc)
            return {}

    async def _safe_post_json(self, url: str, data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return await self._http.post_json(url, data) or {}
        except Exception as exc:  # noqa: BLE001
            logger.debug("POST %s failed: %s", url, exc)
            return {}

    async def _safe_get_text(self, url: str, **kw) -> str:
        try:
            return await self._http.get_text(url, **kw)
        except AttributeError:  # legacy client exposes .get
            return await self._http.get(url, **kw)
        except Exception as exc:  # noqa: BLE001
            logger.debug("GET(text) %s failed: %s", url, exc)
            return ""

    # ------------------------------------------------------------------- #
    async def fetch(self) -> AsyncIterator[RawItem]:
        for company in self._companies:
            async for itm in self._run_for_company(company):
                yield itm
                await asyncio.sleep(self._rl)

    # ------------------------------------------------------------------- #
    async def _run_for_company(
        self, company: Dict[str, Any]
    ) -> AsyncIterator[RawItem]:
        fetched_at = datetime.now(tz=timezone.utc)

        store_id = company.get("store", 1)
        store_pid = company["store_publisher_id"]

        # 1 – groups ----------------------------------------------------
        grp_payload = await self._safe_get_json(
            self._URL_GROUPS,
            params={"store": store_id, "store_publisher_id": store_pid},
        )
        grp_pubs = grp_payload.get("publishers", [])

        yield RawItem(
            source="appmagic.groups",
            payload=json.dumps({"company": company, "groups": grp_pubs}).encode(),
            fetched_at=fetched_at,
        )

        ids = [
            {"store": p.get("store"), "store_publisher_id": p.get("store_publisher_id")}
            for p in grp_pubs
            if p.get("store") and p.get("store_publisher_id")
        ] or [{"store": store_id, "store_publisher_id": store_pid}]

        # 2 – united-publishers search ---------------------------------
        search_payload = await self._safe_post_json(self._URL_SEARCH, {"ids": ids})
        publishers = self._extract_publishers(search_payload)

        yield RawItem(
            source="appmagic.publishers",
            payload=json.dumps({"company": company, "publishers": publishers}).encode(),
            fetched_at=fetched_at,
        )

        # 3 – per publisher -------------------------------------------
        seen: Set[int] = set()
        for pub in publishers:
            up_id = pub.get("id")
            if not up_id or up_id in seen:
                continue
            seen.add(up_id)

            # publisher apps via new search API (if enabled)
            if self._include_apps:
                async for item in self._fetch_publisher_apps(up_id, company, fetched_at):
                    yield item

            # country metrics (if enabled)
            if self._include_country_split:
                ctry_payload = await self._safe_get_json(
                    self._URL_COUNTRIES, params={"united_publisher_id": up_id}
                )
                if ctry_payload:
                    yield RawItem(
                        source="appmagic.publisher.country.metrics",
                        payload=json.dumps(
                            {"united_publisher_id": up_id, "countries": ctry_payload}
                        ).encode(),
                        fetched_at=fetched_at,
                    )

            # HTML page
            for acc in pub.get("accounts", []):
                s = acc.get("storeId") or acc.get("store") or store_id
                pid = acc.get("publisherId") or acc.get("store_publisher_id")
                if not pid:
                    continue
                url = construct_html_url(pub.get("name", ""), s, pid)
                html = await self._safe_get_text(url)
                if html:
                    yield RawItem(
                        source="appmagic.publisher_html",
                        payload=json.dumps(
                            {
                                "up_id": up_id,
                                "store": s,
                                "store_publisher_id": pid,
                                "html_url": url,
                                "html": html,
                            }
                        ).encode(),
                        fetched_at=fetched_at,
                    )

    # ------------------------------------------------------------------- #
    async def _fetch_publisher_apps(
        self, up_id: int, company: Dict[str, Any], fetched_at: datetime
    ) -> AsyncIterator[RawItem]:
        """Fetch apps using the new publisher-applications search API with paging."""
        from_offset = 0
        page_size = 100

        while True:
            params = {
                "sort": "downloads",
                "united_publisher_id": up_id,
                "from": from_offset,
            }
            
            payload = await self._safe_get_json(self._URL_PUBLISHER_APPS, params=params)
            
            # Handle different response structures
            if isinstance(payload, list):
                # If payload is directly a list of applications
                hits = payload
                applications = payload
            elif isinstance(payload, dict):
                # If payload is a dictionary with hits or applications key
                hits = payload.get("hits", payload.get("applications", []))
                applications = hits
            else:
                logger.warning(f"Unexpected payload type: {type(payload)}")
                break
            
            if not hits:
                break
                
            yield RawItem(
                source="appmagic.publisher_apps_api",
                payload=json.dumps({
                    "united_publisher_id": up_id,
                    "company": company,
                    "applications": applications,
                    "from_offset": from_offset,
                }).encode(),
                fetched_at=fetched_at,
            )
            
            # Check if we have more pages
            if len(applications) < page_size:
                break
                
            from_offset += len(applications)

    # ------------------------------------------------------------------- #
    @staticmethod
    def _extract_publishers(payload: Dict[str, Any]) -> List[dict]:
        if "publishers" in payload:
            return payload["publishers"]
        if "data" in payload:
            return payload["data"]
        if "result" in payload:
            return payload["result"]
        return []


# ---------- expose the class as appmagic.AppMagicFetcher ----------------
import sys as _sys  # noqa: E402

_pkg = _sys.modules.get(__name__.rsplit(".", 1)[0])
if _pkg is not None:
    setattr(_pkg, "AppMagicFetcher", AppMagicFetcher)
