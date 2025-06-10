# core/plugins/appmagic/fetcher.py
"""Async *Fetcher* for AppMagic.

‣ identical REST calls and request-payloads to `scrape-company.py`  
‣ shape-guard for `"publishers" | "data" | "result"` wrappers  
‣ publisher-country snapshot endpoint  
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
        rate_limit_s: float = 1.5,  # Increased from 0.8 to reduce 429 errors
        max_retries: int = 6,  # Increased from 4 for better resilience
        http: Optional[HttpClient] = None,
        include_apps: bool = True,
        include_country_split: bool = False,
    ) -> None:
        self._companies = companies
        self._rl = rate_limit_s
        # Configure HttpClient with longer delays for better 429 handling
        self._http = http or HttpClient(
            max_retries=max_retries,
            base_delay=15,  # Increased from default 1.0s
            max_delay=120.0,  # Increased from default 60.0s
        )
        self._include_apps = include_apps
        self._include_country_split = include_country_split
 #------------------------------------------------------------------- #
    # Async context manager for proper resource cleanup
    # ------------------------------------------------------------------- #
    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Async context manager exit - ensure HttpClient and PlaywrightClient sessions are closed."""
        if hasattr(self._http, 'close'):
            await self._http.close()

    # ------------------------------------------------------------------- #
    # resilient wrappers around HttpClient
    # ------------------------------------------------------------------- #
    async def _safe_get_json(self, url: str, **kw) -> Dict[str, Any]:
        try:
            logger.debug(f"Making GET request to: {url} with params: {kw.get('params', {})}")
            result = await self._http.get_json(url, **kw) or {}
            logger.info(f"GET {url} succeeded, response keys: {list(result.keys()) if isinstance(result, dict) else 'not a dict'}")
            if isinstance(result, dict) and result:
                # Log a sample of the response structure
                sample_keys = list(result.keys())[:5]  # First 5 keys
                logger.debug(f"Response sample keys: {sample_keys}")
                for key in sample_keys:
                    if isinstance(result[key], list):
                        logger.debug(f"  {key}: list with {len(result[key])} items")
                    elif isinstance(result[key], dict):
                        logger.debug(f"  {key}: dict with keys {list(result[key].keys())[:3]}")
                    else:
                        logger.debug(f"  {key}: {type(result[key]).__name__}")
            return result
        except Exception as exc:  # noqa: BLE001
            # For rate limiting errors, add extra delay
            if "429" in str(exc) or "rate" in str(exc).lower():
                logger.warning("Rate limit detected for GET %s, adding extra delay: %s", url, exc)
                await asyncio.sleep(30.0)  # Extra 30s delay for rate limits
            else:
                logger.warning("GET %s failed: %s", url, exc)
            return {}

    async def _safe_post_json(self, url: str, data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            logger.debug(f"Making POST request to: {url} with data: {data}")
            result = await self._http.post_json(url, data) or {}
            logger.info(f"POST {url} succeeded, response keys: {list(result.keys()) if isinstance(result, dict) else 'not a dict'}")
            if isinstance(result, dict) and result:
                # Log a sample of the response structure
                sample_keys = list(result.keys())[:5]
                logger.debug(f"Response sample keys: {sample_keys}")
                for key in sample_keys:
                    if isinstance(result[key], list):
                        logger.debug(f"  {key}: list with {len(result[key])} items")
                    elif isinstance(result[key], dict):
                        logger.debug(f"  {key}: dict with keys {list(result[key].keys())[:3]}")
                    else:
                        logger.debug(f"  {key}: {type(result[key]).__name__}")
            return result
        except Exception as exc:  # noqa: BLE001
            # For rate limiting errors, add extra delay
            if "429" in str(exc) or "rate" in str(exc).lower():
                logger.warning("Rate limit detected for POST %s, adding extra delay: %s", url, exc)
                await asyncio.sleep(30.0)  # Extra 30s delay for rate limits
            else:
                logger.warning("POST %s failed: %s", url, exc)
            return {}

    # ------------------------------------------------------------------- #
    async def fetch(self) -> AsyncIterator[RawItem]:
        logger.info(f"Starting AppMagic fetch for {len(self._companies)} companies")
        for i, company in enumerate(self._companies):
            company_name = company.get("name", "Unknown")
            logger.info(f"Fetching data for company {i+1}/{len(self._companies)}: {company_name}")
            item_count = 0
            async for itm in self._run_for_company(company):
                item_count += 1
                logger.debug(f"  Yielding item {item_count} from {company_name}: {itm.source}")
                yield itm
                await asyncio.sleep(self._rl)
            logger.info(f"Completed fetching {item_count} items for {company_name}")
        logger.info("AppMagic fetch completed for all companies")

    # ------------------------------------------------------------------- #
    async def _run_for_company(
        self, company: Dict[str, Any]
    ) -> AsyncIterator[RawItem]:
        company_name = company.get("name", "Unknown")
        logger.info(f"Starting data collection for company: {company_name}")
        fetched_at = datetime.now(tz=timezone.utc)

        store_id = company.get("store", 1)
        store_pid = company["store_publisher_id"]
        logger.debug(f"Company parameters: store_id={store_id}, store_publisher_id={store_pid}")

        # 1 – groups ----------------------------------------------------
        logger.info(f"Step 1: Fetching groups for {company_name}")
        grp_payload = await self._safe_get_json(
            self._URL_GROUPS,
            params={"store": store_id, "store_publisher_id": store_pid},
        )
        grp_pubs = grp_payload.get("publishers", [])
        logger.info(f"Found {len(grp_pubs)} publishers in groups for {company_name}")

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
        logger.debug(f"Publisher IDs to search: {ids}")

        # 2 – united-publishers search ---------------------------------
        logger.info(f"Step 2: Searching united publishers for {company_name}")
        search_payload = await self._safe_post_json(self._URL_SEARCH, {"ids": ids})
        publishers = self._extract_publishers(search_payload)
        logger.info(f"Found {len(publishers)} united publishers for {company_name}")

        yield RawItem(
            source="appmagic.publishers",
            payload=json.dumps({"company": company, "publishers": publishers}).encode(),
            fetched_at=fetched_at,
        )

        # 3 – per publisher -------------------------------------------
        logger.info(f"Step 3: Processing individual publishers for {company_name}")
        seen: Set[int] = set()
        for i, pub in enumerate(publishers):
            up_id = pub.get("id")
            pub_name = pub.get("name", f"Publisher-{up_id}")
            if not up_id or up_id in seen:
                logger.debug(f"Skipping publisher {pub_name} (already processed or no ID)")
                continue
            seen.add(up_id)
            logger.info(f"Processing publisher {i+1}/{len(publishers)}: {pub_name} (ID: {up_id})")

            # publisher apps via new search API (if enabled)
            if self._include_apps:
                logger.debug(f"Fetching apps for publisher {pub_name}")
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

    # ------------------------------------------------------------------- #
    async def _fetch_publisher_apps(
        self, up_id: int, company: Dict[str, Any], fetched_at: datetime
    ) -> AsyncIterator[RawItem]:
        """Fetch apps using the new publisher-applications search API with paging."""
        logger.debug(f"Starting app fetch for publisher {up_id}")
        from_offset = 0
        page_size = 100
        total_apps = 0

        while True:
            params = {
                "sort": "downloads",
                "united_publisher_id": up_id,
                "from": from_offset,
            }
            logger.debug(f"Fetching apps for publisher {up_id}, offset={from_offset}")
            
            payload = await self._safe_get_json(self._URL_PUBLISHER_APPS, params=params)

            hits = payload
            applications = payload
            logger.info(f"Received {len(hits)} apps directly from API for publisher {up_id}")
            
            if not hits:
                logger.debug(f"No more apps found for publisher {up_id}")
                break
            
            logger.debug(f"Found {len(applications)} apps for publisher {up_id} at offset {from_offset}")
            total_apps += len(applications)
                
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
                logger.debug(f"Reached end of apps for publisher {up_id} (got {len(applications)} < {page_size})")
                break
                
            from_offset += len(applications)
        
        logger.info(f"Completed app fetch for publisher {up_id}: {total_apps} total apps")

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
