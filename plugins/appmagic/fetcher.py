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
from core.infra.sel import PlaywrightClient

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
        use_javascript_renderer: bool = True,
    ) -> None:
        self._companies = companies
        self._rl = rate_limit_s
        self._http = http or HttpClient(max_retries=max_retries)
        self._include_apps = include_apps
        self._include_country_split = include_country_split
        self._use_js_renderer = use_javascript_renderer
        self._playwright_client: Optional[PlaywrightClient] = None
 #------------------------------------------------------------------- #
    # Async context manager for proper resource cleanup
    # ------------------------------------------------------------------- #
    async def __aenter__(self):
        """Async context manager entry."""
        if self._use_js_renderer:
            self._playwright_client = PlaywrightClient(
                headless=True,
                stealth=True,
                timeout=30_000
            )
            await self._playwright_client.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - ensure HttpClient and PlaywrightClient sessions are closed."""
        if self._playwright_client:
            await self._playwright_client.stop()
            self._playwright_client = None
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
            logger.warning("POST %s failed: %s", url, exc)
            return {}

    async def _safe_get_text(self, url: str, **kw) -> str:
        try:
            return await self._http.get_text(url, **kw)
        except AttributeError:  # legacy client exposes .get
            return await self._http.get(url, **kw)
        except Exception as exc:  # noqa: BLE001
            logger.debug("GET(text) %s failed: %s", url, exc)
            return ""

    async def _safe_get_html_with_js(self, url: str, wait_for_selector: Optional[str] = None) -> str:
        """
        Fetch HTML content with JavaScript rendering using Playwright.
        
        Args:
            url: URL to fetch
            wait_for_selector: Optional CSS selector to wait for before extracting content
            
        Returns:
            Rendered HTML content or empty string on error
        """
        if not self._playwright_client:
            logger.warning("Playwright client not available, falling back to simple HTTP")
            return await self._safe_get_text(url)
            
        try:
            logger.debug(f"Fetching JavaScript-rendered content from: {url}")
            html = await self._playwright_client.get_page_content(url, wait_for_selector)
            logger.debug(f"Successfully rendered HTML: {len(html)} characters")
            return html
        except Exception as exc:  # noqa: BLE001
            logger.warning("JavaScript rendering failed for %s: %s, falling back to simple HTTP", url, exc)
            return await self._safe_get_text(url)

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

            # HTML page
            # Check both accounts and publisherIds fields
            accounts_data = pub.get("accounts", []) or pub.get("publisherIds", [])
            for acc in accounts_data:
                s = acc.get("storeId") or acc.get("store") or store_id
                pid = acc.get("publisherId") or acc.get("store_publisher_id")
                if not pid:
                    continue
                logger.info(f"Fetching HTML for publisher {pub_name}, store={s}, publisher_id={pid}")
                url = construct_html_url(pub.get("name", ""), s, pid)
                logger.debug(f"HTML URL: {url}")
                
                # Use JavaScript renderer for SPA content if available, otherwise fall back to simple HTTP
                if self._use_js_renderer:
                    # Wait for common AppMagic content selectors to ensure the page is loaded
                    wait_selector = ".publisher-info, .app-card, .stats-card, [data-testid='publisher-stats']"
                    html = await self._safe_get_html_with_js(url, wait_selector)
                else:
                    html = await self._safe_get_text(url)
                    
                logger.info(f"HTML fetch result: {len(html) if html else 0} characters")
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
