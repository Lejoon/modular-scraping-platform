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
        rate_limit_s: float = 1.5,  # Increased from 0.8 to reduce 429 errors
        max_retries: int = 6,  # Increased from 4 for better resilience
        http: Optional[HttpClient] = None,
        include_apps: bool = True,
        include_country_split: bool = False,
        use_html_fallback: bool = False,  # Changed from use_javascript_renderer - now disabled by default
        playwright_timeout: int = 30_000,  # Still configurable for HTML fallback
        wait_selector_timeout: int = 45_000,  # Still configurable for HTML fallback
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
        self._use_html_fallback = use_html_fallback  # Updated variable name
        self._playwright_timeout = playwright_timeout
        self._wait_selector_timeout = wait_selector_timeout
        self._playwright_client: Optional[PlaywrightClient] = None
 #------------------------------------------------------------------- #
    # Async context manager for proper resource cleanup
    # ------------------------------------------------------------------- #
    async def __aenter__(self):
        """Async context manager entry."""
        if self._use_html_fallback:
            self._playwright_client = PlaywrightClient(
                headless=True,
                stealth=True,
                timeout=self._playwright_timeout  # Use configurable timeout
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
            
        max_attempts = 3
        attempt = 0
        
        while attempt < max_attempts:
            attempt += 1
            try:
                logger.debug(f"Fetching JavaScript-rendered content from: {url} (attempt {attempt}/{max_attempts})")
                
                # Try with specific selector first, but if it fails, get content anyway
                if wait_for_selector:
                    try:
                        # Use custom page creation for better control
                        page = await self._playwright_client.new_page()
                        try:
                            await page.goto(url, wait_until="domcontentloaded", timeout=self._playwright_timeout)
                            
                            # Wait for selector with custom timeout and retries
                            logger.debug(f"Waiting for selector: {wait_for_selector} (timeout: {self._wait_selector_timeout}ms)")
                            
                            # Try multiple fallback selectors if the primary one fails
                            selectors_to_try = [
                                wait_for_selector,
                                "body",  # Fallback to body if specific selector fails
                                "html"   # Ultimate fallback
                            ]
                            
                            selector_found = False
                            for selector in selectors_to_try:
                                try:
                                    await page.wait_for_selector(selector, timeout=self._wait_selector_timeout)
                                    logger.debug(f"Successfully found selector: {selector}")
                                    selector_found = True
                                    break
                                except Exception as sel_exc:
                                    logger.debug(f"Selector '{selector}' failed: {sel_exc}")
                                    continue
                            
                            if not selector_found:
                                logger.warning(f"All selectors failed, but continuing to get content")
                            
                            # Additional wait for dynamic content to load
                            await asyncio.sleep(2.0)  # Give SPA time to render
                            
                            html = await page.content()
                            logger.debug(f"Successfully rendered HTML with JavaScript: {len(html)} characters")
                            return html
                            
                        finally:
                            await page.close()
                            
                    except Exception as selector_exc:
                        logger.warning(f"JavaScript rendering with selector failed (attempt {attempt}): {selector_exc}")
                        if attempt < max_attempts:
                            wait_time = 5.0 * attempt  # Progressive delay: 5s, 10s, 15s
                            logger.info(f"Retrying in {wait_time}s...")
                            await asyncio.sleep(wait_time)
                            continue
                        # Fall through to try without selector on final attempt
                
                # Try without selector as fallback
                try:
                    page = await self._playwright_client.new_page()
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=self._playwright_timeout)
                        await asyncio.sleep(3.0)  # Give more time for SPA to render
                        html = await page.content()
                        logger.debug(f"Successfully rendered HTML without selector: {len(html)} characters")
                        return html
                    finally:
                        await page.close()
                except Exception as no_selector_exc:
                    logger.warning(f"JavaScript rendering without selector failed (attempt {attempt}): {no_selector_exc}")
                    if attempt < max_attempts:
                        wait_time = 10.0 * attempt  # Longer delay for complete failures
                        logger.info(f"Retrying JavaScript rendering in {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue
                
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"JavaScript rendering attempt {attempt} failed for {url}: {exc}")
                if attempt < max_attempts:
                    wait_time = 15.0 * attempt  # Even longer delay for major failures
                    logger.info(f"Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
        
        logger.warning("All JavaScript rendering attempts failed for %s, falling back to simple HTTP", url)
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

            # HTML page fetching is now optional since metrics are available via the API
            # HTML fetching can be enabled with use_html_fallback=True if needed for debugging
            if self._use_html_fallback:
                accounts_data = pub.get("accounts", []) or pub.get("publisherIds", [])
                if accounts_data:
                    # Only process the first account to avoid fetching duplicate data
                    acc = accounts_data[0]
                    s = acc.get("storeId") or acc.get("store") or store_id
                    pid = acc.get("publisherId") or acc.get("store_publisher_id")
                    if pid:
                        logger.info(f"HTML fallback: Fetching HTML for publisher {pub_name}, store={s}, publisher_id={pid} (first account only)")
                        if len(accounts_data) > 1:
                            logger.debug(f"Skipping {len(accounts_data) - 1} additional accounts for publisher {pub_name} - data is redundant")
                        url = construct_html_url(pub.get("name", ""), s, pid)
                        logger.debug(f"HTML URL: {url}")
                        
                        # Use JavaScript renderer for SPA content
                        wait_selector = "div.table.ng-star-inserted"
                        html = await self._safe_get_html_with_js(url, wait_selector)
                            
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
                                        "total_accounts": len(accounts_data),  # Include count for reference
                                    }
                                ).encode(),
                                fetched_at=fetched_at,
                            )
                    else:
                        logger.debug(f"First account for publisher {pub_name} has no valid publisher_id, skipping HTML fetch")
                else:
                    logger.debug(f"No accounts found for publisher {pub_name}, skipping HTML fetch")

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
