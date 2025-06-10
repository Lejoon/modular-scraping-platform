# core/plugins/appmagic/parser.py
"""Transform stage 2 / 3 - RawItem → ParsedItem."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple

from core.interfaces import Transform
from core.models import ParsedItem, RawItem

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Utility functions for parsing AppMagic data
# --------------------------------------------------------------------------- #
def parse_appmagic_metric(metric_str: str) -> int:
    """Parse AppMagic metric strings like '1.2K', '3.5M', etc. into integers."""
    if not metric_str or metric_str in ["-", "—", "N/A"]:
        return 0
    
    # Remove any currency symbols and extra whitespace
    metric_str = metric_str.strip().replace("$", "").replace(",", "")
    
    # Handle different suffixes
    if metric_str.lower().endswith('k'):
        return int(float(metric_str[:-1]) * 1000)
    elif metric_str.lower().endswith('m'):
        return int(float(metric_str[:-1]) * 1000000)
    elif metric_str.lower().endswith('b'):
        return int(float(metric_str[:-1]) * 1000000000)
    else:
        try:
            return int(float(metric_str))
        except ValueError:
            return 0


def parse_release_date(date_str: str) -> Optional[str]:
    """Parse release date string and return in ISO format."""
    if not date_str or date_str in ["-", "—", "N/A"]:
        return None
    
    # This would need more sophisticated date parsing based on AppMagic's format
    # For now, return as-is
    return date_str


def extract_united_application_id_from_href(href: str) -> Optional[int]:
    """Extract united application ID from an AppMagic app page href."""
    if not href:
        return None
    
    # Extract ID from URLs like "/app/123456/app-name"
    parts = href.strip('/').split('/')
    if len(parts) >= 2 and parts[0] == 'app':
        try:
            return int(parts[1])
        except ValueError:
            return None
    
    return None


def extract_store_app_id_from_url(url: str, store_id: Optional[int] = None) -> Optional[str]:
    """
    Extract store-specific app ID from store URLs.
    
    Args:
        url: The store URL
        store_id: The store ID (1=Google Play, 2=Apple App Store, etc.)
    
    Returns:
        The extracted store app ID or None if extraction fails
    """
    if not url:
        return None
    
    try:
        # Google Play Store URLs
        if "play.google.com" in url or store_id == 1:
            # URL format: https://play.google.com/store/apps/details?id=com.package.name
            if "id=" in url:
                # Extract everything after 'id=' and before any '&'
                id_part = url.split("id=")[1].split("&")[0]
                return id_part
        
        # Apple App Store URLs
        elif "apps.apple.com" in url or store_id == 2 or store_id == 3:
            # URL formats: 
            # https://apps.apple.com/us/app/app-name/id123456789
            # https://apps.apple.com/gb/app/id372592824
            # Look for /id followed by digits at the end of the path
            match = re.search(r'/id(\d+)(?:/|$)', url)
            if match:
                return f"id{match.group(1)}"
        
        # For other stores or if extraction fails, try to get the last segment
        # This is a fallback that might work for some store formats
        path_segments = url.rstrip('/').split('/')
        if path_segments:
            last_segment = path_segments[-1]
            # If it looks like an ID (contains digits or starts with known prefixes)
            if last_segment and (last_segment.startswith('id') or any(c.isdigit() for c in last_segment)):
                return last_segment
    
    except Exception as e:
        logger.debug(f"Failed to extract store app ID from URL {url}: {e}")
    
    return None


class AppMagicParser(Transform):
    # ------------------------------------------------------------------- #
    @property
    def name(self) -> str:  # noqa: D401
        return "AppMagicParser"

    # ------------------------------------------------------------------- #
    def __init__(self):
        # Store API data temporarily for processing
        self._api_data_by_publisher: Dict[int, List[ParsedItem]] = {}

    # ------------------------------------------------------------------- #
    async def __call__(self, items: AsyncIterator[Any]) -> AsyncIterator[ParsedItem]:
        logger.info("AppMagicParser: Starting parsing process")
        # First pass: collect all data
        all_items = []
        item_count = 0
        async for item in items:
            if not isinstance(item, RawItem):
                yield item
                continue
            all_items.append(item)
            item_count += 1
        
        logger.info(f"AppMagicParser: Collected {item_count} raw items to process")

        # Process items and group by publisher
        processed_items = 0
        for item in all_items:
            try:
                payload = json.loads(item.payload)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Bad JSON in %s: %s", item.source, exc)
                continue

            handler = _HANDLERS.get(item.source)
            if handler is None:
                logger.debug("No parser for %s", item.source)
                continue
            
            logger.debug(f"Processing item {processed_items+1}/{len(all_items)}: {item.source}")
            processed_items += 1

            parsed_items = handler(payload)
            logger.debug(f"Handler for {item.source} produced {len(parsed_items)} parsed items")
            
            # For API publisher apps data, store for processing
            if item.source == "appmagic.publisher_apps_api":
                up_id = payload.get("united_publisher_id")
                if up_id:
                    if up_id not in self._api_data_by_publisher:
                        self._api_data_by_publisher[up_id] = []
                    self._api_data_by_publisher[up_id].extend(parsed_items)
                    logger.debug(f"Stored {len(parsed_items)} API items for publisher {up_id}")
                continue

            # For other sources, yield immediately
            immediate_yield_count = 0
            for parsed in parsed_items:
                yield parsed
                immediate_yield_count += 1
            logger.debug(f"Immediately yielded {immediate_yield_count} items for {item.source}")

        # Process stored API publisher data
        total_processed_items = 0
        for up_id in self._api_data_by_publisher.keys():
            logger.debug(f"Processing API data for publisher {up_id}")
            
            # Process applications
            async for app_item in self._process_applications(up_id):
                yield app_item
                total_processed_items += 1
            
            # Process metrics
            async for metrics_item in self._process_metrics(up_id):
                yield metrics_item
                total_processed_items += 1
            
            # Process store applications
            async for store_item in self._process_store_applications(up_id):
                yield store_item
                total_processed_items += 1
        
        logger.info(f"AppMagicParser: Completed parsing - {total_processed_items} total items generated from API data")

    # ------------------------------------------------------------------- #
    async def _process_applications(self, up_id: int) -> AsyncIterator[ParsedItem]:
        """Process application data for a single publisher."""
        api_items = self._api_data_by_publisher.get(up_id, [])
        
        # Get API applications
        api_apps = [item for item in api_items if item.topic == "appmagic.application" and item.content.get("data_source") == "api"]
        
        logger.debug(f"Publisher {up_id}: Processing {len(api_apps)} API applications")
        
        for api_app in api_apps:
            # Create application data with API-only source
            app_data = api_app.content.copy()
            app_data["data_source"] = "api_only"
            
            yield ParsedItem(
                topic="appmagic.application",
                content=app_data,
            )

    # ------------------------------------------------------------------- #
    async def _process_metrics(self, up_id: int) -> AsyncIterator[ParsedItem]:
        """Process metrics data for a single publisher."""
        api_items = self._api_data_by_publisher.get(up_id, [])
        
        # Get API metrics
        api_metrics = [item for item in api_items if item.topic == "appmagic.application.metrics"]
        
        logger.debug(f"Publisher {up_id}: Processing {len(api_metrics)} API metrics")
        
        for api_metric in api_metrics:
            # Create metrics data from API
            metrics_data = {
                "scrape_date": datetime.utcnow().date().isoformat(),
                "united_application_id": api_metric.content["united_application_id"],
                "snapshot_30d_downloads": api_metric.content.get("snapshot_30d_downloads"),
                "snapshot_30d_revenue": api_metric.content.get("snapshot_30d_revenue"),
                "snapshot_lifetime_downloads": api_metric.content.get("snapshot_lifetime_downloads"),
                "snapshot_lifetime_revenue": api_metric.content.get("snapshot_lifetime_revenue"),
            }
            
            yield ParsedItem(
                topic="appmagic.application.metrics",
                content=metrics_data,
            )

    # ------------------------------------------------------------------- #
    async def _process_store_applications(self, up_id: int) -> AsyncIterator[ParsedItem]:
        """Process store application data for a single publisher."""
        api_items = self._api_data_by_publisher.get(up_id, [])
        
        # Get API store applications
        api_store_apps = [item for item in api_items if item.topic == "appmagic.application.store"]
        
        logger.debug(f"Publisher {up_id}: Processing {len(api_store_apps)} API store applications")
        
        for store_item in api_store_apps:
            yield store_item
        

# ----------------------------------------------------------------------- #
# Handler helpers
# ----------------------------------------------------------------------- #
def _handler_groups(obj: Dict[str, Any]) -> List[ParsedItem]:
    company = obj["company"]
    groups = obj.get("groups", [])
    out: List[ParsedItem] = []

    # If no groups in API response, create a default group based on company
    if not groups:
        # Generate a consistent group_id based on company name/ticker hash
        company_name = company.get("name", "Unknown")
        company_ticker = company.get("ticker", "")
        
        # Create a deterministic group_id based on company info
        group_hash_input = f"{company_name}_{company_ticker}".lower()
        group_id = hash(group_hash_input) % (2**31)  # Ensure positive 32-bit int
        
        groups = [{
            "id": group_id,
            "name": company_name
        }]
        
        logger.info(f"Created default group {group_id} for company '{company_name}' (ticker: {company_ticker})")

    for g in groups:
        company_name = company.get("name", "Unknown")
        group_name = g.get("name", company_name)
        group_hash_input = f"{group_name}_{company.get('ticker', '')}".lower()
        group_id = hash(group_hash_input) % (2**31)
        logger.warning(f"Group had null ID, generated {group_id} for group '{group_name}'")
    
        out.append(
            ParsedItem(
                topic="appmagic.group",
                content={
                    "group_id": group_id,
                    "group_name": g.get("name", company["name"]),
                    "discovered_in_ticker": company.get("ticker"),
                    "first_seen_at": datetime.utcnow().isoformat(),
                },
            )
        )
    return out


def _handler_publishers(obj: Dict[str, Any]) -> List[ParsedItem]:
    company = obj["company"]
    pubs = obj.get("publishers", [])
    out: List[ParsedItem] = []

    # Calculate the group_id that should match the one created in _handler_groups
    company_name = company.get("name", "Unknown")
    company_ticker = company.get("ticker", "")
    group_hash_input = f"{company_name}_{company_ticker}".lower()
    expected_group_id = hash(group_hash_input) % (2**31)  # Same logic as _handler_groups

    for p in pubs:
        if not isinstance(p, dict):
            continue

        out.append(
            ParsedItem(
                topic="appmagic.publisher",
                content={
                    "united_publisher_id": p["id"],
                    "name": p.get("name"),
                    "headquarter_country_code": p.get("headquarter"), 
                    "linkedin_headcount": p.get("linkedin_headcount"),  
                    "min_release_date": p.get("min_release_date"), 
                    "first_app_ad_date": p.get("first_app_ad"), 
                    "group_id": expected_group_id,
                    "first_seen_at": datetime.utcnow().isoformat(),
                },
            )
        )

        # Handle both accounts and publisherIds formats
        accounts_data = p.get("accounts", []) or p.get("publisherIds", [])
        
        for i, acc in enumerate(accounts_data):
            # Extract store and publisher ID from the account object
            store_id = acc.get("storeId") or acc.get("store")
            store_publisher_id = acc.get("publisherId") or acc.get("store_publisher_id")
            
            # Construct HTML URL using the same logic as the fetcher
            html_url = None
            if store_id and store_publisher_id and p.get("name"):
                # Import here to avoid circular imports
                import re
                name_slug = re.sub(r"\s+", "-", p["name"].strip().lower())
                html_url = f"https://appmagic.rocks/publisher/{name_slug}/{store_id}_{store_publisher_id}"
            
            out.append(
                ParsedItem(
                    topic="appmagic.publisher.account",
                    content={
                        "store_id": store_id,
                        "store_publisher_id": store_publisher_id,
                        "united_publisher_id": p["id"],
                        "group_id": expected_group_id,  # Use the same group_id as the publisher
                        "html_url": html_url,
                        "first_seen_at": datetime.utcnow().isoformat(),
                    },
                )
            )

        if p.get("countryCode"):
            out.append(
                ParsedItem(
                    topic="country",
                    content={
                        "country_code": p["countryCode"],
                        "country_name": p.get("countryName") or "",
                        "first_seen_at": datetime.utcnow().isoformat(),
                    },
                )
            )
    return out

# --------------------------------------------------------------------------- #
# Helper functions
# --------------------------------------------------------------------------- #
def extract_worldwide_metrics(metrics_array):
    """
    Extract worldwide (WW) metrics from a metrics array.
    
    Args:
        metrics_array: List or dict containing metrics data
        
    Returns:
        tuple: (downloads, revenue) for worldwide data, or (None, None) if not found
    """
    if isinstance(metrics_array, list):
        # Look for WW country in the array
        ww_metric = next((m for m in metrics_array if isinstance(m, dict) and m.get("country") == "WW"), None)
        if ww_metric:
            return ww_metric.get("downloads"), ww_metric.get("revenue")
    elif isinstance(metrics_array, dict):
        # If it's a dict, assume it's already the metric we want
        return metrics_array.get("downloads"), metrics_array.get("revenue")
    
    return None, None


def _handler_metrics(obj: Dict[str, Any]) -> List[ParsedItem]:
    snap = obj.get("snapshot") or {}
    return [
        ParsedItem(
            topic="appmagic.application.metrics",
            content={
                "scrape_date": datetime.utcnow().date().isoformat(),
                "united_application_id": obj["ua_id"],
                "snapshot_30d_downloads": snap.get("downloads30d"),
                "snapshot_30d_revenue": snap.get("revenue30d"),
                "snapshot_lifetime_downloads": snap.get("downloadLifetime"),
                "snapshot_lifetime_revenue": snap.get("revenueLifetime"),
            },
        )
    ]


def _handler_country_metrics(obj: Dict[str, Any]) -> List[ParsedItem]:
    metrics = obj.get("metrics", [])
    out: List[ParsedItem] = []

    for m in metrics:
        out.append(
            ParsedItem(
                topic="appmagic.application.country_metrics",
                content={
                    "scrape_date": datetime.utcnow().date().isoformat(),
                    "united_application_id": obj.get("ua_id"),
                    "country_code": m.get("country"),
                    "snapshot_30d_downloads": m.get("downloads30d"),
                    "snapshot_30d_revenue": m.get("revenue30d"),
                },
            )
        )
    return out


def _handler_publisher_apps_api(obj: Dict[str, Any]) -> List[ParsedItem]:
    """Handle new search API response for publisher applications."""
    out: List[ParsedItem] = []
    up_id = obj.get("united_publisher_id")
    
    logger.info(f"Processing publisher_apps_api for publisher {up_id}")
    applications = obj.get("applications", [])
    logger.info(f"Found {len(applications)} applications in API data")
        
    # Process applications from API search results
    for i, app in enumerate(applications):
        ua_id = app.get("id")  # API uses 'id', not 'united_application_id'
        logger.debug(f"Processing app {i+1}/{len(applications)}: ua_id={ua_id}, name={app.get('name', 'unknown')}")
        
        if not ua_id:
            logger.warning(f"App {i+1} missing id: {app}")
            continue
            
        # Create application record with data_source marking
        out.append(
            ParsedItem(
                topic="appmagic.application",
                content={
                    "united_application_id": ua_id,
                    "united_publisher_id": up_id,
                    "name": app.get("name"),
                    "icon_url": app.get("icon_url") or app.get("icon"),
                    "release_date": app.get("releaseDate"),  # API uses 'releaseDate'
                    "contains_ads": app.get("contains_ads"),
                    "has_in_app_purchases": app.get("has_in_app_purchases"),
                    "data_source": "api",  # Mark as coming from API
                    "first_seen_at": datetime.utcnow().isoformat(),
                }
            )
        )
        
        # Create metrics record from API data with worldwide (WW) country focus
        metrics_30d = app.get("metrics_30d", [])
        metrics_lifetime = app.get("metrics_lifetime", [])
        
        # Extract worldwide metrics from the arrays using helper function
        downloads_30d, revenue_30d = extract_worldwide_metrics(metrics_30d)
        downloads_lifetime, revenue_lifetime = extract_worldwide_metrics(metrics_lifetime)
        
        out.append(
            ParsedItem(
                topic="appmagic.application.metrics",  # Changed to standard metrics topic
                content={
                    "scrape_date": datetime.utcnow().date().isoformat(),
                    "united_application_id": ua_id,
                    "snapshot_30d_downloads": downloads_30d,
                    "snapshot_30d_revenue": revenue_30d,
                    "snapshot_lifetime_downloads": downloads_lifetime,
                    "snapshot_lifetime_revenue": revenue_lifetime,
                }
            )
        )
            
        # Create store records from applicationIds array
        application_ids = app.get("applications", [])
        logger.debug(f"App {ua_id} has {len(application_ids)} applicationIds entries")
        
        for app_id_entry in application_ids:
            # The API returns "store" field, not "store_id" it is a list item so get first element
            store_id = app_id_entry.get("store")[0]  
            store_url = app_id_entry.get("url")
            store_app_id = app_id_entry.get("store_application_id")
            
            logger.debug(f"Creating store record: store_id={store_id}, store_app_id={store_app_id}, url={store_url}")
            
            out.append(
                ParsedItem(
                    topic="appmagic.application.store",
                    content={
                        "store_id": store_id,
                        "store_app_id_text": store_app_id,
                        "united_application_id": ua_id,
                        "name_on_store": app_id_entry.get("name"),
                        "store_url": store_url,
                        "first_seen_at": datetime.utcnow().isoformat(),
                    }
                )
            )
            
    return out


# Handler registry mapping source names to handler functions
_HANDLERS: Dict[str, Callable[[Dict[str, Any]], List[ParsedItem]]] = {
    "appmagic.groups": _handler_groups,
    "appmagic.publishers": _handler_publishers,
    "appmagic.metrics": _handler_metrics,
    "appmagic.country_metrics": _handler_country_metrics,
    "appmagic.publisher_apps_api": _handler_publisher_apps_api,
}
