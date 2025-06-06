# core/plugins/appmagic/parser.py
"""Transform stage 2 / 3 – RawItem → ParsedItem."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from core.interfaces import Transform
from core.models import ParsedItem, RawItem

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# HTML parsing utilities
# --------------------------------------------------------------------------- #
def parse_appmagic_metric(metric_str: str) -> int:
    """Parse AppMagic metric strings like '>$460 M', '1.2K', etc. to integers."""
    if not metric_str:
        return 0
    
    # Remove leading '>' and whitespace
    clean = metric_str.lstrip('>$ ').strip()
    if not clean:
        return 0
    
    # Extract number and suffix
    parts = clean.split()
    if not parts:
        return 0
    
    number_part = parts[0].replace(',', '')
    
    # Handle suffixes K, M, B
    multiplier = 1
    if number_part.endswith('K'):
        multiplier = 1_000
        number_part = number_part[:-1]
    elif number_part.endswith('M'):
        multiplier = 1_000_000
        number_part = number_part[:-1]
    elif number_part.endswith('B'):
        multiplier = 1_000_000_000
        number_part = number_part[:-1]
    
    try:
        return int(float(number_part) * multiplier)
    except (ValueError, TypeError):
        return 0


def parse_release_date(date_str: str) -> Optional[str]:
    """Parse HTML release date string 'MMM DD, YYYY' to 'YYYY-MM-DD' format."""
    if not date_str:
        return None
    
    # Handle format like "Jan 15, 2023"
    months = {
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
        'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
        'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
    }
    
    try:
        parts = date_str.replace(',', '').split()
        if len(parts) == 3:
            month_name, day, year = parts
            month_num = months.get(month_name)
            if month_num:
                return f"{year}-{month_num}-{day.zfill(2)}"
    except (ValueError, IndexError):
        pass
    
    return None


def extract_united_application_id_from_href(href: str) -> Optional[int]:
    """Extract united_application_id from href like '/app/{store}/{store_specific_id}'."""
    if not href:
        return None
    
    # Look for patterns like /app/1/123456789
    match = re.search(r'/app/(\d+)/([^/]+)', href)
    if match:
        # For now, we don't have a reliable way to get united_application_id from href
        # This would need to be looked up or derived differently
        return None
    
    return None


class AppMagicParser(Transform):
    # ------------------------------------------------------------------- #
    @property
    def name(self) -> str:  # noqa: D401
        return "AppMagicParser"

    # ------------------------------------------------------------------- #
    def __init__(self):
        # Store API and HTML data temporarily for merging
        self._api_data_by_publisher: Dict[int, List[ParsedItem]] = {}
        self._html_data_by_publisher: Dict[int, List[ParsedItem]] = {}

    # ------------------------------------------------------------------- #
    async def __call__(self, items: AsyncIterator[Any]) -> AsyncIterator[ParsedItem]:
        # First pass: collect all data
        all_items = []
        async for item in items:
            if not isinstance(item, RawItem):
                yield item
                continue
            all_items.append(item)

        # Process items and group by publisher
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

            parsed_items = handler(payload)
            
            # For API and HTML data, store for merging
            if item.source == "appmagic.publisher_apps_api":
                up_id = payload.get("united_publisher_id")
                if up_id:
                    if up_id not in self._api_data_by_publisher:
                        self._api_data_by_publisher[up_id] = []
                    self._api_data_by_publisher[up_id].extend(parsed_items)
                continue
            elif item.source in ["appmagic.publisher_html", "appmagic.html"]:
                up_id = payload.get("up_id")
                if up_id:
                    if up_id not in self._html_data_by_publisher:
                        self._html_data_by_publisher[up_id] = []
                    self._html_data_by_publisher[up_id].extend(parsed_items)
                continue
            
            # For other sources, yield immediately
            for parsed in parsed_items:
                yield parsed

        # Now merge API and HTML data
        all_publishers = set(self._api_data_by_publisher.keys()) | set(self._html_data_by_publisher.keys())
        
        for up_id in all_publishers:
            async for merged_item in self._merge_publisher_data(up_id):
                yield merged_item

    # ------------------------------------------------------------------- #
    async def _merge_publisher_data(self, up_id: int) -> AsyncIterator[ParsedItem]:
        """Merge API and HTML data for a single publisher."""
        api_items = self._api_data_by_publisher.get(up_id, [])
        html_items = self._html_data_by_publisher.get(up_id, [])
        
        # Separate API items by type
        api_apps = [item for item in api_items if item.topic == "appmagic.publisher_apps_api"]
        api_metrics = [item for item in api_items if item.topic == "appmagic.publisher_apps_metrics"]
        
        # Separate HTML items by type  
        html_apps = [item for item in html_items if item.topic == "appmagic.publisher_html"]
        non_app_html = [item for item in html_items if item.topic != "appmagic.publisher_html"]
        
        # Yield non-app HTML items as-is
        for item in non_app_html:
            yield item

        # Index by united_application_id
        api_apps_by_id = {item.content["united_application_id"]: item for item in api_apps}
        api_metrics_by_id = {item.content["united_application_id"]: item for item in api_metrics}
        html_apps_by_id = {}
        
        # For HTML apps, we might not have united_application_id, so we'll try to match by name
        for html_item in html_apps:
            ua_id = html_item.content.get("united_application_id")
            if ua_id:
                html_apps_by_id[ua_id] = html_item
            else:
                # Store by name for fallback matching
                name = html_item.content.get("name")
                if name:
                    html_apps_by_id[f"name:{name}"] = html_item

        # Merge data
        all_app_ids = set(api_apps_by_id.keys()) | set(k for k in html_apps_by_id.keys() if isinstance(k, int))
        merged_count = 0
        api_only_count = 0
        html_only_count = 0

        for ua_id in all_app_ids:
            api_app = api_apps_by_id.get(ua_id)
            api_metric = api_metrics_by_id.get(ua_id)
            html_app = html_apps_by_id.get(ua_id)
            
            # Try name matching if no direct ID match
            if not html_app and api_app:
                api_name = api_app.content.get("name")
                if api_name:
                    html_app = html_apps_by_id.get(f"name:{api_name}")

            # Determine data source
            if api_app and html_app:
                data_source = "html_and_api"
                merged_count += 1
            elif api_app:
                data_source = "api_only"
                api_only_count += 1
            else:
                data_source = "html_only"
                html_only_count += 1

            # Create merged app data
            merged_app_data = {}
            
            if api_app:
                merged_app_data.update(api_app.content)
            
            if html_app:
                # HTML data takes precedence for certain fields
                html_content = html_app.content
                merged_app_data.update({
                    "icon_url": html_content.get("icon_url") or merged_app_data.get("icon_url"),
                    "name": html_content.get("name") or merged_app_data.get("name"),
                })
                
                # Use HTML release date if API doesn't have it
                if not merged_app_data.get("release_date") and html_content.get("release_date"):
                    merged_app_data["release_date"] = html_content["release_date"]

            merged_app_data["data_source"] = data_source
            
            # Emit merged UnitedApplications item
            yield ParsedItem(
                topic="appmagic.application",
                content=merged_app_data,
            )
            
            # Create merged metrics data
            merged_metrics_data = {
                "scrape_date": datetime.utcnow().date().isoformat(),
                "united_application_id": ua_id,
            }
            
            if api_metric:
                merged_metrics_data.update({
                    "snapshot_30d_downloads": api_metric.content.get("snapshot_30d_downloads"),
                    "snapshot_30d_revenue": api_metric.content.get("snapshot_30d_revenue"),
                })
            
            if html_app:
                merged_metrics_data.update({
                    "snapshot_lifetime_downloads": html_app.content.get("lifetime_downloads_val"),
                    "snapshot_lifetime_revenue": html_app.content.get("lifetime_revenue_val"),
                })
                
            yield ParsedItem(
                topic="appmagic.application.metrics",
                content=merged_metrics_data,
            )

        # Handle HTML-only apps that couldn't be matched by ID or name
        for key, html_app in html_apps_by_id.items():
            if isinstance(key, str) and key.startswith("name:"):
                # This is a name-based entry that wasn't matched
                name = key[5:]  # Remove "name:" prefix
                
                # Check if this name was already processed
                name_already_processed = any(
                    api_app.content.get("name") == name for api_app in api_apps_by_id.values()
                )
                
                if not name_already_processed:
                    html_only_count += 1
                    html_content = html_app.content.copy()
                    html_content["data_source"] = "html_only"
                    
                    # Generate a temporary ID for HTML-only apps
                    temp_ua_id = hash(f"html_{up_id}_{name}") % (2**31)
                    html_content["united_application_id"] = temp_ua_id
                    
                    yield ParsedItem(
                        topic="appmagic.application",
                        content=html_content,
                    )
                    
                    # Emit metrics for HTML-only app
                    metrics_data = {
                        "scrape_date": datetime.utcnow().date().isoformat(),
                        "united_application_id": temp_ua_id,
                        "snapshot_30d_downloads": None,
                        "snapshot_30d_revenue": None,
                        "snapshot_lifetime_downloads": html_content.get("lifetime_downloads_val"),
                        "snapshot_lifetime_revenue": html_content.get("lifetime_revenue_val"),
                    }
                    
                    yield ParsedItem(
                        topic="appmagic.application.metrics",
                        content=metrics_data,
                    )

        # Emit summary counts
        yield ParsedItem(
            topic="appmagic.publisher_apps_summary",
            content={
                "united_publisher_id": up_id,
                "html_only_count": html_only_count,
                "api_only_count": api_only_count,
                "merged_count": merged_count,
                "scrape_date": datetime.utcnow().date().isoformat(),
            },
        )
        

# ----------------------------------------------------------------------- #
# Handler helpers
# ----------------------------------------------------------------------- #
def _handler_groups(obj: Dict[str, Any]) -> List[ParsedItem]:
    company = obj["company"]
    groups = obj.get("groups", []) or [{"id": None, "name": company["name"]}]
    out: List[ParsedItem] = []

    for g in groups:
        out.append(
            ParsedItem(
                topic="appmagic.group",
                content={
                    "group_id": g["id"],
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

    for p in pubs:
        if not isinstance(p, dict):
            continue

        out.append(
            ParsedItem(
                topic="appmagic.publisher",
                content={
                    "united_publisher_id": p["id"],
                    "name": p.get("name"),
                    "headquarter_country_code": p.get("countryCode"),
                    "linkedin_headcount": p.get("linkedinHeadcount"),
                    "min_release_date": p.get("minReleaseDate"),
                    "first_app_ad_date": p.get("firstAppAdDate"),
                    "group_id": p.get("groupId"),
                    "first_seen_at": datetime.utcnow().isoformat(),
                },
            )
        )

        for acc in p.get("accounts", []):
            out.append(
                ParsedItem(
                    topic="appmagic.publisher.account",
                    content={
                        "store_id": acc.get("storeId") or acc.get("store"),
                        "store_publisher_id": acc.get("publisherId")
                        or acc.get("store_publisher_id"),
                        "united_publisher_id": p["id"],
                        "group_id": p.get("groupId"),
                        "html_url": acc.get("htmlUrl"),
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


def _handler_apps(obj: Dict[str, Any]) -> List[ParsedItem]:
    up_id = obj["up_id"]
    apps = obj.get("apps", [])
    out: List[ParsedItem] = []

    for a in apps:
        ua_id = a.get("id")
        if not ua_id:
            continue

        out.append(
            ParsedItem(
                topic="appmagic.application",
                content={
                    "united_application_id": ua_id,
                    "united_publisher_id": up_id,
                    "name": a.get("name"),
                    "icon_url": a.get("iconUrl") or a.get("icon"),
                    "release_date": a.get("releaseDate"),
                    "contains_ads": a.get("containsAds"),
                    "has_in_app_purchases": a.get("hasInAppPurchases"),
                    "first_seen_at": datetime.utcnow().isoformat(),
                },
            )
        )

        for sa in a.get("applications", []):
            out.append(
                ParsedItem(
                    topic="appmagic.application.store",
                    content={
                        "store_id": sa.get("store"),
                        "store_app_id_text": sa.get("storeAppId") or sa.get("bundleId"),
                        "united_application_id": ua_id,
                        "name_on_store": sa.get("name"),
                        "store_url": sa.get("url"),
                        "first_seen_at": datetime.utcnow().isoformat(),
                    },
                )
            )

        for tag in a.get("tags", []):
            out.append(
                ParsedItem(
                    topic="appmagic.application.tag_link",
                    content={
                        "united_application_id": ua_id,
                        "tag_id": tag.get("id"),
                        "first_associated_at": datetime.utcnow().isoformat(),
                    },
                )
            )
    return out


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
    
    # Process applications from API search results
    for app in obj.get("applications", []):
        ua_id = app.get("united_application_id")
        if not ua_id:
            continue
            
        # Create application record
        out.append(
            ParsedItem(
                topic="appmagic.publisher_apps_api",
                content={
                    "united_application_id": ua_id,
                    "united_publisher_id": up_id,
                    "name": app.get("name"),
                    "icon_url": app.get("icon_url"),
                    "release_date": app.get("release_date"),
                    "contains_ads": app.get("contains_ads"),
                    "has_in_app_purchases": app.get("has_in_app_purchases"),
                    "first_seen_at": datetime.utcnow().isoformat(),
                }
            )
        )
        
        # Create metrics record if snapshot data exists
        snapshot = app.get("snapshot", {})
        if snapshot:
            out.append(
                ParsedItem(
                    topic="appmagic.publisher_apps_metrics", 
                    content={
                        "scrape_date": datetime.utcnow().date().isoformat(),
                        "united_application_id": ua_id,
                        "snapshot_30d_downloads": snapshot.get("downloads_30d"),
                        "snapshot_30d_revenue": snapshot.get("revenue_30d"),
                        "snapshot_lifetime_downloads": snapshot.get("downloads_lifetime"),
                        "snapshot_lifetime_revenue": snapshot.get("revenue_lifetime"),
                    }
                )
            )
            
        # Create store records
        for store_app in app.get("store_applications", []):
            out.append(
                ParsedItem(
                    topic="appmagic.application.store",
                    content={
                        "store_id": store_app.get("store_id"),
                        "store_app_id_text": store_app.get("store_app_id"),
                        "united_application_id": ua_id,
                        "name_on_store": store_app.get("name"),
                        "store_url": store_app.get("url"),
                        "first_seen_at": datetime.utcnow().isoformat(),
                    }
                )
            )
            
    return out


def _handler_publisher_html(obj: Dict[str, Any]) -> List[ParsedItem]:
    """Enhanced HTML parser that extracts app rows from publisher pages."""
    out: List[ParsedItem] = []
    up_id = obj.get("up_id")
    html_content = obj.get("html", "")
    
    if not html_content:
        return out
        
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Look for app rows in the HTML structure
    # This depends on AppMagic's HTML structure - adjust selectors as needed
    app_rows = soup.find_all("tr", class_=lambda x: x and "app-row" in x) or \
               soup.find_all("div", class_=lambda x: x and "app-item" in x) or \
               soup.select(".app-list tr") or \
               soup.select("[data-app-id]")
    
    for row in app_rows:
        try:
            # Extract basic app info
            name_elem = row.find("a", class_="app-name") or row.find(".app-title") or row.find("td", class_="name")
            icon_elem = row.find("img", src=True)
            
            # Extract metrics
            downloads_elem = row.find(text=re.compile(r'\d+[KMB]?\s*(downloads?|DL)', re.I))
            revenue_elem = row.find(text=re.compile(r'\$\d+[KMB]?', re.I))
            release_date_elem = row.find(text=re.compile(r'\w{3}\s+\d{1,2},\s+\d{4}'))
            
            # Try to extract united_application_id from href
            ua_id = None
            if name_elem and name_elem.get("href"):
                ua_id = extract_united_application_id_from_href(name_elem["href"])
            
            # Parse values
            name = name_elem.get_text(strip=True) if name_elem else None
            icon_url = icon_elem["src"] if icon_elem else None
            
            lifetime_downloads_val = parse_appmagic_metric(downloads_elem) if downloads_elem else None
            lifetime_revenue_val = parse_appmagic_metric(revenue_elem) if revenue_elem else None
            release_date = parse_release_date(release_date_elem) if release_date_elem else None
            
            if name:  # Only process if we have a name
                app_data = {
                    "united_publisher_id": up_id,
                    "name": name,
                    "icon_url": icon_url,
                    "release_date": release_date,
                    "lifetime_downloads_val": lifetime_downloads_val,
                    "lifetime_revenue_val": lifetime_revenue_val,
                    "first_seen_at": datetime.utcnow().isoformat(),
                }
                
                if ua_id:
                    app_data["united_application_id"] = ua_id
                    
                out.append(
                    ParsedItem(
                        topic="appmagic.publisher_html",
                        content=app_data
                    )
                )
                
        except (AttributeError, ValueError) as e:
            logger.debug(f"Error parsing app row: {e}")
            continue
    
    return out


# Handler registry mapping source names to handler functions
_HANDLERS: Dict[str, Callable[[Dict[str, Any]], List[ParsedItem]]] = {
    "appmagic.groups": _handler_groups,
    "appmagic.publishers": _handler_publishers,
    "appmagic.apps": _handler_apps,
    "appmagic.metrics": _handler_metrics,
    "appmagic.country_metrics": _handler_country_metrics,
    "appmagic.publisher_apps_api": _handler_publisher_apps_api,
    "appmagic.publisher_html": _handler_publisher_html,
    "appmagic.html": _handler_publisher_html,  # Backward compatibility
}
