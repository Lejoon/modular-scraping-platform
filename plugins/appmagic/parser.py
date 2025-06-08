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
            
            # For API and HTML data, store for merging
            if item.source == "appmagic.publisher_apps_api":
                up_id = payload.get("united_publisher_id")
                if up_id:
                    if up_id not in self._api_data_by_publisher:
                        self._api_data_by_publisher[up_id] = []
                    self._api_data_by_publisher[up_id].extend(parsed_items)
                    logger.debug(f"Stored {len(parsed_items)} API items for publisher {up_id}")
                continue
            elif item.source in ["appmagic.publisher_html", "appmagic.html"]:
                up_id = payload.get("up_id")
                if up_id:
                    if up_id not in self._html_data_by_publisher:
                        self._html_data_by_publisher[up_id] = []
                    self._html_data_by_publisher[up_id].extend(parsed_items)
                    logger.debug(f"Stored {len(parsed_items)} HTML items for publisher {up_id}")
                continue
            
            # For other sources, yield immediately
            immediate_yield_count = 0
            for parsed in parsed_items:
                yield parsed
                immediate_yield_count += 1
            logger.debug(f"Immediately yielded {immediate_yield_count} items for {item.source}")

        # Now merge API and HTML data
        all_publishers = set(self._api_data_by_publisher.keys()) | set(self._html_data_by_publisher.keys())
        logger.info(f"AppMagicParser: Merging data for {len(all_publishers)} publishers")
        
        total_merged_items = 0
        for up_id in all_publishers:
            logger.debug(f"Merging data for publisher {up_id}")
            publisher_items = 0
            async for merged_item in self._merge_publisher_data(up_id):
                yield merged_item
                publisher_items += 1
                total_merged_items += 1
            logger.debug(f"Generated {publisher_items} merged items for publisher {up_id}")
        
        logger.info(f"AppMagicParser: Completed parsing - {total_merged_items} total merged items generated")

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

        # Handle both accounts and publisherIds formats
        accounts_data = p.get("accounts", []) or p.get("publisherIds", [])
        for acc in accounts_data:
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
    
    logger.info(f"Processing publisher_apps_api for publisher {up_id}")
    applications = obj.get("applications", [])
    logger.info(f"Found {len(applications)} applications in API data")
    
    if len(applications) > 0:
        # Log a sample of the first app to see structure
        sample_app = applications[0]
        logger.debug(f"Sample app structure: {list(sample_app.keys()) if isinstance(sample_app, dict) else type(sample_app)}")
        if isinstance(sample_app, dict):
            logger.debug(f"Sample app data: {dict(list(sample_app.items())[:5])}")  # First 5 fields
    
    # Process applications from API search results
    for i, app in enumerate(applications):
        ua_id = app.get("id")  # API uses 'id', not 'united_application_id'
        logger.debug(f"Processing app {i+1}/{len(applications)}: ua_id={ua_id}, name={app.get('name', 'unknown')}")
        
        if not ua_id:
            logger.warning(f"App {i+1} missing id: {app}")
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
                    "release_date": app.get("releaseDate"),  # API uses 'releaseDate'
                    "contains_ads": app.get("contains_ads"),
                    "has_in_app_purchases": app.get("has_in_app_purchases"),
                    "first_seen_at": datetime.utcnow().isoformat(),
                }
            )
        )
        
        # Create metrics record if snapshot data exists
        metrics_30d = app.get("metrics_30d")
        metrics_lifetime = app.get("metrics_lifetime")
        
        # Handle different possible structures for metrics
        downloads_30d = None
        revenue_30d = None
        downloads_lifetime = None
        revenue_lifetime = None
        
        if isinstance(metrics_30d, dict):
            downloads_30d = metrics_30d.get("downloads")
            revenue_30d = metrics_30d.get("revenue")
        elif isinstance(metrics_30d, list) and metrics_30d:
            # If it's a list, take the first item
            first_30d = metrics_30d[0] if isinstance(metrics_30d[0], dict) else {}
            downloads_30d = first_30d.get("downloads")
            revenue_30d = first_30d.get("revenue")
            
        if isinstance(metrics_lifetime, dict):
            downloads_lifetime = metrics_lifetime.get("downloads")
            revenue_lifetime = metrics_lifetime.get("revenue")
        elif isinstance(metrics_lifetime, list) and metrics_lifetime:
            # If it's a list, take the first item
            first_lifetime = metrics_lifetime[0] if isinstance(metrics_lifetime[0], dict) else {}
            downloads_lifetime = first_lifetime.get("downloads")
            revenue_lifetime = first_lifetime.get("revenue")
        
        # Also check direct fields on the app object
        if downloads_30d is None:
            downloads_30d = app.get("downloads")
        if revenue_30d is None:
            revenue_30d = app.get("revenue")
        
        if any([downloads_30d, revenue_30d, downloads_lifetime, revenue_lifetime]):
            out.append(
                ParsedItem(
                    topic="appmagic.publisher_apps_metrics", 
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
            
        # Create store records - check different possible field names
        store_apps = app.get("applications", []) or app.get("store_applications", [])
        for store_app in store_apps:
            out.append(
                ParsedItem(
                    topic="appmagic.application.store",
                    content={
                        "store_id": store_app.get("store_id") or store_app.get("store"),
                        "store_app_id_text": store_app.get("store_app_id") or store_app.get("bundleId") or store_app.get("storeAppId"),
                        "united_application_id": ua_id,
                        "name_on_store": store_app.get("name"),
                        "store_url": store_app.get("url"),
                        "first_seen_at": datetime.utcnow().isoformat(),
                    }
                )
            )
            
    return out


def extract_app_data(app_row):
    """
    Extract app data from a single app row element.
    
    Args:
        app_row: BeautifulSoup element representing an app row
        
    Returns:
        dict: App data with all relevant fields
    """
    app_data = {
        "name": "N/A",
        "appmagic_id_path": "N/A", 
        "store_specific_id": "N/A",
        "genres": [],
        "countries": 0,
        "first_release": "N/A",
        "lifetime_revenue_str": "N/A",
        "lifetime_revenue_val": 0,
        "lifetime_downloads_str": "N/A",
        "lifetime_downloads_val": 0,
    }
    
    # Extract app name and IDs
    name_anchor = app_row.select_one('div.col-1 app-name-stores a.g-app-name')
    if name_anchor:
        app_data["name"] = name_anchor.get_text(strip=True)
        href = name_anchor.get('href')
        if href:
            app_data["appmagic_id_path"] = href
            parts = href.strip('/').split('/')
            if len(parts) > 0: 
                app_data["store_specific_id"] = parts[-1]
    
    # Extract genres
    genre_tags_container = app_row.select_one('div.col-2 div.tags-wrap')
    if genre_tags_container:
        no_value_genre = genre_tags_container.select_one('div.no-value')
        if not no_value_genre or no_value_genre.get_text(strip=True) not in ["-", "—"]:  # Check for both
            genre_spans = genre_tags_container.select('div.tag span.text-overflow')
            app_data["genres"] = [span.get_text(strip=True) for span in genre_spans if span.get_text(strip=True)]

    # Extract countries count
    countries_span = app_row.select_one('div.col-3 span.dashed')
    if countries_span:
        try: 
            app_data["countries"] = int(countries_span.get_text(strip=True))
        except ValueError: 
            app_data["countries"] = 0
    
    # Extract release date
    release_date_span = app_row.select_one('div.col-4 app-release-date span.release-date span')
    if release_date_span: 
        app_data["first_release"] = release_date_span.get_text(strip=True)

    # Extract revenue data
    revenue_div = app_row.select_one('div.col-5')
    if revenue_div:
        text_nodes = [node.strip() for node in revenue_div.find_all(string=True, recursive=False) if node.strip()]
        if text_nodes:
            revenue_str = text_nodes[0]
            # Convert em dash to regular dash for better readability
            if revenue_str == "—":
                revenue_str = "-"
            app_data["lifetime_revenue_str"] = revenue_str
            app_data["lifetime_revenue_val"] = parse_appmagic_metric(revenue_str)
    
    # Extract downloads data
    downloads_div = app_row.select_one('div.col-6')
    if downloads_div:
        text_nodes = [node.strip() for node in downloads_div.find_all(string=True, recursive=False) if node.strip()]
        if text_nodes:
            downloads_str = text_nodes[0]
            # Convert em dash to regular dash for better readability
            if downloads_str == "—":
                downloads_str = "-"
            app_data["lifetime_downloads_str"] = downloads_str
            app_data["lifetime_downloads_val"] = parse_appmagic_metric(downloads_str)
    
    return app_data


def extract_publisher_info(soup):
    """
    Extract publisher information from the page.
    
    Args:
        soup: BeautifulSoup object of the entire page
        
    Returns:
        dict: Publisher information
    """
    publisher_info = {
        "name": "N/A",
        "total_apps": 0,
        "total_downloads": "N/A",
        "total_revenue": "N/A"
    }
    
    # Try to extract publisher name from page title or header
    title_elem = soup.find('title')
    if title_elem:
        title_text = title_elem.get_text(strip=True)
        # Publisher name is often in the title like "Publisher Name - AppMagic"
        if " - " in title_text:
            publisher_info["name"] = title_text.split(" - ")[0].strip()
    
    # Try to extract publisher stats if available
    stats_container = soup.select_one('.publisher-stats, .stats-container')
    if stats_container:
        # Look for total apps count
        apps_stat = stats_container.find(text=re.compile(r'\d+\s*apps?', re.I))
        if apps_stat:
            match = re.search(r'(\d+)', apps_stat)
            if match:
                publisher_info["total_apps"] = int(match.group(1))
    
    return publisher_info


def parse_publisher_page_html(html_content):
    """
    Parse AppMagic publisher page HTML to extract structured data.
    
    Args:
        html_content (str): Raw HTML content from the publisher page
        
    Returns:
        dict: Structured data with keys:
            - publisher_info: Publisher metadata
            - apps: List of app data dictionaries
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Extract publisher information
    publisher_info = extract_publisher_info(soup)
    
    # Extract apps data
    apps_data_list = []
    app_rows_container = soup.select_one('div.table.ng-star-inserted')
    
    if not app_rows_container:
        logger.debug("Warning: Could not find the main app table container.")
        return {"publisher_info": publisher_info, "apps": apps_data_list}
        
    app_rows = app_rows_container.find_all('publisher-app-row', class_='g-item', recursive=False)
    if not app_rows:
        logger.debug("Warning: No app rows found within the table container.")
        return {"publisher_info": publisher_info, "apps": apps_data_list}

    for row in app_rows:
        app_data = extract_app_data(row)
        apps_data_list.append(app_data)
    
    return {"publisher_info": publisher_info, "apps": apps_data_list}


def _handler_publisher_html(obj: Dict[str, Any]) -> List[ParsedItem]:
    """Enhanced HTML parser that extracts app rows from publisher pages using sophisticated selectors."""
    out: List[ParsedItem] = []
    up_id = obj.get("up_id")
    html_content = obj.get("html", "")
    
    if not html_content:
        return out
    
    logger.debug(f"Parsing HTML content for publisher {up_id}: {len(html_content)} characters")
    
    try:
        # Use the sophisticated parser
        parsed_data = parse_publisher_page_html(html_content)
        publisher_info = parsed_data["publisher_info"]
        apps_data = parsed_data["apps"]
        
        logger.debug(f"Found {len(apps_data)} apps for publisher {up_id}")
        
        # Convert each app to a ParsedItem
        for app_data in apps_data:
            # Enhance with united_publisher_id and timestamp
            enhanced_app_data = {
                **app_data,
                "united_publisher_id": up_id,
                "first_seen_at": datetime.utcnow().isoformat(),
                "publisher_name": publisher_info.get("name", "N/A")
            }
            
            # Try to extract united_application_id from href if available
            if app_data.get("appmagic_id_path"):
                ua_id = extract_united_application_id_from_href(app_data["appmagic_id_path"])
                if ua_id:
                    enhanced_app_data["united_application_id"] = ua_id
            
            out.append(
                ParsedItem(
                    topic="appmagic.publisher_html",
                    content=enhanced_app_data
                )
            )
        
        # Also create a publisher info item
        if publisher_info["name"] != "N/A":
            publisher_data = {
                **publisher_info,
                "united_publisher_id": up_id,
                "apps_count": len(apps_data),
                "parsed_at": datetime.utcnow().isoformat()
            }
            
            out.append(
                ParsedItem(
                    topic="appmagic.publisher_info",
                    content=publisher_data
                )
            )
            
    except Exception as e:
        logger.warning(f"Error parsing HTML for publisher {up_id}: {e}")
        # Fallback to basic parsing
        soup = BeautifulSoup(html_content, "html.parser")
        
        # Look for any app-related content as fallback
        app_links = soup.find_all("a", href=re.compile(r'/app/'))
        logger.debug(f"Fallback parser found {len(app_links)} potential app links")
        
        for link in app_links:
            name = link.get_text(strip=True)
            href = link.get("href", "")
            
            if name and href:
                fallback_data = {
                    "united_publisher_id": up_id,
                    "name": name,
                    "appmagic_id_path": href,
                    "first_seen_at": datetime.utcnow().isoformat(),
                    "parsing_method": "fallback"
                }
                
                out.append(
                    ParsedItem(
                        topic="appmagic.publisher_html",
                        content=fallback_data
                    )
                )
    
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
