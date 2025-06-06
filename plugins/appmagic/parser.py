# core/plugins/appmagic/parser.py
"""Transform stage 2 / 3 – RawItem → ParsedItem."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, AsyncIterator, Callable, Dict, List

from bs4 import BeautifulSoup

from core.interfaces import Transform
from core.models import ParsedItem, RawItem

logger = logging.getLogger(__name__)


class AppMagicParser(Transform):
    # ------------------------------------------------------------------- #
    @property
    def name(self) -> str:  # noqa: D401
        return "AppMagicParser"

    # ------------------------------------------------------------------- #
    async def __call__(self, items: AsyncIterator[Any]) -> AsyncIterator[ParsedItem]:
        async for item in items:
            if not isinstance(item, RawItem):
                yield item
                continue

            try:
                payload = json.loads(item.payload)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Bad JSON in %s: %s", item.source, exc)
                continue

            handler = _HANDLERS.get(item.source)
            if handler is None:
                logger.debug("No parser for %s", item.source)
                continue

            for parsed in handler(payload):
                yield parsed


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
    up_id = obj["united_publisher_id"]
    data = obj.get("countries", {})
    out: List[ParsedItem] = []

    datasets: Dict[str, List[dict]]
    if isinstance(data, list):
        datasets = {"Last30Days": data}
    else:
        datasets = {
            "Last30Days": data.get("last30Days") or data.get("last30days") or [],
            "Lifetime": data.get("lifetime") or [],
        }

    for span, rows in datasets.items():
        for row in rows or []:
            out.append(
                ParsedItem(
                    topic="appmagic.publisher.country.metrics",
                    content={
                        "scrape_date": datetime.utcnow().date().isoformat(),
                        "united_publisher_id": up_id,
                        "country_code": row.get("countryCode"),
                        "metric_timespan": span,
                        "revenue": row.get("revenue") or row.get("revenueUsd"),
                        "downloads": row.get("downloads"),
                        "revenue_percent": row.get("revenuePercent"),
                        "downloads_percent": row.get("downloadsPercent"),
                    },
                )
            )
    return out


def _handler_html(obj: Dict[str, Any]) -> List[ParsedItem]:
    html = obj.get("html", "")
    if not html:
        return []

    up_id = obj["up_id"]
    soup = BeautifulSoup(html, "html.parser")
    out: List[ParsedItem] = []

    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        out.append(
            ParsedItem(
                topic="appmagic.publisher.account",
                content={
                    "united_publisher_id": up_id,
                    "html_url": obj.get("html_url"),
                    "icon_url": og["content"],
                    "last_updated_at": datetime.utcnow().isoformat(),
                },
            )
        )

    for tag_node in soup.select("a.tag, span.tag"):
        txt = tag_node.text.strip()
        if txt:
            out.append(
                ParsedItem(
                    topic="appmagic.application.tag_link",
                    content={
                        "united_application_id": 0,  # unknown here
                        "tag_id": txt,
                        "first_associated_at": datetime.utcnow().isoformat(),
                    },
                )
            )
    return out


# ----------------------------------------------------------------------- #
_HANDLERS: Dict[str, Callable[[Dict[str, Any]], List[ParsedItem]]] = {
    "appmagic.groups": _handler_groups,
    "appmagic.publishers": _handler_publishers,
    "appmagic.apps": _handler_apps,
    "appmagic.app.metrics": _handler_metrics,
    "appmagic.publisher.country.metrics": _handler_country_metrics,
    "appmagic.html": _handler_html,
}

# Make `appmagic.AppMagicParser` resolvable ------------------------------
import sys as _sys  # noqa: E402

_pkg = _sys.modules.get(__name__.rsplit(".", 1)[0])
if _pkg is not None:
    setattr(_pkg, "AppMagicParser", AppMagicParser)
