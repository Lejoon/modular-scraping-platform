# core/plugins/appmagic/parser.py
"""Parser stage that converts AppMagic JSON payloads into normalised
:class:`~core.models.ParsedItem` instances understood by *AppMagicSink*.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import AsyncIterator, Callable, Dict, Iterable, List, Any

from core.interfaces import Transform
from core.models import RawItem, ParsedItem

logger = logging.getLogger(__name__)


class AppMagicParser(Transform):
    """Stateless transform – RawItem -> zero..n ParsedItems."""

    name = "AppMagicParser"

    # ------------------------------------------------------------------- #
    # Transform interface
    # ------------------------------------------------------------------- #

    async def __call__(self, items: AsyncIterator[Any]) -> AsyncIterator[ParsedItem]:  # type: ignore[override]
        async for item in items:
            if not isinstance(item, RawItem):
                # Passthrough other objects
                continue

            try:
                payload = json.loads(item.payload)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Malformed JSON in %s: %s", item.source, exc)
                continue

            handler = _HANDLERS.get(item.source)
            if handler is None:
                logger.debug("No handler for %s", item.source)
                continue

            for parsed in handler(payload, item.source):
                yield parsed

# ----------------------------------------------------------------------- #
# Internal helpers
# ----------------------------------------------------------------------- #

def _handler_groups(obj: Dict[str, Any], source: str) -> List[ParsedItem]:
    company = obj["company"]
    groups = obj.get("groups", [])

    # Each group is a distinct row.
    out: List[ParsedItem] = []
    for g in groups or [{"id": None, "name": company["name"]}]:
        content = {
            "group_id": g["id"] if g["id"] is not None else None,
            "group_name": g.get("name", company["name"]),
            "discovered_in_ticker": company.get("ticker"),
            "first_seen_at": datetime.utcnow().isoformat(),
        }
        out.append(
            ParsedItem(
                topic="appmagic.group",
                content=content,
            )
        )
    return out



def _handler_publishers(obj: Dict[str, Any], source: str) -> List[ParsedItem]:
    company = obj["company"]
    pubs = obj.get("publishers", [])
    out: List[ParsedItem] = []

    for p in pubs:
        # Main publisher entity
        out.append(
            ParsedItem(
                topic="appmagic.publisher",
                content={
                    "united_publisher_id": p["id"],
                    "name": p.get("name"),
                    "hq_country_code": p.get("countryCode"),
                    "linkedin_headcount": p.get("linkedinHeadcount"),
                    "min_release_date": p.get("minReleaseDate"),
                    "first_app_ad_date": p.get("firstAppAdDate"),
                    "group_id": p.get("groupId"),
                    "first_seen_at": datetime.utcnow().isoformat(),
                },
            )
        )

        # One row per store account
        for acc in p.get("accounts", []):
            out.append(
                ParsedItem(
                    topic="appmagic.publisher.account",
                    content={
                        "store_id": acc.get("storeId"),
                        "store_publisher_id": acc.get("publisherId"),
                        "united_publisher_id": p["id"],
                        "group_id": p.get("groupId"),
                        "html_url": acc.get("url"),
                        "first_seen_at": datetime.utcnow().isoformat(),
                    },
                )
            )

        # Emit country reference so DB can satisfy FK
        if p.get("countryCode"):
            out.append(
                ParsedItem(
                    topic="country",
                    content={
                        "country_code": p.get("countryCode"),
                        "country_name": p.get("countryName") or "",
                    },
                )
            )

    return out


def _handler_apps(obj: Dict[str, Any], source: str) -> List[ParsedItem]:
    company = obj["company"]
    up_id = obj["up_id"]
    apps = obj.get("apps", [])
    out: List[ParsedItem] = []

    for a in apps:
        ua_id = a["id"]
        out.append(
            ParsedItem(
                topic="appmagic.application",
                content={
                    "united_application_id": ua_id,
                    "name": a.get("name"),
                    "united_publisher_id": up_id,
                    "icon_url": a.get("iconUrl") or a.get("icon"),  # vary by endpoint
                    "release_date": a.get("releaseDate"),
                    "contains_ads": a.get("containsAds"),
                    "has_in_app_purchases": a.get("hasInAppPurchases"),
                    "first_seen_at": datetime.utcnow().isoformat(),
                },
            )
        )

        # Store‑specific id
        for store_app in a.get("applications", []):
            out.append(
                ParsedItem(
                    topic="appmagic.application.store",
                    content={
                        "store_id": store_app.get("storeId"),
                        "store_app_id_text": store_app.get("storeApplicationIdText"),
                        "united_application_id": ua_id,
                        "name_on_store": store_app.get("name"),
                        "store_url": store_app.get("url"),
                        "first_seen_at": datetime.utcnow().isoformat(),
                    },
                )
            )

        # Tags (categorisation)
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


def _handler_metrics(obj: Dict[str, Any], source: str) -> List[ParsedItem]:
    ua_id = obj["ua_id"]
    snap = obj["snapshot"] or {}
    return [
        ParsedItem(
            topic="appmagic.application.metrics",
            content={
                "scrape_date": datetime.utcnow().date().isoformat(),
                "united_application_id": ua_id,
                "snapshot_30d_downloads": snap.get("download30d"),  # example fields
                "snapshot_30d_revenue": snap.get("revenue30d"),
                "snapshot_lifetime_downloads": snap.get("downloadLifetime"),
                "snapshot_lifetime_revenue": snap.get("revenueLifetime"),
            },
        )
    ]


_HANDLERS: Dict[str, Callable[[Dict[str, Any], str], List[ParsedItem]]] = {
    "appmagic.groups": _handler_groups,
    "appmagic.publishers": _handler_publishers,
    "appmagic.apps": _handler_apps,
    "appmagic.app.metrics": _handler_metrics,
}
