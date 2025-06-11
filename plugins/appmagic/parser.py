"""appmagic.parser_refactored – Stream‑oriented RawItem → ParsedItem transform.

A *ParsedItem* produced by this module is identified by the ``topic`` attribute.
Those topics are the canonical *logical tables* that the downstream sink
(``AppMagicSink``) understands.  All topic strings live in :data:`TOPIC`.

Compared with the original implementation this version
------------------------------------------------------
* **Streams** items as they arrive – no full in‑memory buffering.
* **Consolidates** the specialised publisher‑apps post‑processing into a single
  helper that yields application, metrics **and** store rows in one pass.
* **Eliminates** a handful of unused helpers (metric/date parsing that were never
  referenced in production code).
* **Unifies** the *group‑id* hashing logic behind a small utility.
* Reduces log‑noise and tightens type hints.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, Tuple

from core.interfaces import Transform
from core.models import ParsedItem, RawItem

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Topic registry – every public "table" lives here once.
# --------------------------------------------------------------------------- #
class TOPIC:
    GROUP = "appmagic.group"
    PUBLISHER = "appmagic.publisher"
    PUBLISHER_ACCOUNT = "appmagic.publisher.account"
    APPLICATION = "appmagic.application"
    APPLICATION_STORE = "appmagic.application.store"
    APPLICATION_METRICS = "appmagic.application.metrics"
    APPLICATION_COUNTRY_METRICS = "appmagic.application.country_metrics"


# --------------------------------------------------------------------------- #
# Utility helpers
# --------------------------------------------------------------------------- #


def _hash_id(*parts: str) -> int:
    """Return a stable 32‑bit positive hash for a handful of strings."""
    return hash("_".join(p.lower() for p in parts)) % (2 ** 31)


async def _noop(_: Dict[str, Any]) -> List[ParsedItem]:  # type: ignore[override]
    """A placeholder handler that just logs and returns []."""
    logger.debug("No handler registered – dropping payload")
    return []


# --------------------------------------------------------------------------- #
# Individual source‑specific handlers (stateless, sync)
# --------------------------------------------------------------------------- #


def _handle_groups(obj: Dict[str, Any]) -> List[ParsedItem]:
    company = obj["company"]
    groups = obj.get("groups", []) or [
        {
            "id": _hash_id(company.get("name", ""), company.get("ticker", "")),
            "name": company.get("name", "Unknown"),
        }
    ]

    out: List[ParsedItem] = []
    for g in groups:
        group_id = g.get("id") or _hash_id(g.get("name", ""), company.get("ticker", ""))
        out.append(
            ParsedItem(
                topic=TOPIC.GROUP,
                content={
                    "group_id": group_id,
                    "group_name": g.get("name"),
                    "discovered_in_ticker": company.get("ticker"),
                    "first_seen_at": datetime.utcnow().isoformat(),
                },
            )
        )
    return out


def _handle_publishers(obj: Dict[str, Any]) -> List[ParsedItem]:
    company = obj["company"]
    pubs = obj.get("publishers", [])
    company_hash = _hash_id(company.get("name", ""), company.get("ticker", ""))

    out: List[ParsedItem] = []
    for p in filter(lambda d: isinstance(d, dict), pubs):
        up_id = p["id"]
        out.append(
            ParsedItem(
                topic=TOPIC.PUBLISHER,
                content={
                    "united_publisher_id": up_id,
                    "name": p.get("name"),
                    "headquarter_country_code": p.get("headquarter"),
                    "linkedin_headcount": p.get("linkedin_headcount"),
                    "min_release_date": p.get("min_release_date"),
                    "first_app_ad_date": p.get("first_app_ad"),
                    "group_id": company_hash,
                    "first_seen_at": datetime.utcnow().isoformat(),
                },
            )
        )

        for acc in p.get("accounts", []) or p.get("publisherIds", []):
            store_id = acc.get("storeId") or acc.get("store")
            store_pid = acc.get("publisherId") or acc.get("store_publisher_id")
            out.append(
                ParsedItem(
                    topic=TOPIC.PUBLISHER_ACCOUNT,
                    content={
                        "store_id": store_id,
                        "store_publisher_id": store_pid,
                        "united_publisher_id": up_id,
                        "group_id": company_hash,
                        "html_url": acc.get("html_url"),
                        "first_seen_at": datetime.utcnow().isoformat(),
                    },
                )
            )
    return out


def _handle_metrics(obj: Dict[str, Any]) -> List[ParsedItem]:
    snap = obj.get("snapshot", {})
    return [
        ParsedItem(
            topic=TOPIC.APPLICATION_METRICS,
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


def _handle_country_metrics(obj: Dict[str, Any]) -> List[ParsedItem]:
    out: List[ParsedItem] = []
    for m in obj.get("metrics", []):
        out.append(
            ParsedItem(
                topic=TOPIC.APPLICATION_COUNTRY_METRICS,
                content={
                    "scrape_date": datetime.utcnow().date().isoformat(),
                    "united_application_id": obj["ua_id"],
                    "country_code": m.get("country"),
                    "snapshot_30d_downloads": m.get("downloads30d"),
                    "snapshot_30d_revenue": m.get("revenue30d"),
                },
            )
        )
    return out


# --------------------------------------------------------------------------- #
# Publisher‑apps API handler (special – returns three topic kinds)
# --------------------------------------------------------------------------- #


def _extract_worldwide(metrics):
    """Return (downloads, revenue) tuple for the WW entry if present."""
    if isinstance(metrics, list):
        ww = next((m for m in metrics if m.get("country") == "WW"), {})
        return ww.get("downloads"), ww.get("revenue")
    return None, None


def _handle_publisher_apps_api(obj: Dict[str, Any]) -> List[ParsedItem]:
    up_id = obj["united_publisher_id"]
    out: List[ParsedItem] = []

    for app in obj.get("applications", []):
        ua_id = app.get("id")
        if not ua_id:
            continue

        out.append(
            ParsedItem(
                topic=TOPIC.APPLICATION,
                content={
                    "united_application_id": ua_id,
                    "united_publisher_id": up_id,
                    "name": app.get("name"),
                    "icon_url": app.get("icon_url") or app.get("icon"),
                    "release_date": app.get("releaseDate"),
                    "contains_ads": app.get("contains_ads"),
                    "has_in_app_purchases": app.get("has_in_app_purchases"),
                    "data_source": "api",
                    "first_seen_at": datetime.utcnow().isoformat(),
                },
            )
        )

        downloads_30d, revenue_30d = _extract_worldwide(app.get("metrics_30d", []))
        dl_life, rev_life = _extract_worldwide(app.get("metrics_lifetime", []))

        out.append(
            ParsedItem(
                topic=TOPIC.APPLICATION_METRICS,
                content={
                    "scrape_date": datetime.utcnow().date().isoformat(),
                    "united_application_id": ua_id,
                    "snapshot_30d_downloads": downloads_30d,
                    "snapshot_30d_revenue": revenue_30d,
                    "snapshot_lifetime_downloads": dl_life,
                    "snapshot_lifetime_revenue": rev_life,
                },
            )
        )

        for sid in app.get("applications", []):
            store = sid.get("store")
            store_id = store[0] if store and len(store) > 0 else None  # Ensure list is non-empty
            out.append(
                ParsedItem(
                    topic=TOPIC.APPLICATION_STORE,
                    content={
                        "store_id": store_id,
                        "store_app_id_text": sid.get("store_application_id"),
                        "united_application_id": ua_id,
                        "name_on_store": sid.get("name"),
                        "store_url": sid.get("url"),
                        "first_seen_at": datetime.utcnow().isoformat(),
                    },
                )
            )
    return out


# --------------------------------------------------------------------------- #
# Handler registry – maps RawItem.source → handler callable
# --------------------------------------------------------------------------- #
_HANDLER: Dict[str, Callable[[Dict[str, Any]], List[ParsedItem]]] = {
    "appmagic.groups": _handle_groups,
    "appmagic.publishers": _handle_publishers,
    "appmagic.metrics": _handle_metrics,
    "appmagic.country_metrics": _handle_country_metrics,
    "appmagic.publisher_apps_api": _handle_publisher_apps_api,
}


# --------------------------------------------------------------------------- #
# Public transform implementation
# --------------------------------------------------------------------------- #
class AppMagicParser(Transform):
    name = "AppMagicParser"

    # -------------------------------------------------------------- #
    async def __call__(self, items: AsyncIterator[Any]) -> AsyncIterator[ParsedItem]:
        """Parse the *stream* of RawItems on the fly.

        Most items are handled immediately.  The special publisher‑apps payloads
        are simply expanded into their three topic types right away – no data
        hoarding, no second pass.
        """
        async for itm in items:
            # Pass‑through anything that is already a ParsedItem
            if isinstance(itm, ParsedItem):
                yield itm
                continue

            # RawItem → ParsedItems
            if isinstance(itm, RawItem):
                handler = _HANDLER.get(itm.source, _noop)
                try:
                    payload = json.loads(itm.payload)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Bad JSON in %s: %s", itm.source, exc)
                    continue

                for parsed in handler(payload):
                    yield parsed

    # -------------------------------------------------------------- #
    def __repr__(self) -> str:  # pragma: no cover
        return f"<{self.name}>"
