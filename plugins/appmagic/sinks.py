# plugins/appmagic/sinks.py
"""
AppMagicSink
============

• Creates every required table on first use (SQLite: CREATE TABLE IF NOT EXISTS).
• Implements the abstract `handle()` method expected by core.interfaces.Sink.
• Upserts rows according to the ParsedItem → table mapping.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List

from core.interfaces import Sink
from core.models import ParsedItem
from core.infra.db import Database

logger = logging.getLogger(__name__)


class AppMagicSink(Sink):
    # ------------------------------------------------------------------ #
    name = "AppMagicSink"

    # ------------------------------ DDL ------------------------------- #
    _DDL: Dict[str, str] = {
        # reference tables ---------------------------------------------
        "Countries": """
CREATE TABLE IF NOT EXISTS Countries (
    country_code TEXT PRIMARY KEY,
    country_name TEXT,
    first_seen_at DATETIME,
    last_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
""",
        "PublisherGroups": """
CREATE TABLE IF NOT EXISTS PublisherGroups (
    group_id INTEGER PRIMARY KEY,
    group_name TEXT,
    discovered_in_ticker TEXT,
    first_seen_at DATETIME,
    last_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
""",
        # publisher hierarchy ------------------------------------------
        "UnitedPublishers": """
CREATE TABLE IF NOT EXISTS UnitedPublishers (
    united_publisher_id INTEGER PRIMARY KEY,
    name TEXT,
    headquarter_country_code TEXT,
    linkedin_headcount INTEGER,
    min_release_date DATE,
    first_app_ad_date DATE,
    group_id INTEGER,
    first_seen_at DATETIME,
    last_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
""",
        "StoreSpecificPublisherAccounts": """
CREATE TABLE IF NOT EXISTS StoreSpecificPublisherAccounts (
    store_id INTEGER,
    store_publisher_id TEXT,
    united_publisher_id INTEGER,
    group_id INTEGER,
    html_url TEXT,
    icon_url TEXT,
    first_seen_at DATETIME,
    last_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (store_id, store_publisher_id)
);
""",
        # apps ---------------------------------------------------------
        "UnitedApplications": """
CREATE TABLE IF NOT EXISTS UnitedApplications (
    united_application_id INTEGER PRIMARY KEY,
    united_publisher_id INTEGER,
    name TEXT,
    icon_url TEXT,
    release_date DATE,
    contains_ads BOOLEAN,
    has_in_app_purchases BOOLEAN,
    first_seen_at DATETIME,
    last_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
""",
        "StoreSpecificApplications": """
CREATE TABLE IF NOT EXISTS StoreSpecificApplications (
    store_id INTEGER,
    store_app_id_text TEXT,
    united_application_id INTEGER,
    name_on_store TEXT,
    store_url TEXT,
    first_seen_at DATETIME,
    last_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (store_id, store_app_id_text)
);
""",
        "ApplicationSnapshotMetrics": """
CREATE TABLE IF NOT EXISTS ApplicationSnapshotMetrics (
    scrape_date DATE,
    united_application_id INTEGER,
    snapshot_30d_downloads INTEGER,
    snapshot_30d_revenue REAL,
    snapshot_lifetime_downloads INTEGER,
    snapshot_lifetime_revenue REAL,
    PRIMARY KEY (scrape_date, united_application_id)
);
""",
        "ApplicationTagsLink": """
CREATE TABLE IF NOT EXISTS ApplicationTagsLink (
    united_application_id INTEGER,
    tag_id TEXT,
    first_associated_at DATETIME,
    PRIMARY KEY (united_application_id, tag_id)
);
""",
        # NEW table ----------------------------------------------------
        "PublisherCountryMetrics": """
CREATE TABLE IF NOT EXISTS PublisherCountryMetrics (
    scrape_date DATE,
    united_publisher_id INTEGER,
    country_code TEXT,
    metric_timespan TEXT CHECK(metric_timespan IN ('Last30Days','Lifetime')),
    revenue REAL,
    downloads INTEGER,
    revenue_percent REAL,
    downloads_percent REAL,
    PRIMARY KEY (scrape_date,
                 united_publisher_id,
                 country_code,
                 metric_timespan)
);
""",
    }

    # ---------------------------- mapping ---------------------------- #
    _TOPIC_CFG: Dict[str, Dict[str, Any]] = {
        # reference
        "country": {
            "table": "Countries",
            "pk": ["country_code"],
            "cols": ["country_code", "country_name", "first_seen_at"],
        },
        "appmagic.group": {
            "table": "PublisherGroups",
            "pk": ["group_id"],
            "cols": ["group_id", "group_name", "discovered_in_ticker", "first_seen_at"],
        },
        # publishers
        "appmagic.publisher": {
            "table": "UnitedPublishers",
            "pk": ["united_publisher_id"],
            "cols": [
                "united_publisher_id",
                "name",
                "headquarter_country_code",
                "linkedin_headcount",
                "min_release_date",
                "first_app_ad_date",
                "group_id",
                "first_seen_at",
            ],
        },
        "appmagic.publisher.account": {
            "table": "StoreSpecificPublisherAccounts",
            "pk": ["store_id", "store_publisher_id"],
            "cols": [
                "store_id",
                "store_publisher_id",
                "united_publisher_id",
                "group_id",
                "html_url",
                "icon_url",
                "first_seen_at",
            ],
        },
        # apps
        "appmagic.application": {
            "table": "UnitedApplications",
            "pk": ["united_application_id"],
            "cols": [
                "united_application_id",
                "united_publisher_id",
                "name",
                "icon_url",
                "release_date",
                "contains_ads",
                "has_in_app_purchases",
                "first_seen_at",
            ],
        },
        "appmagic.application.store": {
            "table": "StoreSpecificApplications",
            "pk": ["store_id", "store_app_id_text"],
            "cols": [
                "store_id",
                "store_app_id_text",
                "united_application_id",
                "name_on_store",
                "store_url",
                "first_seen_at",
            ],
        },
        "appmagic.application.metrics": {
            "table": "ApplicationSnapshotMetrics",
            "pk": ["scrape_date", "united_application_id"],
            "cols": [
                "scrape_date",
                "united_application_id",
                "snapshot_30d_downloads",
                "snapshot_30d_revenue",
                "snapshot_lifetime_downloads",
                "snapshot_lifetime_revenue",
            ],
        },
        "appmagic.application.tag_link": {
            "table": "ApplicationTagsLink",
            "pk": ["united_application_id", "tag_id"],
            "cols": ["united_application_id", "tag_id", "first_associated_at"],
        },
        # publisher country metrics
        "appmagic.publisher.country.metrics": {
            "table": "PublisherCountryMetrics",
            "pk": [
                "scrape_date",
                "united_publisher_id",
                "country_code",
                "metric_timespan",
            ],
            "cols": [
                "scrape_date",
                "united_publisher_id",
                "country_code",
                "metric_timespan",
                "revenue",
                "downloads",
                "revenue_percent",
                "downloads_percent",
            ],
        },
    }

    # ------------------------------------------------------------------ #
    def __init__(self, db_path: str | Path = "mobile_analytics.db", **_) -> None:
        self.db = Database(db_path=db_path)
        self._ddl_executed = False  # run once lazily

    # ------------------------------------------------------------------ #
    async def handle(self, item: ParsedItem) -> None:  # abstract method ✓
        """Upsert a single ParsedItem."""
        # Lazily create tables on the first ever insert
        if not self._ddl_executed:
            await self._create_all_tables()
            self._ddl_executed = True

        cfg = self._TOPIC_CFG.get(item.topic)
        if cfg is None:  # unknown topic – just ignore
            logger.debug("No sink rule for topic %s", item.topic)
            return

        await self._upsert(cfg["table"], cfg["pk"], cfg["cols"], item.content)

    # ------------------------------------------------------------------ #
    async def __call__(self, stream):
        async for itm in stream:
            if isinstance(itm, ParsedItem):
                await self.handle(itm)
            yield None  # sink is terminal but keeps the pipeline contract

    # ------------------------------------------------------------------ #
    async def _create_all_tables(self) -> None:
        for ddl in self._DDL.values():
            await self.db.execute(ddl)

    # ------------------------------------------------------------------ #
    async def _upsert(
        self, table: str, pk: List[str], cols: List[str], row: Dict[str, Any]
    ) -> None:
        actual_cols, plch = [], []
        for c in cols:
            if c in row:
                actual_cols.append(c)
                plch.append(f":{c}")

        pk_clause = ", ".join(pk)
        update_set = ", ".join(
            f"{c}=excluded.{c}"
            for c in actual_cols
            if c not in pk  # don't update primary-key columns
        )

        sql = f"""
INSERT INTO {table} ({', '.join(actual_cols)})
VALUES ({', '.join(plch)})
ON CONFLICT ({pk_clause}) DO UPDATE SET {update_set};
"""
        await self.db.execute(sql, row)


# ----------------------------------------------------------------------- #
# Make class discoverable as `appmagic.AppMagicSink`
import sys as _sys  # noqa: E402

_pkg = _sys.modules.get(__name__.rsplit(".", 1)[0])
if _pkg:
    setattr(_pkg, "AppMagicSink", AppMagicSink)
