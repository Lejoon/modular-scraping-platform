# core/plugins/appmagic/sinks.py
"""Database sink for everything parsed from AppMagic.

It upserts into the seven tables that make up the *core reference
schema* for the analytics DB.
"""

from __future__ import annotations

import logging
from typing import Any, AsyncIterator, Dict, List

from core.interfaces import Sink
from core.models import ParsedItem
from core.infra.db import Database

logger = logging.getLogger(__name__)


class AppMagicSink(Sink):
    """Upserts ParsedItems into SQLite, auto‑creating tables if needed."""

    name = "AppMagicSink"

    # ------------------------------------------------------------------- #
    # Mapping: ParsedItem.topic -> table meta
    # ------------------------------------------------------------------- #

    _TOPIC_CFG: Dict[str, Dict[str, Any]] = {
        "country": {
            "table": "Countries",
            "pk": ["country_code"],
            "cols": ["country_code", "country_name", "first_seen_at"],
        },
        "appmagic.group": {
            "table": "PublisherGroups",
            "pk": ["group_id"],
            "cols": ["group_id", "group_name", "discovered_in_ticker", "first_seen_at", "last_updated_at"],
        },
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
                "last_updated_at",
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
                "first_seen_at",
                "last_updated_at",
            ],
        },
        "appmagic.application": {
            "table": "UnitedApplications",
            "pk": ["united_application_id"],
            "cols": [
                "united_application_id",
                "name",
                "united_publisher_id",
                "icon_url",
                "release_date",
                "contains_ads",
                "has_in_app_purchases",
                "first_seen_at",
                "last_updated_at",
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
                "last_updated_at",
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
            "cols": [
                "united_application_id",
                "tag_id",
                "first_associated_at",
            ],
        },
    }

    # ------------------------------------------------------------------- #

    def __init__(self, db_path: str = "mobile_analytics.db", **kwargs) -> None:
        self.db = Database(db_path=db_path)

    # ------------------------------------------------------------------- #
    # Transform interface
    # ------------------------------------------------------------------- #

    async def __call__(self, items: AsyncIterator[Any]) -> AsyncIterator[None]:  # type: ignore[override]
        async for item in items:
            if isinstance(item, ParsedItem):
                await self.handle(item)
            yield None  # Sinks are typically terminal

    async def handle(self, item: ParsedItem) -> None:
        cfg = self._TOPIC_CFG.get(item.topic)
        if cfg is None:
            logger.debug("Sink ignoring topic %s", item.topic)
            return

        await self._upsert(cfg["table"], cfg["pk"], cfg["cols"], item.content)

    # ------------------------------------------------------------------- #
    # Upsert helpers
    # ------------------------------------------------------------------- #

    async def _upsert(self, table: str, pk: List[str], cols: List[str], row: Dict[str, Any]) -> None:
        placeholders = ", ".join([f":{c}" for c in cols])
        pk_clause = ", ".join(pk)
        update_set = ", ".join([f"{c}=excluded.{c}" for c in cols if c not in pk])

        # Auto‑update *last_updated_at* whenever non‑PK column changes
        set_last_updated = "last_updated_at=CURRENT_TIMESTAMP" if "last_updated_at" in cols else ""
        if set_last_updated and update_set:
            update_set = f"{update_set}, {set_last_updated}"
        elif set_last_updated:
            update_set = set_last_updated

        sql = f"""
            INSERT INTO {table} ({', '.join(cols)})
            VALUES ({placeholders})
            ON CONFLICT ({pk_clause}) DO UPDATE SET
            {update_set}
        """
        await self.db.execute(sql, row)

    # ------------------------------------------------------------------- #
    # Lifecycle helpers
    # ------------------------------------------------------------------- #

    async def __aenter__(self):
        await self.db.connect()
        await self._create_tables()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.db.close()

    # ------------------------------------------------------------------- #
    # Table bootstrap
    # ------------------------------------------------------------------- #

    async def _create_tables(self) -> None:
        """CREATE TABLE IF NOT EXISTS … for all relevant entities."""

        # Definitions lifted verbatim from *db_setup.py*, trimmed to only the
        # tables required for this plugin (plus minimal *Stores* / *Tags* to
        # satisfy FKs).
        ddl_statements = [
            """
            CREATE TABLE IF NOT EXISTS Countries (
                country_code TEXT PRIMARY KEY,
                country_name TEXT,
                first_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS PublisherGroups (
                group_id INTEGER PRIMARY KEY,
                group_name TEXT NOT NULL UNIQUE,
                discovered_in_ticker TEXT,
                first_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS UnitedPublishers (
                united_publisher_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                headquarter_country_code TEXT,
                linkedin_headcount INTEGER,
                min_release_date TEXT,
                first_app_ad_date TEXT,
                group_id INTEGER,
                first_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (headquarter_country_code) REFERENCES Countries(country_code),
                FOREIGN KEY (group_id) REFERENCES PublisherGroups(group_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS StoreSpecificPublisherAccounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                store_id INTEGER NOT NULL,
                store_publisher_id TEXT NOT NULL,
                united_publisher_id INTEGER NOT NULL,
                group_id INTEGER,
                html_url TEXT,
                first_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (united_publisher_id) REFERENCES UnitedPublishers(united_publisher_id),
                FOREIGN KEY (group_id) REFERENCES PublisherGroups(group_id),
                UNIQUE (store_id, store_publisher_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS UnitedApplications (
                united_application_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                united_publisher_id INTEGER NOT NULL,
                icon_url TEXT,
                release_date TEXT,
                contains_ads BOOLEAN,
                has_in_app_purchases BOOLEAN,
                first_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (united_publisher_id) REFERENCES UnitedPublishers(united_publisher_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ApplicationSnapshotMetrics (
                scrape_date DATE NOT NULL,
                united_application_id INTEGER NOT NULL,
                snapshot_30d_downloads REAL,
                snapshot_30d_revenue REAL,
                snapshot_lifetime_downloads REAL,
                snapshot_lifetime_revenue REAL,
                PRIMARY KEY (scrape_date, united_application_id),
                FOREIGN KEY (united_application_id) REFERENCES UnitedApplications(united_application_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS StoreSpecificApplications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                store_id INTEGER NOT NULL,
                store_app_id_text TEXT NOT NULL,
                united_application_id INTEGER NOT NULL,
                name_on_store TEXT,
                store_url TEXT,
                first_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (united_application_id) REFERENCES UnitedApplications(united_application_id),
                UNIQUE (store_id, store_app_id_text)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS Tags (
                tag_id INTEGER PRIMARY KEY,
                tag_name TEXT NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ApplicationTagsLink (
                united_application_id INTEGER NOT NULL,
                tag_id INTEGER NOT NULL,
                first_associated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (united_application_id, tag_id),
                FOREIGN KEY (united_application_id) REFERENCES UnitedApplications(united_application_id),
                FOREIGN KEY (tag_id) REFERENCES Tags(tag_id)
            );
            """,
            # Minimal Stores (only id) – enough to satisfy FK, the full
            # *store* catalogue lives elsewhere.
            """
            CREATE TABLE IF NOT EXISTS Stores (
                store_id INTEGER PRIMARY KEY,
                store_name TEXT
            );
            """,
        ]

        for ddl in ddl_statements:
            await self.db.execute(ddl)