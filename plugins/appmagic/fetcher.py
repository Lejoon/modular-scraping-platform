# core/plugins/appmagic/fetcher.py
"""Fetcher stage for the *AppMagic* data‑pipeline.

The implementation translates what used to be the ad‑hoc imperative
script `scrape-company.py` into a reusable, schedule‑friendly transform
that yields :class:`~core.models.RawItem` objects.

Key points
~~~~~~~~~~
*  Uses :class:`core.infra.http.HttpClient` (shared across the platform)
*  Respects rate limits via :pydata:`rate_limit_s`
*  Emits *one* RawItem per logical payload so that downstream stages
   can stay fully streaming / memory‑agnostic.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, List, Any, Optional

from core.interfaces import Fetcher
from core.models import RawItem
from core.infra.http import HttpClient

logger = logging.getLogger(__name__)


class AppMagicFetcher(Fetcher):
    """Asynchronously pulls publisher groups, united‑publishers and apps
    for a predefined list of companies.

    Parameters
    ----------
    companies
        Same object the legacy script expected – *list* of *dicts* with at
        minimum keys::

            {
                "name": "Embracer",
                "ticker": "EMBRACB",
                "store": 1,
                "store_publisher_id": "6443412597262225303"
            }

        Additional keys are simply forwarded into the RawItem's metadata
        under *company*.
    rate_limit_s
        Seconds to ``asyncio.sleep`` between requests as a coarse‑grained
        global limiter.
    http
        Optional externally‑managed HttpClient (mainly for testing).
    """

    name = "AppMagicFetcher"

    # ---- End‑points ---------------------------------------------------- #

    API_BASE = "https://api.appmagic.rocks/api/v2"

    URL_GROUPS = API_BASE + "/publishers/groups"
    URL_PUBLISHERS_SEARCH = API_BASE + "/united-publishers/search-by-ids"
    URL_APPS_BY_PUBLISHER = API_BASE + "/united-publishers/{up_id}/apps"
    URL_METRICS_BY_APP = API_BASE + "/united-applications/{ua_id}/metrics?country=WW&currency=USD"

    # ------------------------------------------------------------------- #

    def __init__(
        self,
        *,
        companies: List[Dict[str, Any]],
        rate_limit_s: float = 0.8,
        max_retries: int = 4,
        http: Optional[HttpClient] = None,
    ) -> None:
        self._companies = companies
        self._rate_limit_s = rate_limit_s
        self._max_retries = max_retries
        self._http = http or HttpClient(max_retries=max_retries)

    # ------------------------------------------------------------------- #
    # Transform interface
    # ------------------------------------------------------------------- #

    async def fetch(self) -> AsyncIterator[RawItem]:  # type: ignore[override]
        """Entry‑point invoked by the pipeline orchestrator."""
        for company in self._companies:
            async for item in self._fetch_for_company(company):
                yield item
                # crude but effective RL
                await asyncio.sleep(self._rate_limit_s)

    async def __call__(self, _: AsyncIterator[Any]) -> AsyncIterator[RawItem]:  # noqa: D401
        """Fetcher ignores *upstream* and simply yields raw items."""
        async for raw in self.fetch():
            yield raw

    # ------------------------------------------------------------------- #
    # Private helpers
    # ------------------------------------------------------------------- #

    async def _fetch_for_company(self, company: Dict[str, Any]) -> AsyncIterator[RawItem]:
        """Complete flow for a single company dict."""
        store_id = company.get("store") or 1
        publisher_store_id = company["store_publisher_id"]

        # 1) ----- Groups ------------------------------------------------- #
        params = {"store": store_id, "publisherId": publisher_store_id}
        try:
            group_payload = await self._http.get_json(self.URL_GROUPS, params=params)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Group lookup failed for %s – %s", company["name"], exc)
            group_payload = {"groups": []}  # treat as stand‑alone publisher

        fetched_at = datetime.now(tz=timezone.utc)

        yield RawItem(
            source="appmagic.groups",
            payload=json.dumps(
                {
                    "company": company,
                    "groups": group_payload.get("groups", []),
                }
            ).encode(),
            fetched_at=fetched_at,
        )

        group_ids: list[int] = [g.get("id") for g in group_payload.get("groups", []) if g.get("id")]
        if not group_ids:
            # Single store publisher forms a degenerate group of size 1
            group_ids = [publisher_store_id]

        # 2) ----- United publishers ------------------------------------- #
        pubs_payload = await self._http.post_json(
            self.URL_PUBLISHERS_SEARCH, json={"ids": group_ids}
        )
        yield RawItem(
            source="appmagic.publishers",
            payload=json.dumps(
                {
                    "company": company,
                    "publishers": pubs_payload.get("publishers", pubs_payload),
                }
            ).encode(),
            fetched_at=fetched_at,
        )

        # 3) ----- Apps --------------------------------------------------- #
        # (We de‑duplicate because many publishers repeat across companies.)
        seen_up_ids: set[int] = set()
        for pub in pubs_payload.get("publishers", pubs_payload):
            up_id = pub["id"]
            if up_id in seen_up_ids:
                continue
            seen_up_ids.add(up_id)

            apps_payload = await self._http.get_json(
                self.URL_APPS_BY_PUBLISHER.format(up_id=up_id),
                params={"page": 1, "pageSize": 500},
            )
            yield RawItem(
                source="appmagic.apps",
                payload=json.dumps(
                    {
                        "up_id": up_id,
                        "company": company,
                        "apps": apps_payload.get("applications", apps_payload),
                    }
                ).encode(),
                fetched_at=fetched_at,
            )

            # 4) Optionally snapshot metrics for every app
            for app in apps_payload.get("applications", apps_payload):
                ua_id = app["id"]
                try:
                    metrics_payload = await self._http.get_json(
                        self.URL_METRICS_BY_APP.format(ua_id=ua_id)
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.debug("No metrics for app %s: %s", ua_id, exc)
                    continue

                yield RawItem(
                    source="appmagic.app.metrics",
                    payload=json.dumps(
                        {
                            "ua_id": ua_id,
                            "snapshot": metrics_payload,
                        }
                    ).encode(),
                    fetched_at=fetched_at,
                )
