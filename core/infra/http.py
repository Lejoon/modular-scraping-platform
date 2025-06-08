"""
http.py – Async HTTP client built on *aiohttp* with smart retries,
          transparent 429 / 5xx back-off and per-instance default headers.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Dict, Mapping, MutableMapping, Optional

import aiohttp

logger = logging.getLogger(__name__)


class HttpClient:
    """
    Thin wrapper over *aiohttp.ClientSession* adding:

    * global & per-request headers (keeps user-agent in one place)
    * exponential back-off **with jitter** for 429 / 5xx / network errors
    * transparent parsing of *Retry-After* header
    * async context-manager support
    """

    def __init__(
        self,
        *,
        session: Optional[aiohttp.ClientSession] = None,
        timeout: float = 30.0,
        max_retries: int = 5,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        default_headers: Optional[Mapping[str, str]] = None,
    ) -> None:
        self._external_session = session
        self._timeout = timeout
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._own_session: Optional[aiohttp.ClientSession] = None
        self._default_headers: Dict[str, str] = dict(default_headers or {})

    # ---------------------------------------------- #
    # Async context-manager
    async def __aenter__(self) -> "HttpClient":  # noqa: D401
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: D401
        await self.close()

    # ---------------------------------------------- #
    # Session management
    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._external_session:
            return self._external_session
        if self._own_session is None or self._own_session.closed:
            timeout = aiohttp.ClientTimeout(total=self._timeout)
            self._own_session = aiohttp.ClientSession(timeout=timeout)
        return self._own_session

    async def close(self) -> None:
        if self._own_session and not self._own_session.closed:
            await self._own_session.close()
            self._own_session = None

    # ---------------------------------------------- #
    # Internal helpers
    @staticmethod
    def _parse_retry_after(header_val: str | None) -> Optional[float]:
        """Return seconds given a Retry-After header value."""
        if not header_val:
            return None
        header_val = header_val.strip()
        # seconds
        if header_val.isdigit():
            return float(header_val)
        # HTTP-date
        try:
            retry_at = time.mktime(
                aiohttp.http_parser.try_parse_http_time(header_val)
            )  # type: ignore[arg-type]
            return max(0.0, retry_at - time.time())
        except Exception:
            return None

    def _merge_headers(self, extra: Mapping[str, str] | None) -> Dict[str, str]:
        merged: Dict[str, str] = {**self._default_headers}
        if extra:
            merged.update(extra)
        return merged

    async def _request(
        self,
        method: str,
        url: str,
        *,
        retry_for_status: tuple[int, ...] = (429, 500, 502, 503, 504),
        **kwargs,
    ) -> aiohttp.ClientResponse:
        """Perform a request with retries; returns *aiohttp.ClientResponse*."""
        session = await self._ensure_session()

        headers = self._merge_headers(kwargs.pop("headers", None))
        kwargs["headers"] = headers

        for attempt in range(1, self._max_retries + 1):
            try:
                resp = await session.request(method, url, **kwargs)
                if resp.status not in retry_for_status:
                    resp.raise_for_status()
                    return resp

                # Retry on specific status codes
                retry_after = self._parse_retry_after(resp.headers.get("Retry-After"))
                raise aiohttp.ClientResponseError(
                    resp.request_info,
                    resp.history,
                    status=resp.status,
                    message=f"retryable status {resp.status}",
                    headers=resp.headers,
                )
            except (aiohttp.ClientConnectionError, aiohttp.ClientResponseError) as e:
                # final attempt – re-raise
                if attempt == self._max_retries:
                    logger.error("HTTP %s %s failed after %d attempts: %s", method, url, attempt, e)
                    raise

                # determine sleep time
                sleep_seconds: float
                retry_after_hdr = (
                    e.headers.get("Retry-After") if isinstance(e, aiohttp.ClientResponseError) else None
                )
                retry_after_s = self._parse_retry_after(retry_after_hdr)
                if retry_after_s is not None:
                    sleep_seconds = retry_after_s
                else:
                    exponential = min(self._base_delay * 2 ** (attempt - 1), self._max_delay)
                    jitter = random.uniform(0, self._base_delay)
                    sleep_seconds = exponential + jitter

                logger.warning(
                    "HTTP %s %s failed (attempt %d/%d – will retry in %.1fs): %s",
                    method,
                    url,
                    attempt,
                    self._max_retries,
                    sleep_seconds,
                    str(e).splitlines()[0],
                )
                await asyncio.sleep(sleep_seconds)
            except asyncio.CancelledError:  # pragma: no cover
                raise
            except Exception:  # pragma: no cover
                # Unknown fatal error – no retry
                raise

        # Should never hit here
        raise RuntimeError("Unreachable retry loop")

    # ---------------------------------------------- #
    # Public helpers
    async def get_text(self, url: str, **kwargs) -> str:
        async with await self._request("GET", url, **kwargs) as resp:
            return await resp.text()

    async def get_json(self, url: str, **kwargs) -> Any:
        async with await self._request("GET", url, **kwargs) as resp:
            return await resp.json(content_type=None)

    async def get_bytes(self, url: str, **kwargs) -> bytes:
        # larger timeout for binary payloads
        kwargs.setdefault("timeout", aiohttp.ClientTimeout(total=self._timeout * 2))
        async with await self._request("GET", url, **kwargs) as resp:
            return await resp.read()

    async def post_json(
        self,
        url: str,
        data: Dict[str, Any] | Any,
        *,
        json: bool = True,
        **kwargs,
    ) -> Any:
        if json:
            kwargs["json"] = data
        else:
            kwargs["data"] = data
        async with await self._request("POST", url, **kwargs) as resp:
            return await resp.json(content_type=None)

    # ---------------------------------------------- #
    # Mutators
    def set_default_header(self, key: str, value: str) -> None:
        self._default_headers[key] = value

    def update_default_headers(self, headers: Mapping[str, str]) -> None:
        self._default_headers.update(headers)
