"""
sel.py - Async Playwright helpers for scraping, automation and “stealth” browsing.

Key additions
-------------
* `stealth` flag: - random UA (or user-supplied) - removes navigator.webdriver - disables img/css/fonts (optional)
* Utility coroutines • `dismiss_cookies()` - click one of many selectors • `click_repeatedly()` - keep clicking until hidden / max clicks • `scroll_to_bottom()` - infinite-scroll support
* Async context-manager support:
    async with PlaywrightClient() as pw:
        page = await pw.new_page()
* `extra_launch_kwargs` / `extra_context_kwargs` to expose every Playwright knob without changing the public API.
"""

from __future__ import annotations

import asyncio
import logging
import random
from types import TracebackType
from typing import (
    Any,
    AsyncGenerator,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
)

try:
    from playwright.async_api import (
        async_playwright,
        Browser,
        BrowserContext,
        BrowserType,
        Error as PlaywrightError,
        Page,
        TimeoutError as PlaywrightTimeout,
    )
except ImportError as e:  # pragma: no cover
    raise ImportError(
        "Package 'playwright' is required.  Install with:  pip install playwright"
    ) from e

logger = logging.getLogger(__name__)
DEFAULT_STEALTH_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.%d.%d Safari/537.36"
    % (random.randint(0, 9999), random.randint(0, 199))
)


class PlaywrightClient:
    """
    Thin wrapper around Playwright making the *90 % use-case* trivial
    while keeping escape hatches for everything else.

    Examples
    --------
    async with PlaywrightClient(stealth=True) as pw:
        page = await pw.new_page()
        await page.goto("https://example.com")
        html = await page.content()
    """

    def __init__(
        self,
        *,
        headless: bool = True,
        browser_type: str = "chromium",
        timeout: float = 30_000,
        stealth: bool = False,
        user_agent: Optional[str] = None,
        extra_launch_kwargs: Optional[Dict[str, Any]] = None,
        extra_context_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.headless = headless
        self.browser_type = browser_type.lower()
        self.timeout = timeout
        self.stealth = stealth
        self.user_agent = user_agent
        self._launch_kwargs = extra_launch_kwargs or {}
        self._context_kwargs = extra_context_kwargs or {}

        # Internal Playwright handles
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None

    # --------------------------------------------------------------------- #
    # Async context-manager sugar
    async def __aenter__(self) -> "PlaywrightClient":  # noqa: D401
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        await self.stop()

    # --------------------------------------------------------------------- #
    # Lifecycle helpers
    async def start(self) -> None:
        """Launch browser & default context if not already started."""
        if self._browser:
            return

        self._playwright = await async_playwright().start()
        browser_launcher: BrowserType

        if self.browser_type == "chromium":
            browser_launcher = self._playwright.chromium
        elif self.browser_type == "firefox":
            browser_launcher = self._playwright.firefox
        elif self.browser_type == "webkit":
            browser_launcher = self._playwright.webkit
        else:  # pragma: no cover
            raise ValueError(f"Unsupported browser type: {self.browser_type}")

        self._browser = await browser_launcher.launch(
            headless=self.headless, **self._launch_kwargs
        )

        # ----------------------------- #
        # Set up context
        context_kwargs: Dict[str, Any] = {
            "ignore_https_errors": True,
            **self._context_kwargs,
        }

        if self.stealth:
            context_kwargs.setdefault("user_agent", self.user_agent or DEFAULT_STEALTH_UA)
            # Disable some resource types for additional “stealth” / speed-ups
            context_kwargs.setdefault("java_script_enabled", True)

        self._context = await self._browser.new_context(**context_kwargs)

        # Stealth JS patches
        if self.stealth:
            await self._context.add_init_script(
                """
                // Remove webdriver property
                Object.defineProperty(navigator, 'webdriver', {
                  get: () => undefined
                });
                // Chrome headless fix for plugins and languages
                Object.defineProperty(navigator, 'plugins', {
                  get: () => [1, 2, 3, 4, 5],
                });
                Object.defineProperty(navigator, 'languages', {
                  get: () => ['en-US', 'en'],
                });
                """
            )

        logger.info(
            "Playwright started: %s (headless=%s, stealth=%s)",
            self.browser_type,
            self.headless,
            self.stealth,
        )

    async def stop(self) -> None:
        """Gracefully close context, browser & Playwright."""
        if self._context:
            await self._context.close()
            self._context = None
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None
        logger.info("Playwright stopped")

    # --------------------------------------------------------------------- #
    # High-level page helpers
    async def new_page(self) -> Page:
        """Return a fresh Page with sane defaults."""
        if not self._context:
            await self.start()
        page = await self._context.new_page()
        page.set_default_timeout(self.timeout)
        return page

    async def get_page_content(
        self, url: str, wait_for_selector: Optional[str] = None
    ) -> str:
        """
        Navigate, optionally wait for a selector, return the rendered HTML.

        *Use for quick one-offs; for interactive flows open the page yourself.*
        """
        page = await self.new_page()
        try:
            await page.goto(url)
            if wait_for_selector:
                await page.wait_for_selector(wait_for_selector)
            html = await page.content()
            return html
        finally:
            await page.close()

    # --------------------------------------------------------------------- #
    # Shadow DOM helper kept from original version
    async def extract_shadow_dom_content(
        self,
        url: str,
        shadow_host_selector: str,
        content_selector: str,
    ) -> Optional[str]:
        page = await self.new_page()
        try:
            await page.goto(url)
            await page.wait_for_selector(shadow_host_selector)
            return await page.evaluate(
                f"""
                () => {{
                    const host = document.querySelector('{shadow_host_selector}');
                    if (!host || !host.shadowRoot) return null;
                    const el = host.shadowRoot.querySelector('{content_selector}');
                    return el ? el.textContent : null;
                }}
                """
            )
        finally:
            await page.close()

    # --------------------------------------------------------------------- #
    # Form helper (unchanged, but part of public API)
    async def fill_form_and_submit(
        self,
        url: str,
        form_data: Dict[str, str],
        submit_selector: str,
        wait_for_navigation: bool = True,
    ) -> str:
        page = await self.new_page()
        try:
            await page.goto(url)
            for sel, value in form_data.items():
                await page.fill(sel, value)
            if wait_for_navigation:
                async with page.expect_navigation():
                    await page.click(submit_selector)
            else:
                await page.click(submit_selector)
            return await page.content()
        finally:
            await page.close()

    # ------------------------------------------------------------------ #
    # Utility coroutines (NEW)
    # These are intentionally “free functions” for copy-pasta convenience, but
    # are grouped as `@staticmethod`s to keep the namespace tidy.
    # ------------------------------------------------------------------ #
    @staticmethod
    async def dismiss_cookies(
        page: Page,
        selectors: Sequence[str],
        timeout: int = 3_000,
    ) -> bool:
        """
        Try each selector; click the first one that appears.

        Returns
        -------
        bool
            True if something was clicked, False otherwise.
        """
        for sel in selectors:
            try:
                btn = await page.wait_for_selector(sel, timeout=timeout)
                await btn.click()
                logger.debug("Cookie banner dismissed with selector: %s", sel)
                return True
            except PlaywrightTimeout:
                continue
            except PlaywrightError as e:
                logger.debug("Dismiss cookie failed for %s: %s", sel, e)
        return False

    @staticmethod
    async def click_repeatedly(
        page: Page,
        selector: str,
        *,
        max_clicks: int = 50,
        delay: float = 3.0,
        scroll_into_view: bool = True,
    ) -> int:
        """
        Keep clicking the `selector` until it disappears or `max_clicks` reached.

        Returns
        -------
        int
            Number of successful clicks.
        """
        clicks = 0
        while clicks < max_clicks:
            if not await page.is_visible(selector):
                break
            try:
                if scroll_into_view:
                    await page.eval_on_selector(
                        selector,
                        "el => el.scrollIntoView({block:'center', inline:'center'})",
                    )
                await page.click(selector, timeout=5_000)
                clicks += 1
                if delay:
                    await asyncio.sleep(delay)
            except PlaywrightError:
                break
        return clicks

    @staticmethod
    async def scroll_to_bottom(
        page: Page,
        *,
        step_px: int = 2048,
        delay: float = 0.25,
        max_scrolls: int = 200,
    ) -> None:
        """
        Scroll down chunk-by-chunk until no movement (or max_scrolls).

        Good for lazy-load / infinite scroll pages without explicit “show more”.
        """
        last_height = -1
        for _ in range(max_scrolls):
            height = await page.evaluate("() => document.body.scrollHeight")
            if height == last_height:
                break
            last_height = height
            await page.evaluate(
                f"window.scrollTo(0, {height - step_px});"
            )  # scroll near bottom
            await asyncio.sleep(delay)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            await asyncio.sleep(delay)

    # convenience alias for backward compatibility
    wait_for_element_and_extract = None  # kept only for docs generation


# Re-expose helpers for `from sel import dismiss_cookies` nicety
dismiss_cookies = PlaywrightClient.dismiss_cookies
click_repeatedly = PlaywrightClient.click_repeatedly
scroll_to_bottom = PlaywrightClient.scroll_to_bottom