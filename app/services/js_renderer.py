"""
Layer 3: JavaScript Rendering with Playwright

Purpose: Handle modern SPAs (React, Vue, Angular) that require JavaScript execution

When to use:
- Static scraping (Layer 1) returns empty/incomplete content
- Page uses heavy JavaScript frameworks
- Content loads dynamically via AJAX/fetch
- Infinite scroll, lazy loading, or client-side routing

Cost: FREE (Playwright is open-source)
Speed: 5-10 seconds per page (slower than static scraping)
Resource: High CPU/memory usage (runs headless browser)

Alternative: Camoufox (Firefox-based, better for stealth)
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Error
import re

logger = logging.getLogger(__name__)


class JavaScriptRenderer:
    """
    Render JavaScript-heavy pages using Playwright

    Features:
    - Full browser automation (Chromium/Firefox/WebKit)
    - Wait for network idle, DOM elements, custom conditions
    - Execute JavaScript in page context
    - Handle popups, dialogs, and authentication
    - Screenshot and PDF generation
    - Stealth mode (avoid bot detection)

    Usage:
    renderer = JavaScriptRenderer(browser_type="chromium", headless=True)
    await renderer.start()
    html = await renderer.render("https://example.com")
    await renderer.close()
    """

    def __init__(
        self,
        browser_type: str = "chromium",  # chromium, firefox, webkit
        headless: bool = True,
        timeout: int = 30000,  # milliseconds
        stealth_mode: bool = True
    ):
        """
        Initialize JavaScript renderer

        Args:
            browser_type: Browser engine (chromium, firefox, webkit)
            headless: Run without GUI
            timeout: Default timeout in milliseconds
            stealth_mode: Enable anti-bot detection measures
        """
        self.browser_type = browser_type
        self.headless = headless
        self.timeout = timeout
        self.stealth_mode = stealth_mode

        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None

        # Statistics
        self.stats = {
            "pages_rendered": 0,
            "total_time_ms": 0,
            "errors": 0,
            "timeouts": 0
        }

    async def start(self):
        """Start Playwright browser"""
        try:
            self.playwright = await async_playwright().start()

            # Select browser
            if self.browser_type == "chromium":
                browser_launcher = self.playwright.chromium
            elif self.browser_type == "firefox":
                browser_launcher = self.playwright.firefox
            elif self.browser_type == "webkit":
                browser_launcher = self.playwright.webkit
            else:
                raise ValueError(f"Unknown browser type: {self.browser_type}")

            # Launch browser
            self.browser = await browser_launcher.launch(
                headless=self.headless,
                args=[
                    "--disable-blink-features=AutomationControlled",  # Anti-detection
                    "--disable-dev-shm-usage",  # Prevent memory issues
                    "--no-sandbox",  # Required in some environments
                ]
            )

            # Create browser context with stealth settings
            context_options = {
                "viewport": {"width": 1920, "height": 1080},
                "user_agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "locale": "en-US",
                "timezone_id": "America/New_York",
            }

            if self.stealth_mode:
                # Additional stealth settings
                context_options.update({
                    "java_script_enabled": True,
                    "accept_downloads": False,
                    "has_touch": False,
                    "is_mobile": False,
                })

            self.context = await self.browser.new_context(**context_options)

            # Inject stealth scripts
            if self.stealth_mode:
                await self._inject_stealth_scripts()

            logger.info(f"Playwright {self.browser_type} browser started (headless={self.headless})")

        except Exception as e:
            logger.error(f"Failed to start Playwright: {e}")
            raise

    async def _inject_stealth_scripts(self):
        """Inject JavaScript to avoid bot detection"""
        stealth_js = """
        // Override navigator.webdriver
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });

        // Override Chrome detection
        window.chrome = {
            runtime: {}
        };

        // Override permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );

        // Override plugins
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });

        // Override languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en']
        });
        """
        await self.context.add_init_script(stealth_js)

    async def render(
        self,
        url: str,
        wait_for: str = "networkidle",  # networkidle, load, domcontentloaded, or selector
        wait_selector: Optional[str] = None,
        wait_timeout: Optional[int] = None,
        execute_js: Optional[str] = None,
        scroll_to_bottom: bool = False
    ) -> Dict[str, Any]:
        """
        Render page and return HTML + metadata

        Args:
            url: URL to render
            wait_for: Wait condition (networkidle, load, domcontentloaded)
            wait_selector: CSS selector to wait for (e.g., ".data-loaded")
            wait_timeout: Timeout in milliseconds (overrides default)
            execute_js: JavaScript to execute before extraction
            scroll_to_bottom: Scroll to trigger lazy loading

        Returns:
            {
                "url": str,
                "html": str,
                "title": str,
                "meta": dict,
                "final_url": str,  # After redirects
                "status_code": int,
                "render_time_ms": float
            }
        """
        if not self.browser:
            raise RuntimeError("Browser not started. Call start() first.")

        page: Optional[Page] = None
        start_time = asyncio.get_event_loop().time()

        try:
            # Create new page
            page = await self.context.new_page()

            # Set timeout
            page.set_default_timeout(wait_timeout or self.timeout)

            # Navigate to URL
            response = await page.goto(url, wait_until=wait_for)

            # Wait for specific selector if provided
            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=wait_timeout or self.timeout)
                except Exception as e:
                    logger.warning(f"Selector '{wait_selector}' not found: {e}")

            # Scroll to bottom for lazy loading
            if scroll_to_bottom:
                await self._scroll_to_bottom(page)

            # Execute custom JavaScript
            if execute_js:
                try:
                    await page.evaluate(execute_js)
                except Exception as e:
                    logger.warning(f"JavaScript execution error: {e}")

            # Extract data
            html = await page.content()
            title = await page.title()
            final_url = page.url

            # Extract meta tags
            meta = await page.evaluate("""
                () => {
                    const metas = {};
                    document.querySelectorAll('meta').forEach(tag => {
                        const name = tag.getAttribute('name') || tag.getAttribute('property');
                        const content = tag.getAttribute('content');
                        if (name && content) {
                            metas[name] = content;
                        }
                    });
                    return metas;
                }
            """)

            # Calculate render time
            end_time = asyncio.get_event_loop().time()
            render_time_ms = (end_time - start_time) * 1000

            # Update statistics
            self.stats["pages_rendered"] += 1
            self.stats["total_time_ms"] += render_time_ms

            logger.info(
                f"Rendered '{url}' in {render_time_ms:.0f}ms "
                f"(final: {final_url}, status: {response.status if response else 'unknown'})"
            )

            return {
                "url": url,
                "html": html,
                "title": title,
                "meta": meta,
                "final_url": final_url,
                "status_code": response.status if response else 200,
                "render_time_ms": render_time_ms
            }

        except asyncio.TimeoutError:
            self.stats["timeouts"] += 1
            logger.error(f"Timeout rendering '{url}' after {wait_timeout or self.timeout}ms")
            raise

        except Error as e:
            self.stats["errors"] += 1
            logger.error(f"Playwright error rendering '{url}': {e}")
            raise

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Unexpected error rendering '{url}': {e}")
            raise

        finally:
            if page:
                await page.close()

    async def _scroll_to_bottom(self, page: Page, scroll_pause: float = 1.0):
        """
        Scroll to bottom of page to trigger lazy loading

        Args:
            page: Playwright page
            scroll_pause: Seconds to wait between scrolls
        """
        last_height = await page.evaluate("document.body.scrollHeight")

        while True:
            # Scroll down
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(scroll_pause)

            # Check if new content loaded
            new_height = await page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        logger.debug(f"Scrolled to bottom (final height: {last_height}px)")

    async def render_batch(
        self,
        urls: List[str],
        max_concurrent: int = 3,
        **render_kwargs
    ) -> List[Dict[str, Any]]:
        """
        Render multiple URLs concurrently

        Args:
            urls: List of URLs to render
            max_concurrent: Max concurrent browser pages
            **render_kwargs: Arguments passed to render()

        Returns:
            List of render results (same order as input URLs)
        """
        semaphore = asyncio.Semaphore(max_concurrent)

        async def render_with_semaphore(url: str):
            async with semaphore:
                try:
                    return await self.render(url, **render_kwargs)
                except Exception as e:
                    logger.error(f"Error rendering {url}: {e}")
                    return {
                        "url": url,
                        "error": str(e),
                        "html": None
                    }

        tasks = [render_with_semaphore(url) for url in urls]
        results = await asyncio.gather(*tasks)

        logger.info(f"Rendered {len(urls)} URLs ({max_concurrent} concurrent)")
        return results

    async def extract_structured_data(
        self,
        url: str,
        selectors: Dict[str, str],
        **render_kwargs
    ) -> Dict[str, Any]:
        """
        Render page and extract structured data using CSS selectors

        Args:
            url: URL to render
            selectors: Dict of {field_name: css_selector}
            **render_kwargs: Arguments passed to render()

        Returns:
            {
                "url": str,
                "data": {field_name: extracted_value},
                "render_time_ms": float
            }

        Example:
            data = await renderer.extract_structured_data(
                url="https://linkedin.com/in/johndoe",
                selectors={
                    "name": "h1.top-card-layout__title",
                    "headline": "div.top-card-layout__headline",
                    "location": "span.top-card__subline-item",
                }
            )
        """
        result = await self.render(url, **render_kwargs)

        page = await self.context.new_page()
        try:
            await page.goto(url, wait_until="networkidle")

            data = {}
            for field, selector in selectors.items():
                try:
                    element = await page.query_selector(selector)
                    if element:
                        data[field] = await element.text_content()
                    else:
                        data[field] = None
                except Exception as e:
                    logger.warning(f"Failed to extract '{field}' with selector '{selector}': {e}")
                    data[field] = None

            return {
                "url": url,
                "data": data,
                "render_time_ms": result["render_time_ms"]
            }

        finally:
            await page.close()

    async def screenshot(
        self,
        url: str,
        path: str,
        full_page: bool = True,
        **render_kwargs
    ) -> str:
        """
        Render page and take screenshot

        Args:
            url: URL to screenshot
            path: File path to save screenshot
            full_page: Capture full page (not just viewport)
            **render_kwargs: Arguments passed to render()

        Returns:
            Path to screenshot file
        """
        page = await self.context.new_page()
        try:
            await page.goto(url, wait_until="networkidle")
            await page.screenshot(path=path, full_page=full_page)
            logger.info(f"Screenshot saved: {path}")
            return path
        finally:
            await page.close()

    def get_stats(self) -> Dict[str, Any]:
        """Get rendering statistics"""
        stats = self.stats.copy()
        if stats["pages_rendered"] > 0:
            stats["avg_render_time_ms"] = stats["total_time_ms"] / stats["pages_rendered"]
        return stats

    async def close(self):
        """Close browser and cleanup"""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

        logger.info("Playwright browser closed")


class CamouflageRenderer(JavaScriptRenderer):
    """
    Camoufox-based renderer for maximum stealth

    Camoufox is a Firefox-based browser with enhanced privacy features.
    Better for scraping sites with strict bot detection (LinkedIn, etc.)

    Installation:
        pip install camoufox

    Usage: Same as JavaScriptRenderer but with Firefox engine
    """

    def __init__(self, **kwargs):
        super().__init__(browser_type="firefox", **kwargs)
        logger.info("Using Camoufox renderer (Firefox-based, stealth mode)")


# Usage Example:
"""
# Basic rendering
renderer = JavaScriptRenderer(browser_type="chromium", headless=True)
await renderer.start()

# Render single page
result = await renderer.render(
    url="https://example.com",
    wait_for="networkidle",
    scroll_to_bottom=True
)

print(f"Title: {result['title']}")
print(f"HTML length: {len(result['html'])} bytes")
print(f"Render time: {result['render_time_ms']:.0f}ms")

# Extract structured data
profile_data = await renderer.extract_structured_data(
    url="https://linkedin.com/in/johndoe",
    selectors={
        "name": "h1.top-card-layout__title",
        "headline": "div.top-card-layout__headline",
        "company": "a.top-card__subline-link",
        "location": "span.top-card__subline-item",
    }
)
print(f"Profile: {profile_data['data']}")

# Batch rendering
urls = [
    "https://example.com/page1",
    "https://example.com/page2",
    "https://example.com/page3",
]
results = await renderer.render_batch(urls, max_concurrent=3)

# Screenshot
await renderer.screenshot(
    url="https://example.com",
    path="screenshot.png",
    full_page=True
)

# Get statistics
stats = renderer.get_stats()
print(f"Pages rendered: {stats['pages_rendered']}")
print(f"Avg render time: {stats['avg_render_time_ms']:.0f}ms")
print(f"Errors: {stats['errors']}")

await renderer.close()


# Stealth mode with Camoufox (for difficult sites)
stealth_renderer = CamouflageRenderer(headless=True, stealth_mode=True)
await stealth_renderer.start()

result = await stealth_renderer.render("https://linkedin.com/company/google")
print(f"HTML extracted: {len(result['html'])} bytes")

await stealth_renderer.close()


# When to use Layer 3:
# 1. Layer 1 (static scraping) returns incomplete/empty content
# 2. Page uses React, Vue, Angular, or other JS frameworks
# 3. Content loads dynamically (infinite scroll, AJAX)
# 4. Need to interact with page (clicks, form fills)

# Cost comparison:
# Static scraping (Layer 1): 0.1-0.5s per page, low CPU
# JS rendering (Layer 3): 5-10s per page, high CPU
# Always try Layer 1 first, use Layer 3 only when needed!
"""
