"""
Browser Profile System — Anti-Detection Header Generation

Provides realistic, internally-consistent browser fingerprints for web scraping.
Each profile bundles: User-Agent, sec-ch-ua Client Hints, Accept headers,
Accept-Language (with regional variation), Accept-Encoding, and Sec-Fetch-* headers.

All 3 scraping modules import from here instead of maintaining their own UA lists.

Usage:
    from app.services.browser_profiles import get_headers, smart_delay

    headers = get_headers()                           # random full header set
    headers = get_headers(context="navigate")         # top-level navigation
    headers = get_headers(context="subresource")      # XHR/fetch from page
    headers = get_headers(referer="https://example.com/page")

    await smart_delay(1.0)                            # jittered ~0.5–1.5s
    await smart_delay(2.0, jitter=0.8)                # jittered ~0.4–3.6s
"""

import random
import asyncio
import time
import logging
from collections import defaultdict
from typing import Optional, Dict, Literal, Set
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# ============================================
# BROWSER PROFILE DEFINITIONS (Feb 2026)
# ============================================
# Each profile is a dict with consistent UA + Client Hints.
# Chrome, Edge, Firefox, Safari across Windows, Mac, Linux.

_CHROME_PROFILES = [
    # Chrome 134 Windows
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="134", "Google Chrome";v="134", "Not:A-Brand";v="24"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "chrome",
    },
    # Chrome 133 Windows
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="133", "Google Chrome";v="133", "Not:A-Brand";v="24"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "chrome",
    },
    # Chrome 132 Windows
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="132", "Google Chrome";v="132", "Not:A-Brand";v="24"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "chrome",
    },
    # Chrome 131 Windows
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="131", "Google Chrome";v="131", "Not:A-Brand";v="24"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "chrome",
    },
    # Chrome 134 Mac
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="134", "Google Chrome";v="134", "Not:A-Brand";v="24"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"macOS"',
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "chrome",
    },
    # Chrome 133 Mac
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="133", "Google Chrome";v="133", "Not:A-Brand";v="24"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"macOS"',
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "chrome",
    },
    # Chrome 134 Linux
    {
        "ua": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="134", "Google Chrome";v="134", "Not:A-Brand";v="24"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Linux"',
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "chrome",
    },
    # Chrome 133 Linux
    {
        "ua": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="133", "Google Chrome";v="133", "Not:A-Brand";v="24"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Linux"',
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "chrome",
    },
]

_EDGE_PROFILES = [
    # Edge 134 Windows
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
        "sec_ch_ua": '"Chromium";v="134", "Microsoft Edge";v="134", "Not:A-Brand";v="24"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "edge",
    },
    # Edge 133 Windows
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
        "sec_ch_ua": '"Chromium";v="133", "Microsoft Edge";v="133", "Not:A-Brand";v="24"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "edge",
    },
    # Edge 132 Windows
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",
        "sec_ch_ua": '"Chromium";v="132", "Microsoft Edge";v="132", "Not:A-Brand";v="24"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "edge",
    },
    # Edge 134 Mac
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
        "sec_ch_ua": '"Chromium";v="134", "Microsoft Edge";v="134", "Not:A-Brand";v="24"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"macOS"',
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "edge",
    },
]

_FIREFOX_PROFILES = [
    # Firefox 134 Windows
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0",
        "sec_ch_ua": None,  # Firefox does not send Client Hints
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "firefox",
    },
    # Firefox 133 Windows
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
        "sec_ch_ua": None,
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "firefox",
    },
    # Firefox 134 Mac
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:134.0) Gecko/20100101 Firefox/134.0",
        "sec_ch_ua": None,
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "firefox",
    },
    # Firefox 133 Mac
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
        "sec_ch_ua": None,
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "firefox",
    },
    # Firefox 134 Linux
    {
        "ua": "Mozilla/5.0 (X11; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/134.0",
        "sec_ch_ua": None,
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "firefox",
    },
    # Firefox 132 Windows
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
        "sec_ch_ua": None,
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "firefox",
    },
]

_SAFARI_PROFILES = [
    # Safari 18.3 Mac (Sonoma)
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15",
        "sec_ch_ua": None,  # Safari does not send Client Hints
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
        "accept_encoding": "gzip, deflate, br",  # Safari doesn't support zstd yet
        "browser": "safari",
    },
    # Safari 18.2 Mac
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
        "sec_ch_ua": None,
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
        "accept_encoding": "gzip, deflate, br",
        "browser": "safari",
    },
    # Safari 18.1 Mac
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
        "sec_ch_ua": None,
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
        "accept_encoding": "gzip, deflate, br",
        "browser": "safari",
    },
    # Safari 17.6 Mac (older but still common)
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
        "sec_ch_ua": None,
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
        "accept_encoding": "gzip, deflate, br",
        "browser": "safari",
    },
]

# Combined pool: 26 profiles (8 Chrome + 4 Edge + 6 Firefox + 4 Safari + 4 mobile below)
# Weighted toward Chrome (~50% market share)
_MOBILE_PROFILES = [
    # Chrome Mobile Android
    {
        "ua": "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Mobile Safari/537.36",
        "sec_ch_ua": '"Chromium";v="134", "Google Chrome";v="134", "Not:A-Brand";v="24"',
        "sec_ch_ua_mobile": "?1",
        "sec_ch_ua_platform": '"Android"',
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "chrome_mobile",
    },
    # Chrome Mobile Android older
    {
        "ua": "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Mobile Safari/537.36",
        "sec_ch_ua": '"Chromium";v="133", "Google Chrome";v="133", "Not:A-Brand";v="24"',
        "sec_ch_ua_mobile": "?1",
        "sec_ch_ua_platform": '"Android"',
        "accept_encoding": "gzip, deflate, br, zstd",
        "browser": "chrome_mobile",
    },
    # Safari Mobile iPhone
    {
        "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Mobile/15E148 Safari/604.1",
        "sec_ch_ua": None,
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
        "accept_encoding": "gzip, deflate, br",
        "browser": "safari_mobile",
    },
    # Safari Mobile iPhone older
    {
        "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.7 Mobile/15E148 Safari/604.1",
        "sec_ch_ua": None,
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
        "accept_encoding": "gzip, deflate, br",
        "browser": "safari_mobile",
    },
]

# All desktop profiles (used by default — mobile profiles only added if specifically requested)
ALL_DESKTOP_PROFILES = _CHROME_PROFILES + _EDGE_PROFILES + _FIREFOX_PROFILES + _SAFARI_PROFILES
ALL_PROFILES = ALL_DESKTOP_PROFILES + _MOBILE_PROFILES

# Legacy-compatible flat UA list (for modules that only need the UA string)
SEARCH_USER_AGENTS = [p["ua"] for p in ALL_DESKTOP_PROFILES]
USER_AGENTS = SEARCH_USER_AGENTS  # alias


# ============================================
# ACCEPT-LANGUAGE VARIATION
# ============================================
# Real browsers send different locale preferences.
# We pick one per "session" to stay consistent within a scraping run.

_ACCEPT_LANGUAGES = [
    "en-US,en;q=0.9",
    "en-US,en;q=0.9,es;q=0.8",
    "en-GB,en;q=0.9,en-US;q=0.8",
    "en-US,en;q=0.9,fr;q=0.8",
    "en-US,en;q=0.9,de;q=0.8",
    "en-CA,en;q=0.9,en-US;q=0.8",
    "en-AU,en;q=0.9,en-US;q=0.8",
    "en-US,en;q=0.9,pt;q=0.8,es;q=0.7",
    "en-US,en;q=0.9,ja;q=0.8",
    "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
]


# ============================================
# HEADER GENERATION
# ============================================

def get_random_profile(*, desktop_only: bool = True) -> Dict:
    """Return a random browser profile dict."""
    pool = ALL_DESKTOP_PROFILES if desktop_only else ALL_PROFILES
    return random.choice(pool)


def get_headers(
    *,
    profile: Optional[Dict] = None,
    context: Literal["navigate", "subresource", "search_api"] = "navigate",
    referer: Optional[str] = None,
    desktop_only: bool = True,
) -> Dict[str, str]:
    """
    Generate a complete, internally-consistent header set.

    Args:
        profile: Specific browser profile to use. If None, picks random.
        context: Request context for Sec-Fetch-* headers:
            - "navigate": top-level page load (default)
            - "subresource": XHR/fetch from a page
            - "search_api": API-style request (JSON accept)
        referer: Optional Referer URL to include.
        desktop_only: Whether to exclude mobile profiles.

    Returns:
        Dict of HTTP headers in browser-canonical order.
    """
    if profile is None:
        profile = get_random_profile(desktop_only=desktop_only)

    browser = profile["browser"]
    is_chromium = browser in ("chrome", "edge", "chrome_mobile")
    is_firefox = browser in ("firefox",)
    is_safari = browser in ("safari", "safari_mobile")

    headers = {}

    # -- Browser-canonical header ordering --
    # Chromium sends headers in this order: Host, Connection, sec-ch-ua, sec-ch-ua-mobile,
    # sec-ch-ua-platform, Upgrade-Insecure-Requests, User-Agent, Accept, Sec-Fetch-*,
    # Accept-Encoding, Accept-Language

    if is_chromium:
        headers["Connection"] = "keep-alive"

        # Client Hints (Chrome/Edge only — Firefox and Safari do NOT send these)
        if profile.get("sec_ch_ua"):
            headers["sec-ch-ua"] = profile["sec_ch_ua"]
            headers["sec-ch-ua-mobile"] = profile["sec_ch_ua_mobile"]
            headers["sec-ch-ua-platform"] = profile["sec_ch_ua_platform"]

        if context == "navigate":
            headers["Upgrade-Insecure-Requests"] = "1"

        headers["User-Agent"] = profile["ua"]

        if context == "search_api":
            headers["Accept"] = "application/json, text/plain, */*"
        elif context == "navigate":
            headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
        else:  # subresource
            headers["Accept"] = "*/*"

        # Sec-Fetch headers
        if context == "navigate":
            headers["Sec-Fetch-Site"] = "none" if not referer else "cross-site"
            headers["Sec-Fetch-Mode"] = "navigate"
            headers["Sec-Fetch-User"] = "?1"
            headers["Sec-Fetch-Dest"] = "document"
        elif context == "subresource":
            headers["Sec-Fetch-Site"] = "same-origin"
            headers["Sec-Fetch-Mode"] = "cors"
            headers["Sec-Fetch-Dest"] = "empty"

    elif is_firefox:
        headers["User-Agent"] = profile["ua"]

        if context == "search_api":
            headers["Accept"] = "application/json, text/plain, */*"
        elif context == "navigate":
            headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8"
        else:
            headers["Accept"] = "*/*"

        if context == "navigate":
            headers["Upgrade-Insecure-Requests"] = "1"

        # Firefox sends Sec-Fetch-* but NOT Client Hints
        if context == "navigate":
            headers["Sec-Fetch-Dest"] = "document"
            headers["Sec-Fetch-Mode"] = "navigate"
            headers["Sec-Fetch-Site"] = "none" if not referer else "cross-site"
            headers["Sec-Fetch-User"] = "?1"
        elif context == "subresource":
            headers["Sec-Fetch-Dest"] = "empty"
            headers["Sec-Fetch-Mode"] = "cors"
            headers["Sec-Fetch-Site"] = "same-origin"

        headers["Connection"] = "keep-alive"

    elif is_safari:
        # Safari has simpler headers — no Client Hints, no Sec-Fetch-*
        headers["User-Agent"] = profile["ua"]

        if context == "search_api":
            headers["Accept"] = "application/json, text/plain, */*"
        elif context == "navigate":
            headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        else:
            headers["Accept"] = "*/*"

        headers["Connection"] = "keep-alive"

    # Common headers for all browsers
    headers["Accept-Language"] = random.choice(_ACCEPT_LANGUAGES)
    headers["Accept-Encoding"] = profile["accept_encoding"]

    if referer:
        headers["Referer"] = referer

    # DNT (some users have it on, ~15%)
    if random.random() < 0.15:
        headers["DNT"] = "1"

    return headers


def get_ua() -> str:
    """Return a single random desktop User-Agent string (legacy helper)."""
    return random.choice(SEARCH_USER_AGENTS)


# ============================================
# SMART DELAY UTILITY
# ============================================

async def smart_delay(
    base: float = 1.0,
    jitter: float = 0.5,
    *,
    minimum: float = 0.1,
) -> None:
    """
    Sleep for a randomized duration with human-like jitter.

    Actual sleep = base * (1 - jitter + 2 * jitter * random())
    With default jitter=0.5:  sleep ∈ [base*0.5, base*1.5]

    Args:
        base: Center of the delay range in seconds.
        jitter: Jitter factor 0-1. 0=exact, 0.5=±50%, 1.0=±100% (0 to 2x base).
        minimum: Floor — never sleep less than this (default 0.1s).
    """
    jitter = max(0.0, min(1.0, jitter))  # clamp
    actual = base * (1.0 - jitter + 2.0 * jitter * random.random())
    actual = max(minimum, actual)
    await asyncio.sleep(actual)


async def backoff_delay(
    attempt: int,
    base: float = 1.0,
    max_delay: float = 30.0,
    jitter: float = 0.5,
) -> None:
    """
    Exponential backoff with jitter.

    delay = min(base * 2^attempt, max_delay) * (1 ± jitter)

    Args:
        attempt: Retry attempt number (0-indexed).
        base: Base delay in seconds.
        max_delay: Maximum delay cap.
        jitter: Jitter factor 0-1.
    """
    exp_delay = min(base * (2 ** attempt), max_delay)
    await smart_delay(exp_delay, jitter)


# ============================================
# BROWSER SESSION (Consistent Identity)
# ============================================

class BrowserSession:
    """
    Maintains a consistent browser identity across a scraping session.

    Within a session, the same browser profile (UA, Client Hints) and
    Accept-Language are used for all requests, which is how a real browser
    behaves. Different sessions get different identities.

    Usage:
        session = BrowserSession()
        headers1 = session.get_headers()                     # same UA every time
        headers2 = session.get_headers(referer="https://...")  # same UA, adds referer
    """

    def __init__(self, *, desktop_only: bool = True):
        self._profile = get_random_profile(desktop_only=desktop_only)
        self._accept_language = random.choice(_ACCEPT_LANGUAGES)
        self._has_dnt = random.random() < 0.15

    @property
    def user_agent(self) -> str:
        return self._profile["ua"]

    @property
    def browser_type(self) -> str:
        return self._profile["browser"]

    def get_headers(
        self,
        *,
        context: Literal["navigate", "subresource", "search_api"] = "navigate",
        referer: Optional[str] = None,
    ) -> Dict[str, str]:
        """Generate headers using this session's consistent identity."""
        headers = get_headers(
            profile=self._profile,
            context=context,
            referer=referer,
        )
        # Override with session-consistent values
        headers["Accept-Language"] = self._accept_language
        if self._has_dnt:
            headers["DNT"] = "1"
        elif "DNT" in headers:
            del headers["DNT"]
        return headers


# ============================================
# PER-DOMAIN CONCURRENCY CONTROL
# ============================================

class DomainSemaphoreManager:
    """
    Per-domain concurrency limiter.

    Prevents hammering any single domain while allowing high overall parallelism.
    Uses a global semaphore (max total concurrent requests) AND per-domain semaphores
    (max concurrent requests to the same domain).

    Usage:
        dsm = DomainSemaphoreManager(global_limit=15, per_domain_limit=3)

        async with dsm.acquire("example.com"):
            response = await client.get("https://example.com/page")

    Features:
        - Lazy per-domain semaphore creation (no memory waste for unseen domains)
        - Auto-cleanup of idle domain semaphores after configurable TTL
        - Domain extraction from URLs
        - Stats tracking (active requests per domain, total active)
    """

    def __init__(
        self,
        global_limit: int = 15,
        per_domain_limit: int = 3,
        cleanup_ttl: float = 300.0,  # 5 min before removing idle domain semaphores
    ):
        self._global_sem = asyncio.Semaphore(global_limit)
        self._per_domain_limit = per_domain_limit
        self._domain_sems: Dict[str, asyncio.Semaphore] = {}
        self._domain_last_used: Dict[str, float] = {}
        self._domain_active: Dict[str, int] = defaultdict(int)
        self._total_active: int = 0
        self._cleanup_ttl = cleanup_ttl
        self._lock = asyncio.Lock()

    def _normalize_domain(self, domain_or_url: str) -> str:
        """Extract and normalize domain from URL or plain domain string."""
        if "://" in domain_or_url:
            parsed = urlparse(domain_or_url)
            domain = parsed.netloc or parsed.path
        else:
            domain = domain_or_url
        # Strip www. and port
        domain = domain.lower().split(":")[0]
        if domain.startswith("www."):
            domain = domain[4:]
        return domain

    async def _get_domain_sem(self, domain: str) -> asyncio.Semaphore:
        """Get or create a per-domain semaphore (thread-safe)."""
        async with self._lock:
            if domain not in self._domain_sems:
                self._domain_sems[domain] = asyncio.Semaphore(self._per_domain_limit)
            self._domain_last_used[domain] = time.monotonic()
            return self._domain_sems[domain]

    class _AcquireContext:
        """Async context manager for dual-semaphore acquisition."""

        def __init__(self, manager: "DomainSemaphoreManager", domain: str):
            self._mgr = manager
            self._domain = domain

        async def __aenter__(self):
            # Acquire global first (prevents deadlock ordering)
            await self._mgr._global_sem.acquire()
            try:
                sem = await self._mgr._get_domain_sem(self._domain)
                await sem.acquire()
            except Exception:
                self._mgr._global_sem.release()
                raise
            self._mgr._domain_active[self._domain] += 1
            self._mgr._total_active += 1
            return self

        async def __aexit__(self, *exc):
            self._mgr._domain_active[self._domain] -= 1
            self._mgr._total_active -= 1
            sem = self._mgr._domain_sems.get(self._domain)
            if sem:
                sem.release()
            self._mgr._global_sem.release()
            return False

    def acquire(self, domain_or_url: str) -> "_AcquireContext":
        """Return an async context manager that acquires both global + domain semaphore."""
        domain = self._normalize_domain(domain_or_url)
        return self._AcquireContext(self, domain)

    async def cleanup_idle(self) -> int:
        """Remove semaphores for domains not used recently. Returns count removed."""
        now = time.monotonic()
        removed = 0
        async with self._lock:
            stale = [
                d for d, t in self._domain_last_used.items()
                if now - t > self._cleanup_ttl and self._domain_active.get(d, 0) == 0
            ]
            for d in stale:
                del self._domain_sems[d]
                del self._domain_last_used[d]
                self._domain_active.pop(d, None)
                removed += 1
        if removed:
            logger.debug(f"DomainSemaphoreManager: cleaned up {removed} idle domain slots")
        return removed

    @property
    def stats(self) -> Dict:
        """Current concurrency stats."""
        return {
            "total_active": self._total_active,
            "domains_tracked": len(self._domain_sems),
            "per_domain_active": dict(self._domain_active),
        }

    def start_cleanup_loop(self, interval: float = 60.0):
        """Start a background task that periodically cleans idle domain semaphores."""
        if hasattr(self, '_cleanup_task') and self._cleanup_task and not self._cleanup_task.done():
            return  # Already running
        self._cleanup_task = asyncio.ensure_future(self._periodic_cleanup(interval))

    async def _periodic_cleanup(self, interval: float):
        """Background loop that runs cleanup_idle() every `interval` seconds."""
        try:
            while True:
                await asyncio.sleep(interval)
                await self.cleanup_idle()
        except asyncio.CancelledError:
            pass

    def stop_cleanup_loop(self):
        """Cancel the background cleanup task."""
        if hasattr(self, '_cleanup_task') and self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()


# Singleton instance for use across all scraping modules
_domain_semaphore: Optional[DomainSemaphoreManager] = None


def get_domain_semaphore(
    global_limit: int = 15,
    per_domain_limit: int = 3,
) -> DomainSemaphoreManager:
    """Get or create the global DomainSemaphoreManager singleton."""
    global _domain_semaphore
    if _domain_semaphore is None:
        _domain_semaphore = DomainSemaphoreManager(
            global_limit=global_limit,
            per_domain_limit=per_domain_limit,
        )
        try:
            _domain_semaphore.start_cleanup_loop(interval=60.0)
        except RuntimeError:
            pass  # No event loop yet; cleanup will start on first async usage
    return _domain_semaphore


# ============================================
# ADAPTIVE CONCURRENCY CONTROLLER (AIMD)
# ============================================

class AdaptiveConcurrencyController:
    """
    AIMD (Additive Increase / Multiplicative Decrease) concurrency controller.

    Dynamically adjusts the concurrency level based on success/failure of requests:
    - On success: increase window by 1 (additive increase)
    - On failure/timeout: halve window (multiplicative decrease)
    - Bounded between min_concurrency and max_concurrency

    This is the same algorithm TCP uses for congestion control — proven optimal
    for converging to the right throughput under unknown server capacity.

    Usage:
        controller = AdaptiveConcurrencyController(min_c=2, max_c=20)

        async with controller.acquire():
            response = await client.get(url)
            if response.status_code == 429:
                controller.record_failure()
            else:
                controller.record_success()
    """

    def __init__(
        self,
        min_concurrency: int = 2,
        max_concurrency: int = 20,
        initial_concurrency: int = 8,
        decrease_factor: float = 0.5,
        increase_step: int = 1,
        # Slow-start: double concurrency until first failure (like TCP slow-start)
        slow_start: bool = True,
    ):
        self._min = min_concurrency
        self._max = max_concurrency
        self._current = min(max(initial_concurrency, min_concurrency), max_concurrency)
        self._decrease_factor = decrease_factor
        self._increase_step = increase_step
        self._semaphore = asyncio.Semaphore(self._current)
        self._lock = asyncio.Lock()

        # Slow-start phase: double until first failure
        self._in_slow_start = slow_start
        self._success_count = 0
        self._failure_count = 0
        self._total_requests = 0

        # Stats
        self._adjustments: list = []  # recent (timestamp, direction, new_level)

    def _rebuild_semaphore(self, new_limit: int):
        """Rebuild the semaphore with a new limit (thread-safe)."""
        old = self._current
        self._current = max(self._min, min(new_limit, self._max))
        if self._current != old:
            self._semaphore = asyncio.Semaphore(self._current)
            self._adjustments.append((time.monotonic(), "up" if self._current > old else "down", self._current))
            # Keep only last 50 adjustments
            if len(self._adjustments) > 50:
                self._adjustments = self._adjustments[-50:]
            logger.debug(f"AIMD concurrency: {old} → {self._current}")

    def record_success(self):
        """Call after a successful request to potentially increase concurrency."""
        self._success_count += 1
        self._total_requests += 1

        if self._in_slow_start:
            # Slow-start: double every N successes (aggressive growth)
            if self._success_count % max(self._current, 1) == 0:
                self._rebuild_semaphore(self._current * 2)
        else:
            # Congestion avoidance: linear growth
            if self._success_count % max(self._current, 1) == 0:
                self._rebuild_semaphore(self._current + self._increase_step)

    def record_failure(self):
        """Call after a failed/rate-limited request to decrease concurrency."""
        self._failure_count += 1
        self._total_requests += 1

        # Exit slow-start on first failure
        self._in_slow_start = False

        # Multiplicative decrease
        new_limit = int(self._current * self._decrease_factor)
        self._rebuild_semaphore(new_limit)

    class _AcquireContext:
        def __init__(self, controller: "AdaptiveConcurrencyController"):
            self._ctrl = controller

        async def __aenter__(self):
            await self._ctrl._semaphore.acquire()
            return self

        async def __aexit__(self, *exc):
            self._ctrl._semaphore.release()
            return False

    def acquire(self) -> "_AcquireContext":
        """Return an async context manager for acquiring a concurrency slot."""
        return self._AcquireContext(self)

    @property
    def current_concurrency(self) -> int:
        return self._current

    @property
    def stats(self) -> Dict:
        return {
            "current_concurrency": self._current,
            "min": self._min,
            "max": self._max,
            "in_slow_start": self._in_slow_start,
            "total_requests": self._total_requests,
            "successes": self._success_count,
            "failures": self._failure_count,
            "recent_adjustments": len(self._adjustments),
        }


# ============================================
# DNS PREFETCHING & CONNECTION WARM-UP
# ============================================

class DNSPrefetcher:
    """
    Pre-resolve DNS for a batch of domains before scraping.

    DNS resolution typically takes 20-100ms per domain. By resolving all domains
    upfront in parallel, we eliminate this latency from the critical scraping path.

    Also supports connection warm-up via lightweight HEAD requests to establish
    TCP + TLS connections before the main scraping starts.

    Usage:
        prefetcher = DNSPrefetcher()
        resolved = await prefetcher.prefetch_domains(["example.com", "test.org"])
        # Now scrape — DNS is cached by the OS resolver
    """

    def __init__(self, concurrency: int = 30, warmup_timeout: float = 5.0):
        self._concurrency = concurrency
        self._warmup_timeout = warmup_timeout
        self._resolved: Dict[str, bool] = {}  # domain → success
        self._warmup_done: Set[str] = set()

    async def prefetch_domains(self, domains: list[str]) -> Dict[str, bool]:
        """
        Pre-resolve DNS for a list of domains in parallel.

        Args:
            domains: List of domain names (without protocol)

        Returns:
            Dict mapping domain → whether resolution succeeded
        """
        sem = asyncio.Semaphore(self._concurrency)
        results: Dict[str, bool] = {}

        async def resolve_one(domain: str):
            async with sem:
                try:
                    loop = asyncio.get_event_loop()
                    await asyncio.wait_for(
                        loop.getaddrinfo(domain, 443),
                        timeout=5.0,
                    )
                    results[domain] = True
                    self._resolved[domain] = True
                except Exception:
                    results[domain] = False
                    self._resolved[domain] = False

        # Deduplicate and filter already-resolved
        unique_domains = list(set(d for d in domains if d and d not in self._resolved))

        if not unique_domains:
            return {d: self._resolved.get(d, False) for d in domains if d}

        tasks = [resolve_one(d) for d in unique_domains]
        await asyncio.gather(*tasks, return_exceptions=True)

        logger.info(
            f"DNS prefetch: {sum(1 for v in results.values() if v)}/{len(unique_domains)} "
            f"domains resolved successfully"
        )
        return results

    async def warmup_connections(
        self,
        domains: list[str],
        client: Optional[object] = None,
    ) -> int:
        """
        Establish TCP+TLS connections via HEAD requests.

        This warms up the connection pool so subsequent GET requests skip
        the TCP handshake + TLS negotiation (~100-300ms savings per domain).

        Args:
            domains: List of domains to warm up
            client: httpx.AsyncClient to use (shares its connection pool)

        Returns:
            Number of successful warm-up connections
        """
        import httpx as _httpx

        sem = asyncio.Semaphore(self._concurrency)
        success_count = 0

        async def warmup_one(domain: str):
            nonlocal success_count
            if domain in self._warmup_done:
                return
            async with sem:
                try:
                    url = f"https://{domain}/"
                    if client and hasattr(client, 'head'):
                        await asyncio.wait_for(
                            client.head(url, headers={"User-Agent": get_ua()}, follow_redirects=True),
                            timeout=self._warmup_timeout,
                        )
                    else:
                        async with _httpx.AsyncClient(
                            timeout=self._warmup_timeout,
                            http2=True,
                        ) as temp_client:
                            await temp_client.head(url, headers={"User-Agent": get_ua()}, follow_redirects=True)
                    self._warmup_done.add(domain)
                    success_count += 1
                except Exception:
                    pass  # warm-up failure is non-critical

        unique = [d for d in set(domains) if d and d not in self._warmup_done]
        if not unique:
            return 0

        tasks = [warmup_one(d) for d in unique]
        await asyncio.gather(*tasks, return_exceptions=True)

        logger.info(f"Connection warm-up: {success_count}/{len(unique)} domains ready")
        return success_count

    @property
    def stats(self) -> Dict:
        return {
            "dns_resolved": sum(1 for v in self._resolved.values() if v),
            "dns_failed": sum(1 for v in self._resolved.values() if not v),
            "connections_warmed": len(self._warmup_done),
        }
