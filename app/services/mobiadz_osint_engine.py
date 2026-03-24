"""
THEMOBIADZ OSINT ENGINE V1.0
Deep Open Source Intelligence for Company & People Data Extraction

OSINT CAPABILITIES:
==================

1. COMPANY OSINT:
   - OpenCorporates (company registry data)
   - SEC EDGAR (US public company filings)
   - Companies House UK API
   - Crunchbase public data
   - LinkedIn company pages (public)
   - Google dorking for company data

2. PEOPLE/LEADERSHIP OSINT:
   - LinkedIn public profiles
   - Twitter/X profiles
   - GitHub user profiles
   - Gravatar lookups
   - About.me pages
   - Speaker/conference profiles

3. EMAIL OSINT:
   - Google dorking for emails
   - Email permutation + verification
   - Gravatar email lookup
   - GitHub commit emails
   - Have I Been Pwned (breach check)
   - EmailRep.io reputation

4. PHONE OSINT:
   - Website scraping
   - Social media extraction
   - Business directory lookups

5. DOMAIN OSINT:
   - WHOIS data
   - DNS records (MX, TXT, NS)
   - Subdomain enumeration
   - Technology detection
   - Historical data (Wayback)

6. SOCIAL MEDIA OSINT:
   - Twitter/X profiles
   - Facebook pages
   - Instagram business
   - YouTube channels
   - TikTok profiles
"""

import asyncio
import logging
import re
import json
import hashlib
import random
import time
from typing import Dict, Any, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlparse, urlencode, quote, quote_plus, parse_qs
from collections import defaultdict

import httpx
from bs4 import BeautifulSoup
from cachetools import TTLCache

from app.services.browser_profiles import (
    get_headers, get_ua, smart_delay, backoff_delay,
    SEARCH_USER_AGENTS, USER_AGENTS,
)

# Import consolidated GitHub scraper from ultra engine (BUG 7 fix: avoid duplicate implementations)
try:
    from app.services.mobiadz_ultra_engine import GitHubOrganizationScraper as _GitHubScraper
    _ULTRA_GITHUB_AVAILABLE = True
except ImportError:
    _ULTRA_GITHUB_AVAILABLE = False
    _GitHubScraper = None

logger = logging.getLogger(__name__)


# ============================================
# DATA STRUCTURES
# ============================================

@dataclass
class PersonIntel:
    """Intelligence data for a person"""
    name: str
    title: Optional[str] = None
    company: Optional[str] = None

    # Contact info
    emails: List[str] = field(default_factory=list)
    phones: List[str] = field(default_factory=list)

    # Social profiles
    linkedin_url: Optional[str] = None
    twitter_url: Optional[str] = None
    github_url: Optional[str] = None
    facebook_url: Optional[str] = None

    # Additional data
    location: Optional[str] = None
    bio: Optional[str] = None
    profile_image: Optional[str] = None

    # OSINT metadata
    sources: List[str] = field(default_factory=list)
    confidence_score: int = 0
    last_updated: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class CompanyIntel:
    """Intelligence data for a company"""
    name: str
    domain: Optional[str] = None

    # Company info
    description: Optional[str] = None
    industry: Optional[str] = None
    founded: Optional[str] = None
    size: Optional[str] = None
    headquarters: Optional[str] = None

    # Contact info
    emails: Dict[str, str] = field(default_factory=dict)  # type -> email
    phones: List[str] = field(default_factory=list)
    address: Optional[str] = None

    # Social profiles
    linkedin_url: Optional[str] = None
    twitter_url: Optional[str] = None
    facebook_url: Optional[str] = None

    # Leadership
    leadership: List[PersonIntel] = field(default_factory=list)
    employees: List[PersonIntel] = field(default_factory=list)

    # Technical data
    technologies: List[str] = field(default_factory=list)
    subdomains: List[str] = field(default_factory=list)

    # OSINT metadata
    sources: List[str] = field(default_factory=list)
    confidence_score: int = 0


# ============================================
# USER-AGENT ROTATION POOL (Layer 7)
# Now imported from browser_profiles.py (26+ modern profiles)
# SEARCH_USER_AGENTS imported at top of file
# ============================================

# ============================================
# SEARXNG PUBLIC INSTANCES (Layer 7)
# ============================================

SEARX_INSTANCES = [
    "https://searx.be",
    "https://search.sapti.me",
    "https://searx.tiekoetter.com",
    "https://search.privacyguides.net",
    "https://searx.fmac.xyz",
    "https://priv.au",
    "https://searx.work",
]

# ============================================
# DDGS BACKENDS (Layer 7)
# ============================================

DDGS_BACKENDS = ["api", "html", "lite"]


# ============================================
# CIRCUIT BREAKER (Layer 7)
# ============================================

class SearchEngineCircuitBreaker:
    """
    Circuit breaker for search engines — skip engines that keep failing.
    After `failure_threshold` consecutive failures, the engine is "tripped"
    and won't be retried until `recovery_time` seconds have passed.
    """

    def __init__(self, failure_threshold: int = 3, recovery_time: float = 300.0):
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self.failure_counts: Dict[str, int] = {}
        self.last_failure_time: Dict[str, float] = {}
        self.trip_count: int = 0

    def is_open(self, engine: str) -> bool:
        """Check if circuit breaker is open (engine should be skipped)"""
        count = self.failure_counts.get(engine, 0)
        if count < self.failure_threshold:
            return False
        # Check if recovery time has passed
        last_fail = self.last_failure_time.get(engine, 0)
        if time.time() - last_fail >= self.recovery_time:
            # Half-open: allow a retry
            return False
        return True

    def record_success(self, engine: str) -> None:
        """Reset failure count on success"""
        self.failure_counts[engine] = 0

    def record_failure(self, engine: str) -> None:
        """Increment failure count, may trip the breaker"""
        self.failure_counts[engine] = self.failure_counts.get(engine, 0) + 1
        self.last_failure_time[engine] = time.time()
        if self.failure_counts[engine] >= self.failure_threshold:
            self.trip_count += 1
            logger.warning(f"Circuit breaker TRIPPED for engine '{engine}' after {self.failure_counts[engine]} failures")

    def get_available_engines(self, engines: List[str]) -> List[str]:
        """Filter out tripped engines"""
        return [e for e in engines if not self.is_open(e)]


# ============================================
# SEARCH ENGINE ROTATOR (Layer 7)
# ============================================

class SearchEngineRotator:
    """
    Weighted engine rotation with adaptive success tracking.
    Engines that return more results get higher weight over time.
    Integrates circuit breaker to skip failing engines.
    """

    ALL_ENGINES = ["ddgs", "bing", "brave_api", "searx", "ddg_html"]

    DEFAULT_WEIGHTS = {
        "ddgs": 5.0,
        "bing": 4.0,
        "brave_api": 3.0,
        "searx": 2.0,
        "ddg_html": 1.0,
    }

    def __init__(self, circuit_breaker: Optional[SearchEngineCircuitBreaker] = None):
        self.weights: Dict[str, float] = dict(self.DEFAULT_WEIGHTS)
        self.success_counts: Dict[str, int] = {e: 0 for e in self.ALL_ENGINES}
        self.failure_counts: Dict[str, int] = {e: 0 for e in self.ALL_ENGINES}
        self.total_results: Dict[str, int] = {e: 0 for e in self.ALL_ENGINES}
        self.circuit_breaker = circuit_breaker or SearchEngineCircuitBreaker()
        self.rotation_count: int = 0

    def select_engines(self, count: int = 3) -> List[str]:
        """Select engines using weighted random selection, filtered by circuit breaker"""
        available = self.circuit_breaker.get_available_engines(self.ALL_ENGINES)
        if not available:
            # All tripped — force-allow all (desperation mode)
            available = list(self.ALL_ENGINES)

        self.rotation_count += 1

        # Weighted selection without replacement
        selected = []
        pool = list(available)
        for _ in range(min(count, len(pool))):
            total_weight = sum(self.weights.get(e, 1.0) for e in pool)
            if total_weight <= 0:
                break
            r = random.uniform(0, total_weight)
            cumulative = 0.0
            for engine in pool:
                cumulative += self.weights.get(engine, 1.0)
                if r <= cumulative:
                    selected.append(engine)
                    pool.remove(engine)
                    break

        return selected

    def record_result(self, engine: str, success: bool, result_count: int = 0) -> None:
        """Record search result and adapt weights"""
        if success:
            self.success_counts[engine] = self.success_counts.get(engine, 0) + 1
            self.total_results[engine] = self.total_results.get(engine, 0) + result_count
            self.circuit_breaker.record_success(engine)
            # Boost weight slightly (capped at 10)
            self.weights[engine] = min(10.0, self.weights.get(engine, 1.0) + 0.2)
        else:
            self.failure_counts[engine] = self.failure_counts.get(engine, 0) + 1
            self.circuit_breaker.record_failure(engine)
            # Decrease weight (floor at 0.5)
            self.weights[engine] = max(0.5, self.weights.get(engine, 1.0) - 0.5)

    def get_stats(self) -> Dict[str, Any]:
        """Get rotation diagnostics"""
        return {
            "weights": dict(self.weights),
            "success_counts": dict(self.success_counts),
            "failure_counts": dict(self.failure_counts),
            "total_results": dict(self.total_results),
            "rotation_count": self.rotation_count,
            "circuit_breaker_trips": self.circuit_breaker.trip_count,
        }


# ============================================
# MULTI-ENGINE SEARCH
# ============================================

class MultiEngineSearch:
    """
    Multi-engine web search with intelligent routing (Layer 7 Enhanced).

    Engines: DDGS (9 backends), Bing HTML, Brave API, SearXNG, DDG HTML fallback.
    Features: TTL cache (24h), circuit breaker, weighted rotation, rate limiting,
    exponential backoff, User-Agent rotation, result deduplication.
    """

    def __init__(
        self,
        timeout: int = 30,
        brave_api_key: Optional[str] = None,
        cache_ttl: int = 86400,
        cache_maxsize: int = 2000,
    ):
        self.timeout = timeout
        self.brave_api_key = brave_api_key

        # TTL cache (24h default, 2000 entries max) — replaces unbounded dict
        self.results_cache: TTLCache = TTLCache(maxsize=cache_maxsize, ttl=cache_ttl)
        self.cache_hits: int = 0
        self.cache_misses: int = 0

        # HTTP client with randomized User-Agent
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

        # Engine rotation + circuit breaker
        self.rotator = SearchEngineRotator()

        # Per-engine rate limiting (exponential backoff)
        self._engine_delays: Dict[str, float] = {e: 1.0 for e in SearchEngineRotator.ALL_ENGINES}
        self._engine_last_call: Dict[str, float] = {}
        self._backoff_max: float = 30.0

        # SearXNG instance health tracking
        self._searx_healthy: Dict[str, bool] = {inst: True for inst in SEARX_INSTANCES}
        self._searx_last_check: Dict[str, float] = {}

        # DDGS availability
        self._ddgs_available = True
        try:
            from ddgs import DDGS  # noqa: F401
        except ImportError:
            try:
                from duckduckgo_search import DDGS  # noqa: F401
            except ImportError:
                self._ddgs_available = False
                logger.warning("ddgs not installed. Install with: pip install ddgs")

    def _get_headers(self) -> Dict[str, str]:
        """Get headers with randomized browser profile (modern UAs + Client Hints)"""
        return get_headers(context="navigate")

    # ------------------------------------------
    # INTELLIGENT SEARCH ROUTER (Task 10)
    # ------------------------------------------

    async def search(self, query: str, max_results: int = 20) -> List[Dict[str, Any]]:
        """
        Intelligent search router with TTL cache, engine rotation, and fallback.

        1. Check TTL cache
        2. Select engines via weighted rotator (circuit-breaker filtered)
        3. Try engines sequentially, short-circuit on sufficient results
        4. Merge + deduplicate by URL
        5. Cache and return
        """
        # 1. Cache check
        cache_key = hashlib.md5(f"{query}:{max_results}".encode()).hexdigest()
        if cache_key in self.results_cache:
            self.cache_hits += 1
            return self.results_cache[cache_key]
        self.cache_misses += 1

        # 2. Select engines (weighted, circuit-breaker filtered)
        engines = self.rotator.select_engines(count=3)

        # Ensure brave_api only selected if key is available
        if "brave_api" in engines and not self.brave_api_key:
            engines.remove("brave_api")
        # Ensure ddgs only selected if available
        if "ddgs" in engines and not self._ddgs_available:
            engines.remove("ddgs")

        # 3. Try engines sequentially with rate limiting
        results: List[Dict[str, Any]] = []
        engines_used: List[str] = []

        for engine in engines:
            try:
                engine_results = await self._execute_engine(engine, query, max_results)
                if engine_results:
                    results = self._merge_results(results, engine_results)
                    engines_used.append(engine)
                    self.rotator.record_result(engine, True, len(engine_results))
                    self._reset_delay(engine)
                    if len(results) >= max_results:
                        break
                else:
                    self.rotator.record_result(engine, False, 0)
            except Exception as e:
                logger.debug(f"Search engine '{engine}' error: {e}")
                self.rotator.record_result(engine, False, 0)
                self._increase_delay(engine)

        # 4. If still no results, try ALL remaining engines
        if not results:
            remaining = [e for e in SearchEngineRotator.ALL_ENGINES if e not in engines]
            for engine in remaining:
                if engine == "brave_api" and not self.brave_api_key:
                    continue
                if engine == "ddgs" and not self._ddgs_available:
                    continue
                try:
                    engine_results = await self._execute_engine(engine, query, max_results)
                    if engine_results:
                        results = self._merge_results(results, engine_results)
                        engines_used.append(engine)
                        self.rotator.record_result(engine, True, len(engine_results))
                        break
                except Exception:
                    self.rotator.record_result(engine, False, 0)

        # 5. Cache and return
        final = results[:max_results]
        if final:
            self.results_cache[cache_key] = final

        return final

    async def _execute_engine(
        self, engine: str, query: str, max_results: int
    ) -> List[Dict[str, Any]]:
        """Dispatch to the correct engine method with rate limiting"""
        # Rate limiting with jitter
        await self._rate_limit_wait(engine)

        if engine == "ddgs":
            return await asyncio.to_thread(self._search_ddgs_sync, query, max_results)
        elif engine == "bing":
            return await self._search_bing_html(query, max_results)
        elif engine == "brave_api":
            return await self._search_brave(query, max_results)
        elif engine == "searx":
            return await self._search_searx(query, max_results)
        elif engine == "ddg_html":
            return await self._search_ddg_html(query, max_results)
        else:
            return []

    def _merge_results(
        self, existing: List[Dict[str, Any]], new: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Merge results, deduplicating by URL"""
        seen_urls = {r.get("url", "") for r in existing}
        merged = list(existing)
        for r in new:
            url = r.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                merged.append(r)
        return merged

    # ------------------------------------------
    # RATE LIMITING & BACKOFF (Task 9)
    # ------------------------------------------

    async def _rate_limit_wait(self, engine: str) -> None:
        """Wait with per-engine delay + random jitter"""
        delay = self._engine_delays.get(engine, 1.0)
        jitter = random.uniform(0.3, 1.5)
        last_call = self._engine_last_call.get(engine, 0)
        elapsed = time.time() - last_call
        wait = max(0, (delay + jitter) - elapsed)
        if wait > 0:
            await asyncio.sleep(wait)
        self._engine_last_call[engine] = time.time()

    def _increase_delay(self, engine: str) -> None:
        """Exponential backoff on failure"""
        current = self._engine_delays.get(engine, 1.0)
        self._engine_delays[engine] = min(self._backoff_max, current * 2.0)

    def _reset_delay(self, engine: str) -> None:
        """Reset delay on success"""
        self._engine_delays[engine] = 1.0

    # ------------------------------------------
    # ENGINE 1: DDGS (Task 5 — Backend Rotation)
    # ------------------------------------------

    def _search_ddgs_sync(self, query: str, max_results: int) -> List[Dict[str, Any]]:
        """DDGS search with backend rotation (api/html/lite)"""
        try:
            from ddgs import DDGS
        except ImportError:
            from duckduckgo_search import DDGS

        # Try backends in random order
        backends_to_try = list(DDGS_BACKENDS)
        random.shuffle(backends_to_try)

        for backend in backends_to_try:
            try:
                with DDGS() as ddgs:
                    raw_results = list(ddgs.text(query, max_results=max_results, backend=backend))
                    if raw_results:
                        return [
                            {
                                "title": r.get("title", ""),
                                "url": r.get("href", ""),
                                "snippet": r.get("body", ""),
                            }
                            for r in raw_results
                        ]
            except Exception as e:
                logger.debug(f"DDGS backend '{backend}' error: {e}")
                continue

        return []

    # ------------------------------------------
    # ENGINE 2: BING HTML SCRAPING (Task 6)
    # ------------------------------------------

    async def _search_bing_html(self, query: str, max_results: int = 30) -> List[Dict[str, Any]]:
        """Bing HTML scraping — free, no API key (Bing API retired Aug 2025)"""
        results = []
        seen_urls: Set[str] = set()

        for page in range(3):  # 3 pages max
            if len(results) >= max_results:
                break
            try:
                url = f"https://www.bing.com/search?q={quote_plus(query)}&first={page * 10 + 1}"
                response = await self.client.get(url, headers=self._get_headers())

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")
                    page_count = 0
                    for li in soup.select("li.b_algo"):
                        title_elem = li.select_one("h2 a")
                        snippet_elem = li.select_one(".b_caption p")

                        if title_elem:
                            link = title_elem.get("href", "")
                            if link and link not in seen_urls:
                                seen_urls.add(link)
                                results.append({
                                    "title": title_elem.get_text(strip=True),
                                    "url": link,
                                    "snippet": snippet_elem.get_text(strip=True) if snippet_elem else "",
                                })
                                page_count += 1

                    if page_count == 0:
                        break  # No results on this page

                if page < 2:
                    await smart_delay(0.5)  # Rate limit between pages

            except Exception as e:
                logger.debug(f"Bing HTML page {page} error: {e}")
                break

        return results[:max_results]

    # ------------------------------------------
    # ENGINE 3: BRAVE SEARCH API (existing, preserved)
    # ------------------------------------------

    async def _search_brave(self, query: str, max_results: int) -> List[Dict[str, Any]]:
        """Brave Search API (free tier: ~1000 queries/month)"""
        url = "https://api.search.brave.com/res/v1/web/search"
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": self.brave_api_key,
        }
        params = {"q": query, "count": min(max_results, 20)}

        response = await self.client.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            results = []
            for item in data.get("web", {}).get("results", []):
                results.append({
                    "title": item.get("title", ""),
                    "url": item.get("url", ""),
                    "snippet": item.get("description", ""),
                })
            return results

        return []

    # ------------------------------------------
    # ENGINE 4: SEARXNG (Task 7 — Instance Rotation + Health Check)
    # ------------------------------------------

    async def _search_searx(self, query: str, max_results: int = 20) -> List[Dict[str, Any]]:
        """SearXNG meta-search with instance rotation and health checking"""
        results = []

        # Get healthy instances, shuffled
        healthy = [
            inst for inst in SEARX_INSTANCES
            if self._is_searx_healthy(inst)
        ]
        if not healthy:
            # All marked unhealthy — try all anyway (stale health data)
            healthy = list(SEARX_INSTANCES)
        random.shuffle(healthy)

        for instance in healthy[:3]:  # Try up to 3 instances
            try:
                url = f"{instance}/search"
                params = {"q": query, "format": "json", "language": "en"}
                response = await self.client.get(
                    url, params=params, headers=self._get_headers(), timeout=10
                )

                if response.status_code == 200:
                    try:
                        data = response.json()
                    except Exception:
                        # Non-JSON response — mark unhealthy
                        self._mark_searx_unhealthy(instance)
                        continue

                    for item in data.get("results", [])[:max_results]:
                        results.append({
                            "title": item.get("title", ""),
                            "url": item.get("url", ""),
                            "snippet": item.get("content", ""),
                        })

                    if results:
                        self._mark_searx_healthy(instance)
                        break
                else:
                    # 403, 429, etc — mark unhealthy
                    self._mark_searx_unhealthy(instance)

            except Exception as e:
                logger.debug(f"SearXNG instance {instance} error: {e}")
                self._mark_searx_unhealthy(instance)
                continue

        return results

    def _is_searx_healthy(self, instance: str) -> bool:
        """Check if a SearXNG instance is considered healthy"""
        if not self._searx_healthy.get(instance, True):
            # Check if 1 hour has passed since marking unhealthy
            last_check = self._searx_last_check.get(instance, 0)
            if time.time() - last_check < 3600:  # 1 hour cooldown
                return False
            # Cooldown expired — allow retry
            self._searx_healthy[instance] = True
        return True

    def _mark_searx_unhealthy(self, instance: str) -> None:
        self._searx_healthy[instance] = False
        self._searx_last_check[instance] = time.time()

    def _mark_searx_healthy(self, instance: str) -> None:
        self._searx_healthy[instance] = True

    # ------------------------------------------
    # ENGINE 5: DDG HTML FALLBACK (enhanced with UA rotation)
    # ------------------------------------------

    async def _search_ddg_html(self, query: str, max_results: int) -> List[Dict[str, Any]]:
        """DuckDuckGo HTML fallback with User-Agent rotation"""
        results = []

        url = "https://html.duckduckgo.com/html/"
        data = {"q": query, "kl": "us-en"}

        response = await self.client.post(url, data=data, headers=self._get_headers())

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            for result in soup.select(".result")[:max_results]:
                title_elem = result.select_one(".result__title")
                snippet_elem = result.select_one(".result__snippet")

                if title_elem:
                    link = title_elem.find("a")
                    results.append({
                        "title": title_elem.get_text(strip=True),
                        "url": link.get("href") if link else None,
                        "snippet": snippet_elem.get_text(strip=True) if snippet_elem else "",
                    })

        return results

    async def search_linkedin_profiles(
        self, company: str, titles: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Specialized LinkedIn profile search across engines.
        Parses LinkedIn title format: "Name - Title - Company | LinkedIn"
        """
        if titles is None:
            titles = ["CEO", "CTO", "CFO", "COO", "Founder", "VP", "Director", "Head"]

        title_query = " OR ".join(f'"{t}"' for t in titles[:6])
        query = f'site:linkedin.com/in "{company}" {title_query}'

        results = await self.search(query, max_results=20)

        people = []
        seen_names = set()

        for result in results:
            url = result.get("url", "")
            title_text = result.get("title", "")

            if "linkedin.com/in/" not in url:
                continue

            # Parse LinkedIn title: "Name - Title | LinkedIn" or "Name - Title - Company | LinkedIn"
            name_match = re.match(r'^([^-|]+?)(?:\s*[-\u2013]\s*(.+?))?(?:\s*\|\s*LinkedIn)?$', title_text)
            if name_match:
                name = name_match.group(1).strip()
                job_title = name_match.group(2).strip() if name_match.group(2) else None

                # Clean up title (remove company suffix after second dash)
                if job_title and " - " in job_title:
                    job_title = job_title.split(" - ")[0].strip()

                # Skip if name looks invalid
                if not name or len(name) < 3 or name.lower() in seen_names:
                    continue

                seen_names.add(name.lower())
                people.append({
                    "name": name,
                    "title": job_title,
                    "linkedin_url": url,
                    "source": "multi_engine_linkedin",
                    "snippet": result.get("snippet", "")[:200],
                })

        return people

    async def close(self):
        await self.client.aclose()


# ============================================
# GOOGLE DORKING OSINT
# ============================================

class GoogleDorkingOSINT:
    """
    FREE Google Dorking for OSINT (Layer 7 Enhanced)
    Uses MultiEngineSearch (5 engines with rotation) for robust results.
    Expanded dork patterns: 16 email, 5 phone, 10 leadership, 5 social.
    """

    # ========== EMAIL DORKS (24 patterns) ==========
    EMAIL_DORKS = [
        # Highest priority: direct email patterns
        '"@{domain}" email',
        'site:{domain} "@{domain}"',
        'site:{domain} "email" OR "contact"',
        '"@{domain}" -site:{domain}',
        # Contact/about page targeting
        'intitle:"contact" site:{domain}',
        'inurl:contact site:{domain} email',
        'inurl:about site:{domain} email OR "@{domain}"',
        'inurl:team site:{domain} "@{domain}"',
        'inurl:impressum site:{domain} "@"',
        'inurl:privacy-policy site:{domain} "@"',
        # Third-party sources
        'site:linkedin.com "{company}" "@{domain}"',
        '"{company}" "@{domain}" -site:{domain}',
        'site:github.com "{company}" "@{domain}"',
        'site:crunchbase.com "{company}" email',
        'site:zoominfo.com "{company}" contact',
        'site:apollo.io "{company}" email',
        '"{company}" "@{domain}" site:twitter.com OR site:reddit.com',
        # Document-based discovery
        'filetype:pdf site:{domain} "@"',
        'filetype:xlsx site:{domain} "email"',
        'filetype:csv site:{domain} email',
        'filetype:doc site:{domain} "@{domain}"',
        # Role-targeted patterns
        '"{company}" "email" "director" OR "manager" OR "head"',
        '"@{domain}" "CEO" OR "CTO" OR "founder"',
        '"{company}" "reach us" OR "write to" OR "get in touch" "@{domain}"',
    ]

    # ========== PHONE DORKS (5 patterns) ==========
    PHONE_DORKS = [
        'site:{domain} "phone" OR "tel" OR "call"',
        'site:{domain} "+1" OR "+44" OR "+91"',
        '"{company}" "contact" "phone"',
        '"{company}" "phone number" OR "telephone"',
        'site:{domain} "call us" OR "reach us"',
    ]

    # ========== LEADERSHIP DORKS (10 patterns) ==========
    LEADERSHIP_DORKS = [
        # Original patterns
        'site:linkedin.com/in "{company}" "CEO" OR "CTO" OR "Founder"',
        'site:linkedin.com/in "{company}" "Director" OR "VP" OR "Head"',
        'site:{domain} "team" OR "about" OR "leadership"',
        '"{company}" "CEO" OR "founder" site:crunchbase.com',
        '"{company}" "executive" OR "management" site:bloomberg.com',
        # New aggressive patterns (Layer 7)
        'site:theorg.com "{company}"',
        'site:pitchbook.com "{company}" CEO OR founder',
        'site:glassdoor.com "{company}" CEO OR "managing director"',
        '"{company}" "co-founder" OR "chief" site:techcrunch.com',
        'site:twitter.com "{company}" CEO OR founder',
    ]

    # ========== SOCIAL DORKS (5 patterns — NEW Layer 7) ==========
    SOCIAL_DORKS = [
        'site:twitter.com "{company}" official',
        'site:facebook.com "{company}" official',
        'site:instagram.com "{company}"',
        'site:youtube.com "{company}" official channel',
        '"{company}" social media contact',
    ]

    # ========== ROLE-TARGETED DORKS (Layer 11 — Full-Spectrum Contact Discovery) ==========
    # 8 department categories, each with 6-10 dork patterns for finding specific role contacts

    ROLE_DORKS = {
        "hr": [
            'site:linkedin.com/in "{company}" "HR" OR "Human Resources" OR "People Operations"',
            'site:linkedin.com/in "{company}" "recruiter" OR "talent acquisition" OR "hiring"',
            '"{company}" "HR manager" OR "HR director" OR "head of people" "@{domain}"',
            '"{company}" "recruiting" OR "talent" "@{domain}" filetype:pdf',
            'site:{domain} "careers" OR "jobs" "contact" OR "email"',
            '"{company}" "CHRO" OR "chief people officer" OR "VP people"',
            'site:glassdoor.com "{company}" "HR" OR "recruiter" interview',
            '"{company}" "people operations" OR "employee experience" site:linkedin.com/in',
        ],
        "marketing": [
            'site:linkedin.com/in "{company}" "marketing" OR "CMO" OR "growth"',
            'site:linkedin.com/in "{company}" "brand" OR "communications" OR "content"',
            '"{company}" "marketing director" OR "marketing manager" "@{domain}"',
            '"{company}" "head of marketing" OR "VP marketing" OR "growth lead"',
            '"{company}" "PR" OR "public relations" OR "press" "@{domain}"',
            'site:prnewswire.com "{company}" "media contact"',
            'site:businesswire.com "{company}" "contact"',
            '"{company}" "digital marketing" OR "demand gen" site:linkedin.com/in',
            '"{company}" "social media manager" OR "content strategist" site:linkedin.com/in',
        ],
        "sales": [
            'site:linkedin.com/in "{company}" "sales" OR "account executive" OR "BDR"',
            'site:linkedin.com/in "{company}" "business development" OR "partnerships"',
            '"{company}" "sales director" OR "VP sales" OR "head of sales" "@{domain}"',
            '"{company}" "account manager" OR "sales manager" "@{domain}"',
            '"{company}" "revenue" OR "commercial" OR "enterprise sales" site:linkedin.com/in',
            '"{company}" "SDR" OR "sales development" site:linkedin.com/in',
            'site:{domain} "sales" "contact" OR "inquiries" OR "demo"',
        ],
        "engineering": [
            'site:linkedin.com/in "{company}" "engineering manager" OR "tech lead"',
            'site:linkedin.com/in "{company}" "VP engineering" OR "CTO" OR "architect"',
            'site:github.com/orgs "{company}" members',
            '"{company}" "software engineer" OR "developer" "@{domain}" site:github.com',
            '"{company}" "engineering director" OR "head of engineering" site:linkedin.com/in',
            '"{company}" "devops" OR "SRE" OR "platform engineer" site:linkedin.com/in',
            '"{company}" "frontend" OR "backend" OR "full stack" "lead" site:linkedin.com/in',
            'site:stackoverflow.com/users "{company}" developer',
        ],
        "product": [
            'site:linkedin.com/in "{company}" "product manager" OR "product director"',
            'site:linkedin.com/in "{company}" "head of product" OR "VP product" OR "CPO"',
            '"{company}" "product manager" OR "product lead" "@{domain}"',
            '"{company}" "product designer" OR "UX" OR "design lead" site:linkedin.com/in',
            'site:producthunt.com "{company}" maker',
            '"{company}" "product owner" OR "product strategy" site:linkedin.com/in',
        ],
        "support": [
            'site:linkedin.com/in "{company}" "customer success" OR "customer support"',
            'site:linkedin.com/in "{company}" "support manager" OR "support lead"',
            '"{company}" "customer service" OR "support team" "@{domain}"',
            'site:{domain} "support" OR "help" "contact" OR "email"',
            '"{company}" "customer experience" OR "CX" OR "support director" site:linkedin.com/in',
            '"{company}" "technical support" OR "support engineer" site:linkedin.com/in',
        ],
        "legal": [
            'site:linkedin.com/in "{company}" "general counsel" OR "legal counsel"',
            'site:linkedin.com/in "{company}" "legal" OR "compliance" OR "privacy"',
            '"{company}" "legal director" OR "CLO" OR "head of legal" "@{domain}"',
            'site:{domain} "privacy" OR "legal" OR "terms" "contact" "@{domain}"',
            '"{company}" "data protection officer" OR "DPO" site:linkedin.com/in',
            'site:opencorporates.com "{company}" officer',
        ],
        "finance": [
            'site:linkedin.com/in "{company}" "CFO" OR "finance director" OR "controller"',
            'site:linkedin.com/in "{company}" "finance" OR "accounting" OR "treasurer"',
            '"{company}" "investor relations" OR "IR" "@{domain}"',
            'site:{domain} "investor" OR "finance" "contact" "@{domain}"',
            '"{company}" "accounts payable" OR "billing" "@{domain}"',
            '"{company}" "VP finance" OR "head of finance" site:linkedin.com/in',
        ],
    }

    # ========== DEPARTMENT EMAIL DORKS (Layer 11) ==========
    # Find generic department emails like hr@, marketing@, sales@, etc.
    DEPARTMENT_EMAIL_DORKS = [
        'site:{domain} "hr@" OR "careers@" OR "jobs@" OR "recruiting@" OR "talent@"',
        'site:{domain} "marketing@" OR "pr@" OR "press@" OR "media@" OR "comms@"',
        'site:{domain} "sales@" OR "business@" OR "partnerships@" OR "enterprise@"',
        'site:{domain} "support@" OR "help@" OR "service@" OR "success@"',
        'site:{domain} "legal@" OR "compliance@" OR "privacy@" OR "dpo@"',
        'site:{domain} "finance@" OR "billing@" OR "accounting@" OR "invoicing@" OR "ap@"',
        'site:{domain} "dev@" OR "engineering@" OR "tech@" OR "security@"',
        'site:{domain} "product@" OR "feedback@" OR "design@"',
        '"@{domain}" "hr" OR "careers" OR "marketing" OR "sales" OR "support" -site:{domain}',
        '"{company}" "@{domain}" "department" OR "team" filetype:pdf',
    ]

    def __init__(self, timeout: int = 30, search_engine: Optional[MultiEngineSearch] = None):
        self.timeout = timeout
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )
        # TTL cache (24h, 500 entries) — replaces unbounded dict
        self.results_cache: TTLCache = TTLCache(maxsize=500, ttl=86400)
        # Use shared MultiEngineSearch if provided, else create own
        self.search_engine = search_engine or MultiEngineSearch(timeout=timeout)
        self._owns_search_engine = search_engine is None

    async def search_duckduckgo(self, query: str, max_results: int = 20) -> List[Dict[str, Any]]:
        """Search using MultiEngineSearch (5 engines with rotation)"""
        cache_key = hashlib.md5(query.encode()).hexdigest()
        if cache_key in self.results_cache:
            return self.results_cache[cache_key]

        results = await self.search_engine.search(query, max_results=max_results)

        if results:
            self.results_cache[cache_key] = results

        return results

    # Emails from these prefixes are useless for cold outreach (shared with extraction engine)
    _DORK_NOISE_PREFIXES = {
        "noreply", "no-reply", "no_reply", "donotreply", "do-not-reply",
        "mailer-daemon", "postmaster", "hostmaster", "webmaster",
        "abuse", "spam", "bounce", "daemon", "root", "nobody",
        "example", "test", "testing", "autoresponder", "auto-reply",
        "unsubscribe", "remove", "optout",
    }
    _DORK_NOISE_DOMAINS = {
        "example.com", "example.org", "example.net", "test.com",
        "email.com", "domain.com", "company.com", "website.com",
        "yourcompany.com", "yourdomain.com", "sentry.io",
        "schema.org", "w3.org", "googleapis.com", "gstatic.com",
    }

    async def find_emails(self, company: str, domain: str) -> List[Dict[str, Any]]:
        """Find emails using expanded dorking (24 patterns, up to 12 executed)"""
        emails_found = []

        for dork_template in self.EMAIL_DORKS[:12]:  # Increased from 8 to 12
            try:
                dork = dork_template.format(company=company, domain=domain)
                results = await self.search_duckduckgo(dork, max_results=10)

                for result in results:
                    text = f"{result.get('title', '')} {result.get('snippet', '')} {result.get('url', '')}"

                    # Extract emails from text
                    found = re.findall(r'[\w.+-]+@[\w.-]+\.\w+', text)

                    for email in found:
                        email_lower = email.lower().strip()
                        if "@" not in email_lower:
                            continue

                        local_part = email_lower.split("@")[0]
                        email_domain = email_lower.split("@")[-1]

                        # Skip noise domains
                        if email_domain in self._DORK_NOISE_DOMAINS:
                            continue

                        # Skip useless prefixes (noreply, abuse, postmaster, etc.)
                        if local_part in self._DORK_NOISE_PREFIXES:
                            continue

                        # Skip image/asset/CSS/JS artifacts
                        if any(ext in email_domain for ext in [".png", ".jpg", ".svg", ".css", ".js", ".gif", ".woff"]):
                            continue
                        if any(x in local_part for x in ["webpack", "sentry", "chunk", "module", "0x", "data-"]):
                            continue

                        # Skip placeholder emails
                        if any(p in email_lower for p in ["your-email", "youremail", "user@", "name@", "email@", "someone@", "changeme@"]):
                            continue

                        # Prefer emails matching the target domain, but accept others
                        if domain in email_lower or not any(
                            x in email_lower for x in ['example.com', 'email.com', 'domain.com']
                        ):
                            emails_found.append({
                                "email": email_lower,
                                "source": "google_dork",
                                "context": result.get("snippet", "")[:100]
                            })

                await smart_delay(1.0)  # Rate limiting with jitter

            except Exception as e:
                logger.debug(f"Email dork error: {e}")

        # Deduplicate
        seen = set()
        unique_emails = []
        for e in emails_found:
            if e["email"] not in seen:
                seen.add(e["email"])
                unique_emails.append(e)

        return unique_emails

    async def find_leadership(self, company: str, domain: str) -> List[Dict[str, Any]]:
        """Find leadership/executives using expanded dorking (10 patterns, up to 6 executed)"""
        people_found = []

        for dork_template in self.LEADERSHIP_DORKS[:6]:  # Increased from 3 to 6
            try:
                dork = dork_template.format(company=company, domain=domain)
                results = await self.search_duckduckgo(dork, max_results=10)

                for result in results:
                    url = result.get("url", "")
                    title = result.get("title", "")
                    snippet = result.get("snippet", "")

                    # Extract LinkedIn profiles
                    if "linkedin.com/in/" in url:
                        # Extract name from title (usually "Name - Title | LinkedIn")
                        name_match = re.match(r'^([^-|]+)', title)
                        if name_match:
                            name = name_match.group(1).strip()

                            # Try to extract title
                            title_match = re.search(r'-\s*([^|]+)', title)
                            job_title = title_match.group(1).strip() if title_match else None

                            people_found.append({
                                "name": name,
                                "title": job_title,
                                "linkedin_url": url,
                                "source": "linkedin_dork",
                                "snippet": snippet[:200]
                            })

                await smart_delay(1.0)

            except Exception as e:
                logger.debug(f"Leadership dork error: {e}")

        return people_found

    async def find_social_profiles(self, company: str, domain: str) -> List[Dict[str, Any]]:
        """Find social media profiles using dorking (NEW Layer 7)"""
        profiles_found = []

        for dork_template in self.SOCIAL_DORKS[:4]:
            try:
                dork = dork_template.format(company=company, domain=domain)
                results = await self.search_duckduckgo(dork, max_results=10)

                for result in results:
                    url = result.get("url", "")
                    title = result.get("title", "")

                    # Categorize by platform
                    platform = None
                    if "twitter.com" in url or "x.com" in url:
                        platform = "twitter"
                    elif "facebook.com" in url:
                        platform = "facebook"
                    elif "instagram.com" in url:
                        platform = "instagram"
                    elif "youtube.com" in url:
                        platform = "youtube"

                    if platform:
                        profiles_found.append({
                            "platform": platform,
                            "url": url,
                            "title": title,
                            "source": "social_dork",
                        })

                await smart_delay(1.0)

            except Exception as e:
                logger.debug(f"Social dork error: {e}")

        return profiles_found

    async def find_phones(self, company: str, domain: str) -> List[str]:
        """Find phone numbers using expanded dorking (5 patterns)"""
        phones_found = set()

        phone_patterns = [
            r'\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}',  # US
            r'\+44\s?[0-9]{4}\s?[0-9]{6}',  # UK
            r'\+91[-.\s]?[0-9]{10}',  # India
            r'\+[0-9]{1,3}[-.\s]?[0-9]{6,14}',  # International
        ]

        for dork_template in self.PHONE_DORKS[:3]:  # Increased from 2 to 3
            try:
                dork = dork_template.format(company=company, domain=domain)
                results = await self.search_duckduckgo(dork, max_results=10)

                for result in results:
                    text = f"{result.get('title', '')} {result.get('snippet', '')}"

                    for pattern in phone_patterns:
                        found = re.findall(pattern, text)
                        phones_found.update(found)

                await smart_delay(1.0)

            except Exception as e:
                logger.debug(f"Phone dork error: {e}")

        return list(phones_found)

    async def find_department_contacts(
        self, company: str, domain: str, departments: Optional[List[str]] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Layer 11: Find contacts by department/role using targeted dork patterns.
        Returns dict mapping department name to list of discovered people.
        """
        if departments is None:
            departments = list(self.ROLE_DORKS.keys())

        dept_contacts: Dict[str, List[Dict[str, Any]]] = {}

        for dept in departments:
            dork_templates = self.ROLE_DORKS.get(dept, [])
            if not dork_templates:
                continue

            people_found = []

            # Execute up to 4 dorks per department (balance coverage vs speed)
            for dork_template in dork_templates[:4]:
                try:
                    dork = dork_template.format(company=company, domain=domain)
                    results = await self.search_duckduckgo(dork, max_results=10)

                    for result in results:
                        url = result.get("url", "")
                        title = result.get("title", "")
                        snippet = result.get("snippet", "")

                        # Extract LinkedIn profiles
                        if "linkedin.com/in/" in url:
                            name_match = re.match(r'^([^-|]+)', title)
                            if name_match:
                                name = name_match.group(1).strip()
                                # Skip generic/company names
                                if len(name.split()) < 2 or len(name) > 60:
                                    continue

                                title_match = re.search(r'-\s*([^|]+)', title)
                                job_title = title_match.group(1).strip() if title_match else None

                                people_found.append({
                                    "name": name,
                                    "title": job_title,
                                    "department": dept,
                                    "linkedin_url": url,
                                    "source": f"role_dork_{dept}",
                                    "snippet": snippet[:200]
                                })

                        # Extract emails from snippets
                        emails_in_text = re.findall(
                            r'[\w.+-]+@[\w.-]+\.\w+',
                            f"{title} {snippet}"
                        )
                        for email in emails_in_text:
                            if domain in email.lower() and not any(
                                x in email.lower() for x in ['example.com', 'email.com', 'domain.com']
                            ):
                                people_found.append({
                                    "email": email.lower(),
                                    "department": dept,
                                    "source": f"role_dork_{dept}",
                                    "context": snippet[:100]
                                })

                    await smart_delay(1.0)  # Rate limiting with jitter

                except Exception as e:
                    logger.debug(f"Role dork error ({dept}): {e}")

            # Deduplicate by name or email
            seen_names = set()
            seen_emails = set()
            unique = []
            for p in people_found:
                name = p.get("name", "").lower()
                email = p.get("email", "").lower()
                if name and name not in seen_names:
                    seen_names.add(name)
                    unique.append(p)
                elif email and email not in seen_emails:
                    seen_emails.add(email)
                    unique.append(p)

            if unique:
                dept_contacts[dept] = unique

        return dept_contacts

    async def find_department_emails(self, company: str, domain: str) -> Dict[str, List[str]]:
        """
        Layer 11: Find generic department email addresses (hr@, marketing@, sales@, etc.)
        Returns dict mapping department to list of discovered emails.
        """
        dept_emails: Dict[str, List[str]] = {
            "hr": [], "marketing": [], "sales": [], "support": [],
            "legal": [], "finance": [], "engineering": [], "product": [],
            "general": []
        }

        # Department email prefixes for classification
        EMAIL_DEPT_MAP = {
            "hr": ["hr@", "careers@", "jobs@", "recruiting@", "talent@", "people@", "hiring@"],
            "marketing": ["marketing@", "pr@", "press@", "media@", "comms@", "communications@",
                          "brand@", "growth@", "content@", "social@"],
            "sales": ["sales@", "business@", "partnerships@", "enterprise@", "demo@",
                       "biz@", "revenue@", "deals@"],
            "support": ["support@", "help@", "service@", "success@", "care@", "customers@",
                         "helpdesk@", "ticket@", "cs@"],
            "legal": ["legal@", "compliance@", "privacy@", "dpo@", "counsel@"],
            "finance": ["finance@", "billing@", "accounting@", "invoicing@", "ap@", "ar@",
                         "payments@", "treasurer@"],
            "engineering": ["dev@", "engineering@", "tech@", "security@", "devops@",
                            "infosec@", "bugs@", "api@"],
            "product": ["product@", "feedback@", "design@", "ux@"],
            "general": ["info@", "contact@", "hello@", "admin@", "office@", "team@",
                         "general@", "inquiries@"]
        }

        for dork_template in self.DEPARTMENT_EMAIL_DORKS[:6]:
            try:
                dork = dork_template.format(company=company, domain=domain)
                results = await self.search_duckduckgo(dork, max_results=10)

                for result in results:
                    text = f"{result.get('title', '')} {result.get('snippet', '')}"
                    found = re.findall(r'[\w.+-]+@[\w.-]+\.\w+', text)

                    for email in found:
                        email_lower = email.lower()
                        if domain not in email_lower:
                            continue
                        if any(x in email_lower for x in ['example.com', 'email.com']):
                            continue

                        # Classify by prefix
                        classified = False
                        for dept, prefixes in EMAIL_DEPT_MAP.items():
                            if any(email_lower.startswith(p.split("@")[0] + "@") for p in prefixes):
                                if email_lower not in dept_emails[dept]:
                                    dept_emails[dept].append(email_lower)
                                classified = True
                                break

                        if not classified:
                            if email_lower not in dept_emails["general"]:
                                dept_emails["general"].append(email_lower)

                await smart_delay(1.0)

            except Exception as e:
                logger.debug(f"Department email dork error: {e}")

        # Filter out empty departments
        return {k: v for k, v in dept_emails.items() if v}

    async def close(self):
        await self.client.aclose()
        if self._owns_search_engine:
            await self.search_engine.close()


# ============================================
# LINKEDIN PUBLIC OSINT
# ============================================

class LinkedInPublicOSINT:
    """
    FREE LinkedIn public page scraper
    Extracts company and people info from public pages
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.headers = get_headers(context="navigate")
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def get_company_page(self, company_slug: str) -> Optional[Dict[str, Any]]:
        """Get company info from public LinkedIn page"""
        result = {
            "name": None,
            "description": None,
            "industry": None,
            "size": None,
            "headquarters": None,
            "website": None,
            "linkedin_url": f"https://www.linkedin.com/company/{company_slug}",
            "employees_on_linkedin": None
        }

        try:
            url = f"https://www.linkedin.com/company/{company_slug}"
            response = await self.client.get(url, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                # Try to extract from JSON-LD
                scripts = soup.find_all("script", type="application/ld+json")
                for script in scripts:
                    try:
                        data = json.loads(script.string)
                        if data.get("@type") == "Organization":
                            result["name"] = data.get("name")
                            result["description"] = data.get("description")
                            result["website"] = data.get("url")
                    except Exception:
                        pass

                # Extract from meta tags
                og_title = soup.find("meta", property="og:title")
                if og_title and not result["name"]:
                    result["name"] = og_title.get("content", "").split("|")[0].strip()

                og_desc = soup.find("meta", property="og:description")
                if og_desc and not result["description"]:
                    result["description"] = og_desc.get("content", "")

                return result

        except Exception as e:
            logger.debug(f"LinkedIn company scrape error: {e}")

        return None

    async def search_company_employees(self, company: str) -> List[Dict[str, Any]]:
        """Search for company employees using Google"""
        employees = []

        # Use Google/DuckDuckGo to find LinkedIn profiles
        dork_searcher = GoogleDorkingOSINT()

        try:
            # Search for employees
            query = f'site:linkedin.com/in "{company}" current'
            results = await dork_searcher.search_duckduckgo(query, max_results=20)

            for result in results:
                url = result.get("url", "")
                title = result.get("title", "")

                if "linkedin.com/in/" in url:
                    # Parse name and title from result
                    name_match = re.match(r'^([^-|]+)', title)
                    if name_match:
                        name = name_match.group(1).strip()

                        # Extract job title
                        title_match = re.search(r'-\s*([^|]+)', title)
                        job_title = title_match.group(1).strip() if title_match else None

                        # Check if this person works at the company
                        if company.lower() in title.lower() or company.lower() in result.get("snippet", "").lower():
                            employees.append({
                                "name": name,
                                "title": job_title,
                                "linkedin_url": url,
                                "company": company,
                                "source": "linkedin_search"
                            })

        except Exception as e:
            logger.debug(f"LinkedIn employee search error: {e}")
        finally:
            await dork_searcher.close()

        return employees

    async def search_department_employees(
        self, company: str, departments: Optional[List[str]] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Layer 11: Search LinkedIn for employees by specific department/role.
        Uses targeted Google dorks like site:linkedin.com/in "company" "HR manager".

        Returns dict mapping department to list of people found.
        """
        if departments is None:
            departments = [
                "hr", "marketing", "sales", "engineering",
                "product", "support", "legal", "finance"
            ]

        # Role keywords per department for LinkedIn search
        DEPT_ROLE_KEYWORDS = {
            "hr": [
                '"HR manager" OR "HR director" OR "head of HR"',
                '"recruiter" OR "talent acquisition" OR "people operations"',
                '"CHRO" OR "chief people officer" OR "VP people"',
            ],
            "marketing": [
                '"marketing manager" OR "marketing director" OR "CMO"',
                '"content marketing" OR "brand manager" OR "growth"',
                '"PR manager" OR "communications" OR "social media manager"',
            ],
            "sales": [
                '"sales manager" OR "sales director" OR "VP sales"',
                '"account executive" OR "business development" OR "SDR"',
                '"head of sales" OR "revenue" OR "partnerships manager"',
            ],
            "engineering": [
                '"engineering manager" OR "tech lead" OR "CTO"',
                '"software engineer" OR "developer" OR "architect"',
                '"VP engineering" OR "head of engineering" OR "devops"',
            ],
            "product": [
                '"product manager" OR "product director" OR "CPO"',
                '"product designer" OR "UX lead" OR "head of product"',
            ],
            "support": [
                '"customer success" OR "support manager" OR "support lead"',
                '"customer service" OR "customer experience" OR "CX"',
            ],
            "legal": [
                '"general counsel" OR "legal director" OR "CLO"',
                '"compliance officer" OR "privacy officer" OR "legal manager"',
            ],
            "finance": [
                '"CFO" OR "finance director" OR "controller"',
                '"VP finance" OR "head of finance" OR "treasurer"',
            ],
        }

        dept_results: Dict[str, List[Dict[str, Any]]] = {}
        dork_searcher = GoogleDorkingOSINT()

        try:
            for dept in departments:
                role_queries = DEPT_ROLE_KEYWORDS.get(dept, [])
                if not role_queries:
                    continue

                people_found = []
                seen_urls = set()

                # Execute 2 role queries per department (balance speed vs coverage)
                for role_query in role_queries[:2]:
                    try:
                        query = f'site:linkedin.com/in "{company}" {role_query}'
                        results = await dork_searcher.search_duckduckgo(query, max_results=15)

                        for result in results:
                            url = result.get("url", "")
                            title = result.get("title", "")
                            snippet = result.get("snippet", "")

                            if "linkedin.com/in/" not in url:
                                continue
                            if url in seen_urls:
                                continue
                            seen_urls.add(url)

                            # Parse name from LinkedIn title
                            name_match = re.match(r'^([^-|]+)', title)
                            if not name_match:
                                continue

                            name = name_match.group(1).strip()
                            if len(name.split()) < 2 or len(name) > 60:
                                continue

                            # Verify company match in title or snippet
                            combined = f"{title} {snippet}".lower()
                            if company.lower() not in combined:
                                continue

                            title_match = re.search(r'-\s*([^|]+)', title)
                            job_title = title_match.group(1).strip() if title_match else None

                            people_found.append({
                                "name": name,
                                "title": job_title,
                                "department": dept,
                                "linkedin_url": url,
                                "company": company,
                                "source": f"linkedin_dept_{dept}",
                            })

                        await smart_delay(1.5, jitter=0.6)  # Jittered rate limiting

                    except Exception as e:
                        logger.debug(f"LinkedIn dept search error ({dept}): {e}")

                if people_found:
                    dept_results[dept] = people_found

        except Exception as e:
            logger.debug(f"LinkedIn department search error: {e}")
        finally:
            await dork_searcher.close()

        return dept_results

    async def close(self):
        await self.client.aclose()


# ============================================
# GITHUB OSINT
# ============================================

class GitHubOSINT:
    """
    FREE GitHub OSINT
    Extracts user info, emails from commits, organization data
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.api_url = "https://api.github.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "MobiAdz-OSINT/1.0"
        }
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )
        self.rate_remaining = 60

    async def search_org_members(self, org_name: str, max_members: int = 20) -> List[Dict[str, Any]]:
        """Search organization members and extract emails"""
        members = []

        try:
            # Get public members
            url = f"{self.api_url}/orgs/{org_name}/members"
            params = {"per_page": min(max_members, 100)}

            response = await self.client.get(url, params=params, headers=self.headers)
            self.rate_remaining = int(response.headers.get("X-RateLimit-Remaining", 60))

            if response.status_code == 200:
                member_list = response.json()

                # Layer 14: Parallel member processing with semaphore(5)
                gh_sem = asyncio.Semaphore(5)

                async def fetch_member(member):
                    username = member.get("login")
                    if not username or self.rate_remaining <= 5:
                        return None
                    async with gh_sem:
                        user_data = await self._get_user_details(username)
                        if user_data:
                            email = await self._get_user_email_from_commits(username)
                            await smart_delay(0.2)
                            return {
                                "username": username,
                                "name": user_data.get("name"),
                                "email": email,
                                "bio": user_data.get("bio"),
                                "company": user_data.get("company"),
                                "location": user_data.get("location"),
                                "twitter": user_data.get("twitter_username"),
                                "blog": user_data.get("blog"),
                                "github_url": user_data.get("html_url"),
                                "avatar": user_data.get("avatar_url"),
                                "source": "github_org"
                            }
                    return None

                results = await asyncio.gather(
                    *[fetch_member(m) for m in member_list[:max_members]],
                    return_exceptions=True
                )
                for r in results:
                    if isinstance(r, dict):
                        members.append(r)

        except Exception as e:
            logger.debug(f"GitHub org members error: {e}")

        return members

    async def _get_user_details(self, username: str) -> Optional[Dict[str, Any]]:
        """Get user profile details"""
        try:
            url = f"{self.api_url}/users/{username}"
            response = await self.client.get(url, headers=self.headers)
            self.rate_remaining = int(response.headers.get("X-RateLimit-Remaining", 60))

            if response.status_code == 200:
                return response.json()

        except Exception as e:
            logger.debug(f"GitHub user details error: {e}")

        return None

    async def _get_user_email_from_commits(self, username: str) -> Optional[str]:
        """Extract email from user's public commits"""
        try:
            url = f"{self.api_url}/users/{username}/events/public"
            response = await self.client.get(url, headers=self.headers)
            self.rate_remaining = int(response.headers.get("X-RateLimit-Remaining", 60))

            if response.status_code == 200:
                events = response.json()

                for event in events:
                    if event.get("type") == "PushEvent":
                        commits = event.get("payload", {}).get("commits", [])

                        for commit in commits:
                            author = commit.get("author", {})
                            email = author.get("email", "")

                            # Filter out noreply emails
                            if email and "noreply" not in email.lower() and "github" not in email.lower():
                                return email

        except Exception as e:
            logger.debug(f"GitHub commit email error: {e}")

        return None

    async def search_users(self, query: str, max_results: int = 10) -> List[Dict[str, Any]]:
        """Search GitHub users"""
        users = []

        try:
            url = f"{self.api_url}/search/users"
            params = {"q": query, "per_page": min(max_results, 100)}

            response = await self.client.get(url, params=params, headers=self.headers)
            self.rate_remaining = int(response.headers.get("X-RateLimit-Remaining", 60))

            if response.status_code == 200:
                data = response.json()

                # Layer 14: Parallel user processing with semaphore(5)
                gh_sem = asyncio.Semaphore(5)

                async def fetch_user(item):
                    username = item.get("login")
                    if not username or self.rate_remaining <= 5:
                        return None
                    async with gh_sem:
                        user_data = await self._get_user_details(username)
                        email = await self._get_user_email_from_commits(username)
                        await smart_delay(0.2)
                        if user_data:
                            return {
                                "username": username,
                                "name": user_data.get("name"),
                                "email": email,
                                "bio": user_data.get("bio"),
                                "company": user_data.get("company"),
                                "github_url": user_data.get("html_url"),
                                "source": "github_search"
                            }
                    return None

                results = await asyncio.gather(
                    *[fetch_user(item) for item in data.get("items", [])[:max_results]],
                    return_exceptions=True
                )
                for r in results:
                    if isinstance(r, dict):
                        users.append(r)

        except Exception as e:
            logger.debug(f"GitHub user search error: {e}")

        return users

    async def close(self):
        await self.client.aclose()


# ============================================
# SOCIAL MEDIA OSINT
# ============================================

class SocialMediaOSINT:
    """
    FREE Social Media OSINT
    Extracts data from Twitter, Facebook, Instagram public pages
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.headers = get_headers(context="navigate")
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def find_social_profiles(self, company: str, domain: str) -> Dict[str, Any]:
        """Find all social media profiles for a company"""
        profiles = {
            "twitter": None,
            "facebook": None,
            "instagram": None,
            "youtube": None,
            "tiktok": None,
            "linkedin": None
        }

        dork_searcher = GoogleDorkingOSINT()

        try:
            # Search for Twitter
            twitter_results = await dork_searcher.search_duckduckgo(
                f'site:twitter.com "{company}" OR site:x.com "{company}"',
                max_results=5
            )
            for result in twitter_results:
                url = result.get("url", "")
                if "twitter.com/" in url or "x.com/" in url:
                    if "/status/" not in url and "/search" not in url:
                        profiles["twitter"] = url
                        break

            await smart_delay(1.0)

            # Search for Facebook
            fb_results = await dork_searcher.search_duckduckgo(
                f'site:facebook.com "{company}"',
                max_results=5
            )
            for result in fb_results:
                url = result.get("url", "")
                if "facebook.com/" in url:
                    if "/posts/" not in url and "/photos/" not in url:
                        profiles["facebook"] = url
                        break

            await smart_delay(1.0)

            # Search for Instagram
            ig_results = await dork_searcher.search_duckduckgo(
                f'site:instagram.com "{company}"',
                max_results=5
            )
            for result in ig_results:
                url = result.get("url", "")
                if "instagram.com/" in url:
                    if "/p/" not in url and "/reel/" not in url:
                        profiles["instagram"] = url
                        break

            await smart_delay(1.0)

            # Search for YouTube
            yt_results = await dork_searcher.search_duckduckgo(
                f'site:youtube.com "{company}" channel',
                max_results=5
            )
            for result in yt_results:
                url = result.get("url", "")
                if "youtube.com/" in url:
                    if "/channel/" in url or "/@" in url or "/c/" in url:
                        profiles["youtube"] = url
                        break

            # Search for LinkedIn
            li_results = await dork_searcher.search_duckduckgo(
                f'site:linkedin.com/company "{company}"',
                max_results=5
            )
            for result in li_results:
                url = result.get("url", "")
                if "linkedin.com/company/" in url:
                    profiles["linkedin"] = url
                    break

        except Exception as e:
            logger.debug(f"Social media OSINT error: {e}")
        finally:
            await dork_searcher.close()

        return profiles

    async def get_twitter_info(self, username: str) -> Optional[Dict[str, Any]]:
        """Get Twitter/X profile info (public)"""
        try:
            # Use Nitter (Twitter frontend) for public data
            nitter_instances = [
                "nitter.net",
                "nitter.cz",
                "nitter.privacydev.net"
            ]

            for instance in nitter_instances:
                try:
                    url = f"https://{instance}/{username}"
                    response = await self.client.get(url, headers=self.headers)

                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, "html.parser")

                        name_elem = soup.select_one(".profile-card-fullname")
                        bio_elem = soup.select_one(".profile-bio")

                        return {
                            "username": username,
                            "name": name_elem.get_text(strip=True) if name_elem else None,
                            "bio": bio_elem.get_text(strip=True) if bio_elem else None,
                            "twitter_url": f"https://twitter.com/{username}",
                            "source": "twitter_nitter"
                        }

                except Exception:
                    continue

        except Exception as e:
            logger.debug(f"Twitter OSINT error: {e}")

        return None

    async def close(self):
        await self.client.aclose()


# ============================================
# DOMAIN OSINT
# ============================================

class DomainOSINT:
    """
    FREE Domain OSINT
    WHOIS, DNS, subdomains, technology detection
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.headers = get_headers(context="navigate")
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def get_whois_data(self, domain: str) -> Dict[str, Any]:
        """Get WHOIS data for a domain"""
        result = {
            "domain": domain,
            "registrar": None,
            "creation_date": None,
            "expiration_date": None,
            "registrant_name": None,
            "registrant_email": None,
            "registrant_org": None,
            "admin_email": None,
            "tech_email": None
        }

        try:
            # Use free WHOIS API
            url = f"https://www.whoisxmlapi.com/whoisserver/WhoisService"
            # Note: This is a placeholder - you'd need an API key for full data
            # Alternative: parse whois.domaintools.com or similar

            # Try parsing from web whois service
            web_url = f"https://who.is/whois/{domain}"
            response = await self.client.get(web_url, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                # Extract registrant info
                whois_data = soup.select_one(".whois-data")
                if whois_data:
                    text = whois_data.get_text()

                    # Extract emails
                    emails = re.findall(r'[\w.+-]+@[\w.-]+\.\w+', text)
                    if emails:
                        result["registrant_email"] = emails[0]
                        if len(emails) > 1:
                            result["admin_email"] = emails[1]
                        if len(emails) > 2:
                            result["tech_email"] = emails[2]

                    # Extract organization
                    org_match = re.search(r'Registrant Organization:\s*(.+)', text)
                    if org_match:
                        result["registrant_org"] = org_match.group(1).strip()

                    # Extract registrar
                    registrar_match = re.search(r'Registrar:\s*(.+)', text)
                    if registrar_match:
                        result["registrar"] = registrar_match.group(1).strip()

        except Exception as e:
            logger.debug(f"WHOIS OSINT error: {e}")

        return result

    async def get_dns_records(self, domain: str) -> Dict[str, Any]:
        """Get DNS records for a domain"""
        result = {
            "domain": domain,
            "mx_records": [],
            "txt_records": [],
            "ns_records": [],
            "emails_found": [],
            "email_provider": None
        }

        try:
            # Try using public DNS API
            url = f"https://dns.google/resolve"

            # Get MX records
            response = await self.client.get(url, params={"name": domain, "type": "MX"})
            if response.status_code == 200:
                data = response.json()
                for answer in data.get("Answer", []):
                    mx = answer.get("data", "")
                    result["mx_records"].append(mx)

                    # Detect email provider
                    if "google" in mx.lower():
                        result["email_provider"] = "Google Workspace"
                    elif "outlook" in mx.lower() or "microsoft" in mx.lower():
                        result["email_provider"] = "Microsoft 365"
                    elif "zoho" in mx.lower():
                        result["email_provider"] = "Zoho Mail"

            # Get TXT records
            response = await self.client.get(url, params={"name": domain, "type": "TXT"})
            if response.status_code == 200:
                data = response.json()
                for answer in data.get("Answer", []):
                    txt = answer.get("data", "")
                    result["txt_records"].append(txt)

                    # Extract emails from TXT records
                    emails = re.findall(r'[\w.+-]+@[\w.-]+\.\w+', txt)
                    result["emails_found"].extend(emails)

            # Get NS records
            response = await self.client.get(url, params={"name": domain, "type": "NS"})
            if response.status_code == 200:
                data = response.json()
                for answer in data.get("Answer", []):
                    ns = answer.get("data", "")
                    result["ns_records"].append(ns)

        except Exception as e:
            logger.debug(f"DNS OSINT error: {e}")

        return result

    async def enumerate_subdomains(self, domain: str) -> List[str]:
        """Enumerate subdomains using Certificate Transparency"""
        subdomains = set()

        try:
            # Use crt.sh
            url = f"https://crt.sh/?q=%.{domain}&output=json"
            response = await self.client.get(url)

            if response.status_code == 200:
                try:
                    certs = response.json()

                    for cert in certs:
                        name_value = cert.get("name_value", "")

                        for subdomain in name_value.split("\n"):
                            subdomain = subdomain.strip().replace("*.", "")
                            if subdomain and domain in subdomain:
                                subdomains.add(subdomain)
                except Exception:
                    pass

        except Exception as e:
            logger.debug(f"Subdomain enumeration error: {e}")

        return list(subdomains)[:50]  # Limit results

    async def detect_technologies(self, domain: str) -> List[str]:
        """Detect technologies used by a website"""
        technologies = []

        try:
            url = f"https://{domain}"
            response = await self.client.get(url, headers=self.headers)

            if response.status_code == 200:
                html = response.text
                headers = dict(response.headers)

                # Check for common technologies
                tech_signatures = {
                    "WordPress": ["wp-content", "wp-includes", "wordpress"],
                    "Shopify": ["shopify", "cdn.shopify.com"],
                    "React": ["react", "_reactRootContainer", "react-root"],
                    "Angular": ["ng-app", "angular"],
                    "Vue.js": ["vue", "__vue__"],
                    "Next.js": ["__NEXT_DATA__", "_next"],
                    "Laravel": ["laravel", "csrf-token"],
                    "Django": ["csrfmiddlewaretoken", "django"],
                    "Ruby on Rails": ["rails", "csrf-token"],
                    "Node.js": ["express", "x-powered-by: Express"],
                    "PHP": ["x-powered-by: PHP"],
                    "ASP.NET": ["x-aspnet-version", "__VIEWSTATE"],
                    "Cloudflare": ["cloudflare", "cf-ray"],
                    "AWS": ["amazonaws.com", "x-amz"],
                    "Google Analytics": ["google-analytics.com", "gtag"],
                    "HubSpot": ["hubspot", "hs-scripts"],
                    "Intercom": ["intercom", "intercomSettings"],
                    "Zendesk": ["zendesk", "zdassets"],
                    "Stripe": ["stripe.com", "stripe.js"],
                }

                for tech, signatures in tech_signatures.items():
                    for sig in signatures:
                        if sig.lower() in html.lower() or sig.lower() in str(headers).lower():
                            if tech not in technologies:
                                technologies.append(tech)
                            break

        except Exception as e:
            logger.debug(f"Technology detection error: {e}")

        return technologies

    async def close(self):
        await self.client.aclose()


# ============================================
# EMAIL OSINT
# ============================================

class EmailOSINT:
    """
    FREE Email OSINT
    Email verification, breach checking, reputation
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.headers = get_headers(context="navigate")
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def check_gravatar(self, email: str) -> Optional[Dict[str, Any]]:
        """Check if email has a Gravatar profile"""
        try:
            email_hash = hashlib.md5(email.lower().strip().encode()).hexdigest()

            # Check profile
            profile_url = f"https://www.gravatar.com/{email_hash}.json"
            response = await self.client.get(profile_url)

            if response.status_code == 200:
                data = response.json()
                entry = data.get("entry", [{}])[0]

                return {
                    "email": email,
                    "has_gravatar": True,
                    "display_name": entry.get("displayName"),
                    "profile_url": entry.get("profileUrl"),
                    "avatar_url": f"https://www.gravatar.com/avatar/{email_hash}",
                    "accounts": [
                        {
                            "shortname": acc.get("shortname"),
                            "url": acc.get("url")
                        }
                        for acc in entry.get("accounts", [])
                    ],
                    "source": "gravatar"
                }

        except Exception as e:
            logger.debug(f"Gravatar check error: {e}")

        return {"email": email, "has_gravatar": False}

    async def check_email_reputation(self, email: str) -> Dict[str, Any]:
        """Check email reputation using EmailRep.io"""
        result = {
            "email": email,
            "reputation": "unknown",
            "suspicious": False,
            "references": 0,
            "details": {}
        }

        try:
            url = f"https://emailrep.io/{email}"
            headers = {**self.headers, "Accept": "application/json"}

            response = await self.client.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()

                result["reputation"] = data.get("reputation", "unknown")
                result["suspicious"] = data.get("suspicious", False)
                result["references"] = data.get("references", 0)
                result["details"] = data.get("details", {})

        except Exception as e:
            logger.debug(f"EmailRep check error: {e}")

        return result

    # Layer 11: Known email pattern for a domain (set by detect_email_pattern)
    _domain_patterns: Dict[str, str] = {}

    async def generate_email_permutations(
        self,
        first_name: str,
        last_name: str,
        domain: str,
        middle_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Layer 11: Generate comprehensive email permutations with cultural awareness.
        Supports Western, East Asian (surname-first), Hispanic (two surnames), and
        European naming conventions. If a known pattern exists for the domain, it's
        prioritized first.
        """
        permutations = []

        first = first_name.lower().strip()
        last = last_name.lower().strip()
        first_initial = first[0] if first else ""
        last_initial = last[0] if last else ""
        middle = (middle_name or "").lower().strip()
        middle_initial = middle[0] if middle else ""

        # Base patterns (ordered by global frequency)
        base_patterns = [
            # Tier 1: Most common (~45% of companies)
            (f"{first}.{last}@{domain}", "{first}.{last}", 95),
            # Tier 2: Common (~25%)
            (f"{first_initial}.{last}@{domain}", "{fi}.{last}", 90),
            (f"{first_initial}{last}@{domain}", "{fi}{last}", 88),
            # Tier 3: Frequent (~15%)
            (f"{first}@{domain}", "{first}", 85),
            (f"{first}{last}@{domain}", "{first}{last}", 83),
            (f"{first}_{last}@{domain}", "{first}_{last}", 80),
            (f"{first}-{last}@{domain}", "{first}-{last}", 78),
            # Tier 4: Less common (~10%)
            (f"{last}.{first}@{domain}", "{last}.{first}", 75),
            (f"{last}{first}@{domain}", "{last}{first}", 73),
            (f"{last}@{domain}", "{last}", 70),
            (f"{first}.{last_initial}@{domain}", "{first}.{li}", 68),
            (f"{first}{last_initial}@{domain}", "{first}{li}", 66),
            (f"{last}{first_initial}@{domain}", "{last}{fi}", 64),
            (f"{last}.{first_initial}@{domain}", "{last}.{fi}", 62),
            (f"{first_initial}{last_initial}@{domain}", "{fi}{li}", 60),
            # Tier 5: Occasional patterns
            (f"{last}_{first}@{domain}", "{last}_{first}", 58),
            (f"{last}-{first}@{domain}", "{last}-{first}", 56),
            (f"{first_initial}_{last}@{domain}", "{fi}_{last}", 55),
            (f"{first_initial}-{last}@{domain}", "{fi}-{last}", 54),
        ]

        # Middle name patterns (if available)
        if middle:
            base_patterns.extend([
                (f"{first}.{middle_initial}.{last}@{domain}", "{first}.{mi}.{last}", 82),
                (f"{first}{middle_initial}{last}@{domain}", "{first}{mi}{last}", 78),
                (f"{first_initial}{middle_initial}{last}@{domain}", "{fi}{mi}{last}", 74),
                (f"{first}.{middle}.{last}@{domain}", "{first}.{middle}.{last}", 70),
            ])

        # East Asian patterns (surname-first cultures: Chinese, Japanese, Korean)
        # Many Asian names have the surname first in their native order
        # but may use Western order in email addresses
        if self._looks_east_asian(first_name, last_name):
            base_patterns.extend([
                # Surname-first patterns (common in Asian companies)
                (f"{last}.{first}@{domain}", "{last}.{first} [asian]", 90),
                (f"{last}{first}@{domain}", "{last}{first} [asian]", 88),
                (f"{last}_{first}@{domain}", "{last}_{first} [asian]", 82),
            ])

        # Hispanic patterns (two surnames)
        name_parts = last.split()
        if len(name_parts) == 2:
            paternal, maternal = name_parts
            base_patterns.extend([
                (f"{first}.{paternal}@{domain}", "{first}.{paternal}", 88),
                (f"{first}{paternal}@{domain}", "{first}{paternal}", 84),
                (f"{first_initial}{paternal}@{domain}", "{fi}{paternal}", 80),
                (f"{first}.{paternal}.{maternal}@{domain}", "{first}.{p}.{m}", 76),
            ])

        # If we know the domain's email pattern, prioritize it
        known_pattern = self._domain_patterns.get(domain)
        if known_pattern:
            # Reorder: put matching patterns first with boosted confidence
            reordered = []
            rest = []
            for email, pattern, conf in base_patterns:
                if known_pattern in pattern:
                    reordered.append((email, pattern, min(conf + 10, 99)))
                else:
                    rest.append((email, pattern, max(conf - 10, 40)))
            base_patterns = reordered + rest

        # Build permutation list
        seen_emails = set()
        for email, pattern, confidence in base_patterns:
            if email not in seen_emails and first and last:
                seen_emails.add(email)
                permutations.append({
                    "email": email,
                    "pattern": pattern,
                    "confidence": confidence,
                })

        return permutations

    @staticmethod
    def _looks_east_asian(first_name: str, last_name: str) -> bool:
        """Heuristic to detect East Asian names (Chinese, Japanese, Korean, Vietnamese)."""
        # Common East Asian surname indicators
        common_asian_surnames = {
            # Chinese
            "wang", "li", "zhang", "liu", "chen", "yang", "zhao", "huang", "zhou",
            "wu", "xu", "sun", "ma", "zhu", "hu", "guo", "lin", "he", "gao", "luo",
            "zheng", "liang", "xie", "tang", "feng", "deng", "cao", "peng", "zeng",
            "xiao", "tian", "dong", "pan", "yuan", "gu", "jiang", "wei", "lu", "qin",
            # Japanese
            "sato", "suzuki", "takahashi", "tanaka", "watanabe", "ito", "yamamoto",
            "nakamura", "kobayashi", "kato", "yoshida", "yamada", "sasaki", "matsumoto",
            # Korean
            "kim", "lee", "park", "choi", "jung", "kang", "cho", "yoon", "jang",
            "lim", "han", "oh", "seo", "shin", "kwon", "hwang", "ahn", "song", "yoo",
            # Vietnamese
            "nguyen", "tran", "le", "pham", "hoang", "phan", "vu", "vo", "dang", "bui",
            "do", "ho", "ngo", "duong", "ly",
        }
        last_lower = last_name.lower().strip()
        return last_lower in common_asian_surnames

    def detect_email_pattern(self, known_email: str, first_name: str, last_name: str, domain: str):
        """
        Layer 11: Detect the email pattern used by a domain from a known email.
        Once detected, this pattern is prioritized for all future permutations on this domain.
        """
        if not known_email or not first_name or not last_name:
            return None

        email_local = known_email.split("@")[0].lower()
        first = first_name.lower().strip()
        last = last_name.lower().strip()
        fi = first[0] if first else ""
        li = last[0] if last else ""

        # Pattern detection
        pattern_map = {
            f"{first}.{last}": "{first}.{last}",
            f"{first}{last}": "{first}{last}",
            f"{first}_{last}": "{first}_{last}",
            f"{first}-{last}": "{first}-{last}",
            f"{fi}.{last}": "{fi}.{last}",
            f"{fi}{last}": "{fi}{last}",
            f"{last}.{first}": "{last}.{first}",
            f"{last}{first}": "{last}{first}",
            f"{last}.{fi}": "{last}.{fi}",
            f"{last}{fi}": "{last}{fi}",
            f"{first}": "{first}",
            f"{last}": "{last}",
            f"{first}.{li}": "{first}.{li}",
            f"{fi}_{last}": "{fi}_{last}",
            f"{fi}-{last}": "{fi}-{last}",
        }

        for local_part, pattern in pattern_map.items():
            if email_local == local_part:
                self._domain_patterns[domain] = pattern
                logger.info(f"Detected email pattern for {domain}: {pattern}")
                return pattern

        return None

    async def close(self):
        await self.client.aclose()


# ============================================
# COMPANY REGISTRY OSINT
# ============================================

class CompanyRegistryOSINT:
    """
    FREE Company Registry OSINT
    OpenCorporates, SEC EDGAR, Companies House
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.headers = get_headers(context="navigate")
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def search_opencorporates(self, company_name: str) -> List[Dict[str, Any]]:
        """Search OpenCorporates for company info"""
        results = []

        try:
            url = "https://api.opencorporates.com/v0.4/companies/search"
            params = {"q": company_name, "per_page": 10}

            response = await self.client.get(url, params=params)

            if response.status_code == 200:
                data = response.json()

                for company in data.get("results", {}).get("companies", []):
                    c = company.get("company", {})

                    results.append({
                        "name": c.get("name"),
                        "company_number": c.get("company_number"),
                        "jurisdiction": c.get("jurisdiction_code"),
                        "status": c.get("current_status"),
                        "incorporation_date": c.get("incorporation_date"),
                        "dissolution_date": c.get("dissolution_date"),
                        "company_type": c.get("company_type"),
                        "registered_address": c.get("registered_address_in_full"),
                        "opencorporates_url": c.get("opencorporates_url"),
                        "source": "opencorporates"
                    })

        except Exception as e:
            logger.debug(f"OpenCorporates search error: {e}")

        return results

    # SEC EDGAR headers (required: User-Agent with contact info)
    SEC_HEADERS = {
        "User-Agent": "TheMobiAdz/1.0 contact@themobiadz.com",
        "Accept": "application/json",
    }

    # Regex patterns for extracting officers from SEC filing text
    OFFICER_PATTERNS = [
        # "Name, Title" or "Name - Title"
        re.compile(
            r'([A-Z][a-z]+(?:\s[A-Z]\.?)?\s[A-Z][a-z]+(?:\s[A-Z][a-z]+)?)'
            r'[\s,\-—]+\s*'
            r'((?:Chief|President|Vice\s+President|Secretary|Treasurer|'
            r'Executive\s+Vice|Senior\s+Vice|General\s+Counsel|Controller|'
            r'Director|Officer|SVP|EVP|VP|CEO|CFO|CTO|COO|CIO|CMO|CLO|CHRO)[^,\n]{0,80})',
            re.IGNORECASE
        ),
        # "appointed/named X as Y"
        re.compile(
            r'(?:appointed|named|elected|serves?\s+as)\s+'
            r'([A-Z][a-z]+(?:\s[A-Z]\.?)?\s[A-Z][a-z]+)\s+'
            r'(?:as\s+)?'
            r'((?:Chief|President|Vice|Director|Officer|SVP|EVP|VP|CEO|CFO|CTO|COO)[^,\.\n]{0,80})',
            re.IGNORECASE
        ),
    ]

    async def search_sec_edgar(self, company_name: str) -> List[Dict[str, Any]]:
        """Search SEC EDGAR using EFTS full-text search API (modern, JSON-based)"""
        results = []

        try:
            # EFTS full-text search API
            url = "https://efts.sec.gov/LATEST/search-index"
            params = {
                "q": f'"{company_name}"',
                "dateRange": "custom",
                "startdt": "2022-01-01",
                "enddt": datetime.now().strftime("%Y-%m-%d"),
                "forms": "10-K,DEF 14A",
            }

            response = await self.client.get(url, params=params, headers=self.SEC_HEADERS)

            if response.status_code == 200:
                data = response.json()

                for hit in data.get("hits", {}).get("hits", [])[:10]:
                    source = hit.get("_source", {})
                    results.append({
                        "cik": source.get("entity_id", ""),
                        "name": source.get("entity_name", ""),
                        "form_type": source.get("form_type", ""),
                        "filing_date": source.get("file_date", ""),
                        "file_url": source.get("file_num", ""),
                        "source": "sec_edgar_efts"
                    })

            # Fallback: submissions API for company lookup by name
            if not results:
                search_url = f"https://efts.sec.gov/LATEST/search-index?q=%22{quote(company_name)}%22&forms=10-K"
                response = await self.client.get(search_url, headers=self.SEC_HEADERS)
                if response.status_code == 200:
                    data = response.json()
                    for hit in data.get("hits", {}).get("hits", [])[:5]:
                        source = hit.get("_source", {})
                        results.append({
                            "cik": source.get("entity_id", ""),
                            "name": source.get("entity_name", ""),
                            "form_type": source.get("form_type", ""),
                            "filing_date": source.get("file_date", ""),
                            "source": "sec_edgar_efts"
                        })

        except Exception as e:
            logger.debug(f"SEC EDGAR search error: {e}")

        return results

    async def get_sec_company_officers(self, cik: str) -> List[Dict[str, Any]]:
        """
        Extract officers from SEC filings (DEF 14A proxy statements + 10-K).
        Fetches the actual filing document and parses for officer names/titles.
        """
        officers = []

        try:
            # 1. Get company submissions to find recent DEF 14A or 10-K
            url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"

            response = await self.client.get(url, headers=self.SEC_HEADERS)

            if response.status_code != 200:
                return officers

            data = response.json()
            company_name = data.get("name", "")

            # Find most recent DEF 14A (proxy = best for officers) or 10-K
            filings = data.get("filings", {}).get("recent", {})
            forms = filings.get("form", [])
            accession_numbers = filings.get("accessionNumber", [])
            primary_docs = filings.get("primaryDocument", [])

            target_accession = None
            target_doc = None
            target_form = None

            # Prefer DEF 14A (proxy statement lists all directors with bios)
            for i, form in enumerate(forms):
                if form in ("DEF 14A", "DEFA14A") and i < len(accession_numbers):
                    target_accession = accession_numbers[i].replace("-", "")
                    target_doc = primary_docs[i] if i < len(primary_docs) else None
                    target_form = form
                    break

            # Fallback to 10-K
            if not target_accession:
                for i, form in enumerate(forms):
                    if form == "10-K" and i < len(accession_numbers):
                        target_accession = accession_numbers[i].replace("-", "")
                        target_doc = primary_docs[i] if i < len(primary_docs) else None
                        target_form = form
                        break

            if not target_accession or not target_doc:
                # Return basic company info if no filing found
                officers.append({
                    "company_name": company_name,
                    "cik": cik,
                    "sic_description": data.get("sicDescription"),
                    "source": "sec_edgar_basic"
                })
                return officers

            # 2. Fetch the actual filing document
            cik_clean = cik.lstrip("0") or "0"
            filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{target_accession}/{target_doc}"

            await smart_delay(0.1, jitter=0.3)  # Rate limiting (~10 req/sec)
            response = await self.client.get(filing_url, headers=self.SEC_HEADERS)

            if response.status_code != 200:
                return officers

            filing_text = response.text

            # 3. Parse filing HTML for officers
            soup = BeautifulSoup(filing_text, "html.parser")
            text = soup.get_text(separator="\n")

            # Look for "Executive Officers" section
            exec_section = ""
            lines = text.split("\n")
            in_exec_section = False

            for line in lines:
                line_lower = line.strip().lower()
                if any(kw in line_lower for kw in [
                    "executive officers", "directors and executive",
                    "information about our executive", "our executive officers"
                ]):
                    in_exec_section = True
                    exec_section = ""
                    continue
                if in_exec_section:
                    exec_section += line + "\n"
                    # Stop after ~3000 chars or next major section
                    if len(exec_section) > 3000 or any(kw in line_lower for kw in [
                        "compensation", "security ownership", "item 11", "item 12",
                        "part iii", "part iv"
                    ]):
                        break

            # If we found an exec section, parse it; otherwise parse full text (limited)
            parse_text = exec_section if exec_section else text[:10000]

            seen_names = set()
            for pattern in self.OFFICER_PATTERNS:
                for match in pattern.finditer(parse_text):
                    name = match.group(1).strip()
                    title = match.group(2).strip()

                    # Clean up name and title
                    name = re.sub(r'\s+', ' ', name)
                    title = re.sub(r'\s+', ' ', title).rstrip(',. ')

                    # Validate name (must be 2+ words, not all caps section header)
                    name_parts = name.split()
                    if (len(name_parts) < 2 or
                            name.isupper() or
                            len(name) > 50 or
                            name.lower() in seen_names):
                        continue

                    seen_names.add(name.lower())
                    officers.append({
                        "name": name,
                        "title": title,
                        "company_name": company_name,
                        "cik": cik,
                        "filing_type": target_form,
                        "source": f"sec_edgar_{target_form.lower().replace(' ', '')}"
                    })

            # Also parse tables for structured officer data
            for table in soup.find_all("table"):
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 2:
                        cell_texts = [c.get_text(strip=True) for c in cells[:4]]

                        # Check if first cell looks like a name and second like a title
                        potential_name = cell_texts[0]
                        potential_title = cell_texts[1]

                        name_parts = potential_name.split()
                        if (2 <= len(name_parts) <= 4 and
                                all(p[0].isupper() for p in name_parts if p) and
                                any(kw in potential_title.lower() for kw in [
                                    "chief", "president", "vice", "director",
                                    "officer", "secretary", "treasurer", "svp", "evp", "vp",
                                    "ceo", "cfo", "cto", "coo"
                                ]) and
                                potential_name.lower() not in seen_names):
                            seen_names.add(potential_name.lower())
                            officers.append({
                                "name": potential_name,
                                "title": potential_title,
                                "company_name": company_name,
                                "cik": cik,
                                "filing_type": target_form,
                                "source": f"sec_edgar_{target_form.lower().replace(' ', '')}_table"
                            })

        except Exception as e:
            logger.debug(f"SEC officers extraction error: {e}")

        return officers[:20]  # Cap at 20 officers

    async def search_sec_officers_fulltext(self, company_name: str) -> List[Dict[str, Any]]:
        """
        Direct full-text search for executive officers in SEC filings.
        Uses EFTS API to search for officer mentions across all filings.
        """
        officers = []

        try:
            url = "https://efts.sec.gov/LATEST/search-index"
            params = {
                "q": f'"executive officers" "{company_name}"',
                "forms": "DEF 14A,10-K",
                "dateRange": "custom",
                "startdt": "2023-01-01",
                "enddt": datetime.now().strftime("%Y-%m-%d"),
            }

            response = await self.client.get(url, params=params, headers=self.SEC_HEADERS)

            if response.status_code == 200:
                data = response.json()

                for hit in data.get("hits", {}).get("hits", [])[:3]:
                    source = hit.get("_source", {})
                    cik = source.get("entity_id", "")

                    if cik:
                        # Get actual officers from the filing
                        filing_officers = await self.get_sec_company_officers(cik)
                        officers.extend(filing_officers)
                        await smart_delay(0.2)  # Rate limiting

        except Exception as e:
            logger.debug(f"SEC fulltext officer search error: {e}")

        # Deduplicate by name
        seen = set()
        unique = []
        for officer in officers:
            name_key = officer.get("name", "").lower()
            if name_key and name_key not in seen:
                seen.add(name_key)
                unique.append(officer)

        return unique[:20]

    async def close(self):
        await self.client.aclose()


# ============================================
# PRESS RELEASE OSINT
# ============================================

class PressReleaseOSINT:
    """
    Mine press releases for executive names and leadership changes.
    Sources: GlobeNewswire RSS, PRNewswire, BusinessWire
    """

    # Layer 11: Expanded to capture ALL department roles, not just C-suite
    EXECUTIVE_PATTERNS = [
        # "appointed/named/promoted X as Title" — expanded role list
        re.compile(
            r'(?:appointed|named|promoted|hired|welcomed|announced|brings\s+on|adds)\s+'
            r'([A-Z][a-z]+(?:\s[A-Z]\.?\s?)?[A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s+'
            r'(?:as\s+)?'
            r'((?:Chief|President|CEO|CTO|CFO|COO|CIO|CMO|CHRO|CLO|CPO|CRO|'
            r'Vice\s+President|VP|SVP|EVP|Director|Head\s+of|General\s+Manager|'
            r'Managing\s+Director|Executive|Senior|Partner|Founder|Co-Founder|'
            # Layer 11: HR/People roles
            r'HR\s+Director|HR\s+Manager|Head\s+of\s+People|Talent\s+Acquisition|'
            r'Recruiting\s+Director|People\s+Operations|'
            # Marketing/PR roles
            r'Marketing\s+Director|Marketing\s+Manager|Brand\s+Manager|'
            r'Communications\s+Director|PR\s+Manager|Growth\s+Lead|'
            # Sales/Business roles
            r'Sales\s+Director|Sales\s+Manager|Head\s+of\s+Sales|'
            r'Business\s+Development|Account\s+Director|Revenue\s+Officer|'
            # Engineering/Tech roles
            r'Engineering\s+Manager|Tech\s+Lead|Engineering\s+Director|'
            r'Principal\s+Engineer|Staff\s+Engineer|'
            # Product roles
            r'Product\s+Manager|Product\s+Director|Head\s+of\s+Product|'
            r'Design\s+Director|UX\s+Director|'
            # Support/Success roles
            r'Customer\s+Success|Support\s+Director|CX\s+Director|'
            # Legal/Finance roles
            r'General\s+Counsel|Legal\s+Director|Compliance\s+Officer|'
            r'Finance\s+Director|Controller|Treasurer)[^,\.]{0,80})',
            re.IGNORECASE
        ),
        # "X, Title" pattern — expanded
        re.compile(
            r'([A-Z][a-z]+(?:\s[A-Z]\.?\s?)?[A-Z][a-z]+(?:\s[A-Z][a-z]+)?),\s+'
            r'((?:Chief|President|CEO|CTO|CFO|COO|CIO|CMO|CHRO|CLO|CPO|CRO|'
            r'Vice\s+President|VP|SVP|EVP|Director|Head\s+of|General\s+Manager|'
            r'Executive\s+Vice|Senior\s+Vice|Founder|Co-Founder|'
            r'HR\s+Director|HR\s+Manager|Marketing\s+Director|Sales\s+Director|'
            r'Engineering\s+Manager|Product\s+Manager|Product\s+Director|'
            r'General\s+Counsel|Legal\s+Director|Finance\s+Director|'
            r'Customer\s+Success|Support\s+Director|'
            r'Recruiting\s+Director|Talent\s+Acquisition|'
            r'Brand\s+Manager|PR\s+Manager|Communications)[^,\.]{0,60})',
            re.IGNORECASE
        ),
        # "led by/headed by X, Title" — expanded
        re.compile(
            r'(?:led\s+by|headed\s+by|under|reports?\s+to|managed\s+by|overseen\s+by)\s+'
            r'([A-Z][a-z]+\s[A-Z][a-z]+),?\s+'
            r'((?:CEO|CTO|CFO|COO|President|Founder|VP|Director|Head|Manager|Lead|'
            r'HR|Marketing|Sales|Engineering|Product|Support|Legal|Finance|'
            r'Recruiting|Talent|Brand|PR|Communications|Customer\s+Success)[^,\.]{0,60})',
            re.IGNORECASE
        ),
        # Layer 11: "X joins as/joins company as Title"
        re.compile(
            r'([A-Z][a-z]+(?:\s[A-Z]\.?\s?)?[A-Z][a-z]+)\s+'
            r'(?:joins?\s+(?:as\s+)?|has\s+been\s+(?:appointed|named)\s+)'
            r'((?:Chief|VP|Director|Head|Manager|Lead|Senior|'
            r'HR|Marketing|Sales|Engineering|Product|Support|Legal|Finance)[^,\.]{0,80})',
            re.IGNORECASE
        ),
    ]

    # Layer 11: Media contact extraction pattern
    MEDIA_CONTACT_PATTERN = re.compile(
        r'(?:Media\s+Contact|Press\s+Contact|For\s+(?:more\s+)?information|'
        r'Contact|Media\s+Inquiries|PR\s+Contact)[:\s]*'
        r'([A-Z][a-z]+(?:\s[A-Z]\.?\s?)?[A-Z][a-z]+)',
        re.IGNORECASE
    )
    MEDIA_EMAIL_PATTERN = re.compile(r'[\w.+-]+@[\w.-]+\.\w+')
    MEDIA_PHONE_PATTERN = re.compile(r'\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}')

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.headers = get_headers(context="navigate")
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def search_globenewswire_rss(self, company_name: str) -> List[Dict[str, Any]]:
        """Search GlobeNewswire RSS for personnel announcements"""
        executives = []

        try:
            # GlobeNewswire RSS feed for personnel announcements (subject code 18)
            rss_url = "https://www.globenewswire.com/RssFeed/subjectcode/18-Personnel%20Announcements"

            response = await self.client.get(rss_url, headers=self.headers)

            if response.status_code == 200:
                # Parse RSS XML
                soup = BeautifulSoup(response.text, "xml")
                company_lower = company_name.lower()

                for item in soup.find_all("item")[:50]:
                    title = item.find("title")
                    description = item.find("description")

                    title_text = title.get_text(strip=True) if title else ""
                    desc_text = description.get_text(strip=True) if description else ""
                    full_text = f"{title_text} {desc_text}"

                    # Check if this press release mentions the company
                    if company_lower not in full_text.lower():
                        continue

                    # Extract executive names from text
                    found = self._extract_executives(full_text)
                    for exec_info in found:
                        exec_info["source"] = "globenewswire_rss"
                        exec_info["context"] = title_text[:150]
                        executives.append(exec_info)

        except Exception as e:
            logger.debug(f"GlobeNewswire RSS error: {e}")

        return executives

    async def search_prnewswire(self, company_name: str) -> List[Dict[str, Any]]:
        """Search PRNewswire for executive announcements"""
        executives = []

        try:
            search_url = "https://www.prnewswire.com/news-releases/news-releases-list/"
            params = {
                "keyword": f"{company_name} appoints",
                "page": "1",
                "pagesize": "10"
            }

            response = await self.client.get(search_url, params=params, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                # Find press release links and snippets
                for card in soup.select(".card, .newsreleaseconsolidateresultsbody, .row")[:10]:
                    title_elem = card.select_one("h3, .newsreleaseconsolidateresultstitle a, a")
                    snippet_elem = card.select_one("p, .newsreleaseconsolidateresultssnippet")

                    title_text = title_elem.get_text(strip=True) if title_elem else ""
                    snippet_text = snippet_elem.get_text(strip=True) if snippet_elem else ""
                    full_text = f"{title_text} {snippet_text}"

                    found = self._extract_executives(full_text)
                    for exec_info in found:
                        exec_info["source"] = "prnewswire"
                        exec_info["context"] = title_text[:150]
                        executives.append(exec_info)

        except Exception as e:
            logger.debug(f"PRNewswire search error: {e}")

        return executives

    async def search_businesswire(self, company_name: str) -> List[Dict[str, Any]]:
        """Search BusinessWire for executive announcements"""
        executives = []

        try:
            search_url = "https://www.businesswire.com/portal/site/home/news/"
            params = {
                "searchtype": "all",
                "searchterm": f"{company_name} appoints"
            }

            response = await self.client.get(search_url, params=params, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                for item in soup.select(".bwNewsList li, .eachStory, article")[:10]:
                    title_elem = item.select_one("h3, .bwTitleLink, a")
                    snippet_elem = item.select_one("p, .bwSnippet, .teaser")

                    title_text = title_elem.get_text(strip=True) if title_elem else ""
                    snippet_text = snippet_elem.get_text(strip=True) if snippet_elem else ""
                    full_text = f"{title_text} {snippet_text}"

                    found = self._extract_executives(full_text)
                    for exec_info in found:
                        exec_info["source"] = "businesswire"
                        exec_info["context"] = title_text[:150]
                        executives.append(exec_info)

        except Exception as e:
            logger.debug(f"BusinessWire search error: {e}")

        return executives

    def _extract_executives(self, text: str) -> List[Dict[str, Any]]:
        """Extract executive names and titles from press release text"""
        found = []
        seen_names = set()

        for pattern in self.EXECUTIVE_PATTERNS:
            for match in pattern.finditer(text):
                name = match.group(1).strip()
                title = match.group(2).strip().rstrip(',. ')

                # Validate name
                name_parts = name.split()
                if (len(name_parts) < 2 or len(name) > 40 or
                        name.isupper() or name.lower() in seen_names):
                    continue

                seen_names.add(name.lower())
                found.append({
                    "name": name,
                    "title": title,
                    "source": "press_release"
                })

        return found

    def _extract_media_contacts(self, text: str) -> List[Dict[str, Any]]:
        """
        Layer 11: Extract media/PR contacts from press releases.
        Press releases almost always end with 'Media Contact: Name, email, phone'.
        """
        contacts = []

        # Find media contact sections
        for match in self.MEDIA_CONTACT_PATTERN.finditer(text):
            name = match.group(1).strip()
            # Get the context after the name (next 200 chars) for email/phone
            start_pos = match.end()
            context = text[start_pos:start_pos + 300]

            contact_info = {
                "name": name,
                "title": "Media/PR Contact",
                "department": "marketing",
                "source": "press_release_media_contact",
            }

            # Extract email from context
            email_match = self.MEDIA_EMAIL_PATTERN.search(context)
            if email_match:
                contact_info["email"] = email_match.group(0).lower()

            # Extract phone from context
            phone_match = self.MEDIA_PHONE_PATTERN.search(context)
            if phone_match:
                contact_info["phone"] = phone_match.group(0)

            name_parts = name.split()
            if len(name_parts) >= 2 and len(name) <= 40 and not name.isupper():
                contacts.append(contact_info)

        return contacts

    async def find_executives(self, company_name: str) -> List[Dict[str, Any]]:
        """Combined search across all press release sources"""
        tasks = [
            self.search_globenewswire_rss(company_name),
            self.search_prnewswire(company_name),
            self.search_businesswire(company_name),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_executives = []
        seen_names = set()

        for result in results:
            if isinstance(result, list):
                for exec_info in result:
                    name_key = exec_info.get("name", "").lower()
                    if name_key and name_key not in seen_names:
                        seen_names.add(name_key)
                        all_executives.append(exec_info)

        return all_executives[:15]

    async def find_all_contacts(self, company_name: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Layer 11: Find ALL contacts from press releases - executives AND media contacts.
        Returns dict with 'executives' and 'media_contacts' lists.
        """
        tasks = [
            self.search_globenewswire_rss(company_name),
            self.search_prnewswire(company_name),
            self.search_businesswire(company_name),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_executives = []
        all_media_contacts = []
        seen_names = set()

        for result in results:
            if isinstance(result, list):
                for item in result:
                    name_key = item.get("name", "").lower()
                    if name_key and name_key not in seen_names:
                        seen_names.add(name_key)
                        all_executives.append(item)

                    # Extract media contacts from the context text
                    context = item.get("context", "")
                    if context:
                        media = self._extract_media_contacts(context)
                        for mc in media:
                            mc_name = mc.get("name", "").lower()
                            if mc_name and mc_name not in seen_names:
                                seen_names.add(mc_name)
                                all_media_contacts.append(mc)

        # Also do a targeted web search for media contacts
        try:
            search_engine = MultiEngineSearch(timeout=self.timeout)
            query = f'"{company_name}" "media contact" OR "press contact" email'
            results = await search_engine.search(query, max_results=5)

            for result in results:
                snippet = result.get("snippet", "")
                media = self._extract_media_contacts(snippet)
                for mc in media:
                    mc_name = mc.get("name", "").lower()
                    if mc_name and mc_name not in seen_names:
                        seen_names.add(mc_name)
                        all_media_contacts.append(mc)

            await search_engine.close()
        except Exception as e:
            logger.debug(f"Media contact web search error: {e}")

        return {
            "executives": all_executives[:20],
            "media_contacts": all_media_contacts[:10],
        }

    async def close(self):
        await self.client.aclose()


# ============================================
# COMPANY BLOG OSINT
# ============================================

class CompanyBlogOSINT:
    """
    Extract authors from company blog/news/press/team pages.
    Identifies leadership via blog author bylines and team pages.
    """

    BLOG_PATHS = [
        "/blog", "/news", "/press", "/engineering", "/insights",
        "/about/team", "/team", "/about/leadership", "/about-us",
        "/about", "/our-team", "/leadership",
    ]

    AUTHOR_SELECTORS = [
        '[rel="author"]',
        '[class*="author-name"]',
        '[class*="author_name"]',
        '[class*="byline"]',
        '[itemprop="author"]',
        '.post-author',
        '.entry-author',
        '.article-author',
        '[class*="writer"]',
    ]

    BYLINE_PATTERNS = [
        # "By John Smith, VP Engineering"
        re.compile(
            r'[Bb]y\s+([A-Z][a-z]+(?:\s[A-Z]\.?\s?)?[A-Z][a-z]+)'
            r'(?:\s*,\s*(.+?))?(?:\s*\||$|\s*—)',
        ),
        # "Author: John Smith"
        re.compile(r'[Aa]uthor:\s*([A-Z][a-z]+\s[A-Z][a-z]+)'),
        # "Written by John Smith"
        re.compile(r'[Ww]ritten\s+by\s+([A-Z][a-z]+\s[A-Z][a-z]+)'),
        # "Posted by John Smith"
        re.compile(r'[Pp]osted\s+by\s+([A-Z][a-z]+\s[A-Z][a-z]+)'),
    ]

    TEAM_SELECTORS = [
        '.team-member', '.team-card', '[class*="team"]',
        '.person-card', '[class*="person"]',
        '.leader', '[class*="leadership"]',
        '.member', '.staff-member',
    ]

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.headers = get_headers(context="navigate")
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def extract_blog_authors(self, domain: str) -> List[Dict[str, Any]]:
        """Try common blog paths and extract author names"""
        authors = []
        seen_names = set()

        blog_paths = ["/blog", "/news", "/engineering", "/insights"]

        for path in blog_paths:
            try:
                url = f"https://{domain}{path}"
                response = await self.client.get(url, headers=self.headers)

                if response.status_code != 200:
                    continue

                soup = BeautifulSoup(response.text, "html.parser")

                # Method 1: JSON-LD structured data
                for script in soup.find_all("script", type="application/ld+json"):
                    try:
                        ld_data = json.loads(script.string or "")
                        if isinstance(ld_data, list):
                            for item in ld_data:
                                self._extract_jsonld_author(item, seen_names, authors, path)
                        else:
                            self._extract_jsonld_author(ld_data, seen_names, authors, path)
                    except (json.JSONDecodeError, AttributeError):
                        pass

                # Method 2: Meta tags
                author_meta = soup.find("meta", attrs={"name": "author"})
                if author_meta and author_meta.get("content"):
                    name = author_meta["content"].strip()
                    if name.lower() not in seen_names and len(name.split()) >= 2:
                        seen_names.add(name.lower())
                        authors.append({
                            "name": name, "title": None,
                            "source": "company_blog_meta", "page": path
                        })

                og_author = soup.find("meta", attrs={"property": "article:author"})
                if og_author and og_author.get("content"):
                    name = og_author["content"].strip()
                    if name.lower() not in seen_names and len(name.split()) >= 2:
                        seen_names.add(name.lower())
                        authors.append({
                            "name": name, "title": None,
                            "source": "company_blog_meta", "page": path
                        })

                # Method 3: CSS selectors for author elements
                for selector in self.AUTHOR_SELECTORS:
                    for elem in soup.select(selector)[:10]:
                        name = elem.get_text(strip=True)
                        name = re.sub(r'^[Bb]y\s+', '', name)
                        name_parts = name.split()
                        if (2 <= len(name_parts) <= 4 and
                                name_parts[0][0].isupper() and
                                name.lower() not in seen_names and
                                len(name) < 50):
                            seen_names.add(name.lower())
                            authors.append({
                                "name": name, "title": None,
                                "source": "company_blog_selector", "page": path
                            })

                # Method 4: Byline regex patterns
                text = soup.get_text(separator="\n")
                for pattern in self.BYLINE_PATTERNS:
                    for match in pattern.finditer(text):
                        name = match.group(1).strip()
                        title = match.group(2).strip() if match.lastindex >= 2 and match.group(2) else None

                        if name.lower() not in seen_names and len(name.split()) >= 2:
                            seen_names.add(name.lower())
                            authors.append({
                                "name": name,
                                "title": title.rstrip(',. ') if title else None,
                                "source": "company_blog_byline", "page": path
                            })

                await smart_delay(0.5)

            except Exception as e:
                logger.debug(f"Blog author extraction error for {domain}{path}: {e}")

        return authors[:20]

    async def extract_team_page(self, domain: str) -> List[Dict[str, Any]]:
        """Extract leadership from /team, /about/team, /leadership pages"""
        people = []
        seen_names = set()

        team_paths = ["/about/team", "/team", "/about/leadership", "/leadership", "/our-team", "/about"]

        for path in team_paths:
            try:
                url = f"https://{domain}{path}"
                response = await self.client.get(url, headers=self.headers)

                if response.status_code != 200:
                    continue

                soup = BeautifulSoup(response.text, "html.parser")

                # Check JSON-LD for Person schema
                for script in soup.find_all("script", type="application/ld+json"):
                    try:
                        ld_data = json.loads(script.string or "")
                        items = ld_data if isinstance(ld_data, list) else [ld_data]
                        for item in items:
                            if item.get("@type") == "Person":
                                name = item.get("name", "")
                                title = item.get("jobTitle", "")
                                if name and name.lower() not in seen_names:
                                    seen_names.add(name.lower())
                                    people.append({
                                        "name": name, "title": title,
                                        "source": "team_page_schema", "page": path
                                    })
                    except (json.JSONDecodeError, AttributeError):
                        pass

                # Check team/person card selectors
                for selector in self.TEAM_SELECTORS:
                    for card in soup.select(selector)[:30]:
                        # Look for name in h2, h3, h4, or strong
                        name_elem = card.select_one("h2, h3, h4, strong, [class*='name']")
                        title_elem = card.select_one("p, span, [class*='title'], [class*='role'], [class*='position']")

                        if name_elem:
                            name = name_elem.get_text(strip=True)
                            title = title_elem.get_text(strip=True) if title_elem else None

                            name_parts = name.split()
                            if (2 <= len(name_parts) <= 4 and
                                    name_parts[0][0].isupper() and
                                    name.lower() not in seen_names and
                                    len(name) < 50):
                                seen_names.add(name.lower())
                                people.append({
                                    "name": name,
                                    "title": title[:100] if title else None,
                                    "source": "team_page", "page": path
                                })

                if people:
                    break  # Found team page, don't check other paths

                await smart_delay(0.3)

            except Exception as e:
                logger.debug(f"Team page extraction error for {domain}{path}: {e}")

        return people[:30]

    def _extract_jsonld_author(self, data: dict, seen: set, authors: list, page: str):
        """Helper to extract author from JSON-LD data"""
        author = data.get("author")
        if isinstance(author, dict):
            name = author.get("name", "")
            if name and name.lower() not in seen and len(name.split()) >= 2:
                seen.add(name.lower())
                authors.append({
                    "name": name, "title": None,
                    "source": "company_blog_jsonld", "page": page
                })
        elif isinstance(author, list):
            for a in author:
                if isinstance(a, dict):
                    name = a.get("name", "")
                    if name and name.lower() not in seen and len(name.split()) >= 2:
                        seen.add(name.lower())
                        authors.append({
                            "name": name, "title": None,
                            "source": "company_blog_jsonld", "page": page
                        })

    async def close(self):
        await self.client.aclose()


# ============================================
# CRUNCHBASE OSINT
# ============================================

class CrunchbaseOSINT:
    """
    Crunchbase Basic API for founders and company data.
    Requires optional API key (free tier: org search + entity lookup + autocomplete).
    Falls back to web search if no API key.
    """

    BASE_URL = "https://api.crunchbase.com/api/v4"

    def __init__(self, api_key: Optional[str] = None, timeout: int = 30):
        self.api_key = api_key
        self.timeout = timeout
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )
        self.headers = get_headers(context="search_api")

    async def search_organization(self, company_name: str) -> Optional[str]:
        """Autocomplete search to find org permalink/slug"""
        if not self.api_key:
            return None

        try:
            url = f"{self.BASE_URL}/autocompletes"
            params = {
                "query": company_name,
                "collection_ids": "organizations",
                "limit": 5,
                "user_key": self.api_key
            }

            response = await self.client.get(url, params=params)

            if response.status_code == 200:
                data = response.json()
                entities = data.get("entities", [])
                if entities:
                    # Find best match
                    for entity in entities:
                        identifier = entity.get("identifier", {})
                        if identifier.get("entity_def_id") == "organization":
                            return identifier.get("permalink")

        except Exception as e:
            logger.debug(f"Crunchbase autocomplete error: {e}")

        return None

    async def get_organization(self, permalink: str) -> Dict[str, Any]:
        """Get organization entity with founders"""
        result = {"info": {}, "founders": []}

        if not self.api_key:
            return result

        try:
            url = f"{self.BASE_URL}/entities/organizations/{permalink}"
            params = {
                "card_ids": "founders,fields",
                "user_key": self.api_key
            }

            response = await self.client.get(url, params=params)

            if response.status_code == 200:
                data = response.json()
                properties = data.get("properties", {})

                result["info"] = {
                    "name": properties.get("name"),
                    "short_description": properties.get("short_description"),
                    "founded_on": properties.get("founded_on"),
                    "location": properties.get("location_identifiers", [{}])[0].get("value") if properties.get("location_identifiers") else None,
                    "num_employees": properties.get("num_employees_enum"),
                    "website": properties.get("website", {}).get("value") if isinstance(properties.get("website"), dict) else properties.get("website"),
                    "linkedin": properties.get("linkedin", {}).get("value") if isinstance(properties.get("linkedin"), dict) else None,
                }

                # Extract founders
                founders_card = data.get("cards", {}).get("founders", [])
                for founder in founders_card:
                    founder_props = founder.get("properties", {})
                    result["founders"].append({
                        "name": f"{founder_props.get('first_name', '')} {founder_props.get('last_name', '')}".strip(),
                        "title": founder_props.get("title", "Founder"),
                        "linkedin_url": founder_props.get("linkedin", {}).get("value") if isinstance(founder_props.get("linkedin"), dict) else None,
                        "source": "crunchbase_api"
                    })

        except Exception as e:
            logger.debug(f"Crunchbase org lookup error: {e}")

        return result

    async def find_founders(self, company_name: str) -> List[Dict[str, Any]]:
        """Find company founders via Crunchbase"""
        founders = []

        if self.api_key:
            # Use Crunchbase API
            permalink = await self.search_organization(company_name)
            if permalink:
                org_data = await self.get_organization(permalink)
                founders = org_data.get("founders", [])
        else:
            # Fallback: search web for Crunchbase page and extract from snippet
            try:
                search_engine = MultiEngineSearch(timeout=self.timeout)
                query = f'site:crunchbase.com/organization "{company_name}" founder'
                results = await search_engine.search(query, max_results=5)

                for result in results:
                    snippet = result.get("snippet", "")
                    url = result.get("url", "")

                    if "crunchbase.com/organization" in url:
                        # Try to extract founder names from snippet
                        founder_match = re.findall(
                            r'(?:founded?\s+by|founder[s]?[:\s]+)([A-Z][a-z]+\s[A-Z][a-z]+)',
                            snippet, re.IGNORECASE
                        )
                        for name in founder_match[:3]:
                            founders.append({
                                "name": name,
                                "title": "Founder",
                                "source": "crunchbase_web"
                            })

                await search_engine.close()

            except Exception as e:
                logger.debug(f"Crunchbase web fallback error: {e}")

        return founders[:10]

    async def find_team_members(self, company_name: str) -> List[Dict[str, Any]]:
        """
        Layer 11: Find ALL team members from Crunchbase, not just founders.
        Uses web search for Crunchbase people pages + Google dorks.
        """
        team_members = []
        seen_names = set()

        # First, get founders (existing method)
        founders = await self.find_founders(company_name)
        for f in founders:
            name_key = f.get("name", "").lower()
            if name_key:
                seen_names.add(name_key)
                f["department"] = "leadership"
                team_members.append(f)

        # Search Crunchbase people pages via web search
        try:
            search_engine = MultiEngineSearch(timeout=self.timeout)

            queries = [
                f'site:crunchbase.com/person "{company_name}"',
                f'site:crunchbase.com "{company_name}" "team" OR "people"',
                f'site:crunchbase.com/organization "{company_name}" advisor OR board',
            ]

            for query in queries[:2]:
                try:
                    results = await search_engine.search(query, max_results=10)

                    for result in results:
                        title = result.get("title", "")
                        snippet = result.get("snippet", "")
                        url = result.get("url", "")
                        combined = f"{title} {snippet}"

                        # Extract person names from Crunchbase person pages
                        if "/person/" in url:
                            # Title format: "Name - Crunchbase Person Profile"
                            name_match = re.match(r'^([^-|]+)', title)
                            if name_match:
                                name = name_match.group(1).strip()
                                name_key = name.lower()
                                if (name_key not in seen_names and
                                        len(name.split()) >= 2 and len(name) <= 50):
                                    seen_names.add(name_key)

                                    # Try to extract title from snippet
                                    title_match = re.search(
                                        r'(?:is|was|as)\s+(?:the\s+)?'
                                        r'((?:CEO|CTO|CFO|COO|VP|Director|Head|Manager|'
                                        r'Founder|Co-Founder|Partner|Advisor|Board)[^,\.]{0,60})',
                                        snippet, re.IGNORECASE
                                    )
                                    job_title = title_match.group(1).strip() if title_match else None

                                    # Classify department from title
                                    dept = self._classify_department(job_title or "")

                                    team_members.append({
                                        "name": name,
                                        "title": job_title,
                                        "department": dept,
                                        "source": "crunchbase_web",
                                        "crunchbase_url": url,
                                    })

                        # Extract names from team/people page snippets
                        name_pattern = re.findall(
                            r'([A-Z][a-z]+\s[A-Z][a-z]+(?:\s[A-Z][a-z]+)?)',
                            combined
                        )
                        for name in name_pattern[:5]:
                            name_key = name.lower()
                            if (name_key not in seen_names and
                                    len(name.split()) >= 2 and len(name) <= 40):
                                seen_names.add(name_key)
                                team_members.append({
                                    "name": name,
                                    "title": None,
                                    "department": "unknown",
                                    "source": "crunchbase_web",
                                })

                    await smart_delay(1.0)

                except Exception as e:
                    logger.debug(f"Crunchbase team search error: {e}")

            await search_engine.close()

        except Exception as e:
            logger.debug(f"Crunchbase team member search error: {e}")

        return team_members[:25]

    @staticmethod
    def _classify_department(title: str) -> str:
        """Classify a job title into a department category."""
        title_lower = title.lower()
        DEPT_KEYWORDS = {
            "leadership": ["ceo", "cto", "cfo", "coo", "cio", "president", "founder",
                           "co-founder", "managing director", "general manager"],
            "hr": ["hr", "human resources", "people", "talent", "recruiting", "chro"],
            "marketing": ["marketing", "cmo", "brand", "pr", "communications", "growth",
                          "content", "social media"],
            "sales": ["sales", "cro", "business development", "account", "revenue",
                       "partnerships", "enterprise"],
            "engineering": ["engineer", "tech lead", "architect", "devops", "sre",
                            "development", "software"],
            "product": ["product", "cpo", "ux", "design", "user experience"],
            "support": ["customer success", "support", "cx", "customer service"],
            "legal": ["legal", "counsel", "clo", "compliance", "privacy"],
            "finance": ["finance", "cfo", "controller", "treasurer", "accounting"],
        }
        for dept, keywords in DEPT_KEYWORDS.items():
            if any(kw in title_lower for kw in keywords):
                return dept
        return "unknown"

    async def close(self):
        await self.client.aclose()


# ============================================
# THEORG.COM OSINT
# ============================================

class TheOrgOSINT:
    """
    TheOrg.com API for organizational chart data.
    Optional API key required (100 free credits/month).
    Falls back to web search if no API key.
    """

    BASE_URL = "https://api.theorg.com/v1.1"

    def __init__(self, api_key: Optional[str] = None, timeout: int = 30):
        self.api_key = api_key
        self.timeout = timeout
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def get_org_chart(self, domain: str) -> List[Dict[str, Any]]:
        """Get org chart by company domain"""
        positions = []

        if not self.api_key:
            return positions

        try:
            url = f"{self.BASE_URL}/companies/org-chart"
            params = {"domain": domain}
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Accept": "application/json",
            }

            response = await self.client.get(url, params=params, headers=headers)

            if response.status_code == 200:
                data = response.json()

                for position in data.get("positions", []):
                    positions.append({
                        "name": position.get("fullName", ""),
                        "title": position.get("title", ""),
                        "linkedin_url": position.get("linkedInUrl"),
                        "email": position.get("workEmail"),
                        "manager_id": position.get("managerId"),
                        "source": "theorg_api"
                    })

        except Exception as e:
            logger.debug(f"TheOrg API error: {e}")

        return positions

    async def find_leadership(self, domain: str, company_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Extract leadership from org chart or web search fallback"""
        leaders = []

        if self.api_key:
            positions = await self.get_org_chart(domain)

            # Filter for leadership titles
            leadership_keywords = [
                "chief", "president", "vp", "vice president", "director",
                "head", "svp", "evp", "founder", "co-founder", "ceo",
                "cto", "cfo", "coo", "cio", "cmo", "partner", "managing"
            ]

            for pos in positions:
                title_lower = pos.get("title", "").lower()
                if any(kw in title_lower for kw in leadership_keywords):
                    leaders.append(pos)
        else:
            # Fallback: search web for TheOrg company page
            try:
                search_engine = MultiEngineSearch(timeout=self.timeout)
                query = f'site:theorg.com "{company_name or domain}" CEO OR CTO OR VP'
                results = await search_engine.search(query, max_results=5)

                for result in results:
                    snippet = result.get("snippet", "")
                    url = result.get("url", "")

                    if "theorg.com" in url:
                        # Extract names from snippet
                        name_matches = re.findall(
                            r'([A-Z][a-z]+\s[A-Z][a-z]+)(?:\s*[-–]\s*)((?:CEO|CTO|CFO|COO|VP|Director|Head|President|Founder)[^,\n]{0,40})',
                            snippet
                        )
                        for name, title in name_matches[:5]:
                            leaders.append({
                                "name": name,
                                "title": title.strip(),
                                "source": "theorg_web"
                            })

                await search_engine.close()

            except Exception as e:
                logger.debug(f"TheOrg web fallback error: {e}")

        return leaders[:20]

    async def close(self):
        await self.client.aclose()


# ============================================
# USPTO PATENT OSINT
# ============================================

class PatentOSINT:
    """
    USPTO PatentsView API for inventor/R&D leadership discovery.
    Completely free, no API key required.
    """

    BASE_URL = "https://api.patentsview.org/inventors/query"

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def search_inventors(self, company_name: str, max_results: int = 10) -> List[Dict[str, Any]]:
        """Search for inventors by company (assignee organization)"""
        inventors = []

        try:
            # PatentsView query API
            query_body = {
                "q": {"assignee_organization": company_name},
                "f": [
                    "inventor_first_name",
                    "inventor_last_name",
                    "inventor_city",
                    "inventor_state",
                    "inventor_country",
                ],
                "o": {"per_page": max_results},
                "s": [{"inventor_total_num_patents": "desc"}],
            }

            response = await self.client.post(
                self.BASE_URL,
                json=query_body,
                headers={"Content-Type": "application/json"}
            )

            if response.status_code == 200:
                data = response.json()

                for inv in data.get("inventors", []):
                    first = inv.get("inventor_first_name", "")
                    last = inv.get("inventor_last_name", "")

                    if first and last:
                        location_parts = [
                            inv.get("inventor_city", ""),
                            inv.get("inventor_state", ""),
                            inv.get("inventor_country", ""),
                        ]
                        location = ", ".join(p for p in location_parts if p)

                        inventors.append({
                            "name": f"{first} {last}",
                            "title": "Inventor / R&D",
                            "location": location if location else None,
                            "source": "uspto_patents"
                        })

        except Exception as e:
            logger.debug(f"PatentsView search error: {e}")

        return inventors

    async def find_technical_leaders(self, company_name: str) -> List[Dict[str, Any]]:
        """Find prolific inventors (likely R&D leadership / Principal Engineers)"""
        inventors = await self.search_inventors(company_name, max_results=15)

        # Top inventors with many patents are likely VP Engineering, CTO, Principal Engineer
        # PatentsView sorts by total_num_patents desc by default
        for i, inv in enumerate(inventors):
            if i < 3:
                inv["title"] = "Prolific Inventor / Likely R&D Leadership"
            elif i < 8:
                inv["title"] = "Active Inventor / R&D"
            else:
                inv["title"] = "Inventor"

        return inventors[:10]

    async def close(self):
        await self.client.aclose()


# ============================================
# CONFERENCE SPEAKER OSINT
# ============================================

class ConferenceSpeakerOSINT:
    """
    Conference speaker discovery via Pretalx API and web search.
    Identifies technical leaders who present at conferences.
    """

    # Major conference Pretalx schedule endpoints
    CONFERENCE_APIS = [
        ("PyCon US 2025", "https://pretalx.com/pycon-us-2025/schedule/export/schedule.json"),
        ("PyCon US 2024", "https://pretalx.com/pycon-us-2024/schedule/export/schedule.json"),
        ("DjangoCon US 2024", "https://pretalx.com/djangocon-us-2024/schedule/export/schedule.json"),
        ("EuroPython 2024", "https://pretalx.com/europython-2024/schedule/export/schedule.json"),
        ("PyData Global 2024", "https://pretalx.com/pydata-global-2024/schedule/export/schedule.json"),
    ]

    COMPANY_FROM_BIO_PATTERNS = [
        re.compile(r'(?:at|@|from|with)\s+([A-Z][\w\s&\.]+?)(?:\.|,|\s+where|\s+and|\s+as)', re.IGNORECASE),
        re.compile(r'(?:works?\s+(?:at|for))\s+([A-Z][\w\s&\.]+?)(?:\.|,|\s+as)', re.IGNORECASE),
        re.compile(r'(?:engineer|developer|architect|lead|manager|director)\s+(?:at|@)\s+([A-Z][\w\s&\.]+?)(?:\.|,)', re.IGNORECASE),
    ]

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def search_conference_speakers(self, company_name: str) -> List[Dict[str, Any]]:
        """Search known conference APIs for speakers affiliated with company"""
        speakers = []
        seen_names = set()
        company_lower = company_name.lower()

        for conf_name, schedule_url in self.CONFERENCE_APIS:
            try:
                response = await self.client.get(schedule_url)

                if response.status_code != 200:
                    continue

                data = response.json()
                schedule = data.get("schedule", {})
                conference = schedule.get("conference", {})

                # Iterate through days → rooms → events → persons
                for day in conference.get("days", []):
                    for room_name, events in day.get("rooms", {}).items():
                        for event in events:
                            for person in event.get("persons", []):
                                bio = person.get("biography", "") or ""
                                name = person.get("public_name", "")

                                if not name:
                                    continue

                                # Check if speaker is affiliated with target company
                                if company_lower in bio.lower() or company_lower in name.lower():
                                    if name.lower() not in seen_names:
                                        seen_names.add(name.lower())

                                        # Try to extract title from bio
                                        title = None
                                        title_match = re.search(
                                            r'((?:Senior|Staff|Principal|Lead|Head|VP|Director|Manager|CTO|CEO|Architect)\s*[\w\s]*?)\s+(?:at|@|from)',
                                            bio, re.IGNORECASE
                                        )
                                        if title_match:
                                            title = title_match.group(1).strip()

                                        speakers.append({
                                            "name": name,
                                            "title": title or "Conference Speaker",
                                            "conference": conf_name,
                                            "talk": event.get("title", ""),
                                            "source": "conference_speaker"
                                        })

                await smart_delay(0.3)

            except Exception as e:
                logger.debug(f"Conference API error for {conf_name}: {e}")

        return speakers[:10]

    async def search_via_web(self, company_name: str) -> List[Dict[str, Any]]:
        """Fallback: search web for conference speakers from company"""
        speakers = []

        try:
            search_engine = MultiEngineSearch(timeout=self.timeout)
            query = f'"{company_name}" speaker conference presentation'
            results = await search_engine.search(query, max_results=10)

            seen_names = set()
            for result in results:
                snippet = result.get("snippet", "")

                # Extract names near speaker/presenter keywords
                matches = re.findall(
                    r'([A-Z][a-z]+\s[A-Z][a-z]+)(?:,?\s+(?:from|at|of)\s+' + re.escape(company_name) + r')',
                    snippet, re.IGNORECASE
                )
                for name in matches:
                    if name.lower() not in seen_names:
                        seen_names.add(name.lower())
                        speakers.append({
                            "name": name,
                            "title": "Conference Speaker",
                            "source": "conference_web_search"
                        })

            await search_engine.close()

        except Exception as e:
            logger.debug(f"Conference web search error: {e}")

        return speakers[:5]

    async def close(self):
        await self.client.aclose()


# ============================================
# JOB BOARD CONTACT INTELLIGENCE (Layer 11)
# ============================================

class JobBoardContactIntel:
    """
    Layer 11: Extract hiring manager names, recruiter contacts, and department
    structure from job postings on Indeed, Glassdoor, LinkedIn Jobs, and others.
    Job postings reveal:
    - Recruiter/HR contact names (the person who posted the job)
    - Hiring manager names ("reports to", "hiring manager")
    - Department structure (team sizes, who manages whom)
    - Technologies used (for engineering roles)
    """

    # Patterns to extract contact names from job posting text
    HIRING_PATTERNS = [
        # "Reports to: Name, Title"
        re.compile(
            r'(?:reports?\s+to|reporting\s+to|managed\s+by)\s*:?\s*'
            r'(?:the\s+)?([A-Z][a-z]+\s[A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s*,?\s*'
            r'((?:Director|VP|Manager|Lead|Head|Chief|Senior|Principal)[^,\.\n]{0,60})?',
            re.IGNORECASE
        ),
        # "Contact: Name" / "Questions? Contact Name"
        re.compile(
            r'(?:contact|questions\?\s*(?:contact|reach\s+out\s+to)|'
            r'for\s+(?:more\s+)?info(?:rmation)?)\s*:?\s*'
            r'([A-Z][a-z]+\s[A-Z][a-z]+(?:\s[A-Z][a-z]+)?)',
            re.IGNORECASE
        ),
        # "Hiring Manager: Name"
        re.compile(
            r'(?:hiring\s+manager|recruiter|talent\s+partner|TA\s+contact)\s*:?\s*'
            r'([A-Z][a-z]+\s[A-Z][a-z]+(?:\s[A-Z][a-z]+)?)',
            re.IGNORECASE
        ),
        # "Posted by Name" (LinkedIn-style)
        re.compile(
            r'(?:posted\s+by|listed\s+by)\s+'
            r'([A-Z][a-z]+\s[A-Z][a-z]+)',
            re.IGNORECASE
        ),
    ]

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.headers = get_headers(context="navigate")
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def extract_hiring_contacts(self, company_name: str) -> List[Dict[str, Any]]:
        """
        Extract hiring managers, recruiters, and department contacts from job postings.
        Uses web search across multiple job boards.
        """
        contacts = []
        seen_names = set()

        search_engine = MultiEngineSearch(timeout=self.timeout)

        # Search queries targeting different job boards
        queries = [
            # Indeed
            f'site:indeed.com "{company_name}" "hiring manager" OR "recruiter" OR "contact"',
            # Glassdoor
            f'site:glassdoor.com "{company_name}" "interview" "HR" OR "recruiter"',
            # LinkedIn Jobs
            f'site:linkedin.com/jobs "{company_name}" "posted by"',
            # General job postings with contact info
            f'"{company_name}" job "reports to" OR "hiring manager" OR "recruiter"',
            # Greenhouse/Lever ATS (many companies use these)
            f'site:boards.greenhouse.io "{company_name}" OR site:jobs.lever.co "{company_name}"',
            # Wellfound/AngelList jobs
            f'site:wellfound.com/jobs "{company_name}"',
        ]

        try:
            for query in queries[:4]:
                try:
                    results = await search_engine.search(query, max_results=10)

                    for result in results:
                        title = result.get("title", "")
                        snippet = result.get("snippet", "")
                        url = result.get("url", "")
                        combined = f"{title} {snippet}"

                        # Apply hiring patterns
                        for pattern in self.HIRING_PATTERNS:
                            for match in pattern.finditer(combined):
                                name = match.group(1).strip()
                                job_title = match.group(2).strip() if match.lastindex >= 2 and match.group(2) else None
                                name_key = name.lower()

                                if (name_key not in seen_names and
                                        len(name.split()) >= 2 and len(name) <= 50 and
                                        not name.isupper()):
                                    seen_names.add(name_key)

                                    # Determine department from job posting context
                                    dept = self._infer_department_from_context(combined)

                                    contacts.append({
                                        "name": name,
                                        "title": job_title or "Hiring Contact",
                                        "department": dept,
                                        "source": "job_board",
                                        "job_url": url,
                                        "context": combined[:150],
                                    })

                        # Extract emails from job posting snippets
                        emails = re.findall(r'[\w.+-]+@[\w.-]+\.\w+', combined)
                        for email in emails:
                            email_lower = email.lower()
                            if not any(x in email_lower for x in
                                       ['example.com', 'email.com', 'domain.com',
                                        'indeed.com', 'glassdoor.com', 'linkedin.com']):
                                if email_lower not in seen_names:
                                    seen_names.add(email_lower)
                                    contacts.append({
                                        "email": email_lower,
                                        "department": "hr",
                                        "source": "job_board_email",
                                        "context": combined[:100],
                                    })

                    await smart_delay(1.0)

                except Exception as e:
                    logger.debug(f"Job board search error: {e}")

            # Glassdoor interview section — reveals interviewer names
            try:
                query = f'site:glassdoor.com "{company_name}" interview "HR" OR "recruiter" OR "manager"'
                results = await search_engine.search(query, max_results=5)

                for result in results:
                    snippet = result.get("snippet", "")
                    # Pattern: "I interviewed at Company... the interviewer was Name"
                    interviewer_patterns = [
                        re.compile(
                            r'(?:interviewer|interviewed\s+(?:by|with))\s+'
                            r'([A-Z][a-z]+\s[A-Z][a-z]+)',
                            re.IGNORECASE
                        ),
                        re.compile(
                            r'(?:HR\s+(?:manager|recruiter|representative)|recruiter)\s+'
                            r'([A-Z][a-z]+\s[A-Z][a-z]+)',
                            re.IGNORECASE
                        ),
                    ]

                    for pattern in interviewer_patterns:
                        for match in pattern.finditer(snippet):
                            name = match.group(1).strip()
                            name_key = name.lower()
                            if (name_key not in seen_names and
                                    len(name.split()) >= 2 and len(name) <= 40):
                                seen_names.add(name_key)
                                contacts.append({
                                    "name": name,
                                    "title": "Recruiter/Interviewer",
                                    "department": "hr",
                                    "source": "glassdoor_interview",
                                })

            except Exception as e:
                logger.debug(f"Glassdoor interview search error: {e}")

        finally:
            await search_engine.close()

        return contacts[:20]

    @staticmethod
    def _infer_department_from_context(text: str) -> str:
        """Infer department from job posting context."""
        text_lower = text.lower()
        DEPT_SIGNALS = {
            "engineering": ["software", "engineer", "developer", "devops", "sre",
                            "backend", "frontend", "fullstack", "architect", "data science"],
            "product": ["product manager", "product designer", "ux", "ui", "design"],
            "marketing": ["marketing", "growth", "content", "seo", "brand", "social media"],
            "sales": ["sales", "account executive", "business development", "sdr", "bdr"],
            "hr": ["recruiter", "talent", "people operations", "hr", "human resources"],
            "support": ["customer success", "support", "customer service", "cx"],
            "finance": ["finance", "accounting", "controller", "treasury"],
            "legal": ["legal", "compliance", "counsel", "privacy"],
        }
        for dept, signals in DEPT_SIGNALS.items():
            if any(sig in text_lower for sig in signals):
                return dept
        return "hr"  # Default: job postings are typically HR-related

    async def close(self):
        await self.client.aclose()


# ============================================
# PODCAST GUEST OSINT (Layer 11)
# ============================================

class PodcastGuestOSINT:
    """
    Layer 11: Discover company employees who appeared as podcast guests.
    Sources: Listen Notes, Podchaser, Google/DuckDuckGo web search.
    Podcast guests are typically senior people — directors, VPs, founders,
    product leads, engineering leads, marketing heads.
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.headers = get_headers(context="navigate")
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def search_podcast_guests(self, company_name: str) -> List[Dict[str, Any]]:
        """
        Find company employees who appeared as podcast guests.
        Uses web search to find podcast episodes featuring company members.
        """
        guests = []
        seen_names = set()

        search_engine = MultiEngineSearch(timeout=self.timeout)

        # Multiple search queries for breadth
        queries = [
            f'"{company_name}" podcast guest interview',
            f'"{company_name}" podcast episode speaker',
            f'site:listennotes.com "{company_name}"',
            f'site:podchaser.com "{company_name}"',
            f'site:spotify.com "{company_name}" podcast',
            f'"{company_name}" "joins us" OR "talks about" podcast',
        ]

        try:
            for query in queries[:4]:
                try:
                    results = await search_engine.search(query, max_results=10)

                    for result in results:
                        title = result.get("title", "")
                        snippet = result.get("snippet", "")
                        url = result.get("url", "")
                        combined = f"{title} {snippet}"

                        # Extract person names mentioned alongside company
                        # Pattern: "Name, Title at Company" or "Name from Company"
                        patterns = [
                            re.compile(
                                r'([A-Z][a-z]+\s[A-Z][a-z]+(?:\s[A-Z][a-z]+)?),?\s+'
                                r'((?:CEO|CTO|CFO|COO|VP|Director|Head|Manager|Lead|'
                                r'Founder|Co-Founder|Partner|Chief|Senior|Principal)[^,\.\|]{0,60}?)'
                                r'(?:\s+(?:at|of|from)\s+' + re.escape(company_name) + r')?',
                                re.IGNORECASE
                            ),
                            re.compile(
                                r'([A-Z][a-z]+\s[A-Z][a-z]+)\s+(?:from|of|at)\s+'
                                + re.escape(company_name),
                                re.IGNORECASE
                            ),
                        ]

                        for pattern in patterns:
                            for match in pattern.finditer(combined):
                                name = match.group(1).strip()
                                job_title = match.group(2).strip() if match.lastindex >= 2 else None

                                name_key = name.lower()
                                if (name_key not in seen_names and
                                        len(name.split()) >= 2 and len(name) <= 50 and
                                        not name.isupper()):
                                    seen_names.add(name_key)
                                    guests.append({
                                        "name": name,
                                        "title": job_title,
                                        "department": CrunchbaseOSINT._classify_department(
                                            job_title or ""
                                        ),
                                        "source": "podcast_guest",
                                        "podcast_url": url,
                                        "context": combined[:150],
                                    })

                    await smart_delay(1.0)

                except Exception as e:
                    logger.debug(f"Podcast search error: {e}")

        finally:
            await search_engine.close()

        return guests[:15]

    async def close(self):
        await self.client.aclose()


# ============================================
# WELLFOUND / ANGELLIST TEAM OSINT (Layer 11)
# ============================================

class WellfoundTeamOSINT:
    """
    Layer 11: Scrape Wellfound (formerly AngelList Talent) for startup team members.
    wellfound.com/company/{slug}/people shows all team members with names and roles.
    Excellent for startups — team pages are fully public.
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.headers = get_headers(context="navigate")
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def find_team_members(self, company_name: str) -> List[Dict[str, Any]]:
        """
        Find team members from Wellfound/AngelList.
        Strategy:
        1. Try to find the Wellfound company page via web search
        2. Scrape the people/team page for member names and roles
        3. Fallback to extracting from search snippets
        """
        team_members = []
        seen_names = set()

        search_engine = MultiEngineSearch(timeout=self.timeout)

        try:
            # Step 1: Find the Wellfound company page
            queries = [
                f'site:wellfound.com/company "{company_name}"',
                f'site:angel.co/company "{company_name}"',
                f'site:wellfound.com "{company_name}" team people',
            ]

            wellfound_url = None

            for query in queries[:2]:
                try:
                    results = await search_engine.search(query, max_results=5)

                    for result in results:
                        url = result.get("url", "")
                        title = result.get("title", "")
                        snippet = result.get("snippet", "")

                        if ("wellfound.com/company/" in url or "angel.co/company/" in url):
                            wellfound_url = url
                            # Extract team info from snippet
                            combined = f"{title} {snippet}"
                            self._extract_people_from_text(
                                combined, company_name, team_members, seen_names
                            )

                    await smart_delay(1.0)

                except Exception as e:
                    logger.debug(f"Wellfound search error: {e}")

            # Step 2: Try to scrape the team/people page
            if wellfound_url:
                # Normalize URL and try /people endpoint
                base_url = wellfound_url.rstrip("/")
                if "/people" not in base_url:
                    people_url = f"{base_url}/people"
                else:
                    people_url = base_url

                try:
                    response = await self.client.get(people_url, headers=self.headers)

                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, "html.parser")

                        # Look for team member cards/sections
                        # Wellfound team pages have structured data
                        for script in soup.find_all("script", type="application/ld+json"):
                            try:
                                data = json.loads(script.string)
                                if isinstance(data, dict) and "member" in data:
                                    for member in data["member"]:
                                        name = member.get("name", "")
                                        job_title = member.get("jobTitle", "")
                                        name_key = name.lower()
                                        if (name_key not in seen_names and
                                                len(name.split()) >= 2):
                                            seen_names.add(name_key)
                                            team_members.append({
                                                "name": name,
                                                "title": job_title,
                                                "department": CrunchbaseOSINT._classify_department(
                                                    job_title
                                                ),
                                                "source": "wellfound_team",
                                                "wellfound_url": wellfound_url,
                                            })
                            except (json.JSONDecodeError, KeyError):
                                pass

                        # Fallback: Parse HTML for team member names
                        # Look for common patterns in team page HTML
                        for elem in soup.select(
                            '[data-test="team-member"], .team-member, '
                            '.styles_component__person, [class*="TeamMember"], '
                            '[class*="person"], [class*="founder"]'
                        ):
                            name_elem = elem.select_one(
                                'h3, h4, [class*="name"], [class*="Name"], a'
                            )
                            title_elem = elem.select_one(
                                '[class*="title"], [class*="role"], [class*="position"], p'
                            )

                            if name_elem:
                                name = name_elem.get_text(strip=True)
                                job_title = title_elem.get_text(strip=True) if title_elem else None
                                name_key = name.lower()

                                if (name_key not in seen_names and
                                        len(name.split()) >= 2 and len(name) <= 50):
                                    seen_names.add(name_key)
                                    team_members.append({
                                        "name": name,
                                        "title": job_title,
                                        "department": CrunchbaseOSINT._classify_department(
                                            job_title or ""
                                        ),
                                        "source": "wellfound_team",
                                        "wellfound_url": wellfound_url,
                                    })

                except Exception as e:
                    logger.debug(f"Wellfound scrape error: {e}")

            # Step 3: Additional web search for team member names
            try:
                query = f'"{company_name}" wellfound OR angellist team founder engineer'
                results = await search_engine.search(query, max_results=10)

                for result in results:
                    combined = f"{result.get('title', '')} {result.get('snippet', '')}"
                    self._extract_people_from_text(
                        combined, company_name, team_members, seen_names
                    )

            except Exception as e:
                logger.debug(f"Wellfound web search fallback error: {e}")

        finally:
            await search_engine.close()

        return team_members[:20]

    def _extract_people_from_text(
        self, text: str, company_name: str,
        team_members: List[Dict[str, Any]], seen_names: set
    ):
        """Extract person names and titles from text snippets."""
        # Pattern: "Name, Title" or "Name - Title"
        patterns = [
            re.compile(
                r'([A-Z][a-z]+\s[A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s*[,\-–]\s*'
                r'((?:CEO|CTO|CFO|COO|VP|Director|Head|Manager|Lead|'
                r'Founder|Co-Founder|Engineer|Designer|Developer|Analyst|'
                r'Advisor|Partner|Principal|Senior|Staff|Chief)[^,\.\|]{0,60})',
                re.IGNORECASE
            ),
        ]

        for pattern in patterns:
            for match in pattern.finditer(text):
                name = match.group(1).strip()
                job_title = match.group(2).strip()
                name_key = name.lower()

                if (name_key not in seen_names and
                        len(name.split()) >= 2 and len(name) <= 50 and
                        not name.isupper()):
                    seen_names.add(name_key)
                    team_members.append({
                        "name": name,
                        "title": job_title,
                        "department": CrunchbaseOSINT._classify_department(job_title),
                        "source": "wellfound_web",
                    })

    async def close(self):
        await self.client.aclose()


# ============================================
# MAIN OSINT ENGINE
# ============================================

class MobiAdzOSINTEngine:
    """
    Main OSINT Engine for TheMobiAdz
    Combines all OSINT sources for deep intelligence gathering
    """

    def __init__(
        self,
        timeout: int = 30,
        brave_api_key: Optional[str] = None,
        crunchbase_api_key: Optional[str] = None,
        theorg_api_key: Optional[str] = None,
        search_cache_ttl: int = 86400,
        search_cache_maxsize: int = 2000,
    ):
        self.timeout = timeout

        # Core search engine (shared across modules) — Layer 7 enhanced
        self.search_engine = MultiEngineSearch(
            timeout=timeout,
            brave_api_key=brave_api_key,
            cache_ttl=search_cache_ttl,
            cache_maxsize=search_cache_maxsize,
        )

        # Initialize all OSINT modules (original)
        self.google_dork = GoogleDorkingOSINT(timeout, search_engine=self.search_engine)
        self.linkedin_osint = LinkedInPublicOSINT(timeout)
        # Use consolidated GitHubOrganizationScraper from ultra engine if available,
        # otherwise fall back to local GitHubOSINT (avoids duplicate class implementations)
        if _ULTRA_GITHUB_AVAILABLE:
            self.github_osint = _GitHubScraper(timeout=timeout)
        else:
            self.github_osint = GitHubOSINT(timeout)
        self.social_osint = SocialMediaOSINT(timeout)
        self.domain_osint = DomainOSINT(timeout)
        self.email_osint = EmailOSINT(timeout)
        self.company_registry = CompanyRegistryOSINT(timeout)

        # NEW Layer 5 OSINT modules
        self.press_release = PressReleaseOSINT(timeout)
        self.blog_osint = CompanyBlogOSINT(timeout)
        self.crunchbase = CrunchbaseOSINT(api_key=crunchbase_api_key, timeout=timeout)
        self.theorg = TheOrgOSINT(api_key=theorg_api_key, timeout=timeout)
        self.patent_osint = PatentOSINT(timeout)
        self.conference_osint = ConferenceSpeakerOSINT(timeout)

        # Layer 11: Full-Spectrum Contact Discovery modules
        self.job_board_intel = JobBoardContactIntel(timeout)
        self.podcast_osint = PodcastGuestOSINT(timeout)
        self.wellfound_osint = WellfoundTeamOSINT(timeout)

        # Statistics
        self.stats = {
            "companies_researched": 0,
            "people_found": 0,
            "emails_found": 0,
            "phones_found": 0,
            "social_profiles_found": 0,
            "sec_officers_found": 0,
            "press_execs_found": 0,
            "blog_authors_found": 0,
            "patent_inventors_found": 0,
            "conference_speakers_found": 0,
            # Layer 11: Full-Spectrum Contact Discovery stats
            "dept_contacts_found": 0,
            "dept_emails_found": 0,
            "linkedin_dept_contacts": 0,
            "media_contacts_found": 0,
            "crunchbase_team_found": 0,
            "job_board_contacts_found": 0,
            "podcast_guests_found": 0,
            "wellfound_team_found": 0,
            "email_patterns_detected": 0,
            "sources_used": set()
        }

        # Progress
        self.progress = {
            "stage": "idle",
            "progress": 0,
            "message": "Ready"
        }

    def _update_progress(self, stage: str, progress: int, message: str):
        self.progress = {
            "stage": stage,
            "progress": progress,
            "message": message
        }

    async def deep_company_osint(
        self,
        company_name: str,
        domain: Optional[str] = None,
        find_leadership: bool = True,
        find_employees: bool = True,
        skip_github_orgs: Optional[Set[str]] = None,
        skip_github_users: Optional[Set[str]] = None,
        skip_dns_domains: Optional[Set[str]] = None
    ) -> CompanyIntel:
        """
        Perform deep OSINT on a company.

        This is the main method that combines all OSINT techniques.
        """
        self._update_progress("osint", 0, f"Starting OSINT for {company_name}...")

        # Initialize result
        intel = CompanyIntel(name=company_name, domain=domain)

        # Guess domain if not provided
        if not domain:
            domain = self._guess_domain(company_name)
            intel.domain = domain

        try:
            # 1. Domain OSINT
            self._update_progress("osint", 10, "Gathering domain intelligence...")

            if domain:
                # DNS records
                dns_data = await self.domain_osint.get_dns_records(domain)
                intel.emails["dns"] = dns_data.get("emails_found", [])

                # WHOIS data
                whois_data = await self.domain_osint.get_whois_data(domain)
                if whois_data.get("registrant_email"):
                    intel.emails["whois"] = whois_data["registrant_email"]
                if whois_data.get("registrant_org"):
                    if not intel.name or intel.name == company_name:
                        intel.name = whois_data["registrant_org"]

                # Subdomains
                subdomains = await self.domain_osint.enumerate_subdomains(domain)
                intel.subdomains = subdomains[:20]

                # Technologies
                technologies = await self.domain_osint.detect_technologies(domain)
                intel.technologies = technologies

                intel.sources.append("domain_osint")
                self.stats["sources_used"].add("domain_osint")

            # 2. Social Media Profiles
            self._update_progress("osint", 20, "Finding social media profiles...")

            social_profiles = await self.social_osint.find_social_profiles(company_name, domain or "")
            intel.linkedin_url = social_profiles.get("linkedin")
            intel.twitter_url = social_profiles.get("twitter")
            intel.facebook_url = social_profiles.get("facebook")

            if any(social_profiles.values()):
                intel.sources.append("social_media")
                self.stats["social_profiles_found"] += sum(1 for v in social_profiles.values() if v)

            # 3. Google Dorking for Emails
            self._update_progress("osint", 30, "Searching for emails via dorking...")

            dork_emails = await self.google_dork.find_emails(company_name, domain or "")
            for email_data in dork_emails:
                email = email_data["email"]
                prefix = email.split("@")[0].lower()

                # Classify by prefix into the right category
                if any(kw in prefix for kw in ["support", "help", "care", "service", "customer"]):
                    if "support" not in intel.emails:
                        intel.emails["support"] = email
                elif any(kw in prefix for kw in ["press", "media", "journalist", "newsroom"]):
                    if "press" not in intel.emails:
                        intel.emails["press"] = email
                elif any(kw in prefix for kw in ["marketing", "pr", "ads", "advertising", "growth", "brand"]):
                    if "marketing" not in intel.emails:
                        intel.emails["marketing"] = email
                elif any(kw in prefix for kw in ["sales", "business", "enterprise", "partner", "demo"]):
                    if "sales" not in intel.emails:
                        intel.emails["sales"] = email
                elif any(kw in prefix for kw in ["hr", "career", "recruit", "hiring", "talent", "jobs"]):
                    if "hr" not in intel.emails:
                        intel.emails["hr"] = email
                elif any(kw in prefix for kw in ["dev", "developer", "engineering", "tech", "api", "team"]):
                    if "dev" not in intel.emails:
                        intel.emails["dev"] = email
                elif any(kw in prefix for kw in ["legal", "compliance", "privacy", "gdpr"]):
                    if "legal" not in intel.emails:
                        intel.emails["legal"] = email
                elif any(kw in prefix for kw in ["finance", "billing", "accounts", "invoice"]):
                    if "finance" not in intel.emails:
                        intel.emails["finance"] = email
                elif any(kw in prefix for kw in ["info", "hello", "hi", "contact", "general", "office", "staff"]):
                    if "contact" not in intel.emails:
                        intel.emails["contact"] = email
                    elif "info" not in intel.emails:
                        intel.emails["info"] = email
                else:
                    # Store under generic contact if no contact email yet
                    if "contact" not in intel.emails:
                        intel.emails["contact"] = email

            self.stats["emails_found"] += len(dork_emails)

            # 4. Google Dorking for Phones
            self._update_progress("osint", 40, "Searching for phone numbers...")

            phones = await self.google_dork.find_phones(company_name, domain or "")
            intel.phones = phones[:5]
            self.stats["phones_found"] += len(phones)

            # 5. Company Registry Search
            self._update_progress("osint", 50, "Searching company registries...")

            registry_results = await self.company_registry.search_opencorporates(company_name)
            if registry_results:
                top_result = registry_results[0]
                intel.headquarters = top_result.get("registered_address")
                intel.founded = top_result.get("incorporation_date")
                intel.sources.append("opencorporates")

            # SEC EDGAR for US companies
            sec_results = await self.company_registry.search_sec_edgar(company_name)
            if sec_results:
                intel.sources.append("sec_edgar")

                # 5b. Extract actual officers from SEC filings
                for sec_result in sec_results[:2]:
                    cik = sec_result.get("cik", "")
                    if cik:
                        try:
                            officers = await self.company_registry.get_sec_company_officers(cik)
                            for officer in officers:
                                if officer.get("name") and officer.get("title"):
                                    existing = next(
                                        (l for l in intel.leadership if l.name.lower() == officer["name"].lower()),
                                        None
                                    )
                                    if not existing:
                                        person = PersonIntel(
                                            name=officer["name"],
                                            title=officer["title"],
                                            company=company_name,
                                            sources=[officer.get("source", "sec_edgar")]
                                        )
                                        intel.leadership.append(person)
                                        self.stats["sec_officers_found"] += 1
                                        self.stats["people_found"] += 1
                            if officers:
                                intel.sources.append("sec_edgar_officers")
                        except Exception as e:
                            logger.debug(f"SEC officer extraction error: {e}")
                        break  # Only process first matching CIK

            # 5c. Press Release Executive Mining
            self._update_progress("osint", 52, "Mining press releases for executives...")
            try:
                pr_executives = await self.press_release.find_executives(company_name)
                for exec_info in pr_executives:
                    existing = next(
                        (l for l in intel.leadership if l.name.lower() == exec_info["name"].lower()),
                        None
                    )
                    if not existing:
                        person = PersonIntel(
                            name=exec_info["name"],
                            title=exec_info.get("title"),
                            company=company_name,
                            sources=[exec_info.get("source", "press_release")]
                        )
                        intel.leadership.append(person)
                        self.stats["press_execs_found"] += 1
                        self.stats["people_found"] += 1
                if pr_executives:
                    intel.sources.append("press_releases")
            except Exception as e:
                logger.debug(f"Press release mining error: {e}")

            # 5d. Company Blog Author & Team Page Extraction
            if domain:
                self._update_progress("osint", 54, "Extracting blog authors & team page...")
                try:
                    # Team page first (higher value - names + titles)
                    team_people = await self.blog_osint.extract_team_page(domain)
                    for person_data in team_people[:15]:
                        existing = next(
                            (l for l in intel.leadership + intel.employees
                             if l.name.lower() == person_data["name"].lower()),
                            None
                        )
                        if not existing:
                            person = PersonIntel(
                                name=person_data["name"],
                                title=person_data.get("title"),
                                company=company_name,
                                sources=[person_data.get("source", "team_page")]
                            )
                            # If has leadership title, add to leadership; else employees
                            title_lower = (person_data.get("title") or "").lower()
                            if any(kw in title_lower for kw in [
                                "chief", "ceo", "cto", "cfo", "coo", "president",
                                "founder", "vp", "vice president", "director", "head", "svp", "evp"
                            ]):
                                intel.leadership.append(person)
                            else:
                                intel.employees.append(person)
                            self.stats["blog_authors_found"] += 1
                            self.stats["people_found"] += 1

                    # Blog authors (employees/contributors)
                    blog_authors = await self.blog_osint.extract_blog_authors(domain)
                    for author_data in blog_authors[:10]:
                        existing = next(
                            (p for p in intel.leadership + intel.employees
                             if p.name.lower() == author_data["name"].lower()),
                            None
                        )
                        if not existing:
                            person = PersonIntel(
                                name=author_data["name"],
                                title=author_data.get("title"),
                                company=company_name,
                                sources=[author_data.get("source", "company_blog")]
                            )
                            intel.employees.append(person)
                            self.stats["blog_authors_found"] += 1
                            self.stats["people_found"] += 1

                    if team_people or blog_authors:
                        intel.sources.append("company_blog")
                except Exception as e:
                    logger.debug(f"Blog/team extraction error: {e}")

            # 5e. Crunchbase Founders
            self._update_progress("osint", 56, "Searching Crunchbase for founders...")
            try:
                founders = await self.crunchbase.find_founders(company_name)
                for founder_data in founders:
                    existing = next(
                        (l for l in intel.leadership if l.name.lower() == founder_data["name"].lower()),
                        None
                    )
                    if not existing and founder_data.get("name"):
                        person = PersonIntel(
                            name=founder_data["name"],
                            title=founder_data.get("title", "Founder"),
                            company=company_name,
                            linkedin_url=founder_data.get("linkedin_url"),
                            sources=[founder_data.get("source", "crunchbase")]
                        )
                        intel.leadership.append(person)
                        self.stats["people_found"] += 1
                if founders:
                    intel.sources.append("crunchbase")
            except Exception as e:
                logger.debug(f"Crunchbase search error: {e}")

            # 5f. TheOrg.com Org Chart
            if domain:
                self._update_progress("osint", 57, "Checking TheOrg for org chart...")
                try:
                    theorg_leaders = await self.theorg.find_leadership(domain, company_name)
                    for leader_data in theorg_leaders:
                        existing = next(
                            (l for l in intel.leadership if l.name.lower() == leader_data["name"].lower()),
                            None
                        )
                        if not existing and leader_data.get("name"):
                            person = PersonIntel(
                                name=leader_data["name"],
                                title=leader_data.get("title"),
                                company=company_name,
                                linkedin_url=leader_data.get("linkedin_url"),
                                emails=[leader_data["email"]] if leader_data.get("email") else [],
                                sources=[leader_data.get("source", "theorg")]
                            )
                            intel.leadership.append(person)
                            self.stats["people_found"] += 1
                    if theorg_leaders:
                        intel.sources.append("theorg")
                except Exception as e:
                    logger.debug(f"TheOrg search error: {e}")

            # 5g. USPTO Patent Inventors (R&D leadership)
            self._update_progress("osint", 58, "Searching USPTO for inventors...")
            try:
                inventors = await self.patent_osint.find_technical_leaders(company_name)
                for inv_data in inventors[:5]:
                    existing = next(
                        (p for p in intel.leadership + intel.employees
                         if p.name.lower() == inv_data["name"].lower()),
                        None
                    )
                    if not existing:
                        person = PersonIntel(
                            name=inv_data["name"],
                            title=inv_data.get("title", "Inventor"),
                            company=company_name,
                            location=inv_data.get("location"),
                            sources=["uspto_patents"]
                        )
                        intel.employees.append(person)
                        self.stats["patent_inventors_found"] += 1
                        self.stats["people_found"] += 1
                if inventors:
                    intel.sources.append("patents")
            except Exception as e:
                logger.debug(f"USPTO search error: {e}")

            # 5h. Conference Speaker Discovery
            self._update_progress("osint", 59, "Searching conference speakers...")
            try:
                speakers = await self.conference_osint.search_conference_speakers(company_name)
                if not speakers:
                    speakers = await self.conference_osint.search_via_web(company_name)

                for speaker_data in speakers[:5]:
                    existing = next(
                        (p for p in intel.leadership + intel.employees
                         if p.name.lower() == speaker_data["name"].lower()),
                        None
                    )
                    if not existing:
                        person = PersonIntel(
                            name=speaker_data["name"],
                            title=speaker_data.get("title", "Conference Speaker"),
                            company=company_name,
                            sources=["conference_speaker"]
                        )
                        intel.employees.append(person)
                        self.stats["conference_speakers_found"] += 1
                        self.stats["people_found"] += 1
                if speakers:
                    intel.sources.append("conferences")
            except Exception as e:
                logger.debug(f"Conference speaker search error: {e}")

            # ============================================
            # LAYER 11: FULL-SPECTRUM CONTACT DISCOVERY
            # ============================================

            # 5i. Role-Targeted Google Dorks (find contacts by department)
            self._update_progress("osint", 60, "Finding contacts by department (role dorks)...")
            try:
                dept_contacts = await self.google_dork.find_department_contacts(
                    company_name, domain or ""
                )
                for dept, contacts_list in dept_contacts.items():
                    for contact_data in contacts_list:
                        if contact_data.get("name"):
                            existing = next(
                                (p for p in intel.leadership + intel.employees
                                 if p.name.lower() == contact_data["name"].lower()),
                                None
                            )
                            if not existing:
                                person = PersonIntel(
                                    name=contact_data["name"],
                                    title=contact_data.get("title"),
                                    company=company_name,
                                    linkedin_url=contact_data.get("linkedin_url"),
                                    sources=[contact_data.get("source", f"role_dork_{dept}")]
                                )
                                # Classify: leadership or employee based on title
                                title_lower = (contact_data.get("title") or "").lower()
                                if any(kw in title_lower for kw in [
                                    "chief", "ceo", "cto", "cfo", "coo", "president",
                                    "founder", "vp", "vice president", "director", "head", "svp", "evp"
                                ]):
                                    intel.leadership.append(person)
                                else:
                                    intel.employees.append(person)
                                self.stats["dept_contacts_found"] += 1
                                self.stats["people_found"] += 1
                        elif contact_data.get("email"):
                            # Department email found via role dork
                            email = contact_data["email"]
                            dept_key = contact_data.get("department", "contact")
                            if dept_key not in intel.emails:
                                intel.emails[dept_key] = email
                            self.stats["dept_emails_found"] += 1
                            self.stats["emails_found"] += 1

                if dept_contacts:
                    intel.sources.append("role_dorks")
            except Exception as e:
                logger.debug(f"Role-targeted dork error: {e}")

            # 5j. Department Email Discovery (hr@, marketing@, sales@, etc.)
            self._update_progress("osint", 62, "Finding department emails...")
            try:
                dept_emails = await self.google_dork.find_department_emails(
                    company_name, domain or ""
                )
                for dept, emails_list in dept_emails.items():
                    for email in emails_list:
                        if dept not in intel.emails:
                            intel.emails[dept] = email
                        self.stats["dept_emails_found"] += 1
                        self.stats["emails_found"] += 1

                if dept_emails:
                    intel.sources.append("department_emails")
            except Exception as e:
                logger.debug(f"Department email dork error: {e}")

            # 5k. Role-Targeted LinkedIn Department Searches
            self._update_progress("osint", 64, "Searching LinkedIn by department...")
            try:
                linkedin_dept = await self.linkedin_osint.search_department_employees(
                    company_name
                )
                for dept, people_list in linkedin_dept.items():
                    for person_data in people_list:
                        existing = next(
                            (p for p in intel.leadership + intel.employees
                             if p.name.lower() == person_data["name"].lower()),
                            None
                        )
                        if not existing:
                            person = PersonIntel(
                                name=person_data["name"],
                                title=person_data.get("title"),
                                company=company_name,
                                linkedin_url=person_data.get("linkedin_url"),
                                sources=[person_data.get("source", f"linkedin_dept_{dept}")]
                            )
                            title_lower = (person_data.get("title") or "").lower()
                            if any(kw in title_lower for kw in [
                                "chief", "ceo", "cto", "cfo", "coo", "president",
                                "founder", "vp", "vice president", "director", "head", "svp"
                            ]):
                                intel.leadership.append(person)
                            else:
                                intel.employees.append(person)
                            self.stats["linkedin_dept_contacts"] += 1
                            self.stats["people_found"] += 1

                if linkedin_dept:
                    intel.sources.append("linkedin_departments")
            except Exception as e:
                logger.debug(f"LinkedIn department search error: {e}")

            # 5l. Press Release Media Contacts (Layer 11 — find PR/media contacts)
            self._update_progress("osint", 66, "Extracting media contacts from press releases...")
            try:
                pr_all = await self.press_release.find_all_contacts(company_name)
                for mc in pr_all.get("media_contacts", []):
                    existing = next(
                        (p for p in intel.leadership + intel.employees
                         if p.name.lower() == mc["name"].lower()),
                        None
                    )
                    if not existing:
                        person = PersonIntel(
                            name=mc["name"],
                            title=mc.get("title", "Media/PR Contact"),
                            company=company_name,
                            emails=[mc["email"]] if mc.get("email") else [],
                            sources=["press_release_media"]
                        )
                        intel.employees.append(person)
                        self.stats["media_contacts_found"] += 1
                        self.stats["people_found"] += 1
                        if mc.get("email"):
                            self.stats["emails_found"] += 1

                    if mc.get("phone"):
                        intel.phones.append(mc["phone"])
                        self.stats["phones_found"] += 1

                if pr_all.get("media_contacts"):
                    intel.sources.append("press_media_contacts")
            except Exception as e:
                logger.debug(f"Press release media contact error: {e}")

            # 5m. Crunchbase Full Team (not just founders)
            self._update_progress("osint", 68, "Searching Crunchbase for full team...")
            try:
                cb_team = await self.crunchbase.find_team_members(company_name)
                for member in cb_team:
                    if not member.get("name"):
                        continue
                    existing = next(
                        (p for p in intel.leadership + intel.employees
                         if p.name.lower() == member["name"].lower()),
                        None
                    )
                    if not existing:
                        person = PersonIntel(
                            name=member["name"],
                            title=member.get("title"),
                            company=company_name,
                            sources=[member.get("source", "crunchbase_team")]
                        )
                        dept = member.get("department", "unknown")
                        if dept == "leadership":
                            intel.leadership.append(person)
                        else:
                            intel.employees.append(person)
                        self.stats["crunchbase_team_found"] += 1
                        self.stats["people_found"] += 1

                if cb_team:
                    intel.sources.append("crunchbase_team")
            except Exception as e:
                logger.debug(f"Crunchbase team search error: {e}")

            # 5n. Job Board Contact Intelligence
            self._update_progress("osint", 70, "Extracting contacts from job postings...")
            try:
                jb_contacts = await self.job_board_intel.extract_hiring_contacts(company_name)
                for jb_contact in jb_contacts:
                    if jb_contact.get("name"):
                        existing = next(
                            (p for p in intel.leadership + intel.employees
                             if p.name.lower() == jb_contact["name"].lower()),
                            None
                        )
                        if not existing:
                            person = PersonIntel(
                                name=jb_contact["name"],
                                title=jb_contact.get("title"),
                                company=company_name,
                                sources=[jb_contact.get("source", "job_board")]
                            )
                            intel.employees.append(person)
                            self.stats["job_board_contacts_found"] += 1
                            self.stats["people_found"] += 1
                    elif jb_contact.get("email"):
                        if "hr" not in intel.emails:
                            intel.emails["hr"] = jb_contact["email"]
                        self.stats["emails_found"] += 1

                if jb_contacts:
                    intel.sources.append("job_boards")
            except Exception as e:
                logger.debug(f"Job board contact extraction error: {e}")

            # 5o. Podcast Guest Discovery
            self._update_progress("osint", 72, "Finding podcast guest appearances...")
            try:
                podcast_guests = await self.podcast_osint.search_podcast_guests(company_name)
                for guest in podcast_guests:
                    existing = next(
                        (p for p in intel.leadership + intel.employees
                         if p.name.lower() == guest["name"].lower()),
                        None
                    )
                    if not existing:
                        person = PersonIntel(
                            name=guest["name"],
                            title=guest.get("title"),
                            company=company_name,
                            sources=["podcast_guest"]
                        )
                        dept = guest.get("department", "unknown")
                        if dept == "leadership":
                            intel.leadership.append(person)
                        else:
                            intel.employees.append(person)
                        self.stats["podcast_guests_found"] += 1
                        self.stats["people_found"] += 1

                if podcast_guests:
                    intel.sources.append("podcasts")
            except Exception as e:
                logger.debug(f"Podcast guest search error: {e}")

            # 5p. Wellfound/AngelList Team Scraping
            self._update_progress("osint", 74, "Scraping Wellfound team page...")
            try:
                wf_team = await self.wellfound_osint.find_team_members(company_name)
                for member in wf_team:
                    existing = next(
                        (p for p in intel.leadership + intel.employees
                         if p.name.lower() == member["name"].lower()),
                        None
                    )
                    if not existing:
                        person = PersonIntel(
                            name=member["name"],
                            title=member.get("title"),
                            company=company_name,
                            sources=[member.get("source", "wellfound")]
                        )
                        dept = member.get("department", "unknown")
                        if dept == "leadership":
                            intel.leadership.append(person)
                        else:
                            intel.employees.append(person)
                        self.stats["wellfound_team_found"] += 1
                        self.stats["people_found"] += 1

                if wf_team:
                    intel.sources.append("wellfound")
            except Exception as e:
                logger.debug(f"Wellfound team search error: {e}")

            # ============================================
            # END LAYER 11: FULL-SPECTRUM CONTACT DISCOVERY
            # ============================================

            # 6. Leadership/Employee OSINT (Enhanced with MultiEngineSearch)
            if find_leadership:
                self._update_progress("osint", 76, "Finding leadership team (classic dorks)...")

                # Google dorking for leadership
                leadership_results = await self.google_dork.find_leadership(company_name, domain or "")

                for person_data in leadership_results[:10]:
                    # Skip if already found by Layer 11 modules
                    existing = next(
                        (p for p in intel.leadership
                         if p.name.lower() == person_data.get("name", "").lower()),
                        None
                    )
                    if not existing:
                        person = PersonIntel(
                            name=person_data.get("name", "Unknown"),
                            title=person_data.get("title"),
                            company=company_name,
                            linkedin_url=person_data.get("linkedin_url"),
                            sources=["linkedin_dork"]
                        )
                        intel.leadership.append(person)

                self.stats["people_found"] += len(leadership_results)

            if find_employees:
                self._update_progress("osint", 80, "Finding employees (generic search)...")

                # LinkedIn employee search
                employees = await self.linkedin_osint.search_company_employees(company_name)

                for emp_data in employees[:15]:
                    # Skip if already found by Layer 11 modules
                    existing = next(
                        (p for p in intel.employees
                         if p.name.lower() == emp_data.get("name", "").lower()),
                        None
                    )
                    if not existing:
                        person = PersonIntel(
                            name=emp_data.get("name", "Unknown"),
                            title=emp_data.get("title"),
                            company=company_name,
                            linkedin_url=emp_data.get("linkedin_url"),
                            sources=["linkedin_search"]
                        )
                        intel.employees.append(person)

                self.stats["people_found"] += len(employees)

            # 7. GitHub OSINT (with cross-stage deduplication)
            self._update_progress("osint", 80, "Searching GitHub...")

            org_name = company_name.lower().replace(" ", "").replace("-", "")

            # Skip if already searched in a prior stage (e.g., Ultra extraction)
            _skip_orgs = skip_github_orgs or set()
            if org_name in _skip_orgs:
                logger.debug(f"[DEDUP] Skipping GitHub org '{org_name}' in OSINT (already searched in Ultra)")
                github_members = []
            else:
                _skip_orgs.add(org_name)  # Mark as searched
                github_members = await self.github_osint.search_org_members(org_name, max_members=10)

            for member in github_members:
                # Check if already in employees
                existing = next(
                    (e for e in intel.employees if e.name and member.get("name") and
                     e.name.lower() == member.get("name", "").lower()),
                    None
                )

                if existing:
                    # Update existing with GitHub data
                    if member.get("email"):
                        existing.emails.append(member["email"])
                    existing.github_url = member.get("github_url")
                    existing.sources.append("github")
                else:
                    # Add new person
                    person = PersonIntel(
                        name=member.get("name") or member.get("username"),
                        company=company_name,
                        emails=[member["email"]] if member.get("email") else [],
                        github_url=member.get("github_url"),
                        location=member.get("location"),
                        bio=member.get("bio"),
                        sources=["github"]
                    )
                    intel.employees.append(person)
                    self.stats["people_found"] += 1

                if member.get("email"):
                    self.stats["emails_found"] += 1

            # 8. Email Permutation for ALL discovered people (Layer 11 enhanced)
            self._update_progress("osint", 90, "Generating email permutations for all contacts...")

            if domain:
                # Step 8a: Try to detect email pattern from any known email
                known_emails = []
                for email_type, email in intel.emails.items():
                    if isinstance(email, str) and "@" in email and domain in email:
                        known_emails.append(email)

                # Also check emails on people
                for person in intel.leadership + intel.employees:
                    for e in person.emails:
                        if domain in e:
                            known_emails.append(e)

                # Try to detect the pattern from the first personal email found
                for person in intel.leadership + intel.employees:
                    if person.emails and person.name and " " in person.name:
                        for e in person.emails:
                            if domain in e:
                                parts = person.name.split()
                                detected = self.email_osint.detect_email_pattern(
                                    e, parts[0], parts[-1], domain
                                )
                                if detected:
                                    self.stats["email_patterns_detected"] += 1
                                    break
                        if self.email_osint._domain_patterns.get(domain):
                            break  # Pattern found, stop looking

                # Step 8b: Generate permutations for leadership (top 8)
                for person in intel.leadership[:8]:
                    if person.name and " " in person.name and not person.emails:
                        name_parts = person.name.split()
                        first_name = name_parts[0]
                        last_name = name_parts[-1]
                        middle_name = name_parts[1] if len(name_parts) > 2 else None

                        permutations = await self.email_osint.generate_email_permutations(
                            first_name, last_name, domain, middle_name
                        )
                        person.emails = [p["email"] for p in permutations[:5]]

                # Step 8c: Generate permutations for employees (top 10)
                for person in intel.employees[:10]:
                    if person.name and " " in person.name and not person.emails:
                        name_parts = person.name.split()
                        first_name = name_parts[0]
                        last_name = name_parts[-1]
                        middle_name = name_parts[1] if len(name_parts) > 2 else None

                        permutations = await self.email_osint.generate_email_permutations(
                            first_name, last_name, domain, middle_name
                        )
                        person.emails = [p["email"] for p in permutations[:3]]

            # Calculate confidence score
            intel.confidence_score = self._calculate_confidence(intel)

            self.stats["companies_researched"] += 1
            self._update_progress("osint", 100, f"OSINT complete for {company_name}")

        except Exception as e:
            logger.error(f"OSINT error for {company_name}: {e}")

        return intel

    async def deep_person_osint(
        self,
        name: str,
        company: Optional[str] = None,
        domain: Optional[str] = None
    ) -> PersonIntel:
        """
        Perform deep OSINT on a person.
        """
        self._update_progress("osint", 0, f"Starting OSINT for {name}...")

        intel = PersonIntel(name=name, company=company)

        try:
            # 1. Search Google/DuckDuckGo for the person
            self._update_progress("osint", 20, "Searching for person online...")

            dork_searcher = GoogleDorkingOSINT()

            # LinkedIn search
            linkedin_query = f'site:linkedin.com/in "{name}"'
            if company:
                linkedin_query += f' "{company}"'

            linkedin_results = await dork_searcher.search_duckduckgo(linkedin_query, max_results=5)

            for result in linkedin_results:
                url = result.get("url", "")
                if "linkedin.com/in/" in url:
                    intel.linkedin_url = url

                    # Extract title from result
                    title_match = re.search(r'-\s*([^|]+)', result.get("title", ""))
                    if title_match:
                        intel.title = title_match.group(1).strip()
                    break

            # 2. GitHub search
            self._update_progress("osint", 40, "Searching GitHub...")

            github_users = await self.github_osint.search_users(name, max_results=3)

            for user in github_users:
                user_company = user.get("company", "").lower() if user.get("company") else ""

                if not company or (company and company.lower() in user_company):
                    intel.github_url = user.get("github_url")
                    if user.get("email"):
                        intel.emails.append(user["email"])
                    if user.get("bio"):
                        intel.bio = user["bio"]
                    if user.get("location"):
                        intel.location = user["location"]
                    intel.sources.append("github")
                    break

            # 3. Twitter search
            self._update_progress("osint", 60, "Searching Twitter...")

            twitter_query = f'site:twitter.com "{name}"'
            if company:
                twitter_query += f' "{company}"'

            twitter_results = await dork_searcher.search_duckduckgo(twitter_query, max_results=5)

            for result in twitter_results:
                url = result.get("url", "")
                if "twitter.com/" in url or "x.com/" in url:
                    if "/status/" not in url:
                        intel.twitter_url = url
                        intel.sources.append("twitter")
                        break

            # 4. Email permutation
            self._update_progress("osint", 80, "Generating email permutations...")

            if domain and " " in name:
                name_parts = name.split()
                first_name = name_parts[0]
                last_name = name_parts[-1]

                permutations = await self.email_osint.generate_email_permutations(
                    first_name, last_name, domain
                )

                intel.emails.extend([p["email"] for p in permutations[:5]])

            # 5. Gravatar check
            for email in intel.emails[:3]:
                gravatar_data = await self.email_osint.check_gravatar(email)
                if gravatar_data.get("has_gravatar"):
                    intel.profile_image = gravatar_data.get("avatar_url")
                    intel.sources.append("gravatar")
                    break

            await dork_searcher.close()

            # Calculate confidence
            score = 0
            if intel.linkedin_url:
                score += 30
            if intel.emails:
                score += 25
            if intel.github_url:
                score += 15
            if intel.twitter_url:
                score += 10
            if intel.title:
                score += 10
            if intel.location:
                score += 5
            if intel.bio:
                score += 5

            intel.confidence_score = min(score, 100)

            self._update_progress("osint", 100, f"OSINT complete for {name}")

        except Exception as e:
            logger.error(f"Person OSINT error for {name}: {e}")

        return intel

    def _guess_domain(self, company_name: str) -> str:
        """Guess domain from company name (consistent with ultra engine)"""
        clean = company_name.lower()
        clean = re.sub(r'[^a-z0-9]', '', clean)

        # Remove common corporate suffixes (kept in sync with ultra engine)
        suffixes = ['inc', 'llc', 'ltd', 'corp', 'corporation', 'company', 'co',
                     'limited', 'gmbh', 'ag', 'sa', 'srl', 'bv', 'pty', 'pvt',
                     'technologies', 'technology', 'tech', 'software', 'solutions',
                     'digital', 'media', 'group', 'labs', 'studio', 'studios']
        for suffix in suffixes:
            if clean.endswith(suffix) and len(clean) > len(suffix):
                clean = clean[:-len(suffix)]

        return f"{clean}.com" if clean else f"{company_name.lower().replace(' ', '')}.com"

    def _calculate_confidence(self, intel: CompanyIntel) -> int:
        """Calculate confidence score for company intel"""
        score = 0

        # Basic info
        if intel.name:
            score += 10
        if intel.domain:
            score += 10
        if intel.description:
            score += 5

        # Contact info
        if intel.emails:
            score += min(len(intel.emails) * 5, 20)
        if intel.phones:
            score += 10

        # Social profiles
        if intel.linkedin_url:
            score += 10
        if intel.twitter_url:
            score += 5

        # Leadership
        if intel.leadership:
            score += min(len(intel.leadership) * 3, 15)

        # Employees
        if intel.employees:
            score += min(len(intel.employees) * 2, 10)

        # Data sources
        score += min(len(intel.sources) * 3, 15)

        return min(score, 100)

    def get_stats(self) -> Dict[str, Any]:
        stats = dict(self.stats)
        stats["sources_used"] = list(stats["sources_used"])
        return stats

    def get_progress(self) -> Dict[str, Any]:
        return self.progress

    async def close(self):
        """Close all OSINT modules (fault-tolerant)"""
        modules = [
            self.search_engine, self.google_dork, self.linkedin_osint,
            self.github_osint, self.social_osint, self.domain_osint,
            self.email_osint, self.company_registry,
            # Layer 5 modules
            self.press_release, self.blog_osint, self.crunchbase,
            self.theorg, self.patent_osint, self.conference_osint,
            # Layer 11 modules
            self.job_board_intel, self.podcast_osint, self.wellfound_osint,
        ]
        results = await asyncio.gather(
            *[m.close() for m in modules if m],
            return_exceptions=True
        )
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                logger.warning(f"Error closing OSINT module {i}: {r}")

        logger.info("MobiAdz OSINT Engine closed")


# ============================================
# QUICK START FUNCTIONS
# ============================================

async def quick_company_osint(company_name: str, domain: Optional[str] = None) -> Dict[str, Any]:
    """
    Quick OSINT for a company.

    Example:
        result = await quick_company_osint("Stripe", "stripe.com")
        print(f"Found {len(result['leadership'])} leaders")
    """
    engine = MobiAdzOSINTEngine()

    try:
        intel = await engine.deep_company_osint(
            company_name=company_name,
            domain=domain,
            find_leadership=True,
            find_employees=True
        )

        return {
            "name": intel.name,
            "domain": intel.domain,
            "description": intel.description,
            "headquarters": intel.headquarters,
            "founded": intel.founded,
            "emails": intel.emails,
            "phones": intel.phones,
            "linkedin_url": intel.linkedin_url,
            "twitter_url": intel.twitter_url,
            "leadership": [
                {
                    "name": p.name,
                    "title": p.title,
                    "emails": p.emails,
                    "linkedin_url": p.linkedin_url,
                    "github_url": p.github_url
                }
                for p in intel.leadership
            ],
            "employees": [
                {
                    "name": p.name,
                    "title": p.title,
                    "emails": p.emails,
                    "linkedin_url": p.linkedin_url,
                    "github_url": p.github_url
                }
                for p in intel.employees
            ],
            "technologies": intel.technologies,
            "subdomains": intel.subdomains,
            "sources": intel.sources,
            "confidence_score": intel.confidence_score
        }

    finally:
        await engine.close()


async def quick_person_osint(
    name: str,
    company: Optional[str] = None,
    domain: Optional[str] = None
) -> Dict[str, Any]:
    """
    Quick OSINT for a person.

    Example:
        result = await quick_person_osint("John Doe", "Acme Corp", "acme.com")
    """
    engine = MobiAdzOSINTEngine()

    try:
        intel = await engine.deep_person_osint(
            name=name,
            company=company,
            domain=domain
        )

        return {
            "name": intel.name,
            "title": intel.title,
            "company": intel.company,
            "emails": intel.emails,
            "phones": intel.phones,
            "linkedin_url": intel.linkedin_url,
            "twitter_url": intel.twitter_url,
            "github_url": intel.github_url,
            "location": intel.location,
            "bio": intel.bio,
            "profile_image": intel.profile_image,
            "sources": intel.sources,
            "confidence_score": intel.confidence_score
        }

    finally:
        await engine.close()
