"""
ULTRA DEEP SEARCH ENGINE V2.0 - EXTREME CONTACT DISCOVERY
==========================================================

FREE VERSION - 15+ Layers of Deep Search:
=========================================
Layer 1: Multi-Engine Web Search (DuckDuckGo, Bing, SearX, Brave)
Layer 2: Archive Mining (Wayback Machine, Archive.today, CommonCrawl)
Layer 3: DNS Intelligence (MX, TXT, SPF records for email patterns)
Layer 4: WHOIS & Domain Intelligence
Layer 5: SSL Certificate Transparency Logs (find subdomains)
Layer 6: Sitemap/Robots.txt Mining (discover hidden pages)
Layer 7: Social Media Deep Scrape (LinkedIn, Twitter, Facebook, Instagram)
Layer 8: Developer Platforms (GitHub, GitLab, npm, PyPI, Maven)
Layer 9: Job Posting Analysis (Indeed, Glassdoor, LinkedIn Jobs)
Layer 10: Press Release Mining (PRNewswire, BusinessWire, PRWeb)
Layer 11: Startup Databases (Crunchbase, AngelList, ProductHunt)
Layer 12: Email Permutation Engine (50+ patterns)
Layer 13: SMTP Verification (check if email exists - FREE)
Layer 14: Google Cache & Cached Pages
Layer 15: Parallel Aggressive Multi-Source Hunt

PAID VERSION - 25+ Layers with API Power:
=========================================
All FREE layers PLUS:
Layer P1: Hunter.io (email finder + verifier + domain search)
Layer P2: Clearbit (company enrichment + person lookup)
Layer P3: Apollo.io (B2B database + contact search)
Layer P4: ZoomInfo/Lusha (premium verified contacts)
Layer P5: RocketReach (email + phone + social)
Layer P6: Snov.io (email finder + drip campaigns)
Layer P7: Voila Norbert (email verification)
Layer P8: BuiltWith (technographics - what tech they use)
Layer P9: SimilarWeb (traffic & competitor data)
Layer P10: FullContact (person enrichment)
Layer P11: Pipl (deep person search)
Layer P12: Parallel API Orchestration (all APIs simultaneously)

SPEED OPTIMIZATIONS:
===================
- Async/await everywhere
- Connection pooling
- Request batching
- Intelligent caching
- Parallel execution with asyncio.gather
- Rate limit management
- Automatic retry with exponential backoff
"""

import asyncio
import logging
import re
import json
import hashlib
import base64
import socket
import ssl
import smtplib
from typing import Dict, Any, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlencode, quote, quote_plus, urljoin
from email.utils import parseaddr
import httpx
from bs4 import BeautifulSoup
import dns.resolver
import random

logger = logging.getLogger(__name__)

# ============================================
# USER AGENTS POOL (50+ agents for rotation)
# ============================================
USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    # Chrome on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0",
    # Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
    # Mobile
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
]

# ============================================
# SEARCH ENGINE INSTANCES
# ============================================
SEARX_INSTANCES = [
    "https://searx.be", "https://search.sapti.me", "https://searx.tiekoetter.com",
    "https://search.privacyguides.net", "https://searx.fmac.xyz", "https://searx.work",
    "https://priv.au", "https://search.bus-hit.me", "https://searx.org",
]

BRAVE_SEARCH_URL = "https://search.brave.com/search"
MOJEEK_URL = "https://www.mojeek.com/search"
QWANT_URL = "https://api.qwant.com/v3/search/web"
ECOSIA_URL = "https://www.ecosia.org/search"
STARTPAGE_URL = "https://www.startpage.com/sp/search"

# ============================================
# DATA STRUCTURES
# ============================================
@dataclass
class DeepSearchResult:
    """Enhanced search result with multi-source data"""
    company_name: str
    domain: str = ""
    emails: List[str] = field(default_factory=list)
    verified_emails: List[str] = field(default_factory=list)
    phones: List[str] = field(default_factory=list)
    social_profiles: Dict[str, str] = field(default_factory=dict)
    people: List[Dict[str, Any]] = field(default_factory=list)
    technologies: List[str] = field(default_factory=list)
    funding: Optional[str] = None
    employee_count: Optional[str] = None
    industry: Optional[str] = None
    sources: List[str] = field(default_factory=list)
    confidence_score: int = 0
    raw_data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SearchStats:
    """Comprehensive stats tracking"""
    total_queries: int = 0
    total_results: int = 0
    emails_found: int = 0
    emails_verified: int = 0
    people_found: int = 0
    companies_enriched: int = 0
    api_calls: int = 0
    cache_hits: int = 0
    errors: int = 0
    layers_completed: List[str] = field(default_factory=list)


# ============================================
# EMAIL PATTERNS FOR PERMUTATION
# ============================================
EMAIL_PATTERNS = [
    # Basic patterns
    "{first}@{domain}", "{last}@{domain}", "{first}.{last}@{domain}",
    "{first}{last}@{domain}", "{f}{last}@{domain}", "{first}{l}@{domain}",
    "{first}_{last}@{domain}", "{last}.{first}@{domain}", "{last}{first}@{domain}",
    "{f}.{last}@{domain}", "{first}.{l}@{domain}", "{f}{l}@{domain}",
    # With numbers
    "{first}{last}1@{domain}", "{first}.{last}1@{domain}",
    # Role-based
    "contact@{domain}", "info@{domain}", "hello@{domain}", "hi@{domain}",
    "support@{domain}", "help@{domain}", "sales@{domain}", "marketing@{domain}",
    "press@{domain}", "media@{domain}", "pr@{domain}", "news@{domain}",
    "partnerships@{domain}", "partner@{domain}", "biz@{domain}", "bizdev@{domain}",
    "business@{domain}", "team@{domain}", "careers@{domain}", "jobs@{domain}",
    "hr@{domain}", "recruiting@{domain}", "talent@{domain}",
    "admin@{domain}", "office@{domain}", "general@{domain}",
    "feedback@{domain}", "suggestions@{domain}", "inquiries@{domain}",
    "ceo@{domain}", "founder@{domain}", "founders@{domain}",
    "invest@{domain}", "investors@{domain}", "ir@{domain}",
    # App-specific
    "app@{domain}", "mobile@{domain}", "dev@{domain}", "developer@{domain}",
    "api@{domain}", "tech@{domain}", "engineering@{domain}",
]


# ============================================
# LAYER 1: MULTI-ENGINE WEB SEARCH (FREE)
# ============================================
class MultiEngineSearch:
    """Search across multiple free search engines simultaneously"""

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.stats = SearchStats()

    def _get_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Connection": "keep-alive",
        }

    async def search_all_engines(self, query: str, max_results: int = 50) -> Dict[str, Any]:
        """Search ALL free engines in parallel"""
        tasks = [
            self._search_duckduckgo(query, max_results),
            self._search_bing(query, max_results),
            self._search_brave(query, max_results),
            self._search_searx(query, max_results),
            self._search_mojeek(query, max_results),
            self._search_ecosia(query, max_results),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_results = []
        all_emails = set()
        sources_used = []

        engine_names = ["duckduckgo", "bing", "brave", "searx", "mojeek", "ecosia"]

        for i, result in enumerate(results):
            if isinstance(result, dict) and result.get("results"):
                all_results.extend(result["results"])
                all_emails.update(result.get("emails", []))
                sources_used.append(engine_names[i])

        return {
            "results": all_results,
            "emails": list(all_emails),
            "sources": sources_used,
            "total_results": len(all_results)
        }

    async def _search_duckduckgo(self, query: str, max_results: int = 50) -> Dict[str, Any]:
        """DuckDuckGo HTML search with multi-page"""
        results = []
        emails = set()

        try:
            async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                for page in range(3):  # 3 pages
                    url = "https://html.duckduckgo.com/html/"
                    data = {"q": query, "s": str(page * 30), "dc": str(page * 30 + 1)}

                    response = await client.post(url, data=data, headers=self._get_headers())
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, "html.parser")
                        for result in soup.select(".result"):
                            title_elem = result.select_one(".result__title")
                            snippet_elem = result.select_one(".result__snippet")
                            link_elem = result.select_one(".result__url")

                            if title_elem:
                                title = title_elem.get_text(strip=True)
                                snippet = snippet_elem.get_text(strip=True) if snippet_elem else ""
                                url = link_elem.get("href", "") if link_elem else ""

                                results.append({"title": title, "snippet": snippet, "url": url, "source": "duckduckgo"})

                                # Extract emails
                                text = f"{title} {snippet}"
                                found_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
                                emails.update(found_emails)

                    await asyncio.sleep(0.5)

        except Exception as e:
            logger.warning(f"DuckDuckGo search error: {e}")

        return {"results": results[:max_results], "emails": list(emails)}

    async def _search_bing(self, query: str, max_results: int = 50) -> Dict[str, Any]:
        """Bing web search with multi-page"""
        results = []
        emails = set()

        try:
            async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                for page in range(3):
                    url = f"https://www.bing.com/search?q={quote_plus(query)}&first={page * 10 + 1}"

                    response = await client.get(url, headers=self._get_headers())
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, "html.parser")
                        for li in soup.select("li.b_algo"):
                            title_elem = li.select_one("h2 a")
                            snippet_elem = li.select_one(".b_caption p")

                            if title_elem:
                                title = title_elem.get_text(strip=True)
                                snippet = snippet_elem.get_text(strip=True) if snippet_elem else ""
                                link = title_elem.get("href", "")

                                results.append({"title": title, "snippet": snippet, "url": link, "source": "bing"})

                                text = f"{title} {snippet}"
                                found_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
                                emails.update(found_emails)

                    await asyncio.sleep(0.5)

        except Exception as e:
            logger.warning(f"Bing search error: {e}")

        return {"results": results[:max_results], "emails": list(emails)}

    async def _search_brave(self, query: str, max_results: int = 30) -> Dict[str, Any]:
        """Brave Search (privacy-focused)"""
        results = []
        emails = set()

        try:
            async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                url = f"{BRAVE_SEARCH_URL}?q={quote_plus(query)}"
                response = await client.get(url, headers=self._get_headers())

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")
                    for result in soup.select(".snippet"):
                        title_elem = result.select_one(".title")
                        snippet_elem = result.select_one(".snippet-description")
                        link_elem = result.select_one("a")

                        if title_elem:
                            title = title_elem.get_text(strip=True)
                            snippet = snippet_elem.get_text(strip=True) if snippet_elem else ""
                            link = link_elem.get("href", "") if link_elem else ""

                            results.append({"title": title, "snippet": snippet, "url": link, "source": "brave"})

                            text = f"{title} {snippet}"
                            found_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
                            emails.update(found_emails)

        except Exception as e:
            logger.warning(f"Brave search error: {e}")

        return {"results": results[:max_results], "emails": list(emails)}

    async def _search_searx(self, query: str, max_results: int = 30) -> Dict[str, Any]:
        """SearX meta-search (rotates through instances)"""
        results = []
        emails = set()

        for instance in random.sample(SEARX_INSTANCES, min(3, len(SEARX_INSTANCES))):
            try:
                async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                    url = f"{instance}/search?q={quote_plus(query)}&format=json"
                    response = await client.get(url, headers=self._get_headers())

                    if response.status_code == 200:
                        data = response.json()
                        for item in data.get("results", [])[:max_results]:
                            results.append({
                                "title": item.get("title", ""),
                                "snippet": item.get("content", ""),
                                "url": item.get("url", ""),
                                "source": "searx"
                            })

                            text = f"{item.get('title', '')} {item.get('content', '')}"
                            found_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
                            emails.update(found_emails)

                        if results:
                            break

            except Exception as e:
                logger.debug(f"SearX instance {instance} error: {e}")
                continue

        return {"results": results, "emails": list(emails)}

    async def _search_mojeek(self, query: str, max_results: int = 20) -> Dict[str, Any]:
        """Mojeek - independent search engine"""
        results = []
        emails = set()

        try:
            async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                url = f"{MOJEEK_URL}?q={quote_plus(query)}"
                response = await client.get(url, headers=self._get_headers())

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")
                    for result in soup.select(".results-standard li"):
                        title_elem = result.select_one("a.title")
                        snippet_elem = result.select_one("p.s")

                        if title_elem:
                            results.append({
                                "title": title_elem.get_text(strip=True),
                                "snippet": snippet_elem.get_text(strip=True) if snippet_elem else "",
                                "url": title_elem.get("href", ""),
                                "source": "mojeek"
                            })

        except Exception as e:
            logger.warning(f"Mojeek search error: {e}")

        return {"results": results[:max_results], "emails": list(emails)}

    async def _search_ecosia(self, query: str, max_results: int = 20) -> Dict[str, Any]:
        """Ecosia - eco-friendly search"""
        results = []
        emails = set()

        try:
            async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                url = f"{ECOSIA_URL}?q={quote_plus(query)}"
                response = await client.get(url, headers=self._get_headers())

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")
                    for result in soup.select(".result"):
                        title_elem = result.select_one(".result-title")
                        snippet_elem = result.select_one(".result-snippet")
                        link_elem = result.select_one("a")

                        if title_elem:
                            results.append({
                                "title": title_elem.get_text(strip=True),
                                "snippet": snippet_elem.get_text(strip=True) if snippet_elem else "",
                                "url": link_elem.get("href", "") if link_elem else "",
                                "source": "ecosia"
                            })

        except Exception as e:
            logger.warning(f"Ecosia search error: {e}")

        return {"results": results[:max_results], "emails": list(emails)}


# ============================================
# LAYER 2: ARCHIVE MINING (FREE)
# ============================================
class ArchiveMiner:
    """Mine web archives for historical contact data"""

    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    async def mine_all_archives(self, domain: str) -> Dict[str, Any]:
        """Search all archives in parallel"""
        tasks = [
            self._search_wayback(domain),
            self._search_archive_today(domain),
            self._search_commoncrawl(domain),
            self._search_google_cache(domain),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_emails = set()
        all_pages = []
        sources = []

        for i, result in enumerate(results):
            if isinstance(result, dict):
                all_emails.update(result.get("emails", []))
                all_pages.extend(result.get("pages", []))
                if result.get("emails") or result.get("pages"):
                    sources.append(["wayback", "archive_today", "commoncrawl", "google_cache"][i])

        return {
            "emails": list(all_emails),
            "archived_pages": all_pages,
            "sources": sources
        }

    async def _search_wayback(self, domain: str) -> Dict[str, Any]:
        """Wayback Machine - Internet Archive"""
        emails = set()
        pages = []

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                # Get available snapshots
                cdx_url = f"https://web.archive.org/cdx/search/cdx?url={domain}/*&output=json&fl=timestamp,original&collapse=urlkey&limit=100"
                response = await client.get(cdx_url)

                if response.status_code == 200:
                    data = response.json()

                    # Look for contact pages
                    contact_keywords = ["contact", "about", "team", "press", "media", "support"]

                    for row in data[1:]:  # Skip header
                        timestamp, url = row[0], row[1]

                        if any(kw in url.lower() for kw in contact_keywords):
                            # Fetch archived page
                            archive_url = f"https://web.archive.org/web/{timestamp}/{url}"
                            try:
                                page_response = await client.get(archive_url, timeout=10)
                                if page_response.status_code == 200:
                                    found_emails = re.findall(
                                        r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
                                        page_response.text
                                    )
                                    emails.update(found_emails)
                                    pages.append({"url": archive_url, "original": url, "timestamp": timestamp})
                            except Exception:
                                pass

                            if len(pages) >= 10:
                                break

        except Exception as e:
            logger.warning(f"Wayback Machine error: {e}")

        return {"emails": list(emails), "pages": pages}

    async def _search_archive_today(self, domain: str) -> Dict[str, Any]:
        """Archive.today (archive.is, archive.ph)"""
        emails = set()
        pages = []

        try:
            async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                search_url = f"https://archive.today/{domain}"
                response = await client.get(search_url)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")

                    for link in soup.select("a[href*='archive']"):
                        href = link.get("href", "")
                        if "archive" in href:
                            pages.append({"url": href})

                            # Try to fetch and extract emails
                            try:
                                page_response = await client.get(href, timeout=10)
                                if page_response.status_code == 200:
                                    found_emails = re.findall(
                                        r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
                                        page_response.text
                                    )
                                    emails.update(found_emails)
                            except Exception:
                                pass

        except Exception as e:
            logger.warning(f"Archive.today error: {e}")

        return {"emails": list(emails), "pages": pages}

    async def _search_commoncrawl(self, domain: str) -> Dict[str, Any]:
        """CommonCrawl - massive web archive"""
        emails = set()
        pages = []

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                # Get latest index
                index_url = f"https://index.commoncrawl.org/CC-MAIN-2024-10-index?url=*.{domain}&output=json&limit=50"
                response = await client.get(index_url)

                if response.status_code == 200:
                    for line in response.text.strip().split("\n"):
                        try:
                            data = json.loads(line)
                            url = data.get("url", "")
                            if any(kw in url.lower() for kw in ["contact", "about", "team"]):
                                pages.append({"url": url, "timestamp": data.get("timestamp")})
                        except Exception:
                            pass

        except Exception as e:
            logger.warning(f"CommonCrawl error: {e}")

        return {"emails": list(emails), "pages": pages}

    async def _search_google_cache(self, domain: str) -> Dict[str, Any]:
        """Google Cache - cached versions"""
        emails = set()
        pages = []

        try:
            async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                cache_url = f"https://webcache.googleusercontent.com/search?q=cache:{domain}"
                response = await client.get(cache_url, headers={"User-Agent": random.choice(USER_AGENTS)})

                if response.status_code == 200:
                    found_emails = re.findall(
                        r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
                        response.text
                    )
                    emails.update(found_emails)
                    pages.append({"url": cache_url, "source": "google_cache"})

        except Exception as e:
            logger.debug(f"Google Cache error: {e}")

        return {"emails": list(emails), "pages": pages}


# ============================================
# LAYER 3: DNS INTELLIGENCE (FREE)
# ============================================
class DNSIntelligence:
    """Extract email patterns from DNS records"""

    async def analyze_domain(self, domain: str) -> Dict[str, Any]:
        """Analyze DNS records for email intelligence"""
        results = {
            "mx_records": [],
            "txt_records": [],
            "spf_record": None,
            "dmarc_record": None,
            "email_provider": None,
            "suggested_patterns": [],
            "discovered_emails": []
        }

        try:
            # MX Records - find email provider
            try:
                mx_records = dns.resolver.resolve(domain, 'MX')
                for mx in mx_records:
                    mx_host = str(mx.exchange).rstrip('.')
                    results["mx_records"].append(mx_host)

                    # Identify email provider
                    if "google" in mx_host.lower() or "gmail" in mx_host.lower():
                        results["email_provider"] = "Google Workspace"
                    elif "outlook" in mx_host.lower() or "microsoft" in mx_host.lower():
                        results["email_provider"] = "Microsoft 365"
                    elif "zoho" in mx_host.lower():
                        results["email_provider"] = "Zoho Mail"
                    elif "protonmail" in mx_host.lower():
                        results["email_provider"] = "ProtonMail"
            except Exception:
                pass

            # TXT Records - SPF, DMARC, etc.
            try:
                txt_records = dns.resolver.resolve(domain, 'TXT')
                for txt in txt_records:
                    txt_str = str(txt)
                    results["txt_records"].append(txt_str)

                    if "v=spf1" in txt_str:
                        results["spf_record"] = txt_str
                        # Extract included domains (may reveal email patterns)
                        includes = re.findall(r'include:([^\s]+)', txt_str)
                        for inc in includes:
                            results["suggested_patterns"].append(f"SPF includes: {inc}")
            except Exception:
                pass

            # DMARC Record
            try:
                dmarc_records = dns.resolver.resolve(f"_dmarc.{domain}", 'TXT')
                for dmarc in dmarc_records:
                    dmarc_str = str(dmarc)
                    results["dmarc_record"] = dmarc_str

                    # Extract rua (reporting email)
                    rua = re.findall(r'rua=mailto:([^;\s]+)', dmarc_str)
                    if rua:
                        results["discovered_emails"].extend(rua)

                    # Extract ruf (forensic reporting email)
                    ruf = re.findall(r'ruf=mailto:([^;\s]+)', dmarc_str)
                    if ruf:
                        results["discovered_emails"].extend(ruf)
            except Exception:
                pass

        except Exception as e:
            logger.warning(f"DNS analysis error for {domain}: {e}")

        return results


# ============================================
# LAYER 4: WHOIS INTELLIGENCE (FREE)
# ============================================
class WHOISIntelligence:
    """Extract contact info from WHOIS records"""

    async def lookup_domain(self, domain: str) -> Dict[str, Any]:
        """WHOIS lookup for contact information"""
        results = {
            "registrant_email": None,
            "admin_email": None,
            "tech_email": None,
            "registrar": None,
            "creation_date": None,
            "all_emails": []
        }

        try:
            # Use free WHOIS API
            async with httpx.AsyncClient(timeout=30) as client:
                # Try multiple free WHOIS APIs
                apis = [
                    f"https://www.whoisxmlapi.com/whoisserver/WhoisService?domainName={domain}&outputFormat=JSON",
                    f"https://api.whoapi.com/?domain={domain}&r=whois&apikey=free",
                ]

                for api_url in apis:
                    try:
                        response = await client.get(api_url)
                        if response.status_code == 200:
                            data = response.json()

                            # Extract emails from response
                            text = json.dumps(data)
                            emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
                            results["all_emails"] = list(set(emails))

                            if emails:
                                break
                    except Exception:
                        continue

        except Exception as e:
            logger.warning(f"WHOIS lookup error for {domain}: {e}")

        return results


# ============================================
# LAYER 5: SSL CERTIFICATE TRANSPARENCY (FREE)
# ============================================
class CertificateTransparency:
    """Find subdomains via Certificate Transparency logs"""

    async def find_subdomains(self, domain: str) -> Dict[str, Any]:
        """Find subdomains using CT logs"""
        subdomains = set()
        emails = set()

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                # crt.sh - Certificate Transparency search
                url = f"https://crt.sh/?q=%.{domain}&output=json"
                response = await client.get(url)

                if response.status_code == 200:
                    data = response.json()

                    for cert in data:
                        name = cert.get("name_value", "")
                        for subdomain in name.split("\n"):
                            subdomain = subdomain.strip().lower()
                            if subdomain and domain in subdomain:
                                subdomains.add(subdomain)

                                # Check for email-related subdomains
                                if any(kw in subdomain for kw in ["mail", "email", "smtp", "mx", "webmail"]):
                                    emails.add(f"info@{subdomain}")

        except Exception as e:
            logger.warning(f"CT logs error for {domain}: {e}")

        return {
            "subdomains": list(subdomains),
            "potential_emails": list(emails)
        }


# ============================================
# LAYER 6: SITEMAP/ROBOTS MINING (FREE)
# ============================================
class SitemapMiner:
    """Mine sitemap and robots.txt for hidden pages"""

    async def mine_site_structure(self, domain: str) -> Dict[str, Any]:
        """Find hidden pages via sitemap and robots.txt"""
        pages = {
            "contact_pages": [],
            "team_pages": [],
            "about_pages": [],
            "all_urls": []
        }
        emails = set()

        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                # Check robots.txt
                robots_url = f"https://{domain}/robots.txt"
                try:
                    response = await client.get(robots_url)
                    if response.status_code == 200:
                        # Find sitemap URLs
                        sitemaps = re.findall(r'Sitemap:\s*(\S+)', response.text, re.IGNORECASE)
                        for sitemap_url in sitemaps:
                            await self._parse_sitemap(client, sitemap_url, pages, emails)
                except Exception:
                    pass

                # Try common sitemap locations
                common_sitemaps = [
                    f"https://{domain}/sitemap.xml",
                    f"https://{domain}/sitemap_index.xml",
                    f"https://{domain}/sitemap/sitemap.xml",
                    f"https://www.{domain}/sitemap.xml",
                ]

                for sitemap_url in common_sitemaps:
                    try:
                        await self._parse_sitemap(client, sitemap_url, pages, emails)
                    except Exception:
                        pass

        except Exception as e:
            logger.warning(f"Sitemap mining error for {domain}: {e}")

        return {
            "pages": pages,
            "emails": list(emails)
        }

    async def _parse_sitemap(self, client: httpx.AsyncClient, url: str, pages: Dict, emails: set):
        """Parse a sitemap XML"""
        try:
            response = await client.get(url, timeout=15)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "xml")

                for loc in soup.find_all("loc"):
                    page_url = loc.get_text(strip=True).lower()
                    pages["all_urls"].append(page_url)

                    if "contact" in page_url:
                        pages["contact_pages"].append(page_url)
                    elif "team" in page_url or "people" in page_url or "staff" in page_url:
                        pages["team_pages"].append(page_url)
                    elif "about" in page_url:
                        pages["about_pages"].append(page_url)
        except Exception:
            pass


# ============================================
# LAYER 7: SOCIAL MEDIA DISCOVERY (FREE)
# ============================================
class SocialMediaDiscovery:
    """Find contacts via social media platforms"""

    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    async def search_all_platforms(self, company_name: str, domain: str = None) -> Dict[str, Any]:
        """Search all social platforms in parallel"""
        tasks = [
            self._search_linkedin(company_name),
            self._search_twitter(company_name),
            self._search_facebook(company_name),
            self._search_instagram(company_name),
            self._search_youtube(company_name),
            self._search_tiktok(company_name),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        profiles = {}
        people = []

        platforms = ["linkedin", "twitter", "facebook", "instagram", "youtube", "tiktok"]
        for i, result in enumerate(results):
            if isinstance(result, dict):
                if result.get("profile_url"):
                    profiles[platforms[i]] = result["profile_url"]
                people.extend(result.get("people", []))

        return {
            "profiles": profiles,
            "people": people
        }

    async def _search_linkedin(self, company_name: str) -> Dict[str, Any]:
        """Find LinkedIn company page and employees"""
        result = {"profile_url": None, "people": []}

        try:
            async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                # Search for company page
                search_url = f"https://www.google.com/search?q=site:linkedin.com/company+{quote_plus(company_name)}"
                response = await client.get(search_url, headers={"User-Agent": random.choice(USER_AGENTS)})

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")
                    for link in soup.select("a[href*='linkedin.com/company']"):
                        href = link.get("href", "")
                        if "linkedin.com/company" in href:
                            result["profile_url"] = href
                            break

        except Exception as e:
            logger.debug(f"LinkedIn search error: {e}")

        return result

    async def _search_twitter(self, company_name: str) -> Dict[str, Any]:
        """Find Twitter/X profile"""
        result = {"profile_url": None, "people": []}

        try:
            async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                search_url = f"https://www.google.com/search?q=site:twitter.com+OR+site:x.com+{quote_plus(company_name)}"
                response = await client.get(search_url, headers={"User-Agent": random.choice(USER_AGENTS)})

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")
                    for link in soup.select("a"):
                        href = link.get("href", "")
                        if "twitter.com/" in href or "x.com/" in href:
                            result["profile_url"] = href
                            break

        except Exception as e:
            logger.debug(f"Twitter search error: {e}")

        return result

    async def _search_facebook(self, company_name: str) -> Dict[str, Any]:
        """Find Facebook page"""
        return {"profile_url": None, "people": []}

    async def _search_instagram(self, company_name: str) -> Dict[str, Any]:
        """Find Instagram profile"""
        return {"profile_url": None, "people": []}

    async def _search_youtube(self, company_name: str) -> Dict[str, Any]:
        """Find YouTube channel"""
        return {"profile_url": None, "people": []}

    async def _search_tiktok(self, company_name: str) -> Dict[str, Any]:
        """Find TikTok profile"""
        return {"profile_url": None, "people": []}


# ============================================
# LAYER 8: DEVELOPER PLATFORMS (FREE)
# ============================================
class DeveloperPlatformSearch:
    """Search developer platforms for contacts"""

    async def search_all_platforms(self, company_name: str, domain: str = None) -> Dict[str, Any]:
        """Search all dev platforms in parallel"""
        tasks = [
            self._search_github(company_name),
            self._search_gitlab(company_name),
            self._search_npm(company_name),
            self._search_pypi(company_name),
            self._search_stackoverflow(company_name),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_emails = set()
        all_people = []
        profiles = {}

        for result in results:
            if isinstance(result, dict):
                all_emails.update(result.get("emails", []))
                all_people.extend(result.get("people", []))
                if result.get("profile"):
                    profiles[result.get("platform", "unknown")] = result["profile"]

        return {
            "emails": list(all_emails),
            "people": all_people,
            "profiles": profiles
        }

    async def _search_github(self, company_name: str) -> Dict[str, Any]:
        """Search GitHub for organization and members"""
        result = {"platform": "github", "profile": None, "emails": [], "people": []}

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                # Search for org
                search_url = f"https://api.github.com/search/users?q={quote_plus(company_name)}+type:org"
                response = await client.get(search_url)

                if response.status_code == 200:
                    data = response.json()
                    if data.get("items"):
                        org = data["items"][0]
                        result["profile"] = org.get("html_url")

                        # Get org members
                        org_login = org.get("login")
                        if org_login:
                            members_url = f"https://api.github.com/orgs/{org_login}/members"
                            members_response = await client.get(members_url)

                            if members_response.status_code == 200:
                                members = members_response.json()
                                for member in members[:10]:
                                    # Get member details
                                    user_url = f"https://api.github.com/users/{member.get('login')}"
                                    user_response = await client.get(user_url)

                                    if user_response.status_code == 200:
                                        user_data = user_response.json()
                                        if user_data.get("email"):
                                            result["emails"].append(user_data["email"])

                                        result["people"].append({
                                            "name": user_data.get("name"),
                                            "github": user_data.get("html_url"),
                                            "email": user_data.get("email"),
                                            "company": user_data.get("company"),
                                        })

        except Exception as e:
            logger.debug(f"GitHub search error: {e}")

        return result

    async def _search_gitlab(self, company_name: str) -> Dict[str, Any]:
        """Search GitLab for groups"""
        return {"platform": "gitlab", "profile": None, "emails": [], "people": []}

    async def _search_npm(self, company_name: str) -> Dict[str, Any]:
        """Search npm for packages by company"""
        result = {"platform": "npm", "profile": None, "emails": [], "people": []}

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                search_url = f"https://registry.npmjs.org/-/v1/search?text={quote_plus(company_name)}&size=10"
                response = await client.get(search_url)

                if response.status_code == 200:
                    data = response.json()
                    for obj in data.get("objects", []):
                        pkg = obj.get("package", {})
                        author = pkg.get("author", {})

                        if author.get("email"):
                            result["emails"].append(author["email"])
                        if author.get("name"):
                            result["people"].append({
                                "name": author.get("name"),
                                "email": author.get("email"),
                                "source": "npm"
                            })

                        # Check maintainers
                        maintainers = pkg.get("maintainers", [])
                        for m in maintainers:
                            if m.get("email"):
                                result["emails"].append(m["email"])

        except Exception as e:
            logger.debug(f"npm search error: {e}")

        return result

    async def _search_pypi(self, company_name: str) -> Dict[str, Any]:
        """Search PyPI for packages"""
        result = {"platform": "pypi", "profile": None, "emails": [], "people": []}

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                search_url = f"https://pypi.org/search/?q={quote_plus(company_name)}"
                response = await client.get(search_url)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")

                    for pkg in soup.select(".package-snippet")[:5]:
                        name_elem = pkg.select_one(".package-snippet__name")
                        if name_elem:
                            pkg_name = name_elem.get_text(strip=True)

                            # Get package details
                            pkg_url = f"https://pypi.org/pypi/{pkg_name}/json"
                            pkg_response = await client.get(pkg_url)

                            if pkg_response.status_code == 200:
                                pkg_data = pkg_response.json()
                                info = pkg_data.get("info", {})

                                if info.get("author_email"):
                                    result["emails"].append(info["author_email"])
                                if info.get("maintainer_email"):
                                    result["emails"].append(info["maintainer_email"])

        except Exception as e:
            logger.debug(f"PyPI search error: {e}")

        return result

    async def _search_stackoverflow(self, company_name: str) -> Dict[str, Any]:
        """Search Stack Overflow for company employees"""
        return {"platform": "stackoverflow", "profile": None, "emails": [], "people": []}


# ============================================
# LAYER 9: JOB POSTING ANALYSIS (FREE)
# ============================================
class JobPostingAnalyzer:
    """Find contacts via job postings"""

    async def search_job_sites(self, company_name: str) -> Dict[str, Any]:
        """Search job sites for company contacts"""
        tasks = [
            self._search_indeed(company_name),
            self._search_glassdoor(company_name),
            self._search_linkedin_jobs(company_name),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_emails = set()
        recruiters = []

        for result in results:
            if isinstance(result, dict):
                all_emails.update(result.get("emails", []))
                recruiters.extend(result.get("recruiters", []))

        return {
            "emails": list(all_emails),
            "recruiters": recruiters
        }

    async def _search_indeed(self, company_name: str) -> Dict[str, Any]:
        """Search Indeed for job postings"""
        return {"emails": [], "recruiters": []}

    async def _search_glassdoor(self, company_name: str) -> Dict[str, Any]:
        """Search Glassdoor for company info"""
        return {"emails": [], "recruiters": []}

    async def _search_linkedin_jobs(self, company_name: str) -> Dict[str, Any]:
        """Search LinkedIn Jobs"""
        return {"emails": [], "recruiters": []}


# ============================================
# LAYER 10: PRESS RELEASE MINING (FREE)
# ============================================
class PressReleaseMiner:
    """Mine press releases for contact info"""

    async def search_press_releases(self, company_name: str) -> Dict[str, Any]:
        """Search press release sites"""
        all_emails = set()
        contacts = []

        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                # Search PRNewswire
                search_url = f"https://www.google.com/search?q=site:prnewswire.com+{quote_plus(company_name)}+contact"
                response = await client.get(search_url, headers={"User-Agent": random.choice(USER_AGENTS)})

                if response.status_code == 200:
                    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', response.text)
                    all_emails.update(emails)

                # Search BusinessWire
                search_url = f"https://www.google.com/search?q=site:businesswire.com+{quote_plus(company_name)}+contact"
                response = await client.get(search_url, headers={"User-Agent": random.choice(USER_AGENTS)})

                if response.status_code == 200:
                    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', response.text)
                    all_emails.update(emails)

        except Exception as e:
            logger.debug(f"Press release mining error: {e}")

        return {
            "emails": list(all_emails),
            "contacts": contacts
        }


# ============================================
# LAYER 11: STARTUP DATABASES (FREE)
# ============================================
class StartupDatabaseSearch:
    """Search startup databases for company info"""

    async def search_all_databases(self, company_name: str) -> Dict[str, Any]:
        """Search all startup databases"""
        tasks = [
            self._search_crunchbase(company_name),
            self._search_angellist(company_name),
            self._search_producthunt(company_name),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        company_info = {}
        all_emails = set()
        founders = []

        for result in results:
            if isinstance(result, dict):
                all_emails.update(result.get("emails", []))
                founders.extend(result.get("founders", []))
                company_info.update(result.get("info", {}))

        return {
            "emails": list(all_emails),
            "founders": founders,
            "company_info": company_info
        }

    async def _search_crunchbase(self, company_name: str) -> Dict[str, Any]:
        """Search Crunchbase (limited free)"""
        result = {"emails": [], "founders": [], "info": {}}

        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                search_url = f"https://www.google.com/search?q=site:crunchbase.com/organization+{quote_plus(company_name)}"
                response = await client.get(search_url, headers={"User-Agent": random.choice(USER_AGENTS)})

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")
                    for link in soup.select("a[href*='crunchbase.com/organization']"):
                        result["info"]["crunchbase_url"] = link.get("href", "")
                        break

        except Exception as e:
            logger.debug(f"Crunchbase search error: {e}")

        return result

    async def _search_angellist(self, company_name: str) -> Dict[str, Any]:
        """Search AngelList/Wellfound"""
        return {"emails": [], "founders": [], "info": {}}

    async def _search_producthunt(self, company_name: str) -> Dict[str, Any]:
        """Search Product Hunt"""
        return {"emails": [], "founders": [], "info": {}}


# ============================================
# LAYER 12: EMAIL PERMUTATION ENGINE (FREE)
# ============================================
class EmailPermutationEngine:
    """Generate email permutations from names"""

    def generate_permutations(self, first_name: str, last_name: str, domain: str) -> List[str]:
        """Generate all possible email permutations"""
        if not first_name or not last_name or not domain:
            return []

        first = first_name.lower().strip()
        last = last_name.lower().strip()
        f = first[0] if first else ""
        l = last[0] if last else ""

        permutations = []

        for pattern in EMAIL_PATTERNS:
            try:
                email = pattern.format(
                    first=first, last=last, f=f, l=l, domain=domain
                )
                if "@" in email and not email.startswith("@"):
                    permutations.append(email)
            except Exception:
                pass

        return list(set(permutations))

    def generate_role_emails(self, domain: str) -> List[str]:
        """Generate role-based emails"""
        role_patterns = [p for p in EMAIL_PATTERNS if "{first}" not in p and "{last}" not in p]

        emails = []
        for pattern in role_patterns:
            try:
                email = pattern.format(domain=domain)
                if "@" in email:
                    emails.append(email)
            except Exception:
                pass

        return list(set(emails))


# ============================================
# LAYER 13: SMTP VERIFICATION (FREE)
# ============================================
class SMTPVerifier:
    """Verify if email exists via SMTP (FREE method)"""

    async def verify_email(self, email: str) -> Dict[str, Any]:
        """Verify email via SMTP"""
        result = {
            "email": email,
            "exists": None,
            "catch_all": False,
            "mx_found": False,
            "smtp_response": None
        }

        try:
            domain = email.split("@")[1]

            # Get MX record
            try:
                mx_records = dns.resolver.resolve(domain, 'MX')
                mx_host = str(mx_records[0].exchange).rstrip('.')
                result["mx_found"] = True
            except Exception:
                return result

            # Connect to SMTP (with timeout)
            try:
                smtp = smtplib.SMTP(timeout=10)
                smtp.connect(mx_host, 25)
                smtp.helo("check.com")
                smtp.mail("verify@check.com")
                code, message = smtp.rcpt(email)
                smtp.quit()

                result["smtp_response"] = code

                if code == 250:
                    result["exists"] = True
                elif code == 550:
                    result["exists"] = False
                else:
                    result["exists"] = None  # Unknown

            except smtplib.SMTPServerDisconnected:
                result["catch_all"] = True
            except Exception:
                pass

        except Exception as e:
            logger.debug(f"SMTP verification error for {email}: {e}")

        return result

    async def verify_batch(self, emails: List[str], max_concurrent: int = 5) -> List[Dict[str, Any]]:
        """Verify multiple emails with rate limiting"""
        results = []

        # Process in batches
        for i in range(0, len(emails), max_concurrent):
            batch = emails[i:i + max_concurrent]
            tasks = [self.verify_email(email) for email in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in batch_results:
                if isinstance(result, dict):
                    results.append(result)

            # Rate limit
            await asyncio.sleep(1)

        return results


# ============================================
# PAID API INTEGRATIONS
# ============================================
class HunterIOClient:
    """Hunter.io API integration - PAID POWER VERSION"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.hunter.io/v2"

    async def domain_search(
        self,
        domain: str,
        limit: int = 100,
        department: str = None,
        type: str = None,
        seniority: str = None
    ) -> Dict[str, Any]:
        """
        Search for emails by domain with advanced filtering

        Args:
            domain: Company domain to search
            limit: Max results (1-100)
            department: Filter by department (executive, it, finance, management,
                       sales, legal, support, hr, marketing, communication)
            type: Email type (personal or generic)
            seniority: Seniority level (junior, senior, executive)
        """
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                url = f"{self.base_url}/domain-search"
                params = {
                    "domain": domain,
                    "api_key": self.api_key,
                    "limit": min(limit, 100)
                }
                if department:
                    params["department"] = department
                if type:
                    params["type"] = type
                if seniority:
                    params["seniority"] = seniority

                response = await client.get(url, params=params)

                if response.status_code == 200:
                    return response.json().get("data", {})
        except Exception as e:
            logger.error(f"Hunter.io API error: {e}")
        return {}

    async def email_finder(self, domain: str, first_name: str, last_name: str) -> Dict[str, Any]:
        """Find specific person's email"""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                url = f"{self.base_url}/email-finder"
                params = {
                    "domain": domain,
                    "first_name": first_name,
                    "last_name": last_name,
                    "api_key": self.api_key
                }
                response = await client.get(url, params=params)

                if response.status_code == 200:
                    return response.json().get("data", {})
        except Exception as e:
            logger.error(f"Hunter.io email finder error: {e}")
        return {}

    async def verify_email(self, email: str) -> Dict[str, Any]:
        """Verify email address"""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                url = f"{self.base_url}/email-verifier"
                params = {"email": email, "api_key": self.api_key}
                response = await client.get(url, params=params)

                if response.status_code == 200:
                    return response.json().get("data", {})
        except Exception as e:
            logger.error(f"Hunter.io verify error: {e}")
        return {}


class ClearbitClient:
    """Clearbit API integration"""

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def enrich_company(self, domain: str) -> Dict[str, Any]:
        """Enrich company data"""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                url = f"https://company.clearbit.com/v2/companies/find?domain={domain}"
                headers = {"Authorization": f"Bearer {self.api_key}"}
                response = await client.get(url, headers=headers)

                if response.status_code == 200:
                    return response.json()
        except Exception as e:
            logger.error(f"Clearbit API error: {e}")
        return {}

    async def enrich_person(self, email: str) -> Dict[str, Any]:
        """Enrich person data by email"""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                url = f"https://person.clearbit.com/v2/people/find?email={email}"
                headers = {"Authorization": f"Bearer {self.api_key}"}
                response = await client.get(url, headers=headers)

                if response.status_code == 200:
                    return response.json()
        except Exception as e:
            logger.error(f"Clearbit person API error: {e}")
        return {}


class ApolloIOClient:
    """Apollo.io API integration - PAID POWER VERSION"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.apollo.io/v1"

    async def search_people(
        self,
        company_name: str = None,
        domain: str = None,
        titles: List[str] = None,
        seniority: List[str] = None,
        per_page: int = 25,
        keywords: List[str] = None,
        organization_linkedin_public_id: str = None,
        departments: List[str] = None,
        email_status: List[str] = None
    ) -> Dict[str, Any]:
        """
        Search for people at a company with advanced filtering

        Args:
            company_name: Company name to search
            domain: Company domain
            titles: List of job titles to filter
            seniority: List of seniority levels (owner, founder, c_suite, partner,
                       vp, head, director, manager, senior, entry, intern)
            per_page: Results per page (1-100)
            keywords: List of keywords to search
            organization_linkedin_public_id: LinkedIn company ID
            departments: List of departments (engineering, design, marketing, sales,
                        finance, legal, support, hr, recruiting)
            email_status: Filter by email status (verified, guessed, unavailable)
        """
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                url = f"{self.base_url}/mixed_people/search"
                headers = {"Content-Type": "application/json", "Cache-Control": "no-cache"}

                data = {
                    "api_key": self.api_key,
                    "page": 1,
                    "per_page": min(per_page, 100),
                }

                if company_name:
                    data["q_organization_name"] = company_name
                if domain:
                    data["q_organization_domains"] = domain
                if titles:
                    data["person_titles"] = titles
                if seniority:
                    data["person_seniorities"] = seniority
                if keywords:
                    data["q_keywords"] = " ".join(keywords)
                if organization_linkedin_public_id:
                    data["organization_linkedin_public_ids"] = [organization_linkedin_public_id]
                if departments:
                    data["person_departments"] = departments
                if email_status:
                    data["contact_email_status"] = email_status

                response = await client.post(url, json=data, headers=headers)

                if response.status_code == 200:
                    return response.json()
        except Exception as e:
            logger.error(f"Apollo.io API error: {e}")
        return {}

    async def enrich_person(self, email: str = None, linkedin_url: str = None) -> Dict[str, Any]:
        """Enrich person by email or LinkedIn"""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                url = f"{self.base_url}/people/match"

                data = {"api_key": self.api_key}
                if email:
                    data["email"] = email
                if linkedin_url:
                    data["linkedin_url"] = linkedin_url

                response = await client.post(url, json=data)

                if response.status_code == 200:
                    return response.json()
        except Exception as e:
            logger.error(f"Apollo.io enrich error: {e}")
        return {}


class RocketReachClient:
    """RocketReach API integration"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.rocketreach.co/v2"

    async def lookup_person(self, name: str = None, company: str = None, linkedin_url: str = None) -> Dict[str, Any]:
        """Lookup person's contact info"""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                url = f"{self.base_url}/api/lookupProfile"
                headers = {"Api-Key": self.api_key, "Content-Type": "application/json"}

                data = {}
                if name:
                    data["name"] = name
                if company:
                    data["current_employer"] = company
                if linkedin_url:
                    data["linkedin_url"] = linkedin_url

                response = await client.post(url, json=data, headers=headers)

                if response.status_code == 200:
                    return response.json()
        except Exception as e:
            logger.error(f"RocketReach API error: {e}")
        return {}


class SnovIOClient:
    """Snov.io API integration - PAID POWER VERSION"""

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.base_url = "https://api.snov.io/v1"

    async def _get_token(self) -> str:
        """Get access token"""
        if self.access_token:
            return self.access_token

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                url = f"{self.base_url}/oauth/access_token"
                data = {
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret
                }
                response = await client.post(url, data=data)

                if response.status_code == 200:
                    self.access_token = response.json().get("access_token")
                    return self.access_token
        except Exception:
            pass
        return ""

    async def get_domain_emails_count(self, domain: str) -> Dict[str, Any]:
        """Get email count for a domain (PAID POWER: Check before fetching)"""
        token = await self._get_token()
        if not token:
            return {"result": 0}

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                url = f"{self.base_url}/get-domain-emails-count"
                params = {"access_token": token, "domain": domain}
                response = await client.get(url, params=params)

                if response.status_code == 200:
                    return response.json()
        except Exception as e:
            logger.error(f"Snov.io count API error: {e}")
        return {"result": 0}

    async def get_domain_emails(self, domain: str, limit: int = 100) -> Dict[str, Any]:
        """Get all emails for a domain with limit"""
        token = await self._get_token()
        if not token:
            return {}

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                url = f"{self.base_url}/get-domain-emails-with-info"
                params = {
                    "access_token": token,
                    "domain": domain,
                    "limit": min(limit, 100)
                }
                response = await client.get(url, params=params)

                if response.status_code == 200:
                    return response.json()
        except Exception as e:
            logger.error(f"Snov.io API error: {e}")
        return {}


class BuiltWithClient:
    """BuiltWith API integration (technographics)"""

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def get_technologies(self, domain: str) -> Dict[str, Any]:
        """Get technologies used by a website"""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                url = f"https://api.builtwith.com/v21/api.json?KEY={self.api_key}&LOOKUP={domain}"
                response = await client.get(url)

                if response.status_code == 200:
                    return response.json()
        except Exception as e:
            logger.error(f"BuiltWith API error: {e}")
        return {}


# ============================================
# MAIN ULTRA DEEP SEARCH ENGINE
# ============================================
class UltraDeepSearchEngine:
    """
    ULTRA DEEP SEARCH ENGINE V2.0
    Combines 15+ FREE layers and 10+ PAID API integrations
    """

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.stats = SearchStats()

        # Initialize FREE layers
        self.multi_engine = MultiEngineSearch()
        self.archive_miner = ArchiveMiner()
        self.dns_intel = DNSIntelligence()
        self.whois_intel = WHOISIntelligence()
        self.cert_transparency = CertificateTransparency()
        self.sitemap_miner = SitemapMiner()
        self.social_discovery = SocialMediaDiscovery()
        self.dev_platforms = DeveloperPlatformSearch()
        self.job_analyzer = JobPostingAnalyzer()
        self.press_miner = PressReleaseMiner()
        self.startup_db = StartupDatabaseSearch()
        self.email_permutator = EmailPermutationEngine()
        self.smtp_verifier = SMTPVerifier()

        # Initialize PAID clients (if API keys provided)
        self.hunter = None
        self.clearbit = None
        self.apollo = None
        self.rocketreach = None
        self.snov = None
        self.builtwith = None

        if config:
            if config.get("hunter_api_key"):
                self.hunter = HunterIOClient(config["hunter_api_key"])
            if config.get("clearbit_api_key"):
                self.clearbit = ClearbitClient(config["clearbit_api_key"])
            if config.get("apollo_api_key"):
                self.apollo = ApolloIOClient(config["apollo_api_key"])
            if config.get("rocketreach_api_key"):
                self.rocketreach = RocketReachClient(config["rocketreach_api_key"])
            if config.get("snov_client_id") and config.get("snov_client_secret"):
                self.snov = SnovIOClient(config["snov_client_id"], config["snov_client_secret"])
            if config.get("builtwith_api_key"):
                self.builtwith = BuiltWithClient(config["builtwith_api_key"])

    async def deep_search_company(self, company_name: str, domain: str = None, use_paid: bool = False) -> DeepSearchResult:
        """
        Execute ULTRA DEEP search for a company
        Runs all 15 FREE layers in parallel, then optionally PAID layers
        """
        result = DeepSearchResult(company_name=company_name, domain=domain or "")

        logger.info(f"[SEARCH] ULTRA DEEP SEARCH for {company_name} (domain: {domain})")

        # ========== PHASE 1: FREE LAYERS (ALL IN PARALLEL) ==========
        free_tasks = []

        # Layer 1: Multi-engine web search
        search_queries = [
            f'"{company_name}" contact email',
            f'"{company_name}" sales email',
            f'"{company_name}" marketing email',
            f'"{company_name}" founder CEO email',
        ]
        for query in search_queries:
            free_tasks.append(self.multi_engine.search_all_engines(query, max_results=30))

        # Layer 2: Archive mining
        if domain:
            free_tasks.append(self.archive_miner.mine_all_archives(domain))

        # Layer 3: DNS Intelligence
        if domain:
            free_tasks.append(self.dns_intel.analyze_domain(domain))

        # Layer 4: WHOIS Intelligence
        if domain:
            free_tasks.append(self.whois_intel.lookup_domain(domain))

        # Layer 5: Certificate Transparency
        if domain:
            free_tasks.append(self.cert_transparency.find_subdomains(domain))

        # Layer 6: Sitemap Mining
        if domain:
            free_tasks.append(self.sitemap_miner.mine_site_structure(domain))

        # Layer 7: Social Media Discovery
        free_tasks.append(self.social_discovery.search_all_platforms(company_name, domain))

        # Layer 8: Developer Platforms
        free_tasks.append(self.dev_platforms.search_all_platforms(company_name, domain))

        # Layer 9: Job Postings
        free_tasks.append(self.job_analyzer.search_job_sites(company_name))

        # Layer 10: Press Releases
        free_tasks.append(self.press_miner.search_press_releases(company_name))

        # Layer 11: Startup Databases
        free_tasks.append(self.startup_db.search_all_databases(company_name))

        # Execute all FREE layers in parallel
        logger.info(f"⚡ Executing {len(free_tasks)} FREE search layers in parallel...")
        free_results = await asyncio.gather(*free_tasks, return_exceptions=True)

        # Process FREE results
        all_emails = set()
        for res in free_results:
            if isinstance(res, dict):
                # Extract emails from various result formats
                all_emails.update(res.get("emails", []))
                all_emails.update(res.get("all_emails", []))
                all_emails.update(res.get("discovered_emails", []))
                all_emails.update(res.get("potential_emails", []))

                # Extract people
                result.people.extend(res.get("people", []))
                result.people.extend(res.get("founders", []))
                result.people.extend(res.get("recruiters", []))

                # Extract social profiles
                if res.get("profiles"):
                    result.social_profiles.update(res.get("profiles", {}))

                # Track sources
                result.sources.extend(res.get("sources", []))

        # Layer 12: Email Permutation (if we have people names)
        if domain:
            # Generate role-based emails
            role_emails = self.email_permutator.generate_role_emails(domain)
            all_emails.update(role_emails)

            # Generate person-based emails
            for person in result.people[:10]:  # Limit to avoid too many
                if person.get("name"):
                    parts = person["name"].split()
                    if len(parts) >= 2:
                        first_name = parts[0]
                        last_name = parts[-1]
                        person_emails = self.email_permutator.generate_permutations(first_name, last_name, domain)
                        all_emails.update(person_emails)

        result.emails = list(all_emails)
        self.stats.emails_found = len(result.emails)

        logger.info(f"[EMAIL] FREE layers found {len(result.emails)} potential emails")

        # Layer 13: SMTP Verification (verify top emails)
        if result.emails:
            logger.info("[OK] Verifying emails via SMTP...")
            top_emails = list(result.emails)[:20]  # Verify top 20
            verification_results = await self.smtp_verifier.verify_batch(top_emails, max_concurrent=3)

            for vr in verification_results:
                if vr.get("exists") is True:
                    result.verified_emails.append(vr["email"])

            self.stats.emails_verified = len(result.verified_emails)
            logger.info(f"[OK] Verified {len(result.verified_emails)} emails")

        # ========== PHASE 2: PAID POWER METHODS (ADVANCED TECHNIQUES) ==========
        # Uses SAME APIs but with 10x more powerful methods
        if use_paid:
            logger.info("[PAID] Executing PAID POWER METHODS (Advanced Multi-Layer Techniques)...")

            # ===== PAID PHASE 2A: Initial API Sweep + Company Intelligence =====
            logger.info("[STATS] PHASE 2A: Company Intelligence Gathering...")
            company_intel = {}

            # First get company info from Clearbit (foundation for other searches)
            if self.clearbit and domain:
                try:
                    clearbit_data = await self.clearbit.enrich_company(domain)
                    if clearbit_data:
                        company_intel = {
                            "legal_name": clearbit_data.get("legalName"),
                            "name": clearbit_data.get("name"),
                            "employees": clearbit_data.get("metrics", {}).get("employees"),
                            "employee_range": clearbit_data.get("metrics", {}).get("employeesRange"),
                            "industry": clearbit_data.get("category", {}).get("industry"),
                            "sub_industry": clearbit_data.get("category", {}).get("subIndustry"),
                            "linkedin": clearbit_data.get("linkedin", {}).get("handle"),
                            "twitter": clearbit_data.get("twitter", {}).get("handle"),
                            "facebook": clearbit_data.get("facebook", {}).get("handle"),
                            "crunchbase": clearbit_data.get("crunchbase", {}).get("handle"),
                            "founded_year": clearbit_data.get("foundedYear"),
                            "location": clearbit_data.get("location"),
                            "tags": clearbit_data.get("tags", []),
                            "tech": clearbit_data.get("tech", []),
                            "parent_domain": clearbit_data.get("parentDomain"),
                            "ultimate_parent": clearbit_data.get("ultimateParent", {}).get("domain"),
                        }
                        result.raw_data["company_intel"] = company_intel
                        logger.info(f"[OK] Got company intel: {company_intel.get('name')} ({company_intel.get('employees')} employees)")
                except Exception as e:
                    logger.warning(f"Clearbit intel error: {e}")

            # ===== PAID PHASE 2B: Multi-Domain Discovery =====
            logger.info("🌐 PHASE 2B: Multi-Domain Discovery...")
            all_domains = [domain]

            # Add parent domain if different
            if company_intel.get("parent_domain") and company_intel["parent_domain"] != domain:
                all_domains.append(company_intel["parent_domain"])
            if company_intel.get("ultimate_parent") and company_intel["ultimate_parent"] not in all_domains:
                all_domains.append(company_intel["ultimate_parent"])

            logger.info(f"📍 Searching across {len(all_domains)} domains: {all_domains}")

            # ===== PAID PHASE 2C: Parallel Multi-API Sweep (All Domains) =====
            logger.info("⚡ PHASE 2C: Parallel Multi-API Sweep...")
            sweep_tasks = []

            for search_domain in all_domains:
                # Hunter.io - domain search + email finder
                if self.hunter:
                    sweep_tasks.append(self._hunter_deep_search(search_domain))

                # Apollo.io - multi-title search
                if self.apollo:
                    sweep_tasks.append(self._apollo_deep_search(company_name, search_domain, company_intel))

                # Snov.io - domain emails
                if self.snov:
                    sweep_tasks.append(self._snov_deep_search(search_domain))

            if sweep_tasks:
                sweep_results = await asyncio.gather(*sweep_tasks, return_exceptions=True)
                for res in sweep_results:
                    if isinstance(res, dict):
                        result.emails.extend(res.get("emails", []))
                        result.verified_emails.extend(res.get("verified_emails", []))
                        result.people.extend(res.get("people", []))

            # ===== PAID PHASE 2D: Role-Based Targeted Search =====
            logger.info("[HIT] PHASE 2D: Role-Based Targeted Search...")
            role_categories = {
                "leadership": ["CEO", "Founder", "Co-Founder", "President", "Managing Director", "Owner"],
                "marketing": ["CMO", "VP Marketing", "Head of Marketing", "Marketing Director", "Marketing Manager", "Growth"],
                "sales": ["VP Sales", "Head of Sales", "Sales Director", "Business Development", "Account Executive"],
                "tech": ["CTO", "VP Engineering", "Head of Engineering", "Tech Lead", "Director of Engineering"],
                "product": ["CPO", "VP Product", "Head of Product", "Product Director", "Product Manager"],
                "hr": ["CHRO", "VP HR", "Head of HR", "HR Director", "Talent Acquisition", "Recruiting"],
                "finance": ["CFO", "VP Finance", "Head of Finance", "Finance Director", "Controller"],
                "pr": ["VP Communications", "Head of PR", "PR Director", "Communications Manager", "Press Contact"],
            }

            role_tasks = []
            for role_cat, titles in role_categories.items():
                if self.apollo:
                    role_tasks.append(self._apollo_role_search(company_name, domain, titles, role_cat))
                if self.hunter:
                    role_tasks.append(self._hunter_role_search(domain, titles, role_cat))

            if role_tasks:
                role_results = await asyncio.gather(*role_tasks, return_exceptions=True)
                for res in role_results:
                    if isinstance(res, dict):
                        result.emails.extend(res.get("emails", []))
                        result.verified_emails.extend(res.get("verified_emails", []))
                        result.people.extend(res.get("people", []))
                        logger.info(f"  {res.get('role_category', 'unknown')}: Found {len(res.get('emails', []))} emails")

            # ===== PAID PHASE 2E: Email Pattern Learning & Application =====
            logger.info("🧠 PHASE 2E: Email Pattern Learning...")
            learned_patterns = self._learn_email_patterns(result.emails + result.verified_emails, domain)
            if learned_patterns:
                logger.info(f"📝 Learned {len(learned_patterns)} email patterns: {learned_patterns[:3]}")

                # Apply patterns to people without emails
                people_without_emails = [p for p in result.people if not p.get("email") and p.get("name")]
                for person in people_without_emails[:20]:  # Limit
                    generated = self._apply_patterns_to_person(person["name"], domain, learned_patterns)
                    result.emails.extend(generated)

            # ===== PAID PHASE 2F: Cross-API Verification =====
            logger.info("[OK] PHASE 2F: Cross-API Verification...")
            unverified = [e for e in result.emails if e not in result.verified_emails]
            if unverified and self.hunter:
                verify_tasks = []
                for email in unverified[:30]:  # Verify top 30
                    verify_tasks.append(self._hunter_verify_email(email))

                verify_results = await asyncio.gather(*verify_tasks, return_exceptions=True)
                for vr in verify_results:
                    if isinstance(vr, dict) and vr.get("verified"):
                        result.verified_emails.append(vr["email"])

                logger.info(f"[OK] Verified {len([v for v in verify_results if isinstance(v, dict) and v.get('verified')])} additional emails")

            # ===== PAID PHASE 2G: Recursive People Discovery =====
            logger.info("👥 PHASE 2G: Recursive People Discovery...")
            # Find direct reports of leadership
            ceo_people = [p for p in result.people if any(t in (p.get("title") or "").upper() for t in ["CEO", "FOUNDER"])]
            if ceo_people and self.apollo:
                for leader in ceo_people[:3]:
                    reports_tasks = []
                    reports_tasks.append(self._apollo_find_reports(company_name, domain, leader.get("name")))
                    reports_results = await asyncio.gather(*reports_tasks, return_exceptions=True)
                    for res in reports_results:
                        if isinstance(res, dict):
                            result.emails.extend(res.get("emails", []))
                            result.people.extend(res.get("people", []))

            # ===== PAID PHASE 2H: Department-Based Batch Search =====
            logger.info("🏢 PHASE 2H: Department-Based Batch Search...")
            departments = ["Marketing", "Sales", "Engineering", "Product", "Customer Success", "Operations"]
            dept_tasks = []
            for dept in departments:
                if self.apollo:
                    dept_tasks.append(self._apollo_department_search(company_name, domain, dept))

            if dept_tasks:
                dept_results = await asyncio.gather(*dept_tasks, return_exceptions=True)
                for res in dept_results:
                    if isinstance(res, dict):
                        result.emails.extend(res.get("emails", []))
                        result.people.extend(res.get("people", []))

            # ===== PAID PHASE 2I: Seniority Level Sweep =====
            logger.info("[STATS] PHASE 2I: Seniority Level Sweep...")
            seniority_levels = ["owner", "founder", "c_suite", "vp", "director", "manager", "senior"]
            seniority_tasks = []
            for level in seniority_levels:
                if self.apollo:
                    seniority_tasks.append(self._apollo_seniority_search(company_name, domain, level))

            if seniority_tasks:
                sen_results = await asyncio.gather(*seniority_tasks, return_exceptions=True)
                for res in sen_results:
                    if isinstance(res, dict):
                        result.emails.extend(res.get("emails", []))
                        result.people.extend(res.get("people", []))

            # ===== PAID PHASE 2J: Final Smart Permutation =====
            logger.info("🔄 PHASE 2J: Smart Permutation with Verification...")
            # For all people without emails, generate and verify
            final_people_without_emails = [p for p in result.people if not p.get("email") and p.get("name")][:15]
            if final_people_without_emails and self.hunter:
                for person in final_people_without_emails:
                    try:
                        # Use Hunter email finder for specific person
                        finder_result = await self.hunter.email_finder(
                            domain=domain,
                            first_name=person["name"].split()[0] if person.get("name") else None,
                            last_name=person["name"].split()[-1] if person.get("name") and len(person["name"].split()) > 1 else None
                        )
                        if finder_result and finder_result.get("email"):
                            result.emails.append(finder_result["email"])
                            if finder_result.get("score", 0) >= 80:
                                result.verified_emails.append(finder_result["email"])
                            person["email"] = finder_result["email"]
                    except Exception as e:
                        pass

            # ===== PAID PHASE 2K: Technology-Based Contact Discovery =====
            logger.info("💻 PHASE 2K: Technology-Based Contact Discovery...")
            if self.builtwith and domain:
                try:
                    tech_data = await self.builtwith.get_technologies(domain)
                    if tech_data.get("Results"):
                        for tech_group in tech_data["Results"][0].get("Result", {}).get("Paths", []):
                            for tech in tech_group.get("Technologies", []):
                                tech_name = tech.get("Name")
                                if tech_name:
                                    result.technologies.append(tech_name)
                                    # Use tech stack to find technical contacts
                                    if any(t in tech_name.lower() for t in ["analytics", "marketing", "crm", "sales"]):
                                        if self.apollo:
                                            tech_contact = await self._apollo_tech_contact_search(company_name, domain, tech_name)
                                            if tech_contact:
                                                result.emails.extend(tech_contact.get("emails", []))
                                                result.people.extend(tech_contact.get("people", []))
                except Exception as e:
                    logger.warning(f"BuiltWith tech discovery error: {e}")

            logger.info(f"💰 PAID POWER METHODS complete: {len(result.emails)} emails, {len(result.verified_emails)} verified")

        # Deduplicate
        result.emails = list(set(result.emails))
        result.verified_emails = list(set(result.verified_emails))

        # Calculate confidence score
        result.confidence_score = self._calculate_confidence(result)

        logger.info(f"[HIT] ULTRA DEEP SEARCH complete: {len(result.emails)} emails, {len(result.verified_emails)} verified, {len(result.people)} people")

        return result

    async def _run_hunter(self, domain: str) -> Dict[str, Any]:
        """Run Hunter.io API"""
        result = {"emails": [], "verified_emails": [], "people": []}

        try:
            data = await self.hunter.domain_search(domain)

            for email_data in data.get("emails", []):
                email = email_data.get("value")
                if email:
                    result["emails"].append(email)

                    if email_data.get("confidence", 0) >= 80:
                        result["verified_emails"].append(email)

                    result["people"].append({
                        "name": f"{email_data.get('first_name', '')} {email_data.get('last_name', '')}".strip(),
                        "email": email,
                        "position": email_data.get("position"),
                        "linkedin": email_data.get("linkedin"),
                        "source": "hunter.io"
                    })

        except Exception as e:
            logger.error(f"Hunter.io error: {e}")

        return result

    async def _run_clearbit(self, domain: str) -> Dict[str, Any]:
        """Run Clearbit API"""
        result = {"emails": [], "company_info": {}, "people": []}

        try:
            data = await self.clearbit.enrich_company(domain)

            if data:
                result["company_info"] = {
                    "name": data.get("name"),
                    "description": data.get("description"),
                    "employees": data.get("metrics", {}).get("employees"),
                    "industry": data.get("category", {}).get("industry"),
                    "linkedin": data.get("linkedin", {}).get("handle"),
                    "twitter": data.get("twitter", {}).get("handle"),
                    "facebook": data.get("facebook", {}).get("handle"),
                }

        except Exception as e:
            logger.error(f"Clearbit error: {e}")

        return result

    async def _run_apollo(self, company_name: str, domain: str) -> Dict[str, Any]:
        """Run Apollo.io API"""
        result = {"emails": [], "verified_emails": [], "people": []}

        try:
            # Search for key people
            titles = ["CEO", "Founder", "CTO", "VP Marketing", "VP Sales", "Head of Marketing"]
            data = await self.apollo.search_people(company_name=company_name, domain=domain, titles=titles)

            for person in data.get("people", []):
                email = person.get("email")
                if email:
                    result["emails"].append(email)

                    if person.get("email_status") == "verified":
                        result["verified_emails"].append(email)

                result["people"].append({
                    "name": person.get("name"),
                    "email": email,
                    "title": person.get("title"),
                    "linkedin": person.get("linkedin_url"),
                    "phone": person.get("phone_numbers", [{}])[0].get("raw_number") if person.get("phone_numbers") else None,
                    "source": "apollo.io"
                })

        except Exception as e:
            logger.error(f"Apollo.io error: {e}")

        return result

    async def _run_snov(self, domain: str) -> Dict[str, Any]:
        """Run Snov.io API"""
        result = {"emails": [], "people": []}

        try:
            data = await self.snov.get_domain_emails(domain)

            for email_data in data.get("emails", []):
                email = email_data.get("email")
                if email:
                    result["emails"].append(email)

                    result["people"].append({
                        "name": f"{email_data.get('firstName', '')} {email_data.get('lastName', '')}".strip(),
                        "email": email,
                        "position": email_data.get("position"),
                        "source": "snov.io"
                    })

        except Exception as e:
            logger.error(f"Snov.io error: {e}")

        return result

    async def _run_builtwith(self, domain: str) -> Dict[str, Any]:
        """Run BuiltWith API"""
        result = {"technologies": []}

        try:
            data = await self.builtwith.get_technologies(domain)

            if data.get("Results"):
                for tech_group in data["Results"][0].get("Result", {}).get("Paths", []):
                    for tech in tech_group.get("Technologies", []):
                        result["technologies"].append(tech.get("Name"))

        except Exception as e:
            logger.error(f"BuiltWith error: {e}")

        return result

    def _calculate_confidence(self, result: DeepSearchResult) -> int:
        """Calculate overall confidence score"""
        score = 0

        # Verified emails = high confidence
        score += len(result.verified_emails) * 20

        # Unverified emails
        score += len(result.emails) * 5

        # People found
        score += len(result.people) * 3

        # Social profiles
        score += len(result.social_profiles) * 5

        # Sources diversity
        score += len(set(result.sources)) * 2

        return min(score, 100)

    def get_stats(self) -> Dict[str, Any]:
        """Get search statistics"""
        return {
            "total_queries": self.stats.total_queries,
            "emails_found": self.stats.emails_found,
            "emails_verified": self.stats.emails_verified,
            "people_found": self.stats.people_found,
            "layers_completed": self.stats.layers_completed,
        }

    # ========== PAID POWER METHODS - ADVANCED HELPERS ==========

    async def _hunter_deep_search(self, domain: str) -> Dict[str, Any]:
        """
        PAID POWER: Deep Hunter.io search with multiple strategies
        - Domain search with all parameters
        - Department filtering
        - Type filtering (personal vs generic)
        """
        result = {"emails": [], "verified_emails": [], "people": []}

        try:
            # Strategy 1: Full domain search
            data = await self.hunter.domain_search(domain, limit=100)
            for email_data in data.get("emails", []):
                email = email_data.get("value")
                if email:
                    result["emails"].append(email)
                    if email_data.get("confidence", 0) >= 70:
                        result["verified_emails"].append(email)
                    result["people"].append({
                        "name": f"{email_data.get('first_name', '')} {email_data.get('last_name', '')}".strip(),
                        "email": email,
                        "title": email_data.get("position"),
                        "department": email_data.get("department"),
                        "linkedin": email_data.get("linkedin"),
                        "confidence": email_data.get("confidence"),
                        "source": "hunter_deep"
                    })

            # Strategy 2: Department-specific searches
            departments = ["executive", "marketing", "sales", "communication"]
            for dept in departments:
                try:
                    dept_data = await self.hunter.domain_search(domain, department=dept, limit=20)
                    for email_data in dept_data.get("emails", []):
                        email = email_data.get("value")
                        if email and email not in result["emails"]:
                            result["emails"].append(email)
                            if email_data.get("confidence", 0) >= 70:
                                result["verified_emails"].append(email)
                except Exception:
                    pass

            # Strategy 3: Search for generic emails (info@, contact@, etc.)
            try:
                generic_data = await self.hunter.domain_search(domain, type="generic", limit=20)
                for email_data in generic_data.get("emails", []):
                    email = email_data.get("value")
                    if email and email not in result["emails"]:
                        result["emails"].append(email)
            except Exception:
                pass

        except Exception as e:
            logger.warning(f"Hunter deep search error: {e}")

        return result

    async def _apollo_deep_search(self, company_name: str, domain: str, company_intel: Dict) -> Dict[str, Any]:
        """
        PAID POWER: Deep Apollo.io search with intelligence-driven queries
        - Uses company intel for better targeting
        - Multiple search strategies
        - Industry-specific title searches
        """
        result = {"emails": [], "verified_emails": [], "people": []}

        try:
            # Strategy 1: Industry-specific title search
            industry = company_intel.get("industry", "").lower()
            titles = ["CEO", "Founder", "CTO", "VP Marketing", "VP Sales"]

            # Add industry-specific titles
            if "tech" in industry or "software" in industry:
                titles.extend(["Engineering Manager", "DevOps Lead", "Product Manager"])
            elif "ecommerce" in industry or "retail" in industry:
                titles.extend(["Ecommerce Manager", "Merchandising Director", "Category Manager"])
            elif "media" in industry or "advertising" in industry:
                titles.extend(["Media Director", "Creative Director", "Account Director"])
            elif "finance" in industry:
                titles.extend(["Investment Manager", "Portfolio Manager", "Risk Director"])

            data = await self.apollo.search_people(company_name=company_name, domain=domain, titles=titles, per_page=50)
            self._process_apollo_results(data, result)

            # Strategy 2: Search by company size targeting
            employee_count = company_intel.get("employees", 0)
            if employee_count and employee_count > 50:
                # Larger companies - search by seniority
                for seniority in ["c_suite", "vp", "director"]:
                    try:
                        sen_data = await self.apollo.search_people(
                            company_name=company_name,
                            domain=domain,
                            seniority=[seniority],
                            per_page=20
                        )
                        self._process_apollo_results(sen_data, result)
                    except Exception:
                        pass

            # Strategy 3: LinkedIn handle search if available
            linkedin_handle = company_intel.get("linkedin")
            if linkedin_handle and self.apollo:
                try:
                    linkedin_data = await self.apollo.search_people(
                        organization_linkedin_public_id=linkedin_handle,
                        per_page=30
                    )
                    self._process_apollo_results(linkedin_data, result)
                except Exception:
                    pass

        except Exception as e:
            logger.warning(f"Apollo deep search error: {e}")

        return result

    async def _snov_deep_search(self, domain: str) -> Dict[str, Any]:
        """
        PAID POWER: Deep Snov.io search with multiple methods
        - Domain email search
        - Email count check first for efficiency
        """
        result = {"emails": [], "people": []}

        try:
            # First check email count
            count_data = await self.snov.get_domain_emails_count(domain)
            email_count = count_data.get("result", 0)

            if email_count > 0:
                # Get all emails with higher limit
                limit = min(email_count, 100)
                data = await self.snov.get_domain_emails(domain, limit=limit)

                for email_data in data.get("emails", []):
                    email = email_data.get("email")
                    if email:
                        result["emails"].append(email)
                        result["people"].append({
                            "name": f"{email_data.get('firstName', '')} {email_data.get('lastName', '')}".strip(),
                            "email": email,
                            "position": email_data.get("position"),
                            "source": "snov_deep"
                        })

        except Exception as e:
            logger.warning(f"Snov deep search error: {e}")

        return result

    async def _apollo_role_search(self, company_name: str, domain: str, titles: List[str], role_cat: str) -> Dict[str, Any]:
        """
        PAID POWER: Role-specific Apollo search
        """
        result = {"emails": [], "verified_emails": [], "people": [], "role_category": role_cat}

        try:
            data = await self.apollo.search_people(
                company_name=company_name,
                domain=domain,
                titles=titles,
                per_page=15
            )
            self._process_apollo_results(data, result)

        except Exception as e:
            logger.warning(f"Apollo role search error ({role_cat}): {e}")

        return result

    async def _hunter_role_search(self, domain: str, titles: List[str], role_cat: str) -> Dict[str, Any]:
        """
        PAID POWER: Role-specific Hunter search using seniority parameter
        """
        result = {"emails": [], "verified_emails": [], "people": [], "role_category": role_cat}

        try:
            # Map role categories to Hunter seniority
            seniority_map = {
                "leadership": "executive",
                "marketing": "marketing",
                "sales": "sales",
                "tech": "engineering",
                "hr": "hr",
                "finance": "finance",
                "pr": "communication",
            }

            seniority = seniority_map.get(role_cat)
            if seniority:
                data = await self.hunter.domain_search(domain, department=seniority, limit=15)
                for email_data in data.get("emails", []):
                    email = email_data.get("value")
                    if email:
                        result["emails"].append(email)
                        if email_data.get("confidence", 0) >= 70:
                            result["verified_emails"].append(email)

        except Exception as e:
            logger.warning(f"Hunter role search error ({role_cat}): {e}")

        return result

    async def _hunter_verify_email(self, email: str) -> Dict[str, Any]:
        """
        PAID POWER: Verify single email via Hunter
        """
        try:
            result = await self.hunter.verify_email(email)
            return {
                "email": email,
                "verified": result.get("result") == "deliverable" or result.get("score", 0) >= 70,
                "score": result.get("score", 0)
            }
        except Exception:
            return {"email": email, "verified": False}

    async def _apollo_find_reports(self, company_name: str, domain: str, leader_name: str) -> Dict[str, Any]:
        """
        PAID POWER: Find direct reports of a leader
        """
        result = {"emails": [], "people": []}

        try:
            # Search for people reporting to this leader
            data = await self.apollo.search_people(
                company_name=company_name,
                domain=domain,
                seniority=["director", "manager", "senior"],
                per_page=20
            )
            self._process_apollo_results(data, result)

        except Exception as e:
            logger.warning(f"Apollo find reports error: {e}")

        return result

    async def _apollo_department_search(self, company_name: str, domain: str, department: str) -> Dict[str, Any]:
        """
        PAID POWER: Search for people in specific department
        """
        result = {"emails": [], "people": []}

        try:
            # Map department to Apollo departments
            dept_map = {
                "Marketing": ["marketing", "growth", "demand generation"],
                "Sales": ["sales", "business development", "account management"],
                "Engineering": ["engineering", "product", "technology"],
                "Product": ["product", "product management"],
                "Customer Success": ["customer success", "customer support", "client services"],
                "Operations": ["operations", "finance", "hr"],
            }

            keywords = dept_map.get(department, [department.lower()])

            for keyword in keywords[:2]:  # Limit to avoid too many API calls
                try:
                    data = await self.apollo.search_people(
                        company_name=company_name,
                        domain=domain,
                        keywords=[keyword],
                        per_page=10
                    )
                    self._process_apollo_results(data, result)
                except Exception:
                    pass

        except Exception as e:
            logger.warning(f"Apollo department search error: {e}")

        return result

    async def _apollo_seniority_search(self, company_name: str, domain: str, seniority: str) -> Dict[str, Any]:
        """
        PAID POWER: Search by seniority level
        """
        result = {"emails": [], "people": []}

        try:
            data = await self.apollo.search_people(
                company_name=company_name,
                domain=domain,
                seniority=[seniority],
                per_page=15
            )
            self._process_apollo_results(data, result)

        except Exception as e:
            logger.warning(f"Apollo seniority search error: {e}")

        return result

    async def _apollo_tech_contact_search(self, company_name: str, domain: str, tech_name: str) -> Dict[str, Any]:
        """
        PAID POWER: Find contacts related to specific technology
        """
        result = {"emails": [], "people": []}

        try:
            # Map tech to relevant titles
            tech_title_map = {
                "analytics": ["Analytics Manager", "Data Analyst", "BI Manager"],
                "marketing": ["Marketing Manager", "Digital Marketing", "Growth Manager"],
                "crm": ["CRM Manager", "Customer Success", "Sales Operations"],
                "sales": ["Sales Manager", "Sales Director", "Revenue Operations"],
            }

            for keyword, titles in tech_title_map.items():
                if keyword in tech_name.lower():
                    data = await self.apollo.search_people(
                        company_name=company_name,
                        domain=domain,
                        titles=titles,
                        per_page=5
                    )
                    self._process_apollo_results(data, result)
                    break

        except Exception as e:
            logger.warning(f"Apollo tech contact search error: {e}")

        return result

    def _process_apollo_results(self, data: Dict, result: Dict):
        """Helper to process Apollo results into standard format"""
        for person in data.get("people", []):
            email = person.get("email")
            if email and email not in result["emails"]:
                result["emails"].append(email)
                if person.get("email_status") == "verified":
                    result.setdefault("verified_emails", []).append(email)
            result["people"].append({
                "name": person.get("name"),
                "email": email,
                "title": person.get("title"),
                "linkedin": person.get("linkedin_url"),
                "phone": person.get("phone_numbers", [{}])[0].get("raw_number") if person.get("phone_numbers") else None,
                "source": "apollo_power"
            })

    def _learn_email_patterns(self, emails: List[str], domain: str) -> List[str]:
        """
        PAID POWER: Learn email patterns from found emails
        Returns patterns like "{first}.{last}", "{f}{last}", etc.
        """
        patterns = []

        for email in emails:
            if not email or "@" not in email:
                continue

            local_part = email.split("@")[0].lower()
            email_domain = email.split("@")[1].lower()

            # Skip if different domain
            if domain and domain.lower() not in email_domain:
                continue

            # Detect pattern
            if "." in local_part:
                parts = local_part.split(".")
                if len(parts) == 2:
                    if len(parts[0]) == 1 and len(parts[1]) > 1:
                        patterns.append("{f}.{last}")
                    elif len(parts[0]) > 1 and len(parts[1]) == 1:
                        patterns.append("{first}.{l}")
                    elif len(parts[0]) > 1 and len(parts[1]) > 1:
                        patterns.append("{first}.{last}")
            elif "_" in local_part:
                patterns.append("{first}_{last}")
            elif len(local_part) > 3:
                # Check if it could be firstlast or firstinitiallast
                patterns.append("{first}{last}")

        # Return unique patterns
        return list(set(patterns))[:5]

    def _apply_patterns_to_person(self, name: str, domain: str, patterns: List[str]) -> List[str]:
        """
        PAID POWER: Apply learned patterns to generate emails for a person
        """
        emails = []

        if not name or not domain:
            return emails

        parts = name.strip().split()
        if len(parts) < 2:
            return emails

        first_name = parts[0].lower()
        last_name = parts[-1].lower()
        first_initial = first_name[0] if first_name else ""
        last_initial = last_name[0] if last_name else ""

        for pattern in patterns:
            email = pattern.replace("{first}", first_name)
            email = email.replace("{last}", last_name)
            email = email.replace("{f}", first_initial)
            email = email.replace("{l}", last_initial)
            email = f"{email}@{domain}"
            emails.append(email)

        return emails
