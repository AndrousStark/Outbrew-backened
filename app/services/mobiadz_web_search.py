"""
THEMOBIADZ FREE WEB SEARCH ENGINE V1.0
Multiple Free Web Search Sources for Enhanced Contact Discovery

NO API KEYS REQUIRED - 100% FREE METHODS

SEARCH SOURCES:
==============
1. DuckDuckGo Instant Answers (completely free, no limits)
2. DuckDuckGo HTML scraping (free, rate limited)
3. Bing Web Search (free tier)
4. Google Dorking via direct scraping
5. SearX instances (privacy-focused meta search)
6. Common Crawl Index API (free)
7. Ahmia (onion search for clearnet data)

SEARCH STRATEGIES:
==================
1. Company email discovery
2. Leadership/executive finding
3. App/product developer lookup
4. Marketing contact extraction
5. Social media profile discovery
6. Domain/website discovery
"""

import asyncio
import logging
import re
import json
import random
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import urlparse, urlencode, quote, quote_plus
import httpx
from bs4 import BeautifulSoup

from app.services.browser_profiles import (
    get_headers, get_ua, smart_delay, backoff_delay,
    USER_AGENTS, SEARCH_USER_AGENTS, get_domain_semaphore,
)
from app.services.mobiadz_ultra_engine import LRUCache

logger = logging.getLogger(__name__)


# ============================================
# DATA STRUCTURES
# ============================================

@dataclass
class SearchResult:
    """A single search result"""
    title: str
    url: str
    snippet: str
    source: str  # which search engine
    relevance_score: float = 0.0
    extracted_emails: List[str] = field(default_factory=list)
    extracted_phones: List[str] = field(default_factory=list)
    extracted_names: List[str] = field(default_factory=list)


@dataclass
class WebSearchStats:
    """Statistics for web search operations"""
    total_queries: int = 0
    total_results: int = 0
    duckduckgo_queries: int = 0
    bing_queries: int = 0
    google_dork_queries: int = 0
    searx_queries: int = 0
    emails_found: int = 0
    errors: int = 0


# ============================================
# PUBLIC SEARX INSTANCES (Privacy-focused)
# ============================================

SEARX_INSTANCES = [
    "https://searx.be",
    "https://search.sapti.me",
    "https://searx.tiekoetter.com",
    "https://search.privacyguides.net",
    "https://searx.fmac.xyz",
]


# ============================================
# DUCKDUCKGO FREE SEARCH
# ============================================

class DuckDuckGoSearch:
    """
    DuckDuckGo search - completely FREE, no API keys needed
    Uses both Instant Answers API and HTML scraping
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.instant_api = "https://api.duckduckgo.com/"
        self.html_url = "https://html.duckduckgo.com/html/"

    def _get_headers(self) -> Dict[str, str]:
        return get_headers(context="navigate")

    async def search_instant(self, query: str) -> List[SearchResult]:
        """
        DuckDuckGo Instant Answers API - completely free
        Returns related topics, definitions, and instant answers
        """
        results = []

        try:
            params = {
                "q": query,
                "format": "json",
                "no_html": 1,
                "skip_disambig": 1,
            }

            async with httpx.AsyncClient(
                    transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
                    timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
                ) as client:
                response = await client.get(
                    self.instant_api,
                    params=params,
                    headers=self._get_headers()
                )

                if response.status_code == 200:
                    data = response.json()

                    # Abstract (main result)
                    if data.get("Abstract"):
                        results.append(SearchResult(
                            title=data.get("Heading", query),
                            url=data.get("AbstractURL", ""),
                            snippet=data.get("Abstract", ""),
                            source="duckduckgo_instant",
                            relevance_score=0.9
                        ))

                    # Related topics
                    for topic in data.get("RelatedTopics", [])[:10]:
                        if isinstance(topic, dict) and topic.get("FirstURL"):
                            results.append(SearchResult(
                                title=topic.get("Text", "")[:100],
                                url=topic.get("FirstURL", ""),
                                snippet=topic.get("Text", ""),
                                source="duckduckgo_instant",
                                relevance_score=0.7
                            ))

        except Exception as e:
            logger.warning(f"DuckDuckGo Instant search error: {e}")

        return results

    async def search_html(self, query: str, max_results: int = 30, max_pages: int = 3) -> List[SearchResult]:
        """
        DuckDuckGo HTML search - scrapes results from HTML page
        Better for getting actual web search results
        Now supports MULTI-PAGE scraping for deeper results
        """
        results = []
        seen_urls = set()

        try:
            async with httpx.AsyncClient(
                    transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
                    timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
                ) as client:
                # Scrape multiple pages
                for page in range(max_pages):
                    if len(results) >= max_results:
                        break

                    # DuckDuckGo uses 's' parameter for pagination (results offset)
                    data = {
                        "q": query,
                        "s": str(page * 30),  # 30 results per page offset
                        "dc": str(page * 30 + 1),  # Document count
                    }

                    if page > 0:
                        data["o"] = "json"  # For next pages
                        data["api"] = "d.js"
                        data["nextParams"] = ""
                        data["v"] = "l"
                        data["vqd"] = ""  # Would need from previous response

                    response = await client.post(
                        self.html_url,
                        data=data,
                        headers=self._get_headers(),
                        follow_redirects=True
                    )

                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, "html.parser")

                        # Find result divs
                        page_results = 0
                        for result_div in soup.select(".result"):
                            if len(results) >= max_results:
                                break

                            title_elem = result_div.select_one(".result__title")
                            snippet_elem = result_div.select_one(".result__snippet")
                            url_elem = result_div.select_one(".result__url")
                            link_elem = result_div.select_one("a.result__a")

                            if title_elem and link_elem:
                                url = link_elem.get("href", "")
                                # Clean DuckDuckGo redirect URL
                                if "uddg=" in url:
                                    import urllib.parse
                                    parsed = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
                                    url = parsed.get("uddg", [url])[0]

                                # Deduplicate results
                                if url and url not in seen_urls:
                                    seen_urls.add(url)
                                    results.append(SearchResult(
                                        title=title_elem.get_text(strip=True),
                                        url=url,
                                        snippet=snippet_elem.get_text(strip=True) if snippet_elem else "",
                                        source=f"duckduckgo_html_page{page+1}",
                                        relevance_score=0.8 - (page * 0.1)  # Lower relevance for later pages
                                    ))
                                    page_results += 1

                        # If no results on this page, stop pagination
                        if page_results == 0:
                            break

                        # Rate limiting between pages
                        if page < max_pages - 1:
                            await smart_delay(0.5)
                    else:
                        break  # Stop if page fails

        except Exception as e:
            logger.warning(f"DuckDuckGo HTML search error: {e}")

        return results


# ============================================
# BING FREE SEARCH (Scraping)
# ============================================

class BingSearch:
    """
    Bing Web Search - FREE via scraping
    No API key needed
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.base_url = "https://www.bing.com/search"

    def _get_headers(self) -> Dict[str, str]:
        return get_headers(context="navigate")

    async def search(self, query: str, max_results: int = 30, max_pages: int = 3) -> List[SearchResult]:
        """Search Bing and scrape results - NOW WITH MULTI-PAGE support"""
        results = []
        seen_urls = set()

        try:
            async with httpx.AsyncClient(
                    transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
                    timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
                ) as client:
                # Scrape multiple pages
                for page in range(max_pages):
                    if len(results) >= max_results:
                        break

                    params = {
                        "q": query,
                        "first": page * 10 + 1,  # Bing pagination (1, 11, 21, etc.)
                        "count": 10,
                        "setlang": "en",
                    }

                    response = await client.get(
                        self.base_url,
                        params=params,
                        headers=self._get_headers(),
                        follow_redirects=True
                    )

                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, "html.parser")

                        # Find result items
                        page_results = 0
                        for li in soup.select("li.b_algo"):
                            if len(results) >= max_results:
                                break

                            title_elem = li.select_one("h2 a")
                            snippet_elem = li.select_one(".b_caption p")

                            if title_elem:
                                url = title_elem.get("href", "")
                                if url and url not in seen_urls:
                                    seen_urls.add(url)
                                    results.append(SearchResult(
                                        title=title_elem.get_text(strip=True),
                                        url=url,
                                        snippet=snippet_elem.get_text(strip=True) if snippet_elem else "",
                                        source=f"bing_page{page+1}",
                                        relevance_score=0.75 - (page * 0.1)
                                    ))
                                    page_results += 1

                        # Stop if no results on this page
                        if page_results == 0:
                            break

                        # Rate limiting between pages
                        if page < max_pages - 1:
                            await smart_delay(0.5)
                    else:
                        break

        except Exception as e:
            logger.warning(f"Bing search error: {e}")

        return results


# ============================================
# SEARX META SEARCH (Privacy-focused)
# ============================================

class SearXSearch:
    """
    SearX - Privacy-focused meta search engine
    Aggregates results from multiple search engines
    Uses public instances - completely FREE
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.instances = SEARX_INSTANCES.copy()

    def _get_headers(self) -> Dict[str, str]:
        return get_headers(context="search_api")

    async def search(self, query: str, max_results: int = 30) -> List[SearchResult]:
        """Search using SearX instances"""
        results = []

        # Try multiple instances
        random.shuffle(self.instances)

        for instance in self.instances[:3]:
            try:
                params = {
                    "q": query,
                    "format": "json",
                    "categories": "general",
                    "language": "en",
                }

                async with httpx.AsyncClient(
                    transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
                    timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
                ) as client:
                    response = await client.get(
                        f"{instance}/search",
                        params=params,
                        headers=self._get_headers(),
                        follow_redirects=True
                    )

                    if response.status_code == 200:
                        data = response.json()

                        for item in data.get("results", [])[:max_results]:
                            results.append(SearchResult(
                                title=item.get("title", ""),
                                url=item.get("url", ""),
                                snippet=item.get("content", ""),
                                source=f"searx_{instance.split('//')[1].split('/')[0]}",
                                relevance_score=0.7
                            ))

                        if results:
                            break  # Got results, no need to try more instances

            except Exception as e:
                logger.debug(f"SearX instance {instance} error: {e}")
                continue

        return results


# ============================================
# GOOGLE DORKING (Direct Scraping)
# ============================================

class GoogleDorkSearch:
    """
    Google Dorking via HTML scraping
    Uses advanced search operators for targeted results
    IMPORTANT: Rate limit yourself to avoid blocks
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.base_url = "https://www.google.com/search"

    def _get_headers(self) -> Dict[str, str]:
        return get_headers(context="navigate")

    def build_email_dork(self, company: str, domain: Optional[str] = None) -> str:
        """Build Google dork query to find emails"""
        if domain:
            return f'site:{domain} OR "@{domain}" "{company}" email OR contact'
        return f'"{company}" email contact "@" -@example.com -@email.com'

    def build_leadership_dork(self, company: str) -> str:
        """Build query to find leadership, team leads, and key personnel"""
        return f'"{company}" CEO OR founder OR CTO OR "co-founder" OR director OR "head of" OR "VP" OR "lead developer" site:linkedin.com'

    def build_contact_page_dork(self, company: str) -> str:
        """Build query to find contact pages"""
        return f'"{company}" contact OR "contact us" OR "get in touch" inurl:contact'

    async def search(self, query: str, max_results: int = 20) -> List[SearchResult]:
        """Execute Google search (be careful with rate limits)"""
        results = []

        try:
            params = {
                "q": query,
                "num": min(max_results, 100),
                "hl": "en",
            }

            # Add random delay to avoid rate limiting
            await smart_delay(2.0, jitter=0.7)  # ~0.6–3.4s, anti-fingerprint jitter

            async with httpx.AsyncClient(
                    transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
                    timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
                ) as client:
                response = await client.get(
                    self.base_url,
                    params=params,
                    headers=self._get_headers(),
                    follow_redirects=True
                )

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")

                    # Find result divs (Google changes these frequently)
                    for div in soup.select("div.g")[:max_results]:
                        link = div.select_one("a[href^='http']")
                        title = div.select_one("h3")
                        snippet = div.select_one("div.VwiC3b, span.aCOpRe")

                        if link and title:
                            url = link.get("href", "")
                            # Skip Google's own URLs
                            if "google.com" in url:
                                continue

                            results.append(SearchResult(
                                title=title.get_text(strip=True),
                                url=url,
                                snippet=snippet.get_text(strip=True) if snippet else "",
                                source="google_dork",
                                relevance_score=0.85
                            ))
                elif response.status_code == 429:
                    logger.warning("Google rate limited - try again later")

        except Exception as e:
            logger.warning(f"Google dork search error: {e}")

        return results


# ============================================
# EMAIL EXTRACTION UTILITIES
# ============================================

class EmailExtractor:
    """Extract emails from text/HTML"""

    # Comprehensive email regex
    EMAIL_PATTERN = re.compile(
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b',
        re.IGNORECASE
    )

    # Common disposable email domains to filter
    DISPOSABLE_DOMAINS = {
        "mailinator.com", "guerrillamail.com", "10minutemail.com",
        "tempmail.com", "throwaway.com", "fakeinbox.com", "yopmail.com",
        "getnada.com", "maildrop.cc", "temp-mail.org", "emailondeck.com"
    }

    # Generic emails to filter (low value - automated/system emails only)
    GENERIC_PREFIXES = {
        "noreply", "no-reply", "donotreply", "do-not-reply",
        "postmaster", "mailer-daemon", "nobody", "root",
        "daemon", "bounce", "unsubscribe", "autoresponder",
    }

    @classmethod
    def extract_emails(cls, text: str, domain_filter: Optional[str] = None) -> List[str]:
        """Extract emails from text, optionally filtering by domain"""
        emails = set()

        for match in cls.EMAIL_PATTERN.finditer(text):
            email = match.group().lower()

            # Skip disposable
            domain = email.split("@")[1]
            if domain in cls.DISPOSABLE_DOMAINS:
                continue

            # Skip generic
            prefix = email.split("@")[0]
            if any(prefix.startswith(g) for g in cls.GENERIC_PREFIXES):
                continue

            # Domain filter
            if domain_filter and domain_filter not in email:
                continue

            emails.add(email)

        return list(emails)

    @classmethod
    def categorize_email(cls, email: str) -> str:
        """Categorize email by type - comprehensive department classification"""
        prefix = email.split("@")[0].lower()

        # Order matters: check most specific first
        if any(p in prefix for p in ["sales", "sale", "business", "biz", "enterprise", "partner", "demo", "pricing", "quote", "vendor", "reseller", "wholesale", "b2b", "agency", "client"]):
            return "sales"
        elif any(p in prefix for p in ["press", "journalist", "newsroom", "publicrelations", "mediarelations", "pressoffice"]):
            return "press"
        elif any(p in prefix for p in ["market", "advertis", "media", "pr", "communications", "comms", "growth", "brand", "content", "social", "creative", "events", "newsletter", "blog", "affiliate", "referral", "sponsor", "influencer"]):
            return "marketing"
        elif any(p in prefix for p in ["support", "help", "service", "care", "customer", "helpdesk", "ticket", "abuse", "bug", "issues", "troubleshoot", "onboarding", "success", "training"]):
            return "support"
        elif any(p in prefix for p in ["hr", "career", "careers", "job", "jobs", "recruit", "hiring", "talent", "people", "humanresources", "apply", "employment", "intern"]):
            return "hr"
        elif any(p in prefix for p in ["legal", "compliance", "privacy", "gdpr", "dpo", "dataprotection", "copyright", "dmca"]):
            return "legal"
        elif any(p in prefix for p in ["finance", "billing", "accounts", "invoicing", "payments", "accounting", "invoice", "payable", "receivable", "tax"]):
            return "finance"
        elif any(p in prefix for p in [
            "info", "contact", "hello", "hi", "enquir", "inquiry", "ask", "general",
            "team", "staff", "office", "company", "main", "reception", "feedback",
            "dev", "developer", "developers", "devrel", "engineering", "engineer",
            "tech", "technical", "code", "api", "opensource", "community",
            "research", "product", "design",
        ]):
            return "general"
        else:
            return "other"


# ============================================
# ADDITIONAL FREE DISCOVERY PLATFORMS
# ============================================

class ProductHuntScraper:
    """
    Product Hunt - Great source for startup/app contacts
    FREE to scrape - no API needed
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.base_url = "https://www.producthunt.com"

    def _get_headers(self) -> Dict[str, str]:
        return get_headers(context="navigate")

    async def search_product(self, product_name: str) -> Dict[str, Any]:
        """Search for a product on Product Hunt"""
        results = {"emails": [], "website": None, "makers": [], "social": {}}

        try:
            search_url = f"{self.base_url}/search?q={quote_plus(product_name)}"

            async with httpx.AsyncClient(
                    transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
                    timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
                ) as client:
                response = await client.get(search_url, headers=self._get_headers(), follow_redirects=True)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")

                    # Extract emails from page
                    emails = EmailExtractor.extract_emails(response.text)
                    results["emails"] = emails

                    # Look for product links and maker info
                    for link in soup.select("a[href*='/posts/']"):
                        product_url = link.get("href", "")
                        if product_url and product_name.lower() in link.get_text().lower():
                            # Found the product - now get details
                            full_url = f"{self.base_url}{product_url}" if product_url.startswith("/") else product_url
                            product_page = await client.get(full_url, headers=self._get_headers())

                            if product_page.status_code == 200:
                                product_soup = BeautifulSoup(product_page.text, "html.parser")
                                page_emails = EmailExtractor.extract_emails(product_page.text)
                                results["emails"].extend(page_emails)

                                # Extract website link
                                for ext_link in product_soup.select("a[rel*='noopener']"):
                                    href = ext_link.get("href", "")
                                    if href and "producthunt.com" not in href:
                                        results["website"] = href
                                        break

                            break

        except Exception as e:
            logger.warning(f"Product Hunt search error: {e}")

        return results


class GitHubScraper:
    """
    GitHub - Find developer contacts via repositories
    FREE to scrape
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.base_url = "https://github.com"
        self.api_url = "https://api.github.com"

    def _get_headers(self) -> Dict[str, str]:
        h = get_headers(context="search_api")
        h["Accept"] = "application/vnd.github.v3+json"
        return h

    async def search_org(self, org_name: str) -> Dict[str, Any]:
        """Search for organization on GitHub"""
        results = {"emails": [], "repos": [], "members": [], "website": None}

        try:
            # Search for org
            search_url = f"{self.api_url}/search/users?q={quote_plus(org_name)}+type:org"

            async with httpx.AsyncClient(
                    transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
                    timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
                ) as client:
                response = await client.get(search_url, headers=self._get_headers())

                if response.status_code == 200:
                    data = response.json()
                    if data.get("items"):
                        org = data["items"][0]
                        org_login = org.get("login")

                        # Get org details
                        org_url = f"{self.api_url}/orgs/{org_login}"
                        org_response = await client.get(org_url, headers=self._get_headers())

                        if org_response.status_code == 200:
                            org_data = org_response.json()
                            if org_data.get("email"):
                                results["emails"].append(org_data["email"])
                            if org_data.get("blog"):
                                results["website"] = org_data["blog"]

                        # Get repos for more contact info
                        repos_url = f"{self.api_url}/orgs/{org_login}/repos?per_page=5"
                        repos_response = await client.get(repos_url, headers=self._get_headers())

                        if repos_response.status_code == 200:
                            repos_data = repos_response.json()
                            for repo in repos_data[:5]:
                                results["repos"].append({
                                    "name": repo.get("name"),
                                    "url": repo.get("html_url"),
                                    "description": repo.get("description")
                                })

                        # Get public members
                        members_url = f"{self.api_url}/orgs/{org_login}/public_members?per_page=5"
                        members_response = await client.get(members_url, headers=self._get_headers())

                        if members_response.status_code == 200:
                            members_data = members_response.json()
                            for member in members_data[:5]:
                                # Get member details for email
                                member_url = f"{self.api_url}/users/{member.get('login')}"
                                member_response = await client.get(member_url, headers=self._get_headers())
                                if member_response.status_code == 200:
                                    member_data = member_response.json()
                                    if member_data.get("email"):
                                        results["emails"].append(member_data["email"])
                                    results["members"].append({
                                        "name": member_data.get("name"),
                                        "login": member_data.get("login"),
                                        "email": member_data.get("email"),
                                        "blog": member_data.get("blog")
                                    })
                                await smart_delay(0.2)  # Rate limit

        except Exception as e:
            logger.warning(f"GitHub search error: {e}")

        return results


class AppReviewSiteScraper:
    """
    Scrape app review sites for contact info
    G2, Capterra, GetApp, etc.
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    def _get_headers(self) -> Dict[str, str]:
        return get_headers(context="navigate")

    async def search_g2(self, company_name: str) -> Dict[str, Any]:
        """Search G2 for company info"""
        results = {"emails": [], "website": None, "profile_url": None}

        try:
            search_url = f"https://www.g2.com/search?utf8=%E2%9C%93&query={quote_plus(company_name)}"

            async with httpx.AsyncClient(
                    transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
                    timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
                ) as client:
                response = await client.get(search_url, headers=self._get_headers(), follow_redirects=True)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")

                    # Extract emails
                    emails = EmailExtractor.extract_emails(response.text)
                    results["emails"] = list(set(emails))

                    # Find product links
                    for link in soup.select("a[href*='/products/']"):
                        results["profile_url"] = f"https://www.g2.com{link.get('href', '')}"
                        break

        except Exception as e:
            logger.warning(f"G2 search error: {e}")

        return results

    async def search_capterra(self, company_name: str) -> Dict[str, Any]:
        """Search Capterra for company info"""
        results = {"emails": [], "website": None, "profile_url": None}

        try:
            search_url = f"https://www.capterra.com/search/?search={quote_plus(company_name)}"

            async with httpx.AsyncClient(
                    transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
                    timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
                ) as client:
                response = await client.get(search_url, headers=self._get_headers(), follow_redirects=True)

                if response.status_code == 200:
                    emails = EmailExtractor.extract_emails(response.text)
                    results["emails"] = list(set(emails))

        except Exception as e:
            logger.warning(f"Capterra search error: {e}")

        return results


class DirectWebsiteScraper:
    """
    Directly scrape company websites for contact info
    Goes deep into contact, about, team pages
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    def _get_headers(self, referer: Optional[str] = None) -> Dict[str, str]:
        return get_headers(context="navigate", referer=referer)

    async def deep_scrape_website(self, url: str, max_pages: int = 10) -> Dict[str, Any]:
        """Deep scrape a website for contact info"""
        results = {
            "emails": [],
            "social": {},
            "contact_pages": [],
            "team_pages": [],
            "pages_scraped": 0
        }

        if not url:
            return results

        # Ensure URL has protocol
        if not url.startswith("http"):
            url = f"https://{url}"

        try:
            parsed = urlparse(url)
            base_domain = f"{parsed.scheme}://{parsed.netloc}"

            # Pages to check for contact info - comprehensive including developer pages
            contact_paths = [
                "/contact", "/contact-us", "/contactus",
                "/about", "/about-us", "/aboutus",
                "/team", "/our-team", "/meet-the-team",
                "/company", "/company/about",
                "/support", "/help",
                "/press", "/media", "/newsroom",
                "/careers", "/jobs",
                "/partners", "/partnerships",
                "/advertise", "/advertising",
                # Developer/Engineering pages
                "/developers", "/developer", "/engineering",
                "/devrel", "/community", "/open-source",
                # Additional contact points
                "/people", "/staff", "/directory",
                "/legal", "/imprint", "/impressum",
            ]

            async with httpx.AsyncClient(
                    transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
                    timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
                    follow_redirects=True,
                ) as client:
                # First scrape homepage (no referer — direct navigation)
                try:
                    home_response = await client.get(url, headers=self._get_headers())
                    if home_response.status_code == 200:
                        # Cap response text to 2MB to prevent memory spikes
                        home_text = home_response.text[:2_000_000]
                        home_emails = EmailExtractor.extract_emails(home_text)
                        results["emails"].extend(home_emails)
                        results["pages_scraped"] += 1

                        # Extract social links from homepage
                        soup = BeautifulSoup(home_text, "html.parser")
                        for a in soup.select("a[href]"):
                            href = a.get("href", "").lower()
                            if "linkedin.com/company" in href:
                                results["social"]["linkedin"] = href
                            elif "twitter.com" in href or "x.com" in href:
                                results["social"]["twitter"] = href
                            elif "facebook.com" in href:
                                results["social"]["facebook"] = href
                except Exception:
                    pass

                # Then scrape contact pages (with referer from homepage)
                for path in contact_paths[:max_pages]:
                    try:
                        page_url = f"{base_domain}{path}"
                        response = await client.get(page_url, headers=self._get_headers(referer=url))

                        if response.status_code == 200:
                            page_emails = EmailExtractor.extract_emails(response.text[:2_000_000])
                            results["emails"].extend(page_emails)
                            results["pages_scraped"] += 1

                            if "contact" in path:
                                results["contact_pages"].append(page_url)
                            elif "team" in path or "about" in path:
                                results["team_pages"].append(page_url)

                        await smart_delay(0.3)  # Be nice to servers

                    except Exception:
                        continue

        except Exception as e:
            logger.warning(f"Website scrape error for {url}: {e}")

        # Deduplicate emails
        results["emails"] = list(set(results["emails"]))
        return results


class BraveSearch:
    """
    Brave Search API — requires API key (free tier: ~1000 queries/month).
    Only used when brave_search_api_key is provided.
    """

    def __init__(self, api_key: Optional[str] = None, timeout: int = 30):
        self.api_key = api_key
        self.timeout = timeout
        self.base_url = "https://api.search.brave.com/res/v1/web/search"

    @property
    def available(self) -> bool:
        return bool(self.api_key)

    async def search(self, query: str, max_results: int = 20) -> List[SearchResult]:
        """Search via Brave Search API."""
        if not self.api_key:
            return []

        results = []
        try:
            headers = {
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
                "X-Subscription-Token": self.api_key,
            }
            params = {
                "q": query,
                "count": min(max_results, 20),
            }

            async with httpx.AsyncClient(
                transport=httpx.AsyncHTTPTransport(retries=2, http2=True),
                timeout=httpx.Timeout(connect=10.0, read=self.timeout, write=10.0, pool=5.0),
            ) as client:
                response = await client.get(
                    self.base_url,
                    params=params,
                    headers=headers,
                )

                if response.status_code == 200:
                    data = response.json()
                    web_results = data.get("web", {}).get("results", [])
                    for i, item in enumerate(web_results[:max_results]):
                        results.append(SearchResult(
                            title=item.get("title", ""),
                            url=item.get("url", ""),
                            snippet=item.get("description", ""),
                            source="brave_api",
                            relevance_score=0.85 - (i * 0.02),
                        ))
                elif response.status_code == 429:
                    logger.warning("Brave Search API rate limited")
                else:
                    logger.warning(f"Brave Search API error: {response.status_code}")

        except Exception as e:
            logger.warning(f"Brave search error: {e}")

        return results


# ============================================
# MAIN WEB SEARCH ENGINE
# ============================================

class MobiAdzWebSearch:
    """
    Unified FREE Web Search Engine for TheMobiAdz
    Aggregates results from multiple free search sources
    NOW WITH: Parallel multi-platform search + retry mechanism
    """

    def __init__(self, timeout: int = 30, brave_search_api_key: Optional[str] = None):
        self.timeout = timeout
        # Search engines
        self.duckduckgo = DuckDuckGoSearch(timeout)
        self.bing = BingSearch(timeout)
        self.searx = SearXSearch(timeout)
        self.google_dork = GoogleDorkSearch(timeout)
        self.brave = BraveSearch(api_key=brave_search_api_key, timeout=timeout)
        # Additional discovery platforms
        self.producthunt = ProductHuntScraper(timeout)
        self.github = GitHubScraper(timeout)
        self.app_review = AppReviewSiteScraper(timeout)
        self.website_scraper = DirectWebsiteScraper(timeout)
        # Utilities
        self.email_extractor = EmailExtractor()
        self.stats = WebSearchStats()
        # Per-run in-memory query cache (avoids duplicate HTTP requests across phases)
        self._query_cache = LRUCache(capacity=2000)
        # Cache for discovered useful sources
        self.discovered_sources: Set[str] = set()

    async def search_company_emails(
        self,
        company_name: str,
        domain: Optional[str] = None,
        max_results: int = 50,
        max_pages: int = 3
    ) -> Dict[str, Any]:
        """
        Search for company emails using multiple sources
        Returns aggregated and deduplicated results
        NOW WITH: Multi-page scraping + varied search prompts
        """
        all_results = []
        all_emails = set()

        # VARIED SEARCH PROMPTS - multiple angles to find ALL types of contacts
        queries = [
            # Direct email searches
            f'"{company_name}" email contact',
            f'"{company_name}" email address',
            f'"{company_name}" contact us email',
            # Department-specific
            f'"{company_name}" marketing email sales',
            f'"{company_name}" business development contact',
            f'"{company_name}" partnerships email',
            # Developer/Team contacts - CRITICAL for finding team emails
            f'"{company_name}" developer team email',
            f'"{company_name}" engineering contact email',
            f'"{company_name}" dev team contact',
            f'"{company_name}" technical support email',
            f'"{company_name}" devrel developer relations email',
            # Team/Department emails
            f'"{company_name}" team email contact',
            f'"{company_name}" info email office',
            f'"{company_name}" company representative contact',
            # Social proof searches
            f'"{company_name}" reach out email',
            f'"{company_name}" get in touch',
            # Press/media
            f'"{company_name}" press contact media',
            f'"{company_name}" PR email',
            # HR/Careers
            f'"{company_name}" careers hiring email',
            f'"{company_name}" hr human resources contact',
        ]

        if domain:
            # Domain-specific searches
            queries.extend([
                f'"@{domain}" contact',
                f'"@{domain}" email',
                f'site:{domain} contact email',
                f'site:{domain} "contact us"',
                f'site:{domain} about team',
                f'"{domain}" email address',
            ])

        # Layer 14: Parallelize all query batches with semaphore instead of sequential
        query_sem = asyncio.Semaphore(5)

        async def _cached_engine_search(engine_name, search_coro, query, mr):
            """Wrap a single engine search with per-query cache."""
            cache_key = f"{engine_name}:{query}:{mr}"
            cached = self._query_cache.get(cache_key)
            if cached is not None:
                self.stats.cache_hits = getattr(self.stats, 'cache_hits', 0) + 1
                return cached
            result = await search_coro
            if isinstance(result, list):
                self._query_cache.put(cache_key, result)
            return result

        async def run_query_batch(query):
            async with query_sem:
                engine_tasks = [
                    _cached_engine_search("ddg", self.duckduckgo.search_html(query, max_results=30, max_pages=max_pages), query, 30),
                    _cached_engine_search("bing", self.bing.search(query, max_results=30, max_pages=max_pages), query, 30),
                    _cached_engine_search("searx", self.searx.search(query, max_results=20), query, 20),
                ]
                engine_count = 3
                if self.brave.available:
                    engine_tasks.append(
                        _cached_engine_search("brave", self.brave.search(query, max_results=20), query, 20)
                    )
                    engine_count += 1
                results = await asyncio.gather(*engine_tasks, return_exceptions=True)
                batch_results = []
                for result in results:
                    if isinstance(result, list):
                        batch_results.extend(result)
                self.stats.total_queries += engine_count
                return batch_results

        query_results = await asyncio.gather(
            *[run_query_batch(q) for q in queries],
            return_exceptions=True
        )
        for qr in query_results:
            if isinstance(qr, list):
                all_results.extend(qr)
                self.stats.total_results += len(qr)

        # Extract emails from all results
        for result in all_results:
            if isinstance(result, SearchResult):
                emails = self.email_extractor.extract_emails(
                    f"{result.title} {result.snippet}",
                    domain_filter=domain
                )
                all_emails.update(emails)
                result.extracted_emails = emails

        self.stats.emails_found += len(all_emails)

        return {
            "company": company_name,
            "domain": domain,
            "search_results": all_results,
            "emails_found": list(all_emails),
            "emails_categorized": {
                email: self.email_extractor.categorize_email(email)
                for email in all_emails
            },
            "stats": {
                "total_results": len(all_results),
                "unique_emails": len(all_emails),
                "queries_used": len(queries),
                "pages_scraped": max_pages,
            }
        }

    async def search_leadership(
        self,
        company_name: str,
        max_results: int = 30
    ) -> Dict[str, Any]:
        """Search for company leadership/executives"""
        all_results = []

        queries = [
            f'"{company_name}" CEO founder LinkedIn',
            f'"{company_name}" CTO "co-founder" site:linkedin.com',
            f'"{company_name}" director executive team',
            # Broader team roles - not just C-suite
            f'"{company_name}" "head of" OR "VP" OR "lead" site:linkedin.com',
            f'"{company_name}" "marketing manager" OR "developer" OR "engineer" site:linkedin.com',
            f'"{company_name}" "product manager" OR "business development" site:linkedin.com',
        ]

        for query in queries:
            tasks = [
                self.duckduckgo.search_html(query, max_results=15),
                self.bing.search(query, max_results=15),
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, list):
                    all_results.extend(result)

            self.stats.total_queries += 2

        # Filter for LinkedIn profiles
        linkedin_results = [
            r for r in all_results
            if isinstance(r, SearchResult) and "linkedin.com/in/" in r.url
        ]

        return {
            "company": company_name,
            "all_results": all_results,
            "linkedin_profiles": linkedin_results,
            "stats": {
                "total_results": len(all_results),
                "linkedin_profiles": len(linkedin_results),
            }
        }

    async def search_apps_products(
        self,
        category: str,
        demographic: str,
        max_results: int = 50
    ) -> Dict[str, Any]:
        """Search for apps/products in a category"""
        all_results = []

        # Build category-specific queries
        queries = [
            f"{category} app developer company {demographic}",
            f"best {category} apps {demographic} developer contact",
            f"{category} startup company email {demographic}",
        ]

        for query in queries:
            tasks = [
                self.duckduckgo.search_html(query, max_results=20),
                self.bing.search(query, max_results=20),
                self.searx.search(query, max_results=20),
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, list):
                    all_results.extend(result)

            self.stats.total_queries += 3

        # Extract company websites
        company_urls = set()
        for r in all_results:
            if isinstance(r, SearchResult):
                domain = urlparse(r.url).netloc
                if domain and not any(skip in domain for skip in [
                    "google", "facebook", "twitter", "linkedin",
                    "youtube", "wikipedia", "reddit", "amazon"
                ]):
                    company_urls.add(r.url)

        return {
            "category": category,
            "demographic": demographic,
            "all_results": all_results,
            "company_urls": list(company_urls),
            "stats": {
                "total_results": len(all_results),
                "unique_companies": len(company_urls),
            }
        }

    async def deep_search(
        self,
        company_name: str,
        domain: Optional[str] = None,
        include_leadership: bool = True,
        include_social: bool = True
    ) -> Dict[str, Any]:
        """
        Deep search combining all methods
        Returns comprehensive company intelligence
        """
        results = {
            "company": company_name,
            "domain": domain,
            "emails": {},
            "leadership": [],
            "social_profiles": [],
            "websites": [],
            "search_results": [],
        }

        # Run all searches in parallel
        tasks = [
            self.search_company_emails(company_name, domain),
        ]

        if include_leadership:
            tasks.append(self.search_leadership(company_name))

        all_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process email results
        if len(all_results) > 0 and isinstance(all_results[0], dict):
            results["emails"] = all_results[0].get("emails_categorized", {})
            results["search_results"].extend(all_results[0].get("search_results", []))

        # Process leadership results
        if include_leadership and len(all_results) > 1 and isinstance(all_results[1], dict):
            results["leadership"] = all_results[1].get("linkedin_profiles", [])

        return results

    async def deep_company_contact_search(
        self,
        company_name: str,
        domain: Optional[str] = None,
        website_url: Optional[str] = None,
        max_pages: int = 5
    ) -> Dict[str, Any]:
        """
        ULTRA DEEP Company Contact Search
        Searches multiple angles on the web to find company contacts:
        - Contact pages
        - About pages
        - Team pages
        - Social media profiles
        - Press/media contacts
        - Support channels
        - LinkedIn company page
        - Crunchbase/AngelList
        """
        all_results = []
        all_emails = set()
        social_profiles = {}
        contact_pages = []

        # ========== LAYER 1: Direct Contact Search ==========
        contact_queries = [
            f'"{company_name}" "contact us" email',
            f'"{company_name}" "get in touch" email',
            f'"{company_name}" contact form email',
            f'"{company_name}" contact page',
            f'"{company_name}" reach out',
        ]

        # ========== LAYER 2: Team/About Page Search ==========
        team_queries = [
            f'"{company_name}" "about us" team',
            f'"{company_name}" "meet the team"',
            f'"{company_name}" "our team" leadership',
            f'"{company_name}" founders co-founders',
            f'"{company_name}" management team',
            # Developer/Engineering team
            f'"{company_name}" "engineering team" contact',
            f'"{company_name}" "developer team" email',
            f'"{company_name}" "development team" contact',
            f'"{company_name}" developer relations devrel',
            # Company representative
            f'"{company_name}" representative contact email',
            f'"{company_name}" company contact person',
        ]

        # ========== LAYER 3: Social Media Search ==========
        social_queries = [
            f'"{company_name}" site:linkedin.com/company',
            f'"{company_name}" site:twitter.com',
            f'"{company_name}" site:facebook.com',
            f'"{company_name}" site:instagram.com',
            f'"{company_name}" site:crunchbase.com',
            f'"{company_name}" site:angel.co',
        ]

        # ========== LAYER 4: Sales/Business Contact ==========
        sales_queries = [
            f'"{company_name}" sales contact email',
            f'"{company_name}" business inquiry',
            f'"{company_name}" sales team contact',
            f'"{company_name}" partnership contact',
            f'"{company_name}" advertising contact',
        ]

        # ========== LAYER 5: Support/Help Contact ==========
        support_queries = [
            f'"{company_name}" support email',
            f'"{company_name}" customer service contact',
            f'"{company_name}" help desk email',
            f'"{company_name}" support team',
        ]

        # ========== LAYER 6: Press/Media Contact ==========
        press_queries = [
            f'"{company_name}" press contact',
            f'"{company_name}" media inquiries',
            f'"{company_name}" PR contact email',
            f'"{company_name}" press kit contact',
        ]

        # ========== LAYER 7: Domain-specific Deep Search ==========
        domain_queries = []
        if domain:
            domain_queries = [
                f'site:{domain} contact',
                f'site:{domain} "contact us"',
                f'site:{domain} email',
                f'site:{domain} team',
                f'site:{domain} about',
                f'site:{domain} support',
                f'site:{domain} careers',  # Often has contact info
                f'site:{domain} developer',  # Developer pages
                f'site:{domain} engineering',  # Engineering pages
                f'site:{domain} community',  # Community pages
                f'"@{domain}"',  # Direct email search
                f'"mailto:" site:{domain}',  # Mailto links
                f'"dev@{domain}" OR "team@{domain}" OR "info@{domain}"',  # Common team emails
                f'"engineering@{domain}" OR "hello@{domain}" OR "contact@{domain}"',
            ]

        # Combine all queries
        all_queries = (
            contact_queries + team_queries + social_queries +
            sales_queries + support_queries + press_queries + domain_queries
        )

        # Execute searches with multi-page scraping
        logger.info(f"Deep contact search for {company_name}: {len(all_queries)} queries")

        for i, query in enumerate(all_queries):
            try:
                # Rotate between search engines
                if i % 3 == 0:
                    results = await self.duckduckgo.search_html(query, max_results=30, max_pages=max_pages)
                elif i % 3 == 1:
                    results = await self.bing.search(query, max_results=30, max_pages=max_pages)
                else:
                    results = await self.searx.search(query, max_results=20)

                if results:
                    all_results.extend(results)

                    # Extract emails from results
                    for result in results:
                        if isinstance(result, SearchResult):
                            emails = self.email_extractor.extract_emails(
                                f"{result.title} {result.snippet} {result.url}",
                                domain_filter=domain
                            )
                            all_emails.update(emails)

                            # Identify social profiles
                            url_lower = result.url.lower()
                            if "linkedin.com/company" in url_lower:
                                social_profiles["linkedin_company"] = result.url
                            elif "linkedin.com/in/" in url_lower:
                                if "linkedin_people" not in social_profiles:
                                    social_profiles["linkedin_people"] = []
                                social_profiles["linkedin_people"].append(result.url)
                            elif "twitter.com" in url_lower or "x.com" in url_lower:
                                social_profiles["twitter"] = result.url
                            elif "facebook.com" in url_lower:
                                social_profiles["facebook"] = result.url
                            elif "instagram.com" in url_lower:
                                social_profiles["instagram"] = result.url
                            elif "crunchbase.com" in url_lower:
                                social_profiles["crunchbase"] = result.url
                            elif "angel.co" in url_lower:
                                social_profiles["angellist"] = result.url

                            # Identify contact pages
                            if any(kw in url_lower for kw in ["contact", "about", "team", "support"]):
                                contact_pages.append(result.url)

                self.stats.total_queries += 1
                self.stats.total_results += len(results) if results else 0

                # Rate limiting
                await smart_delay(0.3)

            except Exception as e:
                logger.warning(f"Query failed: {query} - {e}")
                self.stats.errors += 1

        # Categorize found emails
        categorized_emails = {
            email: self.email_extractor.categorize_email(email)
            for email in all_emails
        }

        self.stats.emails_found += len(all_emails)

        return {
            "company": company_name,
            "domain": domain,
            "emails_found": list(all_emails),
            "emails_categorized": categorized_emails,
            "social_profiles": social_profiles,
            "contact_pages": list(set(contact_pages))[:20],  # Dedupe and limit
            "search_results_count": len(all_results),
            "queries_executed": len(all_queries),
            "stats": {
                "total_emails": len(all_emails),
                "sales_emails": sum(1 for e, c in categorized_emails.items() if c == "sales"),
                "marketing_emails": sum(1 for e, c in categorized_emails.items() if c == "marketing"),
                "support_emails": sum(1 for e, c in categorized_emails.items() if c == "support"),
                "general_emails": sum(1 for e, c in categorized_emails.items() if c == "general"),
                "social_profiles_found": len(social_profiles),
                "contact_pages_found": len(contact_pages),
            }
        }

    async def parallel_multi_attempt_search(
        self,
        company_name: str,
        domain: Optional[str] = None,
        website_url: Optional[str] = None,
        product_name: Optional[str] = None,
        max_attempts: int = 4
    ) -> Dict[str, Any]:
        """
        ULTRA POWERFUL Parallel Multi-Attempt Search
        Tries 4+ different methods IN PARALLEL to find contacts

        ATTEMPT 1: Standard web search (DuckDuckGo, Bing, SearX)
        ATTEMPT 2: Deep company contact search (7 layers)
        ATTEMPT 3: Platform-specific search (Product Hunt, GitHub, G2)
        ATTEMPT 4: Direct website deep scrape
        + BONUS: Search discovered useful sources
        """
        all_emails = set()
        all_social = {}
        all_people = []
        sources_searched = []

        logger.info(f"[SEARCH] PARALLEL MULTI-ATTEMPT SEARCH for {company_name} - {max_attempts} attempts")

        # ========== PREPARE ALL TASKS FOR PARALLEL EXECUTION ==========
        tasks = []
        task_names = []

        # ATTEMPT 1: Standard multi-page web search
        tasks.append(self.search_company_emails(company_name, domain, max_results=50, max_pages=5))
        task_names.append("web_search")

        # ATTEMPT 2: Deep company contact search
        tasks.append(self.deep_company_contact_search(company_name, domain, website_url, max_pages=5))
        task_names.append("deep_contact_search")

        # ATTEMPT 3: Leadership search
        tasks.append(self.search_leadership(company_name, max_results=50))
        task_names.append("leadership_search")

        # ATTEMPT 4: Product Hunt (if product name available)
        product_search_name = product_name or company_name
        tasks.append(self.producthunt.search_product(product_search_name))
        task_names.append("producthunt")

        # ATTEMPT 5: GitHub org search
        tasks.append(self.github.search_org(company_name))
        task_names.append("github")

        # ATTEMPT 6: G2 search
        tasks.append(self.app_review.search_g2(company_name))
        task_names.append("g2")

        # ATTEMPT 7: Capterra search
        tasks.append(self.app_review.search_capterra(company_name))
        task_names.append("capterra")

        # ATTEMPT 8: Direct website scrape (if URL available)
        if website_url:
            tasks.append(self.website_scraper.deep_scrape_website(website_url, max_pages=15))
            task_names.append("direct_website")

        # ========== RUN ALL TASKS IN PARALLEL ==========
        logger.info(f"[START] Running {len(tasks)} search tasks in parallel...")
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # ========== PROCESS RESULTS FROM EACH ATTEMPT ==========
        for i, (result, task_name) in enumerate(zip(results, task_names)):
            try:
                if isinstance(result, Exception):
                    logger.warning(f"Task {task_name} failed: {result}")
                    continue

                if isinstance(result, dict):
                    sources_searched.append(task_name)

                    # Extract emails
                    emails = result.get("emails_found", result.get("emails", []))
                    if emails:
                        if isinstance(emails, dict):
                            all_emails.update(emails.keys())
                        elif isinstance(emails, list):
                            all_emails.update(emails)

                    # Extract categorized emails
                    categorized = result.get("emails_categorized", {})
                    if categorized:
                        all_emails.update(categorized.keys())

                    # Extract social profiles
                    social = result.get("social_profiles", result.get("social", {}))
                    if social:
                        all_social.update(social)

                    # Extract people/leadership
                    linkedin_profiles = result.get("linkedin_profiles", [])
                    if linkedin_profiles:
                        for profile in linkedin_profiles:
                            if isinstance(profile, SearchResult):
                                all_people.append({
                                    "name": profile.title.split(" - ")[0] if profile.title else "",
                                    "linkedin": profile.url,
                                    "source": task_name
                                })

                    # GitHub members
                    members = result.get("members", [])
                    for member in members:
                        if isinstance(member, dict) and member.get("email"):
                            all_emails.add(member["email"])
                            all_people.append({
                                "name": member.get("name"),
                                "email": member.get("email"),
                                "source": "github"
                            })

                    # Track discovered sources for future searches
                    contact_pages = result.get("contact_pages", [])
                    for page in contact_pages:
                        self.discovered_sources.add(page)

                    self.stats.total_results += result.get("search_results_count", 0)

            except Exception as e:
                logger.warning(f"Error processing {task_name} results: {e}")

        # ========== BONUS: Search discovered sources ==========
        if self.discovered_sources and len(all_emails) < 3:
            logger.info(f"[SEARCH] Searching {len(self.discovered_sources)} discovered sources...")
            for source_url in list(self.discovered_sources)[:5]:
                try:
                    source_result = await self.website_scraper.deep_scrape_website(source_url, max_pages=3)
                    if source_result.get("emails"):
                        all_emails.update(source_result["emails"])
                        sources_searched.append(f"discovered:{urlparse(source_url).netloc}")
                except Exception:
                    pass

        # ========== CATEGORIZE ALL FOUND EMAILS ==========
        categorized_emails = {
            email: self.email_extractor.categorize_email(email)
            for email in all_emails
        }

        self.stats.emails_found += len(all_emails)

        return {
            "company": company_name,
            "domain": domain,
            "emails_found": list(all_emails),
            "emails_categorized": categorized_emails,
            "social_profiles": all_social,
            "people_found": all_people,
            "sources_searched": sources_searched,
            "attempts_made": len(tasks),
            "stats": {
                "total_emails": len(all_emails),
                "total_people": len(all_people),
                "total_social": len(all_social),
                "sources_count": len(sources_searched),
            }
        }

    async def aggressive_contact_hunt(
        self,
        company_name: str,
        domain: Optional[str] = None,
        website_url: Optional[str] = None,
        retry_count: int = 4
    ) -> Dict[str, Any]:
        """
        AGGRESSIVE Contact Hunt - Retries multiple times with different strategies
        Use this when you REALLY need to find contacts for a company
        """
        all_found_emails = set()
        all_attempts_results = []

        # Different search strategies for each retry
        strategies = [
            # Strategy 1: Full name + variations
            [f'"{company_name}" contact email', f'"{company_name}" info email'],
            # Strategy 2: Domain focus
            [f'"@{domain}"' if domain else f'"{company_name}" email'] + [f'site:{domain} contact' if domain else ''],
            # Strategy 3: Social/LinkedIn
            [f'"{company_name}" linkedin email', f'"{company_name}" twitter contact'],
            # Strategy 4: Industry specific
            [f'"{company_name}" app developer email', f'"{company_name}" startup contact founder'],
        ]

        for attempt in range(min(retry_count, len(strategies))):
            logger.info(f"[HIT] Aggressive hunt attempt {attempt + 1}/{retry_count}")

            # Run parallel search with this strategy's queries
            strategy_queries = strategies[attempt]

            for query in strategy_queries:
                if not query:
                    continue

                try:
                    # Search with multiple engines
                    tasks = [
                        self.duckduckgo.search_html(query, max_results=50, max_pages=5),
                        self.bing.search(query, max_results=50, max_pages=5),
                    ]

                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    for result in results:
                        if isinstance(result, list):
                            for item in result:
                                if isinstance(item, SearchResult):
                                    emails = self.email_extractor.extract_emails(
                                        f"{item.title} {item.snippet} {item.url}",
                                        domain_filter=domain
                                    )
                                    all_found_emails.update(emails)

                    await smart_delay(0.3)

                except Exception as e:
                    logger.warning(f"Strategy {attempt + 1} error: {e}")

            # If we found enough emails, stop
            if len(all_found_emails) >= 3:
                logger.info(f"[OK] Found {len(all_found_emails)} emails after {attempt + 1} attempts")
                break

        # Categorize all found emails
        categorized = {
            email: self.email_extractor.categorize_email(email)
            for email in all_found_emails
        }

        return {
            "company": company_name,
            "emails_found": list(all_found_emails),
            "emails_categorized": categorized,
            "attempts_made": min(retry_count, len(strategies)),
            "success": len(all_found_emails) > 0
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get search statistics"""
        return {
            "total_queries": self.stats.total_queries,
            "total_results": self.stats.total_results,
            "emails_found": self.stats.emails_found,
            "errors": self.stats.errors,
            "discovered_sources": len(self.discovered_sources),
        }


# ============================================
# INTEGRATION FUNCTIONS
# ============================================

async def search_and_extract_contacts(
    company_name: str,
    domain: Optional[str] = None,
    timeout: int = 30
) -> Dict[str, Any]:
    """
    Convenience function to search and extract contacts for a company
    """
    searcher = MobiAdzWebSearch(timeout=timeout)
    return await searcher.deep_search(company_name, domain)


async def batch_search_companies(
    companies: List[Dict[str, str]],
    timeout: int = 30,
    delay_between: float = 1.0
) -> List[Dict[str, Any]]:
    """
    Batch search multiple companies with rate limiting

    Args:
        companies: List of {"name": "...", "domain": "..."} dicts
        timeout: Request timeout
        delay_between: Delay between companies to avoid rate limits
    """
    searcher = MobiAdzWebSearch(timeout=timeout)
    results = []

    for company in companies:
        result = await searcher.deep_search(
            company.get("name", ""),
            company.get("domain")
        )
        results.append(result)

        # Rate limiting
        await smart_delay(delay_between)

    return results
