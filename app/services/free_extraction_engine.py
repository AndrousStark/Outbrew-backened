"""
FREE EXTRACTION ENGINE V3.0 - ULTRA ENHANCED
100% FREE - No API Keys Required

This engine provides MAXIMUM data extraction using ALL available free tools:

SEARCH ENGINES (Layer 1-2):
1. DuckDuckGo Search (free unlimited)
2. Bing Search (HTML scraping)

SCRAPING (Layer 3-4):
3. BeautifulSoup Static Scraping
4. Playwright JS Rendering
5. Team/About Page Deep Crawler

AI/ML (Layer 5):
6. Local AI/ML Models (SpaCy NER, BERT)

EMAIL DISCOVERY (Layer 6-7):
7. Pattern-based Email Finding
8. GitHub Commit Email Extraction (HIGH ACCURACY!)
9. DNS/MX Email Validation

INTELLIGENCE (Layer 8):
10. OSINT Intelligence (WHOIS, DNS, SSL)
11. Certificate Transparency (crt.sh - subdomains)
12. Wayback Machine (historical data)
13. Wikipedia/Wikidata (company info, executives)
14. SEC EDGAR (US public company executives)

VALIDATION (Layer 9):
15. ML Fraud Detection

Zero cost, MAXIMUM accuracy extraction!
"""

import asyncio
import logging
import re
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json

# HTTP client
import httpx

# HTML parsing
from bs4 import BeautifulSoup

# Local services
from app.services.static_scraper import StaticScraperService
from app.services.free_email_finder import FreeEmailFinder
from app.services.ml_nlp_entity_extractor import MLEntityExtractor
from app.services.ml_osint_intelligence_gatherer import OSINTIntelligenceGatherer
from app.services.ml_fraud_detector import FraudDetector
from app.services.js_renderer import JavaScriptRenderer

# Enhanced FREE sources (V3.0)
from app.services.enhanced_free_sources import (
    BingSearch,
    GitHubExtractor,
    WikidataExtractor,
    WaybackMachineExtractor,
    CertificateTransparencyExtractor,
    TeamPageScraper,
    SECEdgarExtractor,
    EnhancedFreeSourcesAggregator
)

logger = logging.getLogger(__name__)


@dataclass
class FreeExtractionConfig:
    """Configuration for FREE extraction engine V3.0"""
    # Search settings
    max_search_results: int = 50
    search_timeout: int = 30

    # Crawling settings
    crawl_depth: int = 3
    max_pages_per_domain: int = 50
    follow_external_links: bool = False

    # JS Rendering
    use_playwright: bool = True
    render_timeout: int = 30

    # AI/ML Settings
    use_local_ai: bool = True
    confidence_threshold: float = 0.6

    # Rate limiting
    requests_per_second: int = 5
    delay_between_requests: float = 0.2

    # Output settings
    max_records: int = 5000
    deduplicate: bool = True

    # Demographics filters (from frontend)
    regions: List[str] = field(default_factory=list)
    company_sizes: List[str] = field(default_factory=list)
    industries: List[str] = field(default_factory=list)
    custom_keywords: List[str] = field(default_factory=list)

    # Sector
    sector: str = "companies"

    # V3.0 Enhanced Sources Settings
    use_bing_search: bool = True  # Additional search engine
    use_github_extraction: bool = True  # GitHub email extraction (HIGH ACCURACY!)
    use_wikidata: bool = True  # Company info from Wikipedia/Wikidata
    use_wayback_machine: bool = True  # Historical data from archive.org
    use_certificate_transparency: bool = True  # Subdomains from crt.sh
    use_team_page_scraper: bool = True  # Deep team page scraping
    use_sec_edgar: bool = True  # US public company filings

    # Source priorities (higher = more priority)
    source_priorities: Dict[str, int] = field(default_factory=lambda: {
        "github_commits": 95,  # Highest - actual email from git
        "team_page": 90,
        "wikidata": 85,
        "sec_edgar": 85,
        "pattern_detection": 80,
        "wayback": 75,
        "bing": 70,
        "duckduckgo": 70
    })


class DuckDuckGoSearch:
    """
    FREE DuckDuckGo search using the lite HTML version.
    No API key required, unlimited searches.
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.base_url = "https://lite.duckduckgo.com/lite/"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.client = httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        self.search_count = 0

    async def search(self, query: str, max_results: int = 30) -> List[Dict[str, str]]:
        """
        Search DuckDuckGo and return results.

        Returns:
            List of dicts with 'title', 'url', 'snippet'
        """
        results = []

        try:
            # DuckDuckGo lite HTML search
            params = {"q": query, "kl": "wt-wt"}
            response = await self.client.post(
                self.base_url,
                data=params,
                headers=self.headers
            )

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                # Parse search results
                for result_row in soup.find_all("tr"):
                    link = result_row.find("a", class_="result-link")
                    snippet_td = result_row.find("td", class_="result-snippet")

                    if link:
                        url = link.get("href", "")
                        title = link.get_text(strip=True)
                        snippet = snippet_td.get_text(strip=True) if snippet_td else ""

                        if url and title:
                            results.append({
                                "title": title,
                                "url": url,
                                "snippet": snippet
                            })

                            if len(results) >= max_results:
                                break

                self.search_count += 1
                logger.info(f"DuckDuckGo search for '{query[:50]}...' returned {len(results)} results")

        except Exception as e:
            logger.error(f"DuckDuckGo search error: {e}")

        return results

    async def search_people(
        self,
        job_titles: List[str],
        locations: Optional[List[str]] = None,
        industries: Optional[List[str]] = None,
        max_results: int = 50
    ) -> List[Dict[str, str]]:
        """
        Search for people with specific job titles.
        Uses advanced search operators.
        """
        all_results = []
        seen_urls: Set[str] = set()

        # Build search queries
        queries = []

        for title in job_titles:
            # Basic title search
            base_query = f'"{title}"'

            # Add location if specified
            if locations:
                for location in locations:
                    queries.append(f'{base_query} "{location}" site:linkedin.com')
                    queries.append(f'{base_query} "{location}" contact email')
            else:
                queries.append(f'{base_query} site:linkedin.com')
                queries.append(f'{base_query} contact email "team"')

            # Add industry-specific searches
            if industries:
                for industry in industries:
                    queries.append(f'{base_query} "{industry}" company')

        # Execute searches
        for query in queries[:10]:  # Limit queries
            results = await self.search(query, max_results=max_results // len(queries))

            for result in results:
                url = result.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_results.append(result)

                    if len(all_results) >= max_results:
                        return all_results

            # Rate limiting
            await asyncio.sleep(1)

        return all_results

    async def close(self):
        await self.client.aclose()

    def get_stats(self) -> Dict[str, int]:
        return {"total_searches": self.search_count}


class FreeExtractionEngine:
    """
    100% FREE Extraction Engine V3.0 - ULTRA ENHANCED

    Combines ALL free tools for MAXIMUM data extraction:

    SEARCH ENGINES:
    - DuckDuckGo Search (discovery)
    - Bing Search (additional coverage)

    SCRAPING:
    - BeautifulSoup (static scraping)
    - Playwright (JS rendering)
    - Team Page Deep Crawler (specialized)

    AI/ML:
    - SpaCy NER (entity extraction)
    - ML Fraud Detection

    EMAIL DISCOVERY:
    - Pattern matching (email finding)
    - GitHub Commit Extraction (HIGH ACCURACY!)
    - DNS/MX (email validation)

    INTELLIGENCE:
    - OSINT (WHOIS, DNS, SSL)
    - Certificate Transparency (crt.sh)
    - Wayback Machine (historical data)
    - Wikidata (company info)
    - SEC EDGAR (US public companies)
    """

    def __init__(self, config: Optional[FreeExtractionConfig] = None):
        self.config = config or FreeExtractionConfig()

        # Core services
        self.search_engine = DuckDuckGoSearch(timeout=self.config.search_timeout)
        self.static_scraper = StaticScraperService()
        self.email_finder = FreeEmailFinder()

        # V3.0 Enhanced sources
        self.bing_search = None
        self.github_extractor = None
        self.wikidata_extractor = None
        self.wayback_extractor = None
        self.crt_extractor = None
        self.team_page_scraper = None
        self.sec_extractor = None
        self.enhanced_aggregator = None

        # Initialize enhanced sources
        self._init_enhanced_sources()

        # Initialize ML/AI services (optional, graceful fallback)
        self.entity_extractor = None
        self.osint_gatherer = None
        self.fraud_detector = None

        if self.config.use_local_ai:
            try:
                self.entity_extractor = MLEntityExtractor()
                logger.info("ML Entity Extractor initialized")
            except Exception as e:
                logger.warning(f"ML Entity Extractor not available: {e}")

            try:
                self.osint_gatherer = OSINTIntelligenceGatherer()
                logger.info("OSINT Intelligence Gatherer initialized")
            except Exception as e:
                logger.warning(f"OSINT not available: {e}")

            try:
                self.fraud_detector = FraudDetector()
                logger.info("Fraud Detector initialized")
            except Exception as e:
                logger.warning(f"Fraud Detector not available: {e}")

        # JavaScript renderer (lazy initialization)
        self.js_renderer = None

        # Statistics (expanded for V3.0)
        self.stats = {
            "urls_discovered": 0,
            "pages_crawled": 0,
            "records_extracted": 0,
            "emails_found": 0,
            "emails_verified": 0,
            "osint_queries": 0,
            "js_renders": 0,
            "fraud_checks": 0,
            # V3.0 Enhanced stats
            "bing_searches": 0,
            "github_emails_extracted": 0,
            "wikidata_queries": 0,
            "wayback_queries": 0,
            "crt_queries": 0,
            "team_pages_scraped": 0,
            "sec_queries": 0,
            "sources_used": [],
            "start_time": None,
            "end_time": None
        }

    def _init_enhanced_sources(self):
        """Initialize V3.0 enhanced FREE sources."""
        try:
            if self.config.use_bing_search:
                self.bing_search = BingSearch(timeout=self.config.search_timeout)
                logger.info("Bing Search initialized")
        except Exception as e:
            logger.warning(f"Bing Search not available: {e}")

        try:
            if self.config.use_github_extraction:
                self.github_extractor = GitHubExtractor(timeout=self.config.search_timeout)
                logger.info("GitHub Extractor initialized (HIGH ACCURACY EMAIL SOURCE!)")
        except Exception as e:
            logger.warning(f"GitHub Extractor not available: {e}")

        try:
            if self.config.use_wikidata:
                self.wikidata_extractor = WikidataExtractor(timeout=self.config.search_timeout)
                logger.info("Wikidata Extractor initialized")
        except Exception as e:
            logger.warning(f"Wikidata not available: {e}")

        try:
            if self.config.use_wayback_machine:
                self.wayback_extractor = WaybackMachineExtractor(timeout=60)
                logger.info("Wayback Machine initialized")
        except Exception as e:
            logger.warning(f"Wayback Machine not available: {e}")

        try:
            if self.config.use_certificate_transparency:
                self.crt_extractor = CertificateTransparencyExtractor(timeout=60)
                logger.info("Certificate Transparency (crt.sh) initialized")
        except Exception as e:
            logger.warning(f"Certificate Transparency not available: {e}")

        try:
            if self.config.use_team_page_scraper:
                self.team_page_scraper = TeamPageScraper(timeout=self.config.search_timeout)
                logger.info("Team Page Scraper initialized")
        except Exception as e:
            logger.warning(f"Team Page Scraper not available: {e}")

        try:
            if self.config.use_sec_edgar:
                self.sec_extractor = SECEdgarExtractor(timeout=self.config.search_timeout)
                logger.info("SEC EDGAR initialized")
        except Exception as e:
            logger.warning(f"SEC EDGAR not available: {e}")

        # Initialize aggregator for combined operations
        try:
            self.enhanced_aggregator = EnhancedFreeSourcesAggregator(
                timeout=self.config.search_timeout
            )
            logger.info("Enhanced Sources Aggregator initialized")
        except Exception as e:
            logger.warning(f"Enhanced Aggregator not available: {e}")

        # Progress tracking
        self.progress = {
            "current_layer": 0,
            "layer_name": "Starting",
            "layer_progress": 0,
            "total_progress": 0,
            "message": "Initializing..."
        }

    async def _init_js_renderer(self):
        """Lazily initialize Playwright renderer"""
        if self.js_renderer is None and self.config.use_playwright:
            self.js_renderer = JavaScriptRenderer(
                browser_type="chromium",
                headless=True,
                stealth_mode=True
            )
            await self.js_renderer.start()

    def _update_progress(
        self,
        layer: int,
        layer_name: str,
        layer_progress: int,
        message: str
    ):
        """Update progress tracking"""
        total_layers = 9
        self.progress = {
            "current_layer": layer,
            "layer_name": layer_name,
            "layer_progress": layer_progress,
            "total_progress": int(((layer - 1) * 100 + layer_progress) / total_layers),
            "message": message
        }

    async def discover_urls(
        self,
        sector: str,
        job_titles: Optional[List[str]] = None,
        locations: Optional[List[str]] = None,
        industries: Optional[List[str]] = None,
        keywords: Optional[List[str]] = None,
        max_results: int = 50
    ) -> List[str]:
        """
        Layer 1: URL Discovery using DuckDuckGo + Bing (V3.0 Enhanced)

        Discovers target URLs based on sector and filters.
        Uses multiple search engines for maximum coverage.
        """
        self._update_progress(1, "Discovery", 0, "Searching multiple engines for target URLs...")

        urls = []
        seen_urls: Set[str] = set()

        # Build search based on sector
        if sector == "recruiters":
            job_titles = job_titles or ["HR Manager", "Recruiter", "Talent Acquisition"]
        elif sector == "companies":
            job_titles = job_titles or ["CEO", "Founder", "Director"]
        elif sector == "clients":
            job_titles = job_titles or ["Manager", "Head of", "VP"]
        elif sector == "customers":
            job_titles = job_titles or ["Buyer", "Procurement", "Customer"]

        # DuckDuckGo Search
        self._update_progress(1, "Discovery", 10, "Searching DuckDuckGo...")
        ddg_results = await self.search_engine.search_people(
            job_titles=job_titles,
            locations=locations,
            industries=industries,
            max_results=max_results // 2
        )

        for result in ddg_results:
            url = result.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                urls.append(url)

        self.stats["sources_used"].append("duckduckgo")

        # Bing Search (V3.0 Enhanced)
        if self.bing_search:
            self._update_progress(1, "Discovery", 30, "Searching Bing...")

            for title in job_titles[:3]:
                query = f'"{title}"'
                if locations:
                    query += f' "{locations[0]}"'
                if industries:
                    query += f' "{industries[0]}"'
                query += ' contact email'

                bing_results = await self.bing_search.search(query, max_results=15)

                for result in bing_results:
                    url = result.get("url", "")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        urls.append(url)

                await asyncio.sleep(0.5)  # Rate limiting

            self.stats["bing_searches"] += 1
            self.stats["sources_used"].append("bing")

        self._update_progress(1, "Discovery", 50, f"Found {len(urls)} from search engines")

        # Additional keyword searches
        if keywords:
            for keyword in keywords[:5]:
                # DuckDuckGo
                additional = await self.search_engine.search(
                    f'{keyword} contact email team',
                    max_results=10
                )
                for result in additional:
                    url = result.get("url", "")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        urls.append(url)

                # Bing
                if self.bing_search:
                    bing_additional = await self.bing_search.search(
                        f'{keyword} contact team about',
                        max_results=10
                    )
                    for result in bing_additional:
                        url = result.get("url", "")
                        if url and url not in seen_urls:
                            seen_urls.add(url)
                            urls.append(url)

        self.stats["urls_discovered"] = len(urls)
        self._update_progress(1, "Discovery", 100, f"Discovered {len(urls)} URLs from multiple sources")

        return urls[:max_results]

    async def extract_from_github(
        self,
        search_query: str,
        max_users: int = 30
    ) -> List[Dict[str, Any]]:
        """
        V3.0 Enhanced: Extract emails from GitHub commits.

        This is one of the BEST free email extraction methods because:
        - Git commits ALWAYS contain the committer's email
        - High confidence (90%+) - actual email used for git
        - Works for developers, tech executives, CTOs
        """
        records = []

        if not self.github_extractor:
            return records

        self._update_progress(6, "GitHub Extraction", 0, "Searching GitHub users...")

        try:
            # Search for users
            users = await self.github_extractor.search_users(search_query, max_users)

            total = len(users)
            for i, user in enumerate(users):
                username = user.get("username")
                if not username:
                    continue

                # Get user details
                user_details = await self.github_extractor.get_user_details(username)

                if user_details:
                    email = user_details.get("email")

                    # If no public email, try commits (HIGH ACCURACY!)
                    if not email:
                        email = await self.github_extractor.get_email_from_commits(username)

                    if email:
                        records.append({
                            "email": email,
                            "name": user_details.get("name") or username,
                            "company": user_details.get("company", "").replace("@", ""),
                            "title": "Developer" if not user_details.get("bio") else None,
                            "github_url": user_details.get("profile_url"),
                            "twitter": user_details.get("twitter"),
                            "location": user_details.get("location"),
                            "source_url": user_details.get("profile_url"),
                            "extraction_method": "github_commits",
                            "confidence": 90,  # HIGH - actual git email
                            "extracted_at": datetime.now(timezone.utc).isoformat()
                        })
                        self.stats["github_emails_extracted"] += 1

                # Rate limiting
                await asyncio.sleep(1)

                progress = int((i + 1) / total * 100)
                self._update_progress(6, "GitHub Extraction", progress, f"Processed {i + 1}/{total} GitHub users")

            self.stats["sources_used"].append("github_commits")
            logger.info(f"Extracted {len(records)} emails from GitHub commits")

        except Exception as e:
            logger.error(f"GitHub extraction error: {e}")

        return records

    async def extract_from_wikidata(
        self,
        company_name: str
    ) -> List[Dict[str, Any]]:
        """
        V3.0 Enhanced: Extract company executives from Wikidata/Wikipedia.

        Returns:
        - CEO, founders, board members
        - Company info (industry, founding date, HQ)
        """
        records = []

        if not self.wikidata_extractor:
            return records

        self._update_progress(8, "Wikidata", 0, f"Searching Wikidata for {company_name}...")

        try:
            # Get company info
            company_info = await self.wikidata_extractor.search_company(company_name)

            if company_info:
                self.stats["wikidata_queries"] += 1

                # Get executives via SPARQL
                executives = await self.wikidata_extractor.get_company_executives(company_name)

                for exec_info in executives:
                    record = {
                        "name": exec_info.get("name"),
                        "company": company_name,
                        "title": exec_info.get("position"),
                        "source_url": f"https://www.wikidata.org/wiki/{exec_info.get('wikidata_id', '')}",
                        "extraction_method": "wikidata",
                        "confidence": 85,
                        "company_info": {
                            "website": company_info.get("website"),
                            "founded": company_info.get("founded"),
                            "industry": company_info.get("industry_id")
                        },
                        "extracted_at": datetime.now(timezone.utc).isoformat()
                    }

                    # If email found in Wikidata
                    if exec_info.get("email"):
                        record["email"] = exec_info["email"]

                    records.append(record)

                self.stats["sources_used"].append("wikidata")
                logger.info(f"Found {len(records)} executives from Wikidata for {company_name}")

        except Exception as e:
            logger.error(f"Wikidata extraction error: {e}")

        return records

    async def extract_from_wayback(
        self,
        domain: str
    ) -> List[str]:
        """
        V3.0 Enhanced: Extract historical emails from Wayback Machine.

        Useful when:
        - Current website doesn't show emails
        - Contact pages have been removed
        - Website blocks scraping
        """
        emails = []

        if not self.wayback_extractor:
            return emails

        self._update_progress(8, "Wayback Machine", 0, f"Searching archive.org for {domain}...")

        try:
            emails = await self.wayback_extractor.extract_emails_from_history(domain)
            self.stats["wayback_queries"] += 1

            if emails:
                self.stats["sources_used"].append("wayback_machine")
                logger.info(f"Found {len(emails)} historical emails from Wayback for {domain}")

        except Exception as e:
            logger.error(f"Wayback extraction error: {e}")

        return emails

    async def extract_team_pages(
        self,
        domain: str
    ) -> List[Dict[str, Any]]:
        """
        V3.0 Enhanced: Deep scrape team/about pages.

        Specializes in finding:
        - Team member names and titles
        - Email addresses
        - LinkedIn profiles
        """
        records = []

        if not self.team_page_scraper:
            return records

        self._update_progress(3, "Team Pages", 0, f"Finding team pages on {domain}...")

        try:
            # Find team pages
            team_pages = await self.team_page_scraper.find_team_pages(domain)

            if team_pages:
                self._update_progress(3, "Team Pages", 30, f"Found {len(team_pages)} team pages")

                for i, page_url in enumerate(team_pages[:5]):  # Limit pages
                    members = await self.team_page_scraper.extract_team_members(page_url)

                    for member in members:
                        if member.get("name") or member.get("email"):
                            member["source_url"] = page_url
                            member["extraction_method"] = "team_page_scraper"
                            member["confidence"] = 90  # High - directly from team page
                            member["extracted_at"] = datetime.now(timezone.utc).isoformat()
                            records.append(member)

                    self.stats["team_pages_scraped"] += 1

                    progress = int((i + 1) / len(team_pages) * 100)
                    self._update_progress(3, "Team Pages", 30 + progress * 0.7, f"Scraped {i + 1}/{len(team_pages)} pages")

                self.stats["sources_used"].append("team_page_scraper")
                logger.info(f"Extracted {len(records)} team members from {domain}")

        except Exception as e:
            logger.error(f"Team page extraction error: {e}")

        return records

    async def extract_from_sec(
        self,
        company_name: str
    ) -> List[Dict[str, Any]]:
        """
        V3.0 Enhanced: Extract executives from SEC EDGAR filings.

        Works for US public companies.
        Returns executives from 10-K, DEF 14A filings.
        """
        records = []

        if not self.sec_extractor:
            return records

        self._update_progress(8, "SEC EDGAR", 0, f"Searching SEC filings for {company_name}...")

        try:
            # Search company
            company = await self.sec_extractor.search_company(company_name)

            if company and company.get("cik"):
                # Get executives
                executives = await self.sec_extractor.get_company_executives(company["cik"])

                for exec_info in executives:
                    records.append({
                        "name": exec_info.get("name"),
                        "company": company_name,
                        "title": exec_info.get("title"),
                        "source_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={company['cik']}",
                        "extraction_method": "sec_edgar",
                        "confidence": 95,  # Very high - official SEC filings
                        "extracted_at": datetime.now(timezone.utc).isoformat()
                    })

                self.stats["sec_queries"] += 1
                self.stats["sources_used"].append("sec_edgar")
                logger.info(f"Found {len(records)} executives from SEC for {company_name}")

        except Exception as e:
            logger.error(f"SEC EDGAR extraction error: {e}")

        return records

    async def discover_subdomains(
        self,
        domain: str
    ) -> List[str]:
        """
        V3.0 Enhanced: Discover subdomains via Certificate Transparency.

        Useful for finding:
        - mail.company.com, careers.company.com
        - Hidden team pages, internal tools
        """
        subdomains = []

        if not self.crt_extractor:
            return subdomains

        try:
            subdomains = await self.crt_extractor.find_subdomains(domain)
            self.stats["crt_queries"] += 1

            if subdomains:
                self.stats["sources_used"].append("certificate_transparency")
                logger.info(f"Found {len(subdomains)} subdomains for {domain}")

        except Exception as e:
            logger.error(f"Certificate Transparency error: {e}")

        return subdomains

    async def extract_from_url(self, url: str) -> List[Dict[str, Any]]:
        """
        Extract data from a single URL using all FREE layers.

        Layers:
        2. Static Scraping
        3. Deep Crawling
        4. JS Rendering (if needed)
        5. AI/ML Entity Extraction
        6. Email Finding
        7. Email Verification
        8. OSINT Intelligence
        9. Fraud Detection
        """
        records = []

        try:
            # Layer 2: Static Scraping
            self._update_progress(2, "Static Scraping", 0, f"Scraping {url[:50]}...")

            result = await self.static_scraper.scrape_url(url, extract_links=True)
            self.stats["pages_crawled"] += 1

            if result["status"] != "success":
                logger.warning(f"Failed to scrape {url}: {result.get('error')}")
                return records

            data = result["data"]
            html_content = data.get("html", "")
            text_content = data.get("text", "")

            # Check if JS rendering is needed
            needs_js = self._needs_js_rendering(html_content)

            # Layer 4: JS Rendering (if needed)
            if needs_js and self.config.use_playwright:
                self._update_progress(4, "JS Rendering", 0, "Rendering JavaScript...")

                try:
                    await self._init_js_renderer()

                    rendered = await self.js_renderer.render(
                        url=url,
                        wait_for="networkidle",
                        timeout=self.config.render_timeout * 1000
                    )

                    html_content = rendered.get("html", html_content)
                    self.stats["js_renders"] += 1

                    # Re-scrape rendered HTML
                    rendered_result = await self.static_scraper.scrape_html(
                        html=html_content,
                        base_url=url
                    )
                    if rendered_result["status"] == "success":
                        data = rendered_result["data"]
                        text_content = data.get("text", text_content)

                    self._update_progress(4, "JS Rendering", 100, "JavaScript rendered")

                except Exception as e:
                    logger.warning(f"JS rendering failed for {url}: {e}")

            # Layer 5: AI/ML Entity Extraction
            self._update_progress(5, "AI/ML Extraction", 0, "Extracting entities...")

            extracted_entities = {}
            if self.entity_extractor:
                try:
                    extracted_entities = await self.entity_extractor.extract_entities(
                        text_content,
                        source_url=url
                    )
                    self._update_progress(5, "AI/ML Extraction", 100, "Entities extracted")
                except Exception as e:
                    logger.warning(f"Entity extraction failed: {e}")

            # Combine scraped data with ML extracted data
            emails = list(set(data.get("emails", []) + extracted_entities.get("emails", [])))
            names = list(set(data.get("names", []) + extracted_entities.get("persons", [])))
            companies = list(set(data.get("companies", []) + extracted_entities.get("organizations", [])))
            titles = data.get("titles", []) + extracted_entities.get("titles", [])
            phones = data.get("phones", [])

            # Extract domain for company fallback
            from urllib.parse import urlparse
            domain = urlparse(url).netloc.replace("www.", "")

            # Create records
            if emails:
                for i, email in enumerate(emails):
                    record = {
                        "email": email,
                        "source_url": url,
                        "extraction_method": "free_engine",
                        "extracted_at": datetime.now(timezone.utc).isoformat()
                    }

                    if i < len(names):
                        record["name"] = names[i]
                    if companies:
                        record["company"] = companies[0]
                    elif domain:
                        record["company"] = domain
                    if i < len(titles):
                        record["title"] = titles[i]
                    if i < len(phones):
                        record["phone"] = phones[i]

                    records.append(record)

            # If no emails but have names + company, try to find emails
            elif names and (companies or domain):
                self._update_progress(6, "Email Finding", 0, "Finding email addresses...")

                company = companies[0] if companies else domain

                for name in names[:10]:  # Limit
                    # Layer 6: Email Finding
                    email_result = await self.email_finder.find_email(
                        name=name,
                        company=company
                    )

                    if email_result and email_result.get("email"):
                        records.append({
                            "email": email_result["email"],
                            "name": name,
                            "company": company,
                            "source_url": url,
                            "email_confidence": email_result.get("confidence", 0.5),
                            "email_pattern": email_result.get("pattern", "unknown"),
                            "extraction_method": "pattern_detection"
                        })
                        self.stats["emails_found"] += 1

                self._update_progress(6, "Email Finding", 100, f"Found {len(records)} emails")

            # Layer 7: Email Verification
            if records:
                self._update_progress(7, "Email Verification", 0, "Verifying emails...")

                for i, record in enumerate(records):
                    if record.get("email"):
                        verification = await self.email_finder.verify_email(record["email"])
                        record.update({
                            "email_verified": verification.get("deliverable", False),
                            "email_verification_confidence": verification.get("confidence", 0),
                            "email_disposable": verification.get("disposable", False),
                            "email_role": verification.get("role", False)
                        })
                        self.stats["emails_verified"] += 1

                    progress = int((i + 1) / len(records) * 100)
                    self._update_progress(7, "Email Verification", progress, f"Verified {i + 1}/{len(records)}")

            # Layer 8: OSINT Intelligence
            if self.osint_gatherer and domain:
                self._update_progress(8, "OSINT", 0, "Gathering intelligence...")

                try:
                    osint_data = await self.osint_gatherer.gather_intelligence(domain)
                    self.stats["osint_queries"] += 1

                    # Enrich records with OSINT data
                    for record in records:
                        if osint_data:
                            record["osint_data"] = {
                                "domain_info": osint_data.get("whois", {}),
                                "dns_records": osint_data.get("dns", {}),
                                "technologies": osint_data.get("technologies", [])
                            }

                    self._update_progress(8, "OSINT", 100, "Intelligence gathered")

                except Exception as e:
                    logger.warning(f"OSINT gathering failed for {domain}: {e}")

            # Layer 9: Fraud Detection
            if self.fraud_detector and records:
                self._update_progress(9, "Fraud Detection", 0, "Checking for fraud...")

                try:
                    for record in records:
                        fraud_score = await self.fraud_detector.check_record(record)
                        record["fraud_score"] = fraud_score.get("score", 0)
                        record["fraud_flags"] = fraud_score.get("flags", [])
                        record["is_valid"] = fraud_score.get("is_valid", True)
                        self.stats["fraud_checks"] += 1

                    self._update_progress(9, "Fraud Detection", 100, "Fraud check complete")

                except Exception as e:
                    logger.warning(f"Fraud detection failed: {e}")

            self.stats["records_extracted"] += len(records)

        except Exception as e:
            logger.error(f"Error extracting from {url}: {e}")

        return records

    async def run_extraction(
        self,
        urls: Optional[List[str]] = None,
        auto_discover: bool = True,
        use_enhanced_sources: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Run complete extraction pipeline V3.0.

        Args:
            urls: List of URLs to extract from (optional)
            auto_discover: If True, discover URLs automatically
            use_enhanced_sources: If True, use all V3.0 enhanced sources

        Returns:
            List of extracted records
        """
        self.stats["start_time"] = datetime.now(timezone.utc).isoformat()
        all_records = []

        try:
            # Step 1: Discover URLs if needed
            if auto_discover and not urls:
                urls = await self.discover_urls(
                    sector=self.config.sector,
                    locations=self.config.regions,
                    industries=self.config.industries,
                    keywords=self.config.custom_keywords,
                    max_results=self.config.max_search_results
                )

            if not urls:
                logger.warning("No URLs to extract from")
                return all_records

            # V3.0: Extract domains for enhanced processing
            domains_to_process: Set[str] = set()
            company_names: Set[str] = set()

            for url in urls:
                try:
                    from urllib.parse import urlparse
                    parsed = urlparse(url)
                    domain = parsed.netloc.replace("www.", "")
                    if domain:
                        domains_to_process.add(domain)
                        # Try to extract company name from domain
                        company_name = domain.split(".")[0].replace("-", " ").title()
                        if len(company_name) > 2:
                            company_names.add(company_name)
                except Exception:
                    continue

            # Step 2: Extract from each URL (standard method)
            total_urls = len(urls)

            for i, url in enumerate(urls):
                if len(all_records) >= self.config.max_records:
                    logger.info(f"Reached max records limit: {self.config.max_records}")
                    break

                self._update_progress(
                    2, "Extraction",
                    int((i / total_urls) * 100),
                    f"Processing URL {i + 1}/{total_urls}"
                )

                records = await self.extract_from_url(url)
                all_records.extend(records)

                # Rate limiting
                await asyncio.sleep(self.config.delay_between_requests)

            # V3.0 ENHANCED SOURCES
            if use_enhanced_sources:
                self._update_progress(3, "Enhanced Sources", 0, "Running V3.0 enhanced extraction...")

                # Team Page Deep Scraping
                if self.team_page_scraper:
                    self._update_progress(3, "Team Pages", 0, "Deep scraping team pages...")
                    for domain in list(domains_to_process)[:10]:  # Limit
                        team_records = await self.extract_team_pages(domain)
                        all_records.extend(team_records)
                        await asyncio.sleep(0.5)

                # GitHub Email Extraction (HIGH ACCURACY!)
                if self.github_extractor and self.config.sector in ["companies", "recruiters"]:
                    self._update_progress(6, "GitHub", 0, "Extracting from GitHub commits...")
                    for company in list(company_names)[:5]:
                        github_records = await self.extract_from_github(
                            f"{company} developer",
                            max_users=10
                        )
                        all_records.extend(github_records)

                # Wikidata Company Intelligence
                if self.wikidata_extractor:
                    self._update_progress(8, "Wikidata", 0, "Searching company executives...")
                    for company in list(company_names)[:5]:
                        wiki_records = await self.extract_from_wikidata(company)
                        all_records.extend(wiki_records)

                # Wayback Machine Historical Data
                if self.wayback_extractor:
                    self._update_progress(8, "Wayback", 50, "Searching historical archives...")
                    for domain in list(domains_to_process)[:5]:
                        historical_emails = await self.extract_from_wayback(domain)
                        for email in historical_emails:
                            # Create record for historical email
                            all_records.append({
                                "email": email,
                                "company": domain,
                                "source_url": f"https://web.archive.org/web/{domain}",
                                "extraction_method": "wayback_machine",
                                "confidence": 75,
                                "extracted_at": datetime.now(timezone.utc).isoformat()
                            })

                # SEC EDGAR for US Companies
                if self.sec_extractor:
                    for company in list(company_names)[:3]:
                        sec_records = await self.extract_from_sec(company)
                        all_records.extend(sec_records)

            # Deduplication
            if self.config.deduplicate:
                all_records = self._deduplicate_records(all_records)

            self.stats["end_time"] = datetime.now(timezone.utc).isoformat()
            self.stats["records_extracted"] = len(all_records)

            # Log sources used
            unique_sources = list(set(self.stats.get("sources_used", [])))
            logger.info(f"V3.0 Extraction complete: {len(all_records)} records from {len(unique_sources)} sources: {unique_sources}")

            self._update_progress(9, "Complete", 100, f"Extracted {len(all_records)} records from {len(unique_sources)} sources")

        except Exception as e:
            logger.error(f"Extraction pipeline error: {e}")
            self.stats["end_time"] = datetime.now(timezone.utc).isoformat()

        return all_records

    async def run_deep_company_extraction(
        self,
        company_name: str,
        domain: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        V3.0: Deep extraction for a single company using ALL sources.

        This is the most comprehensive extraction method.
        Uses: Wikidata, SEC, GitHub, Team Pages, Wayback, CRT.sh

        Returns:
            {
                "company_name": "Example Inc",
                "domain": "example.com",
                "records": [...],
                "company_info": {...},
                "sources_used": [...]
            }
        """
        self.stats["start_time"] = datetime.now(timezone.utc).isoformat()

        result = {
            "company_name": company_name,
            "domain": domain,
            "records": [],
            "company_info": {},
            "subdomains": [],
            "sources_used": []
        }

        # Guess domain if not provided
        if not domain:
            domain = company_name.lower().replace(" ", "").replace("-", "") + ".com"
            result["domain"] = domain

        self._update_progress(1, "Deep Extraction", 0, f"Starting deep extraction for {company_name}...")

        try:
            # 1. Team Page Scraping
            if self.team_page_scraper:
                self._update_progress(2, "Team Pages", 0, "Finding team members...")
                team_records = await self.extract_team_pages(domain)
                result["records"].extend(team_records)
                if team_records:
                    result["sources_used"].append("team_pages")

            # 2. Wikidata
            if self.wikidata_extractor:
                self._update_progress(3, "Wikidata", 0, "Searching Wikidata...")
                wiki_records = await self.extract_from_wikidata(company_name)
                result["records"].extend(wiki_records)
                if wiki_records:
                    result["sources_used"].append("wikidata")
                    # Extract company info
                    if wiki_records and wiki_records[0].get("company_info"):
                        result["company_info"] = wiki_records[0]["company_info"]

            # 3. SEC EDGAR
            if self.sec_extractor:
                self._update_progress(4, "SEC EDGAR", 0, "Checking SEC filings...")
                sec_records = await self.extract_from_sec(company_name)
                result["records"].extend(sec_records)
                if sec_records:
                    result["sources_used"].append("sec_edgar")

            # 4. GitHub
            if self.github_extractor:
                self._update_progress(5, "GitHub", 0, "Searching GitHub...")
                github_records = await self.extract_from_github(
                    f"{company_name}",
                    max_users=20
                )
                result["records"].extend(github_records)
                if github_records:
                    result["sources_used"].append("github")

            # 5. Wayback Machine
            if self.wayback_extractor:
                self._update_progress(6, "Wayback", 0, "Searching archives...")
                wayback_emails = await self.extract_from_wayback(domain)
                for email in wayback_emails:
                    result["records"].append({
                        "email": email,
                        "company": company_name,
                        "extraction_method": "wayback_machine",
                        "confidence": 75,
                        "extracted_at": datetime.now(timezone.utc).isoformat()
                    })
                if wayback_emails:
                    result["sources_used"].append("wayback_machine")

            # 6. Certificate Transparency (subdomains)
            if self.crt_extractor:
                self._update_progress(7, "Subdomains", 0, "Discovering subdomains...")
                subdomains = await self.discover_subdomains(domain)
                result["subdomains"] = subdomains
                if subdomains:
                    result["sources_used"].append("certificate_transparency")

            # 7. Standard URL extraction from main domain
            self._update_progress(8, "Main Site", 0, "Extracting from main site...")
            main_site_records = await self.extract_from_url(f"https://{domain}")
            result["records"].extend(main_site_records)

            # Deduplicate
            if self.config.deduplicate:
                result["records"] = self._deduplicate_records(result["records"])

            self.stats["end_time"] = datetime.now(timezone.utc).isoformat()
            self._update_progress(9, "Complete", 100, f"Deep extraction complete: {len(result['records'])} records")

            logger.info(f"Deep extraction for {company_name}: {len(result['records'])} records from {result['sources_used']}")

        except Exception as e:
            logger.error(f"Deep company extraction error: {e}")
            self.stats["end_time"] = datetime.now(timezone.utc).isoformat()

        return result

    def _needs_js_rendering(self, html: str) -> bool:
        """Check if page needs JavaScript rendering"""
        indicators = [
            'id="root"',
            'id="app"',
            'id="__next"',
            'Loading...',
            'Please enable JavaScript',
            'This page requires JavaScript',
            'noscript'
        ]
        return any(indicator.lower() in html.lower() for indicator in indicators)

    def _deduplicate_records(self, records: List[Dict]) -> List[Dict]:
        """Remove duplicate records by email"""
        seen_emails: Set[str] = set()
        unique_records = []

        for record in records:
            email = record.get("email", "").lower()
            if email and email not in seen_emails:
                seen_emails.add(email)
                unique_records.append(record)

        return unique_records

    def get_stats(self) -> Dict[str, Any]:
        """Get extraction statistics"""
        return {
            **self.stats,
            "search_stats": self.search_engine.get_stats()
        }

    def get_progress(self) -> Dict[str, Any]:
        """Get current progress"""
        return self.progress

    async def close(self):
        """Close all services (including V3.0 enhanced sources)"""
        # Core services
        await self.search_engine.close()
        await self.static_scraper.close()
        await self.email_finder.close()

        if self.js_renderer:
            await self.js_renderer.close()

        if self.osint_gatherer:
            await self.osint_gatherer.close()

        # V3.0 Enhanced sources
        if self.bing_search:
            await self.bing_search.close()

        if self.github_extractor:
            await self.github_extractor.close()

        if self.wikidata_extractor:
            await self.wikidata_extractor.close()

        if self.wayback_extractor:
            await self.wayback_extractor.close()

        if self.crt_extractor:
            await self.crt_extractor.close()

        if self.team_page_scraper:
            await self.team_page_scraper.close()

        if self.sec_extractor:
            await self.sec_extractor.close()

        if self.enhanced_aggregator:
            await self.enhanced_aggregator.close()

        logger.info("FREE Extraction Engine V3.0 closed")


# Quick start function for immediate use
async def quick_start_extraction(
    sector: str = "companies",
    regions: Optional[List[str]] = None,
    industries: Optional[List[str]] = None,
    keywords: Optional[List[str]] = None,
    max_records: int = 100,
    use_enhanced_sources: bool = True
) -> List[Dict[str, Any]]:
    """
    Quick start function for FREE extraction V3.0.

    Just call this function with basic parameters and get results!

    V3.0 ENHANCED SOURCES:
    - DuckDuckGo + Bing (dual search)
    - Team page deep scraping
    - GitHub commit email extraction (HIGH ACCURACY!)
    - Wikidata company intelligence
    - Wayback Machine historical data
    - SEC EDGAR (US public companies)
    - Certificate Transparency (subdomains)

    Example:
        records = await quick_start_extraction(
            sector="recruiters",
            regions=["North America"],
            industries=["Technology"],
            max_records=50
        )
    """
    config = FreeExtractionConfig(
        sector=sector,
        regions=regions or [],
        industries=industries or [],
        custom_keywords=keywords or [],
        max_records=max_records,
        use_playwright=True,
        use_local_ai=True,
        # V3.0 Enhanced sources
        use_bing_search=use_enhanced_sources,
        use_github_extraction=use_enhanced_sources,
        use_wikidata=use_enhanced_sources,
        use_wayback_machine=use_enhanced_sources,
        use_certificate_transparency=use_enhanced_sources,
        use_team_page_scraper=use_enhanced_sources,
        use_sec_edgar=use_enhanced_sources
    )

    engine = FreeExtractionEngine(config)

    try:
        records = await engine.run_extraction(
            auto_discover=True,
            use_enhanced_sources=use_enhanced_sources
        )
        return records
    finally:
        await engine.close()


async def quick_deep_company_extraction(
    company_name: str,
    domain: Optional[str] = None
) -> Dict[str, Any]:
    """
    Quick deep extraction for a single company using ALL V3.0 sources.

    This is the most comprehensive FREE extraction method!

    Example:
        result = await quick_deep_company_extraction(
            company_name="Microsoft",
            domain="microsoft.com"
        )

        print(f"Found {len(result['records'])} contacts")
        print(f"Sources used: {result['sources_used']}")
    """
    config = FreeExtractionConfig(
        use_playwright=True,
        use_local_ai=True,
        use_bing_search=True,
        use_github_extraction=True,
        use_wikidata=True,
        use_wayback_machine=True,
        use_certificate_transparency=True,
        use_team_page_scraper=True,
        use_sec_edgar=True
    )

    engine = FreeExtractionEngine(config)

    try:
        result = await engine.run_deep_company_extraction(
            company_name=company_name,
            domain=domain
        )
        return result
    finally:
        await engine.close()


# V3.0 Feature summary
"""
FREE EXTRACTION ENGINE V3.0 - ULTRA ENHANCED

NEW SOURCES (all 100% FREE):
1. Bing Search - Additional search engine for more coverage
2. GitHub API - Extract emails from commits (60 req/hour free, HIGH ACCURACY!)
3. Wikidata/Wikipedia - Company info, executives, founders
4. Wayback Machine - Historical emails from archived pages
5. Certificate Transparency (crt.sh) - Discover subdomains
6. Team Page Scraper - Deep scrape /team, /about, /leadership pages
7. SEC EDGAR - US public company executives

BEST EMAIL SOURCES (by accuracy):
1. GitHub Commits (90%+) - Actual email used for git commits
2. Team Pages (90%) - Directly from company website
3. SEC EDGAR (95%) - Official SEC filings
4. Wikidata (85%) - Wikipedia data
5. Pattern Detection (80%) - Generated + validated
6. Wayback Machine (75%) - Historical data

USAGE:

# Quick extraction with all sources
records = await quick_start_extraction(
    sector="companies",
    industries=["Technology"],
    max_records=100
)

# Deep single company extraction
result = await quick_deep_company_extraction(
    company_name="Example Inc",
    domain="example.com"
)

# Custom configuration
config = FreeExtractionConfig(
    sector="recruiters",
    regions=["North America"],
    use_github_extraction=True,  # Enable GitHub (recommended!)
    use_wikidata=True,
    use_wayback_machine=False,  # Disable if slow
)

engine = FreeExtractionEngine(config)
records = await engine.run_extraction()
await engine.close()
"""
