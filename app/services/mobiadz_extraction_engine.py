"""
THEMOBIADZ EXTRACTION ENGINE V2.0 - ULTRA INTEGRATED
Specialized App/Game/E-commerce Company Data Extraction

AI/ML-POWERED ULTRA FEATURES:
=============================
- SpaCy NER for entity extraction
- 50+ email patterns with permutation generator
- Advanced data structures (Bloom Filter, LRU Cache, Trie)
- DNS & SSL certificate intelligence
- GitHub organization email extraction
- Wayback Machine historical email discovery
- Fuzzy matching for deduplication
- Email verification with MX records

This engine focuses on finding:
- Mobile App Developers (Android/iOS)
- Game Development Companies
- E-commerce Platforms
- Product-based Companies
- Ads-based Companies

Data Sources (20+):
1. Google Play Store (apps, games, developers)
2. Apple App Store (iTunes API)
3. Microsoft Store
4. Steam (games)
5. Company Websites (6-7 level deep scraping)
6. LinkedIn (company profiles)
7. Crunchbase (startup data)
8. ProductHunt (new products)
9. GitHub Organizations & Commits
10. npm Registry (maintainer emails)
11. HackerNews mentions
12. DNS/MX Records
13. SSL Certificate Transparency (crt.sh)
14. Wayback Machine historical data
15. WHOIS domain data

Two Modes:
- FREE: All free scraping methods + AI/ML enhancements
- PAID: APIs + enhanced data (with free fallback)
"""

import asyncio
import logging
import math
import os
import re
import json
import random
import tempfile
import xml.etree.ElementTree as ET
from collections import deque
from typing import Dict, Any, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlparse, urlencode, quote, parse_qs, urljoin
from enum import Enum

import httpx
import tldextract
from bs4 import BeautifulSoup

from app.services.browser_profiles import (
    get_headers, get_ua, smart_delay, backoff_delay,
    SEARCH_USER_AGENTS, USER_AGENTS, BrowserSession,
    get_domain_semaphore, DNSPrefetcher, AdaptiveConcurrencyController,
)

# google-play-scraper package (pip install google-play-scraper)
# Provides direct access to developerEmail - no HTML parsing needed
try:
    from google_play_scraper import search as gplay_search, app as gplay_app
    from google_play_scraper.exceptions import NotFoundError as GPlayNotFoundError
    HAS_GPLAY_SCRAPER = True
except ImportError:
    HAS_GPLAY_SCRAPER = False

# Playwright for JavaScript-rendered pages (fallback for SPAs)
try:
    from playwright.async_api import async_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

# pdfplumber for extracting emails from PDF files (privacy policies, annual reports)
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

# Import Ultra Engine components
from app.services.mobiadz_ultra_engine import (
    BloomFilter,
    LRUCache,
    TTLFileCache,
    CircuitBreaker,
    retry_with_backoff,
    EmailPatternTrie,
    PriorityURLQueue,
    NLPEntityExtractor,
    EmailPermutationGenerator,
    EmailVerifier,
    EmailPatternDetector,
    FuzzyMatcher,
    GitHubOrganizationScraper,
    NPMPackageScraper,
    HackerNewsScraper,
    DNSIntelligence,
    SSLCertificateIntelligence,
    WaybackIntelligence,
    MobiAdzUltraEngine
)

# Import OSINT Engine components
from app.services.mobiadz_osint_engine import (
    MobiAdzOSINTEngine,
    GoogleDorkingOSINT,
    LinkedInPublicOSINT,
    GitHubOSINT,
    SocialMediaOSINT,
    DomainOSINT,
    EmailOSINT,
    CompanyRegistryOSINT,
    PersonIntel,
    CompanyIntel
)

# Import FREE Web Search Engine
from app.services.mobiadz_web_search import (
    MobiAdzWebSearch,
    DuckDuckGoSearch,
    BingSearch,
    SearXSearch,
    GoogleDorkSearch,
    EmailExtractor,
    SearchResult
)

logger = logging.getLogger(__name__)

# Module-level tldextract instance (uses bundled snapshot, no network on first call)
_tld_extractor = tldextract.TLDExtract(suffix_list_urls=[], fallback_to_snapshot=True)


# ============================================
# LAYER 9: SOURCE RELIABILITY & ROLE ENGAGEMENT CONSTANTS
# ============================================

# Source reliability weights (0.0 - 1.0) based on industry benchmarks
# Higher = more reliable/trustworthy email source
SOURCE_RELIABILITY = {
    # Direct verification (highest trust)
    "smtp_verified": 0.95,
    "ms365_verified": 0.93,
    "verified_emails": 0.92,
    # Official company sources
    "store_email": 0.90,        # App store developer email (verified by store)
    "website_contact": 0.85,    # Company contact page (publicly listed)
    "website_scrape": 0.80,     # Company website (other pages)
    # Infrastructure-derived
    "dns_verified": 0.80,       # DNS TXT/SPF record email
    "dns_intel": 0.75,          # DNS/MX intelligence
    "bimi_verified": 0.78,      # BIMI-verified domain
    "ssl_certs": 0.70,          # Certificate Transparency logs
    # Code/developer platforms
    "github_commits": 0.70,     # Git commit emails (real, but may be personal)
    "github_org": 0.65,         # GitHub organization profile
    "npm": 0.65,                # npm registry maintainer
    # Business directories
    "osint_sec_edgar_officers": 0.80,  # SEC EDGAR filings (legal documents)
    "osint_crunchbase": 0.70,   # Crunchbase company data
    "osint_theorg": 0.65,       # TheOrg organizational charts
    "osint_linkedin": 0.65,     # LinkedIn public data
    "osint_press_releases": 0.60,  # Press releases
    "osint_company_blog": 0.55, # Company blog/team page
    "osint_patents": 0.55,      # Patent filings
    "osint_conferences": 0.50,  # Conference speaker lists
    # Search-derived (lower trust — may be stale)
    "web_search_linkedin": 0.55,
    "web_search_deep": 0.45,
    "osint_google_dorking": 0.40,
    "hackernews": 0.40,
    "wayback": 0.35,            # Archive.org (explicitly historical/stale)
    "producthunt": 0.40,
    # Generated (lowest trust — unverified)
    "email_permutation": 0.25,
    "pattern_match": 0.30,
    "role_guess": 0.20,
    "bounce_scored": 0.50,
    "security_gateway": 0.60,
    "technologies": 0.30,
    "subdomains": 0.40,
}

# Role-based email engagement scores (0.0 - 1.0)
# Based on B2B cold email benchmarks: hr@ has 8.5% reply rate (highest)
# Personal named emails (john.smith@) get 1.0 (full confidence)
ROLE_ENGAGEMENT = {
    # High engagement — actively monitored, decision-makers
    "hr": 0.65, "recruiting": 0.65, "careers": 0.60, "hiring": 0.60, "talent": 0.60,
    "partnerships": 0.60, "partner": 0.60, "business": 0.55, "bizdev": 0.55,
    "sales": 0.55, "enterprise": 0.55, "demo": 0.55,
    # Medium engagement — functional teams
    "marketing": 0.50, "growth": 0.50, "brand": 0.50,
    "press": 0.50, "media": 0.50, "pr": 0.50, "communications": 0.50,
    "product": 0.45, "feedback": 0.45,
    # Developer/tech teams — moderate for tech outreach
    "dev": 0.40, "developer": 0.40, "developers": 0.40, "devrel": 0.45,
    "engineering": 0.40, "engineer": 0.40, "tech": 0.40, "technical": 0.40,
    "api": 0.40, "opensource": 0.40, "community": 0.45,
    # Support — ticketing systems, low personal engagement
    "support": 0.35, "help": 0.35, "care": 0.35, "customer": 0.35,
    "service": 0.35, "helpdesk": 0.30,
    # Leadership — prestigious but rarely read by actual exec
    "ceo": 0.30, "cto": 0.35, "cfo": 0.25, "coo": 0.25,
    "founder": 0.35, "president": 0.25,
    # Generic — high spam trap risk, low deliverability
    "contact": 0.25, "hello": 0.25, "info": 0.20, "general": 0.20,
    "team": 0.30, "office": 0.25, "main": 0.20,
    # Administrative — RFC-mandated, spam trap risk
    "admin": 0.15, "administrator": 0.15, "postmaster": 0.10,
    "webmaster": 0.15, "hostmaster": 0.10,
    # Finance/legal — low outreach relevance
    "billing": 0.25, "accounts": 0.25, "finance": 0.25, "invoice": 0.25,
    "legal": 0.20, "compliance": 0.20, "privacy": 0.20,
    # Never contact
    "noreply": 0.05, "no-reply": 0.05, "donotreply": 0.05,
    "abuse": 0.05, "spam": 0.05, "bounce": 0.05,
}

# Freshness decay constant: calibrated to 25% annual decay
# e^(-lambda * 365) = 0.75 → lambda = 0.000788
FRESHNESS_DECAY_LAMBDA = 0.000788


class ExtractionCancelled(Exception):
    """Raised when extraction is cancelled by user."""
    pass

# Import ULTRA DEEP Search Engine V2.0 (15+ FREE layers + 10+ PAID APIs)
try:
    from app.services.ultra_deep_search import (
        UltraDeepSearchEngine,
        MultiEngineSearch,
        ArchiveMiner,
        DNSIntelligence as DNSIntelV2,
        WHOISIntelligence,
        CertificateTransparency,
        SitemapMiner,
        SocialMediaDiscovery,
        DeveloperPlatformSearch,
        JobPostingAnalyzer,
        PressReleaseMiner,
        StartupDatabaseSearch,
        EmailPermutationEngine,
        SMTPVerifier,
        HunterIOClient,
        ClearbitClient,
        ApolloIOClient,
        RocketReachClient,
        SnovIOClient,
        BuiltWithClient,
    )
    ULTRA_DEEP_AVAILABLE = True
    logger.info("[OK] ULTRA DEEP Search Engine V2.0 loaded successfully")
except ImportError as e:
    ULTRA_DEEP_AVAILABLE = False
    logger.warning(f"ULTRA DEEP Search Engine not available - some features disabled: {e}")


class Demographic(Enum):
    """Geographic regions for targeting"""
    USA = "usa"
    EUROPE = "europe"
    UK = "uk"
    AUSTRALIA = "australia"
    SINGAPORE = "singapore"
    EAST_ASIA = "east_asia"  # Japan, Korea, China, Taiwan
    SOUTH_ASIA = "south_asia"  # India, Pakistan, Bangladesh
    MIDDLE_EAST = "middle_east"
    RUSSIA = "russia"
    LATIN_AMERICA = "latin_america"
    AFRICA = "africa"
    SOUTHEAST_ASIA = "southeast_asia"  # Thailand, Vietnam, Indonesia, Philippines
    GLOBAL = "global"


class ProductCategory(Enum):
    """Types of products/companies to search"""
    MOBILE_APPS = "mobile_apps"
    ANDROID_APPS = "android_apps"
    IOS_APPS = "ios_apps"
    GAMES = "games"
    ECOMMERCE = "ecommerce"
    PRODUCT_BASED = "product_based"
    ADS_BASED = "ads_based"
    SAAS = "saas"
    FINTECH = "fintech"
    HEALTH_TECH = "health_tech"
    ED_TECH = "ed_tech"
    SOCIAL_MEDIA = "social_media"
    STREAMING = "streaming"
    PRODUCTIVITY = "productivity"
    ENTERPRISE = "enterprise"
    # Layer 10: New non-app categories
    JOBS = "jobs"
    RECRUITMENT = "recruitment"
    STARTUPS = "startups"


# Country codes for app stores by demographic
DEMOGRAPHIC_COUNTRIES = {
    Demographic.USA: ["us"],
    Demographic.EUROPE: ["de", "fr", "es", "it", "nl", "pl", "se", "no", "dk", "fi", "be", "at", "ch"],
    Demographic.UK: ["gb"],
    Demographic.AUSTRALIA: ["au", "nz"],
    Demographic.SINGAPORE: ["sg"],
    Demographic.EAST_ASIA: ["jp", "kr", "cn", "tw", "hk"],
    Demographic.SOUTH_ASIA: ["in", "pk", "bd", "lk"],
    Demographic.MIDDLE_EAST: ["ae", "sa", "eg", "il", "tr"],
    Demographic.RUSSIA: ["ru"],
    Demographic.LATIN_AMERICA: ["br", "mx", "ar", "co", "cl", "pe"],
    Demographic.AFRICA: ["za", "ng", "ke", "eg"],
    Demographic.SOUTHEAST_ASIA: ["th", "vn", "id", "ph", "my"],
    Demographic.GLOBAL: ["us", "gb", "de", "jp", "in", "br"]
}

# Category keywords for app store search (kept short for store queries)
CATEGORY_KEYWORDS = {
    ProductCategory.MOBILE_APPS: ["mobile app", "app developer", "mobile application"],
    ProductCategory.ANDROID_APPS: ["android app", "android developer", "google play"],
    ProductCategory.IOS_APPS: ["ios app", "iphone app", "app store developer"],
    ProductCategory.GAMES: ["mobile game", "game developer", "gaming studio", "video game"],
    ProductCategory.ECOMMERCE: ["ecommerce", "online store", "marketplace", "shopping app"],
    ProductCategory.PRODUCT_BASED: ["product company", "product startup", "consumer product"],
    ProductCategory.ADS_BASED: ["advertising platform", "ad network", "adtech", "digital advertising"],
    ProductCategory.SAAS: ["saas", "software as a service", "cloud software", "b2b software"],
    ProductCategory.FINTECH: ["fintech", "payment app", "banking app", "crypto", "trading app"],
    ProductCategory.HEALTH_TECH: ["health app", "fitness app", "medical app", "healthcare tech"],
    ProductCategory.ED_TECH: ["education app", "learning app", "edtech", "online course"],
    ProductCategory.SOCIAL_MEDIA: ["social media app", "social network", "messaging app"],
    ProductCategory.STREAMING: ["streaming app", "video streaming", "music streaming", "ott"],
    ProductCategory.PRODUCTIVITY: ["productivity app", "task management", "note app", "calendar app"],
    ProductCategory.ENTERPRISE: ["enterprise software", "business app", "b2b app", "crm"],
    ProductCategory.JOBS: ["job board", "hiring platform", "recruitment app"],
    ProductCategory.RECRUITMENT: ["hr software", "talent acquisition", "ATS app"],
    ProductCategory.STARTUPS: ["startup", "new app", "launch app"],
}

# ============================================
# LAYER 10: MULTI-SOURCE DISCOVERY ROUTING
# ============================================
# Which discovery sources to use per category.
# "app_stores"       — Google Play, App Store, F-Droid, Microsoft, Steam, Huawei
# "web_search"       — DuckDuckGo, Bing, SearX, Google Dork company discovery
# "job_boards"       — RemoteOK API, HN Who's Hiring, web-search Indeed/Glassdoor/Wellfound
# "startup_databases"— ProductHunt, YC Directory, G2/Capterra, TechCrunch funding

CATEGORY_DISCOVERY_SOURCES = {
    # App-centric: primary=app stores, secondary=web search
    ProductCategory.MOBILE_APPS:   ["app_stores", "web_search"],
    ProductCategory.ANDROID_APPS:  ["app_stores", "web_search"],
    ProductCategory.IOS_APPS:      ["app_stores", "web_search"],
    ProductCategory.GAMES:         ["app_stores", "web_search", "startup_databases"],
    # Hybrid: both app stores and web/business sources
    ProductCategory.ECOMMERCE:     ["app_stores", "web_search", "startup_databases"],
    ProductCategory.SOCIAL_MEDIA:  ["app_stores", "web_search", "startup_databases"],
    ProductCategory.STREAMING:     ["app_stores", "web_search", "startup_databases"],
    ProductCategory.PRODUCTIVITY:  ["app_stores", "web_search", "startup_databases"],
    # Business-first: primary=web search + directories, secondary=app stores
    ProductCategory.SAAS:          ["web_search", "startup_databases", "job_boards", "app_stores"],
    ProductCategory.FINTECH:       ["web_search", "startup_databases", "job_boards", "app_stores"],
    ProductCategory.HEALTH_TECH:   ["web_search", "startup_databases", "job_boards", "app_stores"],
    ProductCategory.ED_TECH:       ["web_search", "startup_databases", "job_boards", "app_stores"],
    ProductCategory.ENTERPRISE:    ["web_search", "startup_databases", "job_boards", "app_stores"],
    ProductCategory.PRODUCT_BASED: ["web_search", "startup_databases", "app_stores"],
    ProductCategory.ADS_BASED:     ["web_search", "startup_databases", "job_boards"],
    # Non-app categories: NO app stores at all
    ProductCategory.JOBS:          ["web_search", "job_boards", "startup_databases"],
    ProductCategory.RECRUITMENT:   ["web_search", "job_boards", "startup_databases"],
    ProductCategory.STARTUPS:      ["web_search", "startup_databases", "job_boards"],
}

# Web search query templates per category — many permutations for broad discovery
# Each list contains varied queries to maximize unique company discovery
WEB_DISCOVERY_QUERIES = {
    ProductCategory.MOBILE_APPS: [
        "top mobile app companies {year}",
        "best mobile app developers list",
        "mobile app development companies hiring",
        "leading app development studios",
        "top app companies contact email",
        "mobile app startup funding {year}",
        "app developer company team about",
    ],
    ProductCategory.ANDROID_APPS: [
        "top android app development companies {year}",
        "best android developers studio",
        "android app company hiring",
        "leading android development firms",
        "android app startup list",
    ],
    ProductCategory.IOS_APPS: [
        "top ios app development companies {year}",
        "best iphone app developers studio",
        "ios development company hiring",
        "swift development firm list",
    ],
    ProductCategory.GAMES: [
        "top indie game studios {year}",
        "best game development companies",
        "gaming startups funding {year}",
        "PC game development studios list",
        "mobile game companies hiring",
        "esports companies {year}",
        "game engine companies",
        "cloud gaming startups",
        "gaming infrastructure companies",
        "indie game studio contact email team",
        "video game company CEO founder",
        "gaming startup Series A {year}",
    ],
    ProductCategory.ECOMMERCE: [
        "top ecommerce companies {year}",
        "best DTC brands startups {year}",
        "ecommerce platform companies hiring",
        "online marketplace startups",
        "headless commerce companies",
        "Shopify partner companies list",
        "ecommerce SaaS tools list",
        "retail tech startups {year}",
        "ecommerce startup CEO contact email",
        "direct to consumer brands funded {year}",
    ],
    ProductCategory.PRODUCT_BASED: [
        "top product companies {year}",
        "consumer product startups funding",
        "product company hiring team",
        "best product startups list",
        "consumer tech companies {year}",
        "hardware startup companies list",
    ],
    ProductCategory.ADS_BASED: [
        "top adtech companies {year}",
        "advertising technology startups",
        "programmatic advertising companies",
        "martech companies funded {year}",
        "DSP SSP companies list",
        "ad network companies {year}",
        "contextual advertising startups",
        "adtech startup CEO email contact",
        "digital advertising company team",
        "retail media ad tech startups",
    ],
    ProductCategory.SAAS: [
        "top B2B SaaS companies {year}",
        "best SaaS startups list {year}",
        "SaaS companies Series A funding {year}",
        "Y Combinator SaaS companies",
        "cloud software companies hiring {year}",
        "SaaS unicorns {year}",
        "top SaaS tools for sales HR marketing",
        "B2B software startup CEO contact email",
        "SaaS company team about",
        "product-led growth SaaS companies",
        "best subscription software companies",
        "SaaS startup funding announcements {year}",
        "leading SaaS vendors {year}",
        "fastest growing SaaS companies {year}",
        "enterprise SaaS companies list",
    ],
    ProductCategory.FINTECH: [
        "top fintech companies {year}",
        "fintech startups Series A {year}",
        "best fintech apps {year}",
        "fintech unicorns list {year}",
        "payments startup funding {year}",
        "neobank companies {year}",
        "insurtech startup list",
        "wealthtech companies hiring",
        "regtech companies {year}",
        "embedded finance startups",
        "financial technology CEO contact email",
        "fintech company team leadership",
        "BNPL companies list {year}",
        "crypto fintech startup list",
    ],
    ProductCategory.HEALTH_TECH: [
        "top healthtech companies {year}",
        "digital health startups funding {year}",
        "telemedicine companies list",
        "medtech startups Series A {year}",
        "health AI companies {year}",
        "remote patient monitoring companies",
        "mental health tech startups {year}",
        "healthcare SaaS companies hiring",
        "clinical software startups",
        "healthtech CEO email contact team",
        "digital therapeutics companies list",
    ],
    ProductCategory.ED_TECH: [
        "top edtech companies {year}",
        "edtech startups funding {year}",
        "online learning platforms list",
        "education technology companies hiring",
        "edtech unicorns {year}",
        "K-12 edtech startups",
        "corporate learning software companies",
        "LMS software companies list",
        "upskilling platform startups",
        "edtech company CEO contact email",
    ],
    ProductCategory.SOCIAL_MEDIA: [
        "top social media companies {year}",
        "social network startups {year}",
        "messaging app companies list",
        "social platform startup funding",
        "community platform companies {year}",
        "social media company team contact",
    ],
    ProductCategory.STREAMING: [
        "top streaming companies {year}",
        "video streaming startups",
        "music streaming companies list",
        "OTT platform companies {year}",
        "podcast platform startups",
        "streaming technology companies hiring",
    ],
    ProductCategory.PRODUCTIVITY: [
        "top productivity software companies {year}",
        "best productivity tool startups",
        "task management software companies",
        "note-taking app companies list",
        "project management tool companies {year}",
        "productivity startup CEO contact email",
        "workflow automation companies",
    ],
    ProductCategory.ENTERPRISE: [
        "top enterprise software companies {year}",
        "B2B enterprise SaaS vendors {year}",
        "enterprise software startups funding",
        "digital transformation companies",
        "enterprise tech unicorns {year}",
        "CRM software companies list",
        "ERP software companies {year}",
        "workflow automation enterprise software",
        "enterprise company CEO contact email team",
        "middleware companies {year}",
    ],
    ProductCategory.JOBS: [
        "top HR tech companies {year}",
        "job board software companies",
        "recruitment platform startups {year}",
        "hiring software companies list",
        "AI recruiting companies {year}",
        "workforce management software companies",
        "talent marketplace startups",
        "job board company CEO contact email",
        "HR technology companies hiring",
        "people analytics startups {year}",
        "staffing technology companies",
        "freelance platform companies",
    ],
    ProductCategory.RECRUITMENT: [
        "recruitment software startups {year}",
        "ATS software companies list",
        "HR SaaS companies funding {year}",
        "talent acquisition platform companies",
        "applicant tracking system companies",
        "recruiting tool startups",
        "HR tech startup CEO contact email",
        "people operations software companies",
        "HRIS software companies list",
        "employer branding companies {year}",
    ],
    ProductCategory.STARTUPS: [
        "YC companies {year} list",
        "Y Combinator batch {year}",
        "seed funded startups {year}",
        "TechStars portfolio companies {year}",
        "top startups to watch {year}",
        "fastest growing startups {year}",
        "venture backed startups hiring {year}",
        "new startup CEO contact email team",
        "startup of the year {year}",
        "500 Global portfolio companies",
        "best new startups launched {year}",
        "pre-seed startups {year}",
    ],
}

# Site-specific dork queries for deeper discovery per category
WEB_DISCOVERY_SITE_DORKS = {
    ProductCategory.SAAS: [
        'site:g2.com "top rated" SaaS',
        'site:capterra.com "best software"',
        'site:builtin.com "SaaS companies"',
        'site:techcrunch.com "SaaS" "raises" {year}',
        'site:wellfound.com SaaS startup',
    ],
    ProductCategory.FINTECH: [
        'site:crunchbase.com "fintech" funding',
        'site:techcrunch.com "fintech" "raises" {year}',
        'site:wellfound.com fintech startup',
    ],
    ProductCategory.HEALTH_TECH: [
        'site:crunchbase.com "health technology" funding',
        'site:techcrunch.com "digital health" raises {year}',
    ],
    ProductCategory.ED_TECH: [
        'site:producthunt.com "education" "learning"',
        'site:techcrunch.com "edtech" raises {year}',
    ],
    ProductCategory.GAMES: [
        'site:wellfound.com gaming startup',
        'site:techcrunch.com "gaming" "raises" {year}',
    ],
    ProductCategory.ENTERPRISE: [
        'site:g2.com "enterprise" "top rated"',
        'site:gartner.com "magic quadrant" software',
    ],
    ProductCategory.JOBS: [
        'site:g2.com "recruiting" OR "ATS" top rated',
        'site:wellfound.com HR tech startup',
    ],
    ProductCategory.RECRUITMENT: [
        'site:g2.com "applicant tracking" top rated',
        'site:capterra.com "recruiting software"',
    ],
    ProductCategory.STARTUPS: [
        'site:ycombinator.com/companies',
        'site:techcrunch.com "startup" "raises" {year}',
        'site:wellfound.com startup company',
    ],
    ProductCategory.ADS_BASED: [
        'site:techcrunch.com "adtech" raises funding',
        'site:chiefmartec.com martech company',
    ],
    ProductCategory.ECOMMERCE: [
        'site:producthunt.com "ecommerce" OR "e-commerce"',
        'site:techcrunch.com "ecommerce" raises {year}',
    ],
}

# LinkedIn dork queries per category (used with DuckDuckGo/Bing)
WEB_DISCOVERY_LINKEDIN_DORKS = {
    ProductCategory.SAAS: [
        'site:linkedin.com/company "SaaS" "software"',
        'site:linkedin.com/company "B2B software" "employees"',
    ],
    ProductCategory.FINTECH: [
        'site:linkedin.com/company "fintech" "financial technology"',
    ],
    ProductCategory.HEALTH_TECH: [
        'site:linkedin.com/company "healthtech" OR "health technology"',
    ],
    ProductCategory.ED_TECH: [
        'site:linkedin.com/company "edtech" "online learning"',
    ],
    ProductCategory.GAMES: [
        'site:linkedin.com/company "gaming" "game studio"',
    ],
    ProductCategory.ENTERPRISE: [
        'site:linkedin.com/company "enterprise software"',
    ],
    ProductCategory.JOBS: [
        'site:linkedin.com/company "HR tech" OR "recruitment"',
    ],
    ProductCategory.RECRUITMENT: [
        'site:linkedin.com/company "talent acquisition" OR "ATS"',
    ],
    ProductCategory.STARTUPS: [
        'site:linkedin.com/company "startup" "founded"',
    ],
    ProductCategory.ADS_BASED: [
        'site:linkedin.com/company "adtech" OR "advertising technology"',
    ],
    ProductCategory.ECOMMERCE: [
        'site:linkedin.com/company "ecommerce" OR "e-commerce"',
    ],
}

# Job board search terms per category (for Indeed, Glassdoor, Wellfound discovery)
JOB_BOARD_SEARCH_TERMS = {
    ProductCategory.SAAS: ["SaaS engineer", "SaaS product manager", "B2B software developer"],
    ProductCategory.FINTECH: ["fintech developer", "payments engineer", "fintech product manager"],
    ProductCategory.HEALTH_TECH: ["healthtech developer", "health tech product manager", "clinical software engineer"],
    ProductCategory.ED_TECH: ["edtech developer", "education technology engineer", "LMS developer"],
    ProductCategory.ENTERPRISE: ["enterprise software engineer", "B2B developer", "enterprise architect"],
    ProductCategory.GAMES: ["game developer", "game designer", "unity developer", "unreal engine developer"],
    ProductCategory.ECOMMERCE: ["ecommerce developer", "Shopify developer", "marketplace engineer"],
    ProductCategory.ADS_BASED: ["adtech engineer", "programmatic developer", "advertising tech"],
    ProductCategory.JOBS: ["HR tech developer", "ATS developer", "recruiting platform engineer"],
    ProductCategory.RECRUITMENT: ["HR software developer", "talent tech engineer", "HRIS developer"],
    ProductCategory.STARTUPS: ["startup engineer", "early stage developer", "startup product manager"],
    ProductCategory.PRODUCT_BASED: ["product developer", "consumer tech engineer"],
    ProductCategory.SOCIAL_MEDIA: ["social media engineer", "social platform developer"],
    ProductCategory.STREAMING: ["streaming engineer", "video platform developer"],
    ProductCategory.PRODUCTIVITY: ["productivity tool developer", "workflow engineer"],
    ProductCategory.MOBILE_APPS: ["mobile app developer", "iOS developer", "Android developer"],
    ProductCategory.ANDROID_APPS: ["Android developer", "Kotlin developer"],
    ProductCategory.IOS_APPS: ["iOS developer", "Swift developer"],
}

# Startup database topic/category slugs for directory scraping
STARTUP_DB_TOPICS = {
    ProductCategory.SAAS: {"producthunt": "saas", "yc_industry": "B2B", "g2_category": "saas"},
    ProductCategory.FINTECH: {"producthunt": "fintech", "yc_industry": "Fintech", "g2_category": "financial-services-software"},
    ProductCategory.HEALTH_TECH: {"producthunt": "health-and-fitness", "yc_industry": "Healthcare", "g2_category": "healthcare"},
    ProductCategory.ED_TECH: {"producthunt": "education", "yc_industry": "Education", "g2_category": "e-learning"},
    ProductCategory.ENTERPRISE: {"producthunt": "productivity", "yc_industry": "B2B", "g2_category": "enterprise-resource-planning-erp"},
    ProductCategory.GAMES: {"producthunt": "gaming", "yc_industry": "Consumer", "g2_category": "game-development"},
    ProductCategory.ECOMMERCE: {"producthunt": "e-commerce", "yc_industry": "Consumer", "g2_category": "e-commerce"},
    ProductCategory.ADS_BASED: {"producthunt": "marketing", "yc_industry": "B2B", "g2_category": "advertising"},
    ProductCategory.JOBS: {"producthunt": "hiring-and-recruiting", "yc_industry": "B2B", "g2_category": "recruiting"},
    ProductCategory.RECRUITMENT: {"producthunt": "hiring-and-recruiting", "yc_industry": "B2B", "g2_category": "applicant-tracking-systems-ats"},
    ProductCategory.STARTUPS: {"producthunt": "tech", "yc_industry": "B2B", "g2_category": "startup-tools"},
    ProductCategory.PRODUCT_BASED: {"producthunt": "tech", "yc_industry": "Consumer", "g2_category": "product-management"},
    ProductCategory.SOCIAL_MEDIA: {"producthunt": "social-media-marketing", "yc_industry": "Consumer", "g2_category": "social-media-management"},
    ProductCategory.STREAMING: {"producthunt": "video", "yc_industry": "Consumer", "g2_category": "video-streaming"},
    ProductCategory.PRODUCTIVITY: {"producthunt": "productivity", "yc_industry": "B2B", "g2_category": "productivity"},
    ProductCategory.MOBILE_APPS: {"producthunt": "iphone", "yc_industry": "Consumer", "g2_category": "mobile-development"},
    ProductCategory.ANDROID_APPS: {"producthunt": "android", "yc_industry": "Consumer", "g2_category": "mobile-development"},
    ProductCategory.IOS_APPS: {"producthunt": "iphone", "yc_industry": "Consumer", "g2_category": "mobile-development"},
}


@dataclass
class MobiAdzConfig:
    """Configuration for TheMobiAdz Extraction Engine V2.0 - ULTRA"""
    # Target settings
    demographics: List[Demographic] = field(default_factory=lambda: [Demographic.USA])
    categories: List[ProductCategory] = field(default_factory=lambda: [ProductCategory.MOBILE_APPS])

    # Search settings
    max_apps_per_category: int = 50
    max_companies: int = 200
    target_contacts: int = 1000  # Target number of contacts to find (100-5000)
    search_timeout: int = 30

    # Scraping depth
    website_scrape_depth: int = 8  # BFS depth levels (not page count!)
    max_pages_per_site: int = 15  # Total pages per company website (reduced from 40 to prevent hanging)

    # App Store sources (Layer 1 discovery)
    use_fdroid: bool = True  # F-Droid open source apps (developer emails often public)
    use_microsoft_store: bool = True  # Microsoft Store apps (enterprise/productivity)
    use_huawei_appgallery: bool = True  # Huawei AppGallery (Asian markets)

    # Mode
    use_paid_apis: bool = False

    # API Keys (for paid mode)
    google_api_key: Optional[str] = None
    hunter_api_key: Optional[str] = None
    apollo_api_key: Optional[str] = None
    clearbit_api_key: Optional[str] = None

    # Rate limiting
    delay_between_requests: float = 0.3

    # Output
    deduplicate: bool = True

    # Exclusion filters (for re-runs: avoid duplicating results from prior jobs)
    exclude_domains: List[str] = field(default_factory=list)  # Skip these company domains
    exclude_emails: List[str] = field(default_factory=list)  # Skip contacts with these emails

    # ========== ULTRA ENGINE SETTINGS ==========
    # AI/ML Features
    use_nlp_extraction: bool = True  # SpaCy NER for entity extraction
    use_email_permutations: bool = True  # Generate 50+ email patterns
    use_email_verification: bool = True  # Verify emails via MX records
    use_smtp_verification: bool = True  # SMTP RCPT TO verification
    use_pattern_detection: bool = True  # EmailHunter-style pattern detection
    use_catchall_detection: bool = True  # Catch-all domain detection
    use_warmup_scoring: bool = True  # Domain email infrastructure scoring
    use_role_email_discovery: bool = True  # Web search for role emails
    use_fuzzy_matching: bool = True  # Fuzzy deduplication

    # Layer 6: Email Verification Controls
    use_ms365_verification: bool = True  # Microsoft 365 GetCredentialType (free, no API key)
    use_domain_age_check: bool = True  # WHOIS domain age check (free)
    use_bounce_scoring: bool = True  # Bounce probability scoring (0-100)
    smtp_port_fallback: bool = True  # Try ports 25→587→465 with STARTTLS
    smtp_greylisting_retry: bool = True  # Retry on 450/451/452 greylisting codes
    smtp_rate_limit_delay: float = 2.0  # Min seconds between same-domain SMTP checks
    smtp_max_per_domain: int = 10  # Max SMTP checks per domain per session

    # Additional Data Sources
    use_github_extraction: bool = True  # GitHub org & commit emails
    use_npm_extraction: bool = True  # npm registry maintainer emails
    use_hackernews_mentions: bool = True  # HackerNews company mentions
    use_dns_intelligence: bool = True  # DNS TXT/MX record emails
    use_ssl_subdomains: bool = True  # Certificate Transparency subdomains
    use_wayback_machine: bool = True  # Historical email extraction

    # Advanced Data Structures
    use_bloom_filter: bool = True  # O(1) URL deduplication
    use_lru_cache: bool = True  # API response caching
    use_file_cache: bool = True  # File-based HTTP response caching with TTL
    use_circuit_breaker: bool = True  # Per-domain circuit breaker
    bloom_filter_size: int = 0  # 0 = auto-size based on target_contacts (recommended)
    bloom_filter_fp_rate: float = 0.01  # 1% false positive rate
    lru_cache_capacity: int = 2000
    file_cache_ttl: int = 86400  # 24 hour default TTL for cached responses (user-specified minimum)
    circuit_breaker_threshold: int = 3  # Failures before opening circuit
    circuit_breaker_cooldown: int = 300  # Seconds before retrying open circuit

    # Deep mode
    deep_extraction_mode: bool = True  # Enable all Ultra features

    # ========== OSINT ENGINE SETTINGS ==========
    # Enable OSINT
    use_osint: bool = True  # Enable deep OSINT for companies and people

    # OSINT Features
    osint_find_leadership: bool = True  # Find CEO, CTO, founders, directors
    osint_find_employees: bool = True  # Find employees via LinkedIn/GitHub
    osint_google_dorking: bool = True  # Use Google dorking for emails/phones
    osint_social_media: bool = True  # Find social media profiles
    osint_company_registry: bool = True  # Search OpenCorporates, SEC EDGAR
    osint_domain_intel: bool = True  # WHOIS, DNS, subdomains, tech detection
    osint_email_permutation: bool = True  # Generate email permutations for people
    osint_gravatar_lookup: bool = True  # Check Gravatar for profile photos

    # OSINT Depth
    osint_max_leadership: int = 10  # Max leadership/executives to find
    osint_max_employees: int = 20  # Max employees to find per company
    osint_max_email_permutations: int = 5  # Email variations per person

    # Layer 5: OSINT Source Controls
    osint_use_sec_edgar_officers: bool = True  # Parse SEC filings for officers (free)
    osint_use_press_releases: bool = True  # Mine press releases for exec names (free)
    osint_use_blog_authors: bool = True  # Extract blog/team page authors (free)
    osint_use_crunchbase: bool = False  # Crunchbase API (requires API key)
    osint_use_theorg: bool = False  # TheOrg.com API (requires API key)
    osint_use_patents: bool = True  # USPTO PatentsView for inventors (free)
    osint_use_conference_speakers: bool = True  # Conference speaker discovery (free)

    # Layer 5: API Keys (optional)
    brave_api_key: Optional[str] = None  # Brave Search API key
    crunchbase_api_key: Optional[str] = None  # Crunchbase Basic API key
    theorg_api_key: Optional[str] = None  # TheOrg.com API key

    # ========== FREE WEB SEARCH SETTINGS ==========
    # Enable FREE web search (DuckDuckGo, Bing, SearX - NO API keys needed)
    use_free_web_search: bool = True  # Enable multi-engine web search
    use_duckduckgo: bool = True  # DuckDuckGo (completely free)
    use_bing_search: bool = True  # Bing web search (free)
    use_searx: bool = True  # SearX meta-search (free, privacy-focused)
    use_google_dorking: bool = True  # Google dorking for targeted results
    web_search_max_results: int = 50  # Max results per search
    web_search_delay: float = 1.0  # Delay between searches to avoid rate limits
    brave_search_api_key: Optional[str] = None  # Brave Search API key for web search engine

    # ========== LAYER 7: WEB SEARCH ENHANCEMENT ==========
    search_cache_ttl: int = 86400          # 24h TTL for search result cache
    search_cache_maxsize: int = 2000       # Max cached search queries
    search_engine_rotation: bool = True    # Rotate between search engines
    search_circuit_breaker: bool = True    # Disable failing engines temporarily
    search_bing_scraping: bool = True      # Bing HTML scraping (free)
    search_searx_enabled: bool = True      # SearXNG meta-search
    search_backoff_initial: float = 1.0    # Initial backoff delay (seconds)
    search_backoff_max: float = 30.0       # Max backoff delay (seconds)

    # ========== ULTRA DEEP SEARCH V2.0 SETTINGS ==========
    # Master switch for ULTRA DEEP extraction (15+ FREE layers + 10+ PAID APIs)
    use_ultra_deep_search: bool = True  # Enable ULTRA DEEP search engine

    # FREE LAYERS (no API keys needed)
    ultra_deep_multi_engine: bool = True  # 6 search engines in parallel
    ultra_deep_archive_mining: bool = True  # Wayback, Archive.today, CommonCrawl
    ultra_deep_dns_intel: bool = True  # MX, TXT, SPF, DMARC records
    ultra_deep_whois: bool = True  # Domain WHOIS contacts
    ultra_deep_cert_transparency: bool = True  # CT logs subdomain discovery
    ultra_deep_sitemap_mining: bool = True  # Sitemap/robots.txt pages
    ultra_deep_social_media: bool = True  # LinkedIn, Twitter, Facebook discovery
    ultra_deep_developer_platforms: bool = True  # GitHub, GitLab, npm, PyPI
    ultra_deep_job_postings: bool = True  # Indeed, Glassdoor, LinkedIn Jobs
    ultra_deep_press_releases: bool = True  # PRNewswire, BusinessWire
    ultra_deep_startup_databases: bool = True  # Crunchbase, AngelList, ProductHunt
    ultra_deep_email_permutation: bool = True  # 50+ email patterns
    ultra_deep_smtp_verify: bool = True  # FREE SMTP verification

    # PAID API INTEGRATIONS (need API keys)
    ultra_deep_use_paid_apis: bool = False  # Enable PAID API integrations

    # PAID API Keys (for maximum extraction power)
    hunter_io_api_key: Optional[str] = None  # Hunter.io for email discovery
    clearbit_api_key_v2: Optional[str] = None  # Clearbit for company enrichment
    apollo_io_api_key: Optional[str] = None  # Apollo.io for contacts/leads
    rocketreach_api_key: Optional[str] = None  # RocketReach for verified emails
    snov_io_api_key: Optional[str] = None  # Snov.io client_id (user ID)
    snov_io_api_secret: Optional[str] = None  # Snov.io client_secret (separate from client_id)
    builtwith_api_key: Optional[str] = None  # BuiltWith for tech stack

    # ULTRA DEEP Performance settings
    ultra_deep_max_concurrent: int = 10  # Max concurrent requests
    ultra_deep_timeout: int = 60  # Per-layer timeout seconds
    ultra_deep_retry_count: int = 3  # Retries per failed request


@dataclass
class AppData:
    """Data structure for app/product information"""
    app_id: str
    app_name: str
    developer_name: str
    developer_id: Optional[str] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    store: str = "unknown"  # playstore, appstore, microsoft, steam
    store_url: Optional[str] = None
    developer_url: Optional[str] = None
    developer_website: Optional[str] = None
    developer_email: Optional[str] = None  # NEW: Email directly from store page
    icon_url: Optional[str] = None
    rating: Optional[float] = None
    reviews_count: Optional[int] = None
    downloads: Optional[str] = None
    price: Optional[str] = None
    description: Optional[str] = None
    demographic: Optional[str] = None
    extracted_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class CompanyContact:
    """Data structure for company contact information"""
    company_name: str
    app_or_product: Optional[str] = None
    product_category: Optional[str] = None
    demographic: Optional[str] = None

    # Company info
    company_website: Optional[str] = None
    company_domain: Optional[str] = None
    company_description: Optional[str] = None
    company_size: Optional[str] = None
    company_industry: Optional[str] = None
    company_founded: Optional[str] = None
    company_location: Optional[str] = None
    company_linkedin: Optional[str] = None
    company_phones: List[str] = field(default_factory=list)

    # Contact emails
    contact_email: Optional[str] = None  # info@, contact@, hello@
    marketing_email: Optional[str] = None
    sales_email: Optional[str] = None
    support_email: Optional[str] = None
    press_email: Optional[str] = None

    # People
    people: List[Dict[str, Any]] = field(default_factory=list)

    # App store data
    playstore_url: Optional[str] = None
    appstore_url: Optional[str] = None

    # Metadata
    data_sources: List[str] = field(default_factory=list)
    confidence_score: int = 0
    extracted_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    # Email verification status: "verified" (>80% confidence), "maybe" (50-80%), "not_verified" (<50%)
    email_verification_status: str = "not_verified"
    email_verification_confidence: int = 0
    email_mx_valid: bool = False
    email_is_disposable: bool = False
    email_is_role_based: bool = False

    # Layer 9: Per-email source tracking (which sources found each email)
    # Maps email field → list of source names, e.g. {"contact_email": ["website_contact", "dns_verified"]}
    email_sources: Dict[str, List[str]] = field(default_factory=dict)

    # Layer 9: Freshness tracking
    last_verified_at: Optional[str] = None  # ISO timestamp of last verification
    email_freshness_score: float = 1.0  # 0.0-1.0, decays over time

    # Layer 9: Domain reputation
    domain_reputation_score: int = 0  # 0-100 composite domain reputation

    # Layer 9: Role engagement score (how likely this contact responds to outreach)
    role_engagement_score: float = 0.5  # 0.0-1.0

    # Layer 15: Email warm-up score (domain infrastructure quality for deliverability)
    email_warmth_score: int = 0  # 0-100

    # Catch-all domain flag
    domain_is_catchall: bool = False


class GooglePlayScraper:
    """
    Google Play Store Scraper V2.0 - Package-Powered

    PRIMARY: Uses google-play-scraper package (pip install google-play-scraper)
    - search() for app discovery
    - app() returns developerEmail DIRECTLY (no HTML parsing!)
    FALLBACK: HTML scraping if package not installed
    """

    def __init__(self, timeout: int = 30, shared_client: Optional[httpx.AsyncClient] = None):
        self.timeout = timeout
        self.base_url = "https://play.google.com"
        self.use_package = HAS_GPLAY_SCRAPER
        self.headers = get_headers(context="navigate")
        self.client = shared_client or httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )
        if self.use_package:
            logger.info("GooglePlayScraper: Using google-play-scraper package (developerEmail direct)")
        else:
            logger.warning("GooglePlayScraper: Package not installed, falling back to HTML scraping")

    async def search_apps(
        self,
        query: str,
        country: str = "us",
        category: Optional[str] = None,
        max_results: int = 30
    ) -> List[AppData]:
        """Search for apps on Google Play Store"""
        if self.use_package:
            return await self._search_via_package(query, country, max_results)
        return await self._search_via_html(query, country, max_results)

    async def _search_via_package(
        self, query: str, country: str, max_results: int
    ) -> List[AppData]:
        """Search using google-play-scraper package"""
        apps = []
        try:
            # gplay_search is synchronous - run in thread pool
            results = await asyncio.to_thread(
                gplay_search, query, lang="en", country=country, n_hits=max_results
            )

            for result in results[:max_results]:
                app_id = result.get("appId", "")
                if not app_id:
                    continue

                # Get full details (including developerEmail) for each app
                app_data = await self.get_app_details(app_id, country)
                if app_data:
                    app_data.demographic = country
                    apps.append(app_data)

                await smart_delay(0.2)

            emails_found = sum(1 for a in apps if a.developer_email)
            logger.info(
                f"PlayStore search '{query}' ({country}): "
                f"{len(apps)} apps, {emails_found} emails (via package)"
            )

        except Exception as e:
            logger.error(f"PlayStore package search error: {e}")
            # Fallback to HTML if package fails
            return await self._search_via_html(query, country, max_results)

        return apps

    async def _search_via_html(
        self, query: str, country: str, max_results: int
    ) -> List[AppData]:
        """Fallback: Search using HTML scraping"""
        apps = []
        try:
            search_url = f"{self.base_url}/store/search"
            params = {"q": query, "c": "apps", "gl": country, "hl": "en"}

            response = await self.client.get(search_url, params=params, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                app_links = soup.select("a[href*='/store/apps/details']")

                seen_ids = set()
                for link in app_links[:max_results * 2]:
                    href = link.get("href", "")
                    if "id=" in href:
                        app_id = href.split("id=")[1].split("&")[0]
                        if app_id not in seen_ids:
                            seen_ids.add(app_id)
                            app_data = await self.get_app_details(app_id, country)
                            if app_data:
                                app_data.demographic = country
                                apps.append(app_data)
                                if len(apps) >= max_results:
                                    break
                            await smart_delay(0.5)

                logger.info(f"PlayStore search '{query}' ({country}): found {len(apps)} apps (HTML fallback)")

        except Exception as e:
            logger.error(f"PlayStore HTML search error: {e}")

        return apps

    async def get_app_details(self, app_id: str, country: str = "us") -> Optional[AppData]:
        """Get detailed app information including developerEmail"""
        if self.use_package:
            return await self._details_via_package(app_id, country)
        return await self._details_via_html(app_id, country)

    async def _details_via_package(self, app_id: str, country: str) -> Optional[AppData]:
        """Get app details using google-play-scraper package - returns developerEmail directly"""
        try:
            result = await asyncio.to_thread(
                gplay_app, app_id, lang="en", country=country
            )

            return AppData(
                app_id=app_id,
                app_name=result.get("title", app_id),
                developer_name=result.get("developer", "Unknown"),
                developer_id=result.get("developerId"),
                developer_url=f"{self.base_url}/store/apps/dev?id={result.get('developerId', '')}" if result.get("developerId") else None,
                developer_website=result.get("developerWebsite"),
                developer_email=result.get("developerEmail"),  # DIRECT from package!
                store="playstore",
                store_url=result.get("url") or f"{self.base_url}/store/apps/details?id={app_id}",
                category=result.get("genre"),
                rating=result.get("score"),
                downloads=result.get("installs"),
                description=result.get("summary") or (result.get("description", "")[:500] if result.get("description") else None)
            )

        except Exception as e:
            logger.error(f"PlayStore package details error for {app_id}: {e}")
            # Fallback to HTML
            return await self._details_via_html(app_id, country)

    async def _details_via_html(self, app_id: str, country: str) -> Optional[AppData]:
        """Fallback: Get app details using HTML scraping"""
        try:
            url = f"{self.base_url}/store/apps/details"
            params = {"id": app_id, "gl": country, "hl": "en"}

            response = await self.client.get(url, params=params, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                title_elem = soup.select_one("h1[itemprop='name']") or soup.select_one("h1")
                app_name = title_elem.get_text(strip=True) if title_elem else app_id

                dev_elem = soup.select_one("a[href*='/store/apps/dev']")
                developer_name = dev_elem.get_text(strip=True) if dev_elem else "Unknown"
                developer_url = self.base_url + dev_elem.get("href") if dev_elem else None
                developer_id = None
                if developer_url and "id=" in developer_url:
                    developer_id = developer_url.split("id=")[1].split("&")[0]

                developer_website = None
                website_elem = soup.select_one("a[aria-label*='website']") or \
                               soup.select_one("a[href^='http']:not([href*='play.google.com']):not([href*='google.com'])")
                if website_elem:
                    href = website_elem.get("href", "")
                    if href.startswith("http") and "google.com" not in href:
                        developer_website = href

                if not developer_website:
                    all_links = soup.find_all("a", href=True)
                    for link in all_links:
                        href = link.get("href", "")
                        if href.startswith("http") and not any(x in href for x in ["google.com", "play.google.com", "android.com", "youtube.com"]):
                            developer_website = href
                            break

                rating = None
                rating_elem = soup.select_one("[itemprop='ratingValue']") or soup.select_one("div[aria-label*='Rated']")
                if rating_elem:
                    rating_text = rating_elem.get("content") or rating_elem.get_text()
                    try:
                        rating = float(re.search(r'[\d.]+', rating_text).group())
                    except Exception as e:
                        logger.debug(f"Non-critical error in PlayStore rating parsing: {e}")

                downloads = None
                download_elem = soup.find(string=re.compile(r'\d+[KMB]?\+?\s*(downloads|installs)', re.I))
                if download_elem:
                    downloads = download_elem.strip()

                category_elem = soup.select_one("a[href*='/store/apps/category/']")
                category = category_elem.get_text(strip=True) if category_elem else None

                developer_email = None
                mailto_link = soup.select_one("a[href^='mailto:']")
                if mailto_link:
                    developer_email = mailto_link.get("href", "").replace("mailto:", "").split("?")[0]

                if not developer_email:
                    email_pattern = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
                    dev_sections = soup.select("div[class*='developer'], div[class*='contact'], div[class*='additional']")
                    search_text = ""
                    if dev_sections:
                        search_text = " ".join(s.get_text() for s in dev_sections)
                    else:
                        for tag in soup(["script", "style", "noscript", "link", "meta"]):
                            tag.decompose()
                        search_text = soup.get_text()

                    emails_found = email_pattern.findall(search_text)
                    NOISE_DOMAINS = {
                        "google.com", "android.com", "example.com", "email.com",
                        "sentry.io", "schema.org", "w3.org", "googleapis.com",
                        "gstatic.com", "facebook.com", "twitter.com", "play.google.com",
                        "apple.com", "microsoft.com", "github.com", "test.com",
                        "domain.com", "company.com", "yourcompany.com", "yourdomain.com",
                        "example.org", "example.net", "website.com",
                    }
                    NOISE_PREFIXES = {
                        "noreply", "no-reply", "no_reply", "donotreply", "do-not-reply",
                        "mailer-daemon", "postmaster", "webmaster", "hostmaster",
                        "abuse", "spam", "bounce", "daemon", "root", "nobody",
                        "test", "testing", "example", "autoresponder",
                    }

                    for email in emails_found:
                        email_lower = email.lower()
                        domain = email_lower.split("@")[-1] if "@" in email_lower else ""
                        prefix = email_lower.split("@")[0] if "@" in email_lower else ""
                        if domain in NOISE_DOMAINS:
                            continue
                        if prefix in NOISE_PREFIXES:
                            continue
                        tld = domain.split(".")[-1] if "." in domain else ""
                        if len(tld) < 2 or tld in {"js", "css", "png", "jpg", "svg", "gif", "woff", "ttf"}:
                            continue
                        developer_email = email
                        break

                return AppData(
                    app_id=app_id,
                    app_name=app_name,
                    developer_name=developer_name,
                    developer_id=developer_id,
                    developer_url=developer_url,
                    developer_website=developer_website,
                    developer_email=developer_email,
                    store="playstore",
                    store_url=f"{self.base_url}/store/apps/details?id={app_id}",
                    category=category,
                    rating=rating,
                    downloads=downloads
                )

        except Exception as e:
            logger.error(f"PlayStore HTML details error for {app_id}: {e}")

        return None

    async def get_developer_apps(self, developer_id: str, max_apps: int = 20) -> List[AppData]:
        """Get all apps from a developer (HTML only - package doesn't support this)"""
        apps = []
        try:
            url = f"{self.base_url}/store/apps/dev"
            params = {"id": developer_id, "hl": "en"}

            response = await self.client.get(url, params=params, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                app_links = soup.select("a[href*='/store/apps/details']")
                seen_ids = set()

                for link in app_links:
                    href = link.get("href", "")
                    if "id=" in href:
                        app_id = href.split("id=")[1].split("&")[0]
                        if app_id not in seen_ids:
                            seen_ids.add(app_id)
                            app_name = link.get_text(strip=True) or app_id
                            apps.append(AppData(
                                app_id=app_id,
                                app_name=app_name,
                                developer_name="",
                                developer_id=developer_id,
                                store="playstore",
                                store_url=f"{self.base_url}/store/apps/details?id={app_id}"
                            ))
                            if len(apps) >= max_apps:
                                break

        except Exception as e:
            logger.error(f"PlayStore developer apps error: {e}")

        return apps

    async def get_top_charts(
        self,
        category: str = "APPLICATION",
        chart: str = "topselling_free",
        country: str = "us",
        max_results: int = 50
    ) -> List[AppData]:
        """Get top charts apps (HTML-based, no package support for charts)"""
        apps = []
        try:
            url = f"{self.base_url}/store/apps/top"
            params = {"gl": country, "hl": "en"}

            response = await self.client.get(url, params=params, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                app_links = soup.select("a[href*='/store/apps/details']")
                seen_ids = set()

                for link in app_links[:max_results * 2]:
                    href = link.get("href", "")
                    if "id=" in href:
                        app_id = href.split("id=")[1].split("&")[0]
                        if app_id not in seen_ids:
                            seen_ids.add(app_id)
                            app_data = await self.get_app_details(app_id, country)
                            if app_data:
                                apps.append(app_data)
                            if len(apps) >= max_results:
                                break
                            await smart_delay(0.3)

        except Exception as e:
            logger.error(f"PlayStore top charts error: {e}")

        return apps

    async def close(self):
        await self.client.aclose()


class AppStoreScraper:
    """
    Apple App Store Scraper V2.0 - Enhanced Email Extraction

    FIXED: iTunes API returns NO developer emails. Now we:
    1. Use iTunes Search API to get app list (fast, reliable)
    2. Scrape actual App Store web pages to get developer support URL & privacy policy
    3. Extract developer emails from support/privacy URLs
    4. Mine the App Store page HTML for contact links
    """

    # Email regex for extracting from page text
    EMAIL_RE = re.compile(r'[a-zA-Z0-9][a-zA-Z0-9._%+\-]*@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

    def __init__(self, timeout: int = 30, shared_client: Optional[httpx.AsyncClient] = None):
        self.timeout = timeout
        self.search_url = "https://itunes.apple.com/search"
        self.lookup_url = "https://itunes.apple.com/lookup"
        self.web_url = "https://apps.apple.com"
        self.headers = get_headers(context="navigate")
        self.client = shared_client or httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def _scrape_appstore_page(self, app_url: str) -> Dict[str, Optional[str]]:
        """
        Scrape the actual App Store web page to extract developer contact info.
        The App Store web page contains:
        - Developer website link
        - App Support link (often has contact form or email)
        - Privacy Policy link
        These are NOT available via the iTunes API.
        """
        result = {"developer_website": None, "developer_email": None,
                  "support_url": None, "privacy_url": None}
        try:
            response = await self.client.get(app_url, headers=self.headers)
            if response.status_code != 200:
                return result

            soup = BeautifulSoup(response.text, "html.parser")

            # Look for information section links
            # App Store pages have a section with "Developer Website", "App Support", "Privacy Policy"
            for link in soup.find_all("a", href=True):
                text = link.get_text(strip=True).lower()
                href = link.get("href", "")

                if "developer website" in text or "developer web site" in text:
                    result["developer_website"] = href
                elif "app support" in text:
                    result["support_url"] = href
                elif "privacy policy" in text or "privacy" in text:
                    result["privacy_url"] = href

            # Also check for any mailto: links on the page
            for mailto in soup.select("a[href^='mailto:']"):
                email = mailto.get("href", "").replace("mailto:", "").split("?")[0].lower().strip()
                if email and "@" in email and len(email) > 5:
                    result["developer_email"] = email
                    break

            # Check meta tags for contact info
            for meta in soup.find_all("meta"):
                content = meta.get("content", "")
                if "@" in content:
                    emails = self.EMAIL_RE.findall(content)
                    if emails:
                        result["developer_email"] = emails[0].lower()
                        break

        except Exception as e:
            logger.debug(f"App Store page scrape error for {app_url}: {e}")

        return result

    async def _extract_email_from_url(self, url: str) -> Optional[str]:
        """Try to extract an email from a developer's support or privacy page."""
        if not url or not url.startswith("http"):
            return None
        try:
            response = await self.client.get(url, headers=self.headers)
            if response.status_code != 200:
                return None

            # Don't parse huge pages
            text = response.text[:50000]
            emails = self.EMAIL_RE.findall(text)

            # Filter out common false positives
            for email in emails:
                email = email.lower()
                # Skip image files, CSS, JS references
                if any(x in email for x in [".png", ".jpg", ".jpeg", ".svg", ".gif", ".css", ".js", ".woff", ".ico", ".webp", "webpack", "sentry", "chunk", "module", "bundle"]):
                    continue
                # Skip clearly-fake placeholder emails
                if any(x in email for x in ["example.com", "example.org", "email.com", "domain.com", "company.com", "your-email", "youremail", "user@", "name@", "someone@", "changeme@", "test.com"]):
                    continue
                # Skip useless-for-outreach prefixes
                prefix = email.split("@")[0] if "@" in email else ""
                if prefix in {"noreply", "no-reply", "donotreply", "do-not-reply", "mailer-daemon", "postmaster", "webmaster", "hostmaster", "abuse", "spam", "bounce", "test", "testing", "root", "nobody"}:
                    continue
                return email

        except Exception as e:
            logger.debug(f"Non-critical error in App Store developer email extraction: {e}")
        return None

    async def search_apps(
        self,
        query: str,
        country: str = "us",
        max_results: int = 30
    ) -> List[AppData]:
        """
        Search for apps on App Store using iTunes API,
        then enhance with web page scraping for developer emails.
        """
        apps = []

        try:
            params = {
                "term": query,
                "country": country,
                "media": "software",
                "limit": min(max_results, 200)
            }

            response = await self.client.get(self.search_url, params=params, headers=self.headers)

            if response.status_code == 200:
                data = response.json()

                for result in data.get("results", [])[:max_results]:
                    app_id = str(result.get("trackId", ""))

                    # From iTunes API: sellerUrl (developer website, no email)
                    developer_website = result.get("sellerUrl")
                    developer_email = None

                    # === ENHANCEMENT: Scrape actual App Store page for email ===
                    track_url = result.get("trackViewUrl")
                    if track_url:
                        try:
                            page_data = await self._scrape_appstore_page(track_url)

                            # Use page-discovered website if API didn't have one
                            if not developer_website and page_data.get("developer_website"):
                                developer_website = page_data["developer_website"]

                            # Direct email from page
                            if page_data.get("developer_email"):
                                developer_email = page_data["developer_email"]

                            # If no email yet, try support URL
                            if not developer_email and page_data.get("support_url"):
                                developer_email = await self._extract_email_from_url(
                                    page_data["support_url"]
                                )

                            # If still no email, try privacy policy URL
                            if not developer_email and page_data.get("privacy_url"):
                                developer_email = await self._extract_email_from_url(
                                    page_data["privacy_url"]
                                )

                            await smart_delay(0.2)  # Rate limit

                        except Exception as e:
                            logger.debug(f"AppStore page enhancement error for {app_id}: {e}")

                    apps.append(AppData(
                        app_id=app_id,
                        app_name=result.get("trackName", ""),
                        developer_name=result.get("artistName", ""),
                        developer_id=str(result.get("artistId", "")),
                        developer_website=developer_website,
                        developer_email=developer_email,  # NEW: extracted from page scraping
                        store="appstore",
                        store_url=track_url,
                        developer_url=result.get("artistViewUrl"),
                        category=result.get("primaryGenreName"),
                        icon_url=result.get("artworkUrl512") or result.get("artworkUrl100"),
                        rating=result.get("averageUserRating"),
                        reviews_count=result.get("userRatingCount"),
                        price=str(result.get("price", "Free")),
                        description=result.get("description", "")[:500],
                        demographic=country
                    ))

                emails_found = sum(1 for a in apps if a.developer_email)
                websites_found = sum(1 for a in apps if a.developer_website)
                logger.info(
                    f"AppStore search '{query}' ({country}): "
                    f"{len(apps)} apps, {websites_found} websites, {emails_found} emails"
                )

        except Exception as e:
            logger.error(f"AppStore search error: {e}")

        return apps

    async def get_app_details(self, app_id: str, country: str = "us") -> Optional[AppData]:
        """Get detailed app information using lookup API + page scraping"""
        try:
            params = {
                "id": app_id,
                "country": country
            }

            response = await self.client.get(self.lookup_url, params=params, headers=self.headers)

            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])

                if results:
                    result = results[0]
                    developer_website = result.get("sellerUrl")
                    developer_email = None

                    # Enhance with page scraping
                    track_url = result.get("trackViewUrl")
                    if track_url:
                        try:
                            page_data = await self._scrape_appstore_page(track_url)
                            if not developer_website and page_data.get("developer_website"):
                                developer_website = page_data["developer_website"]
                            if page_data.get("developer_email"):
                                developer_email = page_data["developer_email"]
                        except Exception as e:
                            logger.debug(f"Non-critical error in App Store page enhancement: {e}")

                    return AppData(
                        app_id=app_id,
                        app_name=result.get("trackName", ""),
                        developer_name=result.get("artistName", ""),
                        developer_id=str(result.get("artistId", "")),
                        developer_website=developer_website,
                        developer_email=developer_email,
                        store="appstore",
                        store_url=track_url,
                        developer_url=result.get("artistViewUrl"),
                        category=result.get("primaryGenreName"),
                        rating=result.get("averageUserRating"),
                        reviews_count=result.get("userRatingCount"),
                        price=str(result.get("price", "Free")),
                        demographic=country
                    )

        except Exception as e:
            logger.error(f"AppStore lookup error for {app_id}: {e}")

        return None

    async def get_developer_apps(self, developer_id: str, country: str = "us") -> List[AppData]:
        """Get all apps from a developer"""
        apps = []

        try:
            params = {
                "id": developer_id,
                "entity": "software",
                "country": country
            }

            response = await self.client.get(self.lookup_url, params=params, headers=self.headers)

            if response.status_code == 200:
                data = response.json()

                for result in data.get("results", []):
                    if result.get("wrapperType") == "software":
                        apps.append(AppData(
                            app_id=str(result.get("trackId", "")),
                            app_name=result.get("trackName", ""),
                            developer_name=result.get("artistName", ""),
                            developer_id=developer_id,
                            developer_website=result.get("sellerUrl"),
                            store="appstore",
                            store_url=result.get("trackViewUrl"),
                            category=result.get("primaryGenreName"),
                            demographic=country
                        ))

        except Exception as e:
            logger.error(f"AppStore developer apps error: {e}")

        return apps

    async def close(self):
        await self.client.aclose()


class SteamScraper:
    """
    FREE Steam Store Scraper
    Extracts game and developer information
    """

    def __init__(self, timeout: int = 30, shared_client: Optional[httpx.AsyncClient] = None):
        self.timeout = timeout
        self.base_url = "https://store.steampowered.com"
        self.api_url = "https://api.steampowered.com"
        self.headers = get_headers(context="navigate")
        self.client = shared_client or httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def search_games(
        self,
        query: str,
        max_results: int = 30
    ) -> List[AppData]:
        """Search for games on Steam"""
        games = []

        try:
            # Steam search API
            url = f"{self.base_url}/search/suggest"
            params = {
                "term": query,
                "f": "games",
                "cc": "US",
                "l": "english"
            }

            response = await self.client.get(url, params=params, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                for item in soup.select("a.match")[:max_results]:
                    href = item.get("href", "")
                    name_elem = item.select_one(".match_name")

                    if "/app/" in href:
                        app_id = href.split("/app/")[1].split("/")[0]

                        game_data = await self.get_game_details(app_id)
                        if game_data:
                            games.append(game_data)

                        await smart_delay(0.3)

        except Exception as e:
            logger.error(f"Steam search error: {e}")

        return games

    async def get_game_details(self, app_id: str) -> Optional[AppData]:
        """Get game details from Steam API"""
        try:
            url = f"{self.base_url}/api/appdetails"
            params = {"appids": app_id, "l": "english"}

            response = await self.client.get(url, params=params, headers=self.headers)

            if response.status_code == 200:
                data = response.json()

                if data.get(app_id, {}).get("success"):
                    game = data[app_id]["data"]

                    # Extract developer website
                    website = game.get("website")

                    developers = game.get("developers", ["Unknown"])
                    publisher = game.get("publishers", ["Unknown"])[0]

                    return AppData(
                        app_id=app_id,
                        app_name=game.get("name", ""),
                        developer_name=developers[0] if developers else publisher,
                        developer_website=website,
                        store="steam",
                        store_url=f"{self.base_url}/app/{app_id}",
                        category="Game",
                        subcategory=", ".join([g.get("description", "") for g in game.get("genres", [])[:3]]),
                        price=game.get("price_overview", {}).get("final_formatted", "Free"),
                        description=game.get("short_description", "")
                    )

        except Exception as e:
            logger.error(f"Steam game details error for {app_id}: {e}")

        return None

    async def get_top_sellers(self, max_results: int = 50) -> List[AppData]:
        """Get Steam top sellers"""
        games = []

        try:
            url = f"{self.base_url}/search/"
            params = {
                "filter": "topsellers",
                "cc": "US"
            }

            response = await self.client.get(url, params=params, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                for item in soup.select("a.search_result_row")[:max_results]:
                    href = item.get("href", "")
                    if "/app/" in href:
                        app_id = href.split("/app/")[1].split("/")[0]

                        game_data = await self.get_game_details(app_id)
                        if game_data:
                            games.append(game_data)

                        await smart_delay(0.3)

        except Exception as e:
            logger.error(f"Steam top sellers error: {e}")

        return games

    async def close(self):
        await self.client.aclose()


class FDroidScraper:
    """
    F-Droid Open Source App Store Scraper

    F-Droid hosts open source Android apps. Developer emails are frequently
    public in the metadata YAML files hosted on GitLab (AuthorEmail field).

    Data extraction approach:
    1. Search via F-Droid website
    2. Get app package details for source repo links
    3. Fetch metadata YAML from GitLab for AuthorEmail, AuthorName, AuthorWebSite
    """

    EMAIL_RE = re.compile(r'[a-zA-Z0-9][a-zA-Z0-9._%+\-]*@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

    def __init__(self, timeout: int = 30, shared_client: Optional[httpx.AsyncClient] = None):
        self.timeout = timeout
        self.base_url = "https://f-droid.org"
        self.search_url = "https://search.f-droid.org"
        self.metadata_base = "https://gitlab.com/fdroid/fdroiddata/-/raw/master/metadata"
        self.headers = get_headers(context="navigate")
        self.client = shared_client or httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def search_apps(
        self,
        query: str,
        max_results: int = 20
    ) -> List[AppData]:
        """Search for apps on F-Droid"""
        apps = []
        try:
            # F-Droid search page
            search_url = f"{self.search_url}/"
            params = {"q": query, "lang": "en"}

            response = await self.client.get(search_url, params=params, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                # F-Droid search results are links to package pages
                app_links = soup.select("a[href*='/packages/']")
                if not app_links:
                    # Alternative: try the main site search
                    app_links = soup.select("a[href*='f-droid.org/en/packages/']")

                seen_ids = set()
                for link in app_links[:max_results * 2]:
                    href = link.get("href", "")

                    # Extract package name from URL like /en/packages/org.example.app/
                    package_match = re.search(r'/packages/([a-zA-Z0-9_.]+)', href)
                    if package_match:
                        package_name = package_match.group(1)
                        if package_name not in seen_ids:
                            seen_ids.add(package_name)

                            app_data = await self._get_app_details(package_name)
                            if app_data:
                                apps.append(app_data)
                                if len(apps) >= max_results:
                                    break

                            await smart_delay(0.3)

                logger.info(f"F-Droid search '{query}': found {len(apps)} apps")

        except Exception as e:
            logger.error(f"F-Droid search error: {e}")

        return apps

    async def _get_app_details(self, package_name: str) -> Optional[AppData]:
        """Get app details from F-Droid metadata YAML on GitLab"""
        developer_name = "Unknown"
        developer_email = None
        developer_website = None
        app_name = package_name
        source_repo = None
        description = None

        try:
            # Try to fetch metadata YAML from GitLab (most reliable for emails)
            yml_url = f"{self.metadata_base}/{package_name}.yml"
            response = await self.client.get(yml_url, headers=self.headers)

            if response.status_code == 200:
                text = response.text
                # Parse YAML fields manually (avoid adding pyyaml dependency)
                for line in text.split("\n"):
                    line = line.strip()
                    if line.startswith("AuthorEmail:"):
                        developer_email = line.split(":", 1)[1].strip().strip("'\"")
                    elif line.startswith("AuthorName:"):
                        developer_name = line.split(":", 1)[1].strip().strip("'\"")
                    elif line.startswith("AuthorWebSite:"):
                        developer_website = line.split(":", 1)[1].strip().strip("'\"")
                    elif line.startswith("Name:") and app_name == package_name:
                        app_name = line.split(":", 1)[1].strip().strip("'\"")
                    elif line.startswith("SourceCode:"):
                        source_repo = line.split(":", 1)[1].strip().strip("'\"")
                    elif line.startswith("Summary:"):
                        description = line.split(":", 1)[1].strip().strip("'\"")

        except Exception as e:
            logger.debug(f"F-Droid metadata fetch error for {package_name}: {e}")

        # Fallback: scrape the F-Droid package page
        if not developer_email:
            try:
                page_url = f"{self.base_url}/en/packages/{package_name}/"
                response = await self.client.get(page_url, headers=self.headers)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")

                    # Look for email in page content
                    for mailto in soup.select("a[href^='mailto:']"):
                        email = mailto.get("href", "").replace("mailto:", "").split("?")[0].strip()
                        if email and "@" in email:
                            developer_email = email
                            break

                    # Look for developer/author info
                    if developer_name == "Unknown":
                        author_elem = soup.find(string=re.compile(r'Author', re.I))
                        if author_elem:
                            parent = author_elem.find_parent()
                            if parent:
                                name_text = parent.get_text(strip=True)
                                name_text = re.sub(r'^Author[:\s]*', '', name_text, flags=re.I)
                                if name_text:
                                    developer_name = name_text[:100]

                    # Get source repo link if not from YAML
                    if not source_repo:
                        for link in soup.select("a[href*='github.com'], a[href*='gitlab.com']"):
                            source_repo = link.get("href", "")
                            break

                    # Get website
                    if not developer_website:
                        for link in soup.select("a[href^='http']"):
                            href = link.get("href", "")
                            text = link.get_text(strip=True).lower()
                            if "website" in text or "homepage" in text:
                                developer_website = href
                                break

            except Exception as e:
                logger.debug(f"F-Droid page scrape error for {package_name}: {e}")

        if developer_name == "Unknown" and not developer_email:
            return None

        return AppData(
            app_id=package_name,
            app_name=app_name,
            developer_name=developer_name,
            developer_email=developer_email,
            developer_website=developer_website or source_repo,
            store="fdroid",
            store_url=f"{self.base_url}/en/packages/{package_name}/",
            description=description
        )

    async def close(self):
        await self.client.aclose()


class MicrosoftStoreScraper:
    """
    Microsoft Store Scraper

    Scrapes apps.microsoft.com for Windows/Xbox apps.
    Extracts publisher info, support links, and emails from app detail pages.
    """

    EMAIL_RE = re.compile(r'[a-zA-Z0-9][a-zA-Z0-9._%+\-]*@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

    def __init__(self, timeout: int = 30, shared_client: Optional[httpx.AsyncClient] = None):
        self.timeout = timeout
        self.base_url = "https://apps.microsoft.com"
        self.headers = get_headers(context="navigate")
        self.client = shared_client or httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def search_apps(
        self,
        query: str,
        country: str = "us",
        max_results: int = 20
    ) -> List[AppData]:
        """Search for apps on Microsoft Store"""
        apps = []
        try:
            # Scrape the Microsoft Store search page
            search_url = f"{self.base_url}/search"
            params = {
                "query": query,
                "hl": "en-us",
                "gl": country.upper()
            }

            response = await self.client.get(search_url, params=params, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                # Microsoft Store uses card-based layout
                # Look for app links in search results
                app_links = soup.select("a[href*='/detail/']")
                if not app_links:
                    app_links = soup.select("a[href*='/store/detail/']")

                seen_ids = set()
                for link in app_links[:max_results * 2]:
                    href = link.get("href", "")
                    # Extract app ID from URL
                    # URLs like: /detail/app-name/9NBLGGH4NNS1
                    parts = href.rstrip("/").split("/")
                    app_id = parts[-1] if parts else ""

                    if app_id and app_id not in seen_ids and len(app_id) > 5:
                        seen_ids.add(app_id)

                        app_data = await self._get_app_details(app_id, href, country)
                        if app_data:
                            apps.append(app_data)
                            if len(apps) >= max_results:
                                break

                        await smart_delay(0.3)

                logger.info(f"Microsoft Store search '{query}' ({country}): found {len(apps)} apps")

        except Exception as e:
            logger.error(f"Microsoft Store search error: {e}")

        return apps

    async def _get_app_details(
        self, app_id: str, relative_url: str, country: str
    ) -> Optional[AppData]:
        """Get app details from Microsoft Store detail page"""
        try:
            # Build full URL
            if relative_url.startswith("http"):
                detail_url = relative_url
            else:
                detail_url = f"{self.base_url}{relative_url}"

            response = await self.client.get(detail_url, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                # Extract app name
                title_elem = soup.select_one("h1") or soup.select_one("[class*='title']")
                app_name = title_elem.get_text(strip=True) if title_elem else app_id

                # Extract publisher/developer
                developer_name = "Unknown"
                publisher_elem = soup.find(string=re.compile(r'Publish', re.I))
                if publisher_elem:
                    parent = publisher_elem.find_parent()
                    if parent:
                        # Look for the next sibling or link
                        dev_link = parent.find("a")
                        if dev_link:
                            developer_name = dev_link.get_text(strip=True)
                        else:
                            text = parent.get_text(strip=True)
                            text = re.sub(r'^Publisher[:\s]*', '', text, flags=re.I)
                            if text:
                                developer_name = text[:100]

                # Extract developer website and email
                developer_website = None
                developer_email = None

                # Look for support/website links
                for link in soup.find_all("a", href=True):
                    text = link.get_text(strip=True).lower()
                    href = link.get("href", "")

                    if href.startswith("mailto:"):
                        email = href.replace("mailto:", "").split("?")[0].strip()
                        if email and "@" in email:
                            developer_email = email
                    elif ("website" in text or "developer" in text or "publisher" in text) and href.startswith("http"):
                        if "microsoft.com" not in href:
                            developer_website = href

                # Look for email patterns in page text
                if not developer_email:
                    # Check support/additional info sections
                    info_sections = soup.select("[class*='info'], [class*='detail'], [class*='support']")
                    for section in info_sections:
                        emails = self.EMAIL_RE.findall(section.get_text())
                        for email in emails:
                            if not any(x in email.lower() for x in ["microsoft.com", "example.com", "sentry.io"]):
                                developer_email = email
                                break
                        if developer_email:
                            break

                # Extract category
                category = None
                cat_elem = soup.find(string=re.compile(r'Category', re.I))
                if cat_elem:
                    parent = cat_elem.find_parent()
                    if parent:
                        cat_link = parent.find("a")
                        if cat_link:
                            category = cat_link.get_text(strip=True)

                # Extract rating
                rating = None
                rating_elem = soup.select_one("[class*='rating']")
                if rating_elem:
                    try:
                        rating_text = rating_elem.get_text()
                        rating_match = re.search(r'([\d.]+)', rating_text)
                        if rating_match:
                            rating = float(rating_match.group(1))
                    except Exception as e:
                        logger.debug(f"Non-critical error in Microsoft Store rating parsing: {e}")

                return AppData(
                    app_id=app_id,
                    app_name=app_name,
                    developer_name=developer_name,
                    developer_email=developer_email,
                    developer_website=developer_website,
                    store="microsoft",
                    store_url=detail_url,
                    category=category,
                    rating=rating,
                    demographic=country
                )

        except Exception as e:
            logger.error(f"Microsoft Store detail error for {app_id}: {e}")

        return None

    async def close(self):
        await self.client.aclose()


class HuaweiAppGalleryScraper:
    """
    Huawei AppGallery Scraper

    Huawei AppGallery is the 3rd largest app store globally.
    Strong in Asian markets (China, Southeast Asia, Middle East).

    Uses the AppGallery web search + detail page scraping.
    """

    EMAIL_RE = re.compile(r'[a-zA-Z0-9][a-zA-Z0-9._%+\-]*@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

    def __init__(self, timeout: int = 30, shared_client: Optional[httpx.AsyncClient] = None):
        self.timeout = timeout
        self.base_url = "https://appgallery.huawei.com"
        self.headers = get_headers(context="navigate")
        self.client = shared_client or httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def search_apps(
        self,
        query: str,
        country: str = "us",
        max_results: int = 20
    ) -> List[AppData]:
        """Search for apps on Huawei AppGallery"""
        apps = []
        try:
            # Use the Huawei AppGallery internal search API
            api_url = "https://web-dre.hispace.dbankcloud.cn/uowap/index"
            params = {
                "method": "internal.getTabDetail",
                "serviceType": "20",
                "reqPageNum": "1",
                "maxResults": str(min(max_results, 25)),
                "uri": f"searchword%7C{quote(query)}",
                "locale": "en_US",
                "zone": "",
            }

            response = await self.client.get(api_url, params=params, headers=self.headers)

            if response.status_code == 200:
                try:
                    data = response.json()
                    layout_data = data.get("layoutData", [])

                    for section in layout_data:
                        data_list = section.get("dataList", [])
                        for item in data_list:
                            if len(apps) >= max_results:
                                break

                            app_id = item.get("appid", "")
                            app_name = item.get("name", "")
                            developer_name = item.get("developer", "") or item.get("devName", "")

                            if app_id and app_name:
                                # Get detail page for email/website
                                detail_data = await self._get_app_details(app_id)

                                apps.append(AppData(
                                    app_id=str(app_id),
                                    app_name=app_name,
                                    developer_name=developer_name or "Unknown",
                                    developer_email=detail_data.get("email") if detail_data else None,
                                    developer_website=detail_data.get("website") if detail_data else None,
                                    store="huawei",
                                    store_url=f"{self.base_url}/#/app/{app_id}",
                                    category=item.get("category", ""),
                                    rating=item.get("score"),
                                    downloads=item.get("downCountDesc"),
                                    demographic=country
                                ))

                except (json.JSONDecodeError, KeyError) as e:
                    logger.debug(f"Huawei API JSON parse error: {e}")

            # Fallback: scrape the web search page
            if not apps:
                apps = await self._search_via_web(query, country, max_results)

            logger.info(f"Huawei AppGallery search '{query}' ({country}): found {len(apps)} apps")

        except Exception as e:
            logger.error(f"Huawei AppGallery search error: {e}")

        return apps

    async def _get_app_details(self, app_id: str) -> Optional[Dict[str, Optional[str]]]:
        """Get app details from Huawei AppGallery detail page"""
        try:
            # Try the internal API for app detail
            api_url = "https://web-dre.hispace.dbankcloud.cn/uowap/index"
            params = {
                "method": "internal.getTabDetail",
                "serviceType": "20",
                "reqPageNum": "1",
                "uri": f"app%7C{app_id}",
                "locale": "en_US",
            }

            response = await self.client.get(api_url, params=params, headers=self.headers)
            if response.status_code == 200:
                try:
                    data = response.json()
                    layout_data = data.get("layoutData", [])
                    for section in layout_data:
                        data_list = section.get("dataList", [])
                        for item in data_list:
                            email = item.get("developerEmail") or item.get("devEmail")
                            website = item.get("developerWebsite") or item.get("devWebsite") or item.get("webUrl")
                            if email or website:
                                return {"email": email, "website": website}
                except (json.JSONDecodeError, KeyError):
                    pass

            # Fallback: scrape the web page
            page_url = f"{self.base_url}/#/app/{app_id}"
            response = await self.client.get(page_url, headers=self.headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                email = None
                website = None

                # Look for mailto links
                for mailto in soup.select("a[href^='mailto:']"):
                    email = mailto.get("href", "").replace("mailto:", "").split("?")[0].strip()
                    if email and "@" in email:
                        break

                # Look for developer website
                for link in soup.find_all("a", href=True):
                    href = link.get("href", "")
                    if href.startswith("http") and "huawei" not in href and "dbankcloud" not in href:
                        website = href
                        break

                if email or website:
                    return {"email": email, "website": website}

        except Exception as e:
            logger.debug(f"Huawei detail error for {app_id}: {e}")

        return None

    async def _search_via_web(
        self, query: str, country: str, max_results: int
    ) -> List[AppData]:
        """Fallback: scrape the web search page"""
        apps = []
        try:
            search_url = f"{self.base_url}/#/search/{quote(query)}"
            response = await self.client.get(search_url, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                # Look for app cards/links
                app_links = soup.select("a[href*='/app/']")
                seen_ids = set()

                for link in app_links[:max_results * 2]:
                    href = link.get("href", "")
                    app_id_match = re.search(r'/app/(\w+)', href)
                    if app_id_match:
                        app_id = app_id_match.group(1)
                        if app_id not in seen_ids:
                            seen_ids.add(app_id)

                            app_name = link.get_text(strip=True) or app_id

                            apps.append(AppData(
                                app_id=app_id,
                                app_name=app_name,
                                developer_name="Unknown",
                                store="huawei",
                                store_url=f"{self.base_url}/#/app/{app_id}",
                                demographic=country
                            ))

                            if len(apps) >= max_results:
                                break

        except Exception as e:
            logger.debug(f"Huawei web search error: {e}")

        return apps

    async def close(self):
        await self.client.aclose()


class CompanyWebsiteScraper:
    """
    Deep Company Website Scraper V2.0 - Bulletproof BFS Crawler

    FIXED: Proper BFS depth tracking (was: depth++ per page, now: per BFS level)
    NEW: Sitemap/robots.txt parsing, structured data extraction,
         security.txt/humans.txt, smart URL prioritization, UA rotation
    """

    # High-priority pages crawled first (depth 0 - known paths)
    PRIORITY_PAGES = [
        "/contact", "/contact-us", "/contactus", "/get-in-touch",
        "/about", "/about-us", "/aboutus", "/who-we-are",
        "/team", "/our-team", "/leadership", "/management", "/executives",
        "/company", "/company/about", "/company/team", "/company/contact",
        # Developer/Engineering pages - often have team emails
        "/developers", "/developer", "/dev", "/engineering",
        "/devrel", "/developer-relations",
        "/community", "/open-source", "/oss",
        # International contact pages
        "/kontakt", "/kontakta-oss", "/contacto", "/contatti",
        # Founder/CEO pages
        "/founder", "/founders", "/ceo", "/co-founders",
        # Reach-out / inquiry pages
        "/inquiry", "/enquiry", "/reach-out", "/write-to-us",
        "/feedback", "/request-demo", "/demo",
    ]

    # Medium-priority pages (depth 0 but crawled after priority)
    SECONDARY_PAGES = [
        "/support", "/help", "/help-center",
        "/press", "/media", "/newsroom", "/news",
        "/careers", "/jobs", "/work-with-us",
        "/partners", "/partnerships", "/become-a-partner",
        "/advertise", "/advertising", "/media-kit",
        "/business", "/enterprise", "/b2b",
        "/investors", "/investor-relations",
        "/legal", "/privacy", "/privacy-policy", "/imprint", "/impressum",
        # Additional developer/team pages
        "/contribute", "/contributing", "/docs/contact",
        "/api", "/api/contact", "/integrations",
        "/research", "/labs", "/blog/contact",
        "/people", "/staff", "/directory",
        # Frequently overlooked pages with emails
        "/info", "/faq", "/resources",
        "/sponsor", "/sponsorship",
        "/vendors", "/suppliers",
        "/distributors", "/resellers",
        "/affiliate", "/affiliates",
        "/colophon", "/credits",
        "/sitemap",
    ]

    # Special files that often contain contact info
    SPECIAL_FILES = [
        "/.well-known/security.txt", "/security.txt",
        "/humans.txt",
    ]

    # Robust email regex (fixed: no literal | in char class)
    GENERAL_EMAIL_RE = re.compile(
        r'[a-zA-Z0-9][a-zA-Z0-9._%+\-]*@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
        re.IGNORECASE
    )

    # Email classification prefixes - COMPREHENSIVE for all department types
    EMAIL_CLASSIFY = {
        "contact": {
            "contact", "info", "hello", "hi", "general", "enquiry", "inquiry", "ask",
            "team", "staff", "office", "company", "main", "reception", "front",
            "feedback", "suggestions", "write", "reach", "connect",
            "dev", "developer", "developers", "devrel", "engineering", "engineer",
            "tech", "technical", "code", "api", "opensource", "community",
            "research", "science", "data", "product", "design",
        },
        "marketing": {
            "marketing", "ads", "advertising", "media", "pr", "press", "communications", "comms",
            "growth", "brand", "content", "social", "creative", "events", "webinar",
            "newsletter", "news", "newsroom", "editor", "editorial", "blog",
            "affiliate", "referral", "sponsor", "sponsorship", "influencer",
        },
        "sales": {
            "sales", "business", "enterprise", "partner", "partnerships", "deals", "commercial",
            "demo", "pricing", "quote", "proposal", "vendor", "procurement",
            "reseller", "wholesale", "b2b", "agency", "client", "accounts",
        },
        "support": {
            "support", "help", "care", "service", "customer", "helpdesk", "ticket",
            "abuse", "security", "report", "bug", "bugs", "issues", "troubleshoot",
            "onboarding", "success", "training", "docs", "documentation",
        },
        "press": {
            "press", "media", "journalist", "editor", "newsroom", "publicrelations",
            "spokesperson", "mediarelations", "pressoffice",
        },
        "hr": {
            "hr", "career", "careers", "jobs", "recruit", "recruiting", "recruitment",
            "hiring", "talent", "people", "humanresources", "apply", "employment",
            "internship", "intern", "internships",
        },
        "legal": {
            "legal", "compliance", "privacy", "gdpr", "dpo", "dataprotection",
            "terms", "copyright", "dmca", "ip", "regulatory",
        },
        "finance": {
            "finance", "billing", "accounts", "invoicing", "payments", "accounting",
            "invoice", "payable", "receivable", "treasury", "tax",
        },
    }

    # User-Agent pool imported from browser_profiles (26+ modern profiles)
    # Keep class-level alias for backward compat with any code referencing self.USER_AGENTS
    USER_AGENTS = SEARCH_USER_AGENTS

    # URL patterns that are wasteful to crawl
    SKIP_URL_PATTERNS = re.compile(
        r'\.(css|js|png|jpg|jpeg|gif|svg|ico|woff2?|ttf|eot|mp[34]|avi|mov|zip|tar|gz)(\?.*)?$',
        re.IGNORECASE
    )

    # PDF/document URLs — not skipped, but routed to PDF extractor instead of HTML parser
    PDF_URL_PATTERN = re.compile(r'\.(pdf|doc[x]?)(\?.*)?$', re.IGNORECASE)

    # URL patterns that are likely to have contact info (for prioritization)
    CONTACT_URL_KEYWORDS = re.compile(
        r'contact|about|team|leadership|people|staff|management|company|press|investor|imprint|impressum'
        r'|developer|engineering|devrel|community|careers|support|partners|directory',
        re.IGNORECASE
    )

    def __init__(self, timeout: int = 30, max_depth: int = 8, shared_client: Optional[httpx.AsyncClient] = None):
        self.timeout = timeout
        self.max_depth = max_depth  # BFS depth levels (not page count!)
        # Consistent browser session — same UA/Accept-Language for entire crawl
        self._session = BrowserSession()
        self._ua = self._session.user_agent
        self.headers = self._session.get_headers()
        # Use shared connection pool if provided, otherwise create own
        self.client = shared_client or httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )
        self._owns_client = shared_client is None  # Track ownership for cleanup
        # Lazy-initialized Playwright browser for SPA fallback
        self._playwright = None
        self._browser = None
        self._js_rendered_count = 0  # Track how many pages were JS-rendered
        # Crawl-delay from robots.txt (populated during sitemap parsing)
        self._robots_crawl_delay: Optional[float] = None
        # Per-page Referer tracking for realistic browsing patterns
        self._last_page_url: Optional[str] = None

        # Build EmailPatternTrie for O(prefix) classification (faster than keyword iteration)
        self._email_trie = EmailPatternTrie()
        self._email_trie.build_from_categories(self.EMAIL_CLASSIFY)

    def _classify_email(self, email: str) -> str:
        """Classify email by its prefix using Trie (O(prefix_len)) with keyword fallback."""
        # Fast path: Trie lookup
        trie_result = self._email_trie.classify(email)
        if trie_result:
            return trie_result["category"]
        # Fallback: keyword substring matching (catches compound prefixes like "dev-marketing")
        prefix = email.split("@")[0].lower()
        for category, keywords in self.EMAIL_CLASSIFY.items():
            if any(kw in prefix for kw in keywords):
                return category
        return "other"

    # Emails from these prefixes are useless for cold outreach
    _USELESS_COLD_OUTREACH_PREFIXES = {
        "noreply", "no-reply", "no_reply", "donotreply", "do-not-reply",
        "mailer-daemon", "postmaster", "hostmaster", "webmaster",
        "abuse", "spam", "bounce", "daemon",
        "example", "test", "testing", "root", "nobody",
        "autoresponder", "auto-reply", "autoreply",
        "unsubscribe", "remove", "optout",
    }

    # Template/placeholder email patterns
    _PLACEHOLDER_PATTERNS = {
        "your-email", "youremail", "your.email", "user@", "name@",
        "email@", "someone@", "john.doe@", "jane.doe@", "firstname@",
        "lastname@", "username@", "changeme@", "placeholder@",
        "yourname@", "your_name@",
    }

    # Noise domains that are never real contact emails
    _NOISE_DOMAINS = {
        "example.com", "example.org", "example.net", "test.com",
        "email.com", "domain.com", "company.com", "website.com",
        "yourcompany.com", "yourdomain.com", "yoursite.com",
        "sentry.io", "schema.org", "w3.org", "googleapis.com",
        "gstatic.com", "cloudfront.net", "amazonaws.com",
        "herokuapp.com", "wixsite.com", "squarespace.com",
        "wordpress.com", "wordpress.org", "wpengine.com",
        "gravatar.com", "shields.io", "badge.fury.io",
        "travis-ci.org", "circleci.com", "codecov.io",
        "readthedocs.org", "readthedocs.io",
    }

    def _is_valid_email(self, email: str, domain: str) -> bool:
        """Filter out false-positive emails (CSS classes, JS vars, image filenames, placeholders, useless role emails)."""
        email = email.lower().strip()
        # Must have @ and valid TLD
        if "@" not in email or len(email) < 6:
            return False
        # Skip common false positives
        local, at_domain = email.split("@", 1)
        if len(local) < 2 or len(at_domain) < 4:
            return False
        # Skip image/asset references (in both local and domain parts)
        if any(ext in at_domain for ext in [".png", ".jpg", ".jpeg", ".svg", ".gif", ".css", ".js", ".woff", ".woff2", ".ttf", ".eot", ".ico", ".webp", ".bmp", ".tiff"]):
            return False
        if any(ext in local for ext in [".png", ".jpg", ".svg", ".gif", ".css", ".js"]):
            return False
        # Skip sentry, webpack, and other non-email patterns
        if any(x in local for x in ["webpack", "sentry", "chunk", "module", "0x", "data-", "font-", "icon-", "img-", "image-", "asset", "static", "bundle", "vendor"]):
            return False
        # Must end with valid TLD (2-10 chars)
        tld = at_domain.rsplit(".", 1)[-1] if "." in at_domain else ""
        if not tld or len(tld) < 2 or len(tld) > 10 or not tld.isalpha():
            return False
        # Skip template/placeholder emails
        if any(p in email for p in self._PLACEHOLDER_PATTERNS):
            return False
        # Skip noise domains (CDNs, CI tools, schema providers, etc.)
        if at_domain in self._NOISE_DOMAINS:
            return False
        # Skip useless-for-outreach prefixes (noreply, abuse, postmaster, etc.)
        if local in self._USELESS_COLD_OUTREACH_PREFIXES:
            return False
        # Also match prefixes with dots/hyphens (e.g. no-reply.alerts)
        base_local = local.split(".")[0].split("-")[0].split("_")[0]
        if base_local in self._USELESS_COLD_OUTREACH_PREFIXES:
            return False
        return True

    def _should_skip_url(self, url: str) -> bool:
        """Check if URL should be skipped (static assets, anchors, etc.)."""
        if self.SKIP_URL_PATTERNS.search(url):
            return True
        parsed = urlparse(url)
        # Skip fragment-only links, javascript:, mailto:, tel:
        if parsed.scheme in ("javascript", "mailto", "tel", "data"):
            return True
        if not parsed.netloc and not parsed.path:
            return True
        return False

    def _url_priority_score(self, url: str) -> int:
        """Score URL by likelihood of containing contact info (lower = higher priority)."""
        if self.CONTACT_URL_KEYWORDS.search(url):
            return 0  # High priority
        path = urlparse(url).path.lower()
        if any(p in path for p in ["/blog", "/news", "/article", "/post"]):
            return 3  # Low priority - blogs rarely have contact emails
        return 1  # Normal priority

    async def _fetch_sitemap_urls(self, domain: str) -> List[str]:
        """Parse sitemap.xml and robots.txt to discover contact/about pages."""
        contact_urls = []
        base_url = f"https://{domain}"

        # 1. Check robots.txt for Sitemap directives AND Crawl-delay
        sitemap_urls_to_check = [f"{base_url}/sitemap.xml"]
        try:
            robots_resp = await self.client.get(
                f"{base_url}/robots.txt", headers=self.headers
            )
            if robots_resp.status_code == 200:
                in_wildcard_block = False
                for line in robots_resp.text.splitlines():
                    line = line.strip()
                    if line.lower().startswith("user-agent:"):
                        agent = line.split(":", 1)[1].strip()
                        in_wildcard_block = (agent == "*")
                    elif line.lower().startswith("sitemap:"):
                        sitemap_url = line.split(":", 1)[1].strip()
                        if sitemap_url and sitemap_url not in sitemap_urls_to_check:
                            sitemap_urls_to_check.append(sitemap_url)
                    elif line.lower().startswith("crawl-delay:") and in_wildcard_block:
                        try:
                            crawl_delay = float(line.split(":", 1)[1].strip())
                            # Respect Crawl-delay but cap at 30s to prevent abuse
                            crawl_delay = min(max(crawl_delay, 0.5), 30.0)
                            self._robots_crawl_delay = crawl_delay
                            logger.info(f"Respecting Crawl-delay: {crawl_delay}s for {domain}")
                        except (ValueError, IndexError):
                            pass
        except Exception as e:
            logger.debug(f"Non-critical error in robots.txt fetch: {e}")

        # 2. Parse sitemaps for contact/about/team pages
        for sitemap_url in sitemap_urls_to_check[:3]:  # Max 3 sitemaps
            try:
                resp = await self.client.get(sitemap_url, headers=self.headers)
                if resp.status_code == 200 and resp.text.strip():
                    # Handle both XML sitemaps and sitemap index files
                    text = resp.text
                    # Extract URLs using regex (more forgiving than XML parser)
                    loc_matches = re.findall(r'<loc>(.*?)</loc>', text, re.IGNORECASE)
                    for url in loc_matches:
                        url = url.strip()
                        # Check if it's a sub-sitemap
                        if url.endswith('.xml') or 'sitemap' in url.lower():
                            try:
                                sub_resp = await self.client.get(url, headers=self.headers)
                                if sub_resp.status_code == 200:
                                    sub_locs = re.findall(r'<loc>(.*?)</loc>', sub_resp.text, re.IGNORECASE)
                                    for sub_url in sub_locs:
                                        if self.CONTACT_URL_KEYWORDS.search(sub_url):
                                            contact_urls.append(sub_url.strip())
                            except Exception as e:
                                logger.debug(f"Non-critical error in sub-sitemap fetch: {e}")
                        elif self.CONTACT_URL_KEYWORDS.search(url):
                            contact_urls.append(url)
            except Exception as e:
                logger.debug(f"Non-critical error in sitemap parsing: {e}")

        return contact_urls[:20]  # Cap at 20 sitemap-discovered URLs

    async def _fetch_special_files(self, domain: str) -> Dict[str, Any]:
        """Check security.txt and humans.txt for contact info."""
        extra_data = {"emails": set(), "people": [], "phone": None}
        base_url = f"https://{domain}"

        for file_path in self.SPECIAL_FILES:
            try:
                resp = await self.client.get(
                    f"{base_url}{file_path}", headers=self.headers
                )
                if resp.status_code == 200 and len(resp.text) < 10000:
                    text = resp.text
                    # Extract emails
                    emails = self.GENERAL_EMAIL_RE.findall(text)
                    for e in emails:
                        if self._is_valid_email(e, domain):
                            extra_data["emails"].add(e.lower())
                    # security.txt specific: Contact field
                    for line in text.splitlines():
                        if line.lower().startswith("contact:"):
                            val = line.split(":", 1)[1].strip()
                            if "@" in val:
                                email = val.replace("mailto:", "")
                                if self._is_valid_email(email, domain):
                                    extra_data["emails"].add(email.lower())
            except Exception as e:
                logger.debug(f"Non-critical error in special file scraping: {e}")
            await smart_delay(0.1)

        return extra_data

    def _extract_structured_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract contact info from JSON-LD, Schema.org, and OpenGraph structured data."""
        structured = {"emails": set(), "phones": set(), "social": {}, "company": {}}

        # 1. JSON-LD (@type: Organization, LocalBusiness, etc.)
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                # Handle both single object and array
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    # Email from contactPoint or direct email field
                    for key in ["email", "contactPoint"]:
                        val = item.get(key)
                        if isinstance(val, str) and "@" in val:
                            structured["emails"].add(val.lower().replace("mailto:", ""))
                        elif isinstance(val, list):
                            for cp in val:
                                if isinstance(cp, dict):
                                    e = cp.get("email", "")
                                    if "@" in e:
                                        structured["emails"].add(e.lower().replace("mailto:", ""))
                        elif isinstance(val, dict):
                            e = val.get("email", "")
                            if "@" in e:
                                structured["emails"].add(e.lower().replace("mailto:", ""))
                    # Telephone
                    phone = item.get("telephone")
                    if phone:
                        structured["phones"].add(str(phone))
                    # Name
                    name = item.get("name")
                    if name and isinstance(name, str):
                        structured["company"]["name"] = name
                    # Social profiles from sameAs
                    same_as = item.get("sameAs", [])
                    if isinstance(same_as, list):
                        for url in same_as:
                            if "linkedin.com" in str(url):
                                structured["social"]["linkedin"] = url
                            elif "twitter.com" in str(url) or "x.com" in str(url):
                                structured["social"]["twitter"] = url
                            elif "facebook.com" in str(url):
                                structured["social"]["facebook"] = url
                    # Address
                    addr = item.get("address")
                    if isinstance(addr, dict):
                        parts = [addr.get("streetAddress", ""), addr.get("addressLocality", ""),
                                 addr.get("addressRegion", ""), addr.get("postalCode", ""),
                                 addr.get("addressCountry", "")]
                        full = ", ".join(p for p in parts if p)
                        if full:
                            structured["company"]["address"] = full
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue

        # 2. HTML Microdata (itemscope/itemprop) - another Schema.org format
        org_types = re.compile(r'schema\.org/(Organization|LocalBusiness|Corporation|Company)', re.I)
        for org in soup.find_all(attrs={"itemtype": org_types}):
            try:
                # Email
                email_elem = org.find(attrs={"itemprop": "email"})
                if email_elem:
                    email = (email_elem.get("content") or
                             email_elem.get("href", "").replace("mailto:", "") or
                             email_elem.get_text(strip=True))
                    if "@" in email:
                        structured["emails"].add(email.lower().strip())

                # Telephone
                phone_elem = org.find(attrs={"itemprop": "telephone"})
                if phone_elem:
                    phone = phone_elem.get("content") or phone_elem.get_text(strip=True)
                    if phone:
                        structured["phones"].add(phone)

                # Name
                name_elem = org.find(attrs={"itemprop": "name"})
                if name_elem:
                    name = name_elem.get("content") or name_elem.get_text(strip=True)
                    if name and not structured["company"].get("name"):
                        structured["company"]["name"] = name

                # Address (nested itemprop)
                addr_elem = org.find(attrs={"itemprop": "address"})
                if addr_elem and not structured["company"].get("address"):
                    parts = []
                    for prop in ["streetAddress", "addressLocality", "addressRegion",
                                 "postalCode", "addressCountry"]:
                        elem = addr_elem.find(attrs={"itemprop": prop})
                        if elem:
                            parts.append(elem.get("content") or elem.get_text(strip=True))
                    if parts:
                        structured["company"]["address"] = ", ".join(p for p in parts if p)

                # ContactPoint (nested itemtype)
                cp_type = re.compile(r'schema\.org/ContactPoint', re.I)
                for cp in org.find_all(attrs={"itemtype": cp_type}):
                    cp_email = cp.find(attrs={"itemprop": "email"})
                    if cp_email:
                        email = (cp_email.get("content") or
                                 cp_email.get("href", "").replace("mailto:", "") or
                                 cp_email.get_text(strip=True))
                        if "@" in email:
                            structured["emails"].add(email.lower().strip())
                    cp_phone = cp.find(attrs={"itemprop": "telephone"})
                    if cp_phone:
                        phone = cp_phone.get("content") or cp_phone.get_text(strip=True)
                        if phone:
                            structured["phones"].add(phone)
            except Exception as e:
                logger.debug(f"Non-critical error in structured data extraction: {e}")
                continue

        return structured

    async def scrape_company(
        self,
        domain: str,
        max_pages: int = 30
    ) -> Dict[str, Any]:
        """
        Deep scrape a company website for contacts and information.

        FIXED: Uses proper BFS depth tracking.
        - max_depth controls how many link-levels deep we go (default 8)
        - max_pages controls total pages crawled (default 30)
        - Priority pages (contact/about/team) are crawled at depth 0
        - Sitemap/robots.txt parsed for additional discovery
        - Structured data (JSON-LD) extracted for high-quality contact info
        """
        result = {
            "domain": domain,
            "emails": {
                "contact": set(),
                "marketing": set(),
                "sales": set(),
                "support": set(),
                "press": set(),
                "hr": set(),
                "legal": set(),
                "finance": set(),
                "other": set()
            },
            # Track which page each email was found on for confidence scoring
            # Maps email -> {"page_url": str, "page_type": "contact"|"about"|"team"|"footer"|"other", "confidence": float}
            "email_page_sources": {},
            "social_links": {
                "linkedin": None,
                "twitter": None,
                "facebook": None,
                "instagram": None
            },
            "company_info": {
                "name": None,
                "description": None,
                "address": None,
                "phone": None
            },
            "people": [],
            "pages_scraped": 0,
            "pdfs_processed": 0,
            "js_rendered_pages": 0
        }

        base_url = f"https://{domain}"
        scraped_urls: Set[str] = set()

        # === PRIORITY URL QUEUE: replaces deque for intelligent scheduling ===
        # Contact/team pages get highest priority, blog/news get lowest
        url_queue = PriorityURLQueue()

        # Priority 100: Homepage
        url_queue.push(base_url, priority=100, depth=0)

        # Priority 90: High-value contact pages (most likely to have emails)
        for page in self.PRIORITY_PAGES:
            url_queue.push(f"{base_url}{page}", priority=90, depth=0)

        # Priority 70: Secondary pages (support, press, careers, legal)
        for page in self.SECONDARY_PAGES:
            url_queue.push(f"{base_url}{page}", priority=70, depth=0)

        # === PARALLEL: Fetch sitemap URLs and special files concurrently ===
        sitemap_task = asyncio.create_task(self._fetch_sitemap_urls(domain))
        special_files_task = asyncio.create_task(self._fetch_special_files(domain))

        # Priority 50: Sitemap-discovered pages
        try:
            sitemap_urls = await asyncio.wait_for(sitemap_task, timeout=10)
            for url in sitemap_urls:
                url_queue.push(url, priority=50, depth=0)
        except (asyncio.TimeoutError, Exception):
            pass

        # Get special file data (security.txt, humans.txt)
        try:
            special_data = await asyncio.wait_for(special_files_task, timeout=8)
            for email in special_data.get("emails", set()):
                cat = self._classify_email(email)
                result["emails"][cat].add(email)
        except (asyncio.TimeoutError, Exception):
            pass

        # === MAIN PRIORITY-DRIVEN CRAWL ===
        # Circuit breaker reference (injected by engine if available)
        cb = getattr(self, '_circuit_breaker', None)
        consecutive_failures = 0
        max_consecutive_failures = 10  # Per-site fallback circuit breaker
        pdfs_processed = 0
        max_pdfs_per_site = 5

        while len(url_queue) > 0 and result["pages_scraped"] < max_pages:
            # Check per-domain circuit breaker first
            if cb and cb.is_open(domain):
                logger.debug(f"[CIRCUIT BREAKER] Skipping {domain} - circuit is open")
                break

            popped = url_queue.pop()
            if popped is None:
                break
            current_url, current_depth = popped

            # Skip if already scraped
            if current_url in scraped_urls:
                continue

            # Don't follow links deeper than max_depth
            if current_depth > self.max_depth:
                continue

            # Skip static assets (but NOT PDFs - they go to PDF extractor)
            if self._should_skip_url(current_url):
                continue

            scraped_urls.add(current_url)

            try:
                # === PDF EXTRACTION: Route PDF/doc URLs to dedicated extractor ===
                if self.PDF_URL_PATTERN.search(current_url):
                    if pdfs_processed < max_pdfs_per_site:
                        pdf_emails = await self._extract_emails_from_pdf(current_url, domain)
                        for email in pdf_emails:
                            cat = self._classify_email(email)
                            result["emails"][cat].add(email)
                        pdfs_processed += 1
                        result["pdfs_processed"] = pdfs_processed
                    continue  # Don't try to parse PDF as HTML

                page_data = await self._scrape_page(current_url, domain)

                if page_data:
                    result["pages_scraped"] += 1
                    consecutive_failures = 0
                    if cb:
                        cb.record_success(domain)

                    # Collect emails and track page source for confidence scoring
                    page_path = urlparse(current_url).path.lower()
                    # Determine page type for email confidence scoring
                    if any(kw in page_path for kw in ["/contact", "/get-in-touch", "/reach", "/inquiry", "/kontakt"]):
                        page_type = "contact"
                        page_confidence = 0.95
                    elif any(kw in page_path for kw in ["/about", "/who-we-are", "/company"]):
                        page_type = "about"
                        page_confidence = 0.85
                    elif any(kw in page_path for kw in ["/team", "/our-team", "/leadership", "/people", "/staff", "/management"]):
                        page_type = "team"
                        page_confidence = 0.90
                    elif any(kw in page_path for kw in ["/imprint", "/impressum", "/legal"]):
                        page_type = "legal"
                        page_confidence = 0.80
                    elif any(kw in page_path for kw in ["/privacy", "/privacy-policy"]):
                        page_type = "privacy"
                        page_confidence = 0.70
                    elif page_path in ("", "/", "/index.html", "/index.php"):
                        page_type = "homepage"
                        page_confidence = 0.75
                    else:
                        page_type = "other"
                        page_confidence = 0.50

                    for email_type, emails in page_data.get("emails", {}).items():
                        if email_type in result["emails"]:
                            result["emails"][email_type].update(emails)
                            # Track page source for each email (keep highest confidence)
                            for email in emails:
                                existing = result["email_page_sources"].get(email)
                                if not existing or existing["confidence"] < page_confidence:
                                    result["email_page_sources"][email] = {
                                        "page_url": current_url,
                                        "page_type": page_type,
                                        "confidence": page_confidence,
                                    }

                    # Collect social links
                    for platform, link in page_data.get("social_links", {}).items():
                        if link and not result["social_links"].get(platform):
                            result["social_links"][platform] = link

                    # Collect company info
                    for key, value in page_data.get("company_info", {}).items():
                        if value and not result["company_info"].get(key):
                            result["company_info"][key] = value

                    # Collect people (dedup by name)
                    existing_names = {p.get("name", "").lower() for p in result["people"]}
                    for person in page_data.get("people", []):
                        if person.get("name", "").lower() not in existing_names:
                            result["people"].append(person)
                            existing_names.add(person.get("name", "").lower())

                    # === Add discovered links with intelligent priority scoring ===
                    internal_links = page_data.get("internal_links", [])
                    for link in internal_links[:15]:  # Up to 15 links per page
                        if link not in scraped_urls and link not in url_queue:
                            # Score link by contact-relevance
                            score = self._url_priority_score(link)
                            # Convert score (lower=better) to priority (higher=better)
                            # score 0 → priority 30, score 1 → priority 20, score 3 → priority 5
                            link_priority = max(5, 30 - score * 10)
                            url_queue.push(link, priority=link_priority, depth=current_depth + 1)
                else:
                    consecutive_failures += 1
                    if cb:
                        cb.record_failure(domain)

                # Fallback circuit breaker (consecutive failures within same crawl)
                if consecutive_failures >= max_consecutive_failures:
                    logger.debug(f"Circuit breaker triggered for {domain} after {consecutive_failures} failures")
                    break

                # Adaptive delay: respect robots.txt Crawl-delay if present,
                # otherwise slower for deep pages, faster for priority pages
                if self._robots_crawl_delay:
                    delay = self._robots_crawl_delay
                else:
                    delay = 0.2 if current_depth == 0 else 0.3 + random.random() * 0.2
                await smart_delay(delay)

            except Exception as e:
                logger.debug(f"Error scraping {current_url}: {e}")
                consecutive_failures += 1
                if cb:
                    cb.record_failure(domain)

        # Convert sets to lists
        for email_type in result["emails"]:
            result["emails"][email_type] = list(result["emails"][email_type])

        # Update JS rendering stats
        result["js_rendered_pages"] = self._js_rendered_count

        total_emails = sum(len(v) for v in result["emails"].values() if isinstance(v, list))
        logger.info(
            f"[CRAWLER] {domain}: scraped {result['pages_scraped']} pages "
            f"({pdfs_processed} PDFs, {self._js_rendered_count} JS-rendered), "
            f"found {total_emails} emails, {len(result['people'])} people"
        )

        return result

    async def _scrape_page(self, url: str, domain: str) -> Optional[Dict[str, Any]]:
        """Scrape a single page for data - enhanced with structured data extraction."""
        try:
            html_text = None

            # Check file cache first (if available via engine reference)
            if hasattr(self, '_file_cache') and self._file_cache:
                cached = self._file_cache.get(url)
                if cached:
                    html_text = cached

            if html_text is None:
                # Referer chaining: use last visited page on same domain, or domain root
                referer = self._last_page_url if self._last_page_url else f"https://{domain}/"
                headers = self._session.get_headers(context="navigate", referer=referer)

                # Check Content-Length before downloading (skip >5MB pages)
                MAX_PAGE_BYTES = 5 * 1024 * 1024  # 5MB cap
                response = await self.client.get(url, headers=headers)
                # Track for next request's Referer
                self._last_page_url = url

                if response.status_code != 200:
                    return None

                # Skip non-HTML responses
                content_type = response.headers.get("content-type", "")
                if "text/html" not in content_type and "application/xhtml" not in content_type:
                    return None

                # Skip oversized responses (PDFs, large HTML dumps)
                content_length = response.headers.get("content-length")
                if content_length and int(content_length) > MAX_PAGE_BYTES:
                    logger.debug(f"Skipping oversized page ({content_length} bytes): {url}")
                    return None

                # Truncate response text to 2MB of decoded text to avoid memory spikes
                html_text = response.text[:2_000_000]

                # Store in file cache for future runs
                if hasattr(self, '_file_cache') and self._file_cache:
                    self._file_cache.put(url, html_text)

            soup = BeautifulSoup(html_text, "html.parser")

            # === SPA DETECTION: Check if page is a JS-rendered shell ===
            # Before decomposing script tags, check for SPA markers
            body = soup.find("body")
            spa_markers = (
                soup.find("div", id="root") or soup.find("div", id="app") or
                soup.find("div", id="__next") or soup.find("div", id="__nuxt") or
                soup.find("div", id="___gatsby")
            )

            # Remove script/style tags to avoid false email matches in JS/CSS
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()

            text = soup.get_text(separator=" ")

            # If visible text is minimal AND SPA markers found, try JS rendering
            visible_text = text.strip()
            if len(visible_text) < 200 and spa_markers and HAS_PLAYWRIGHT:
                logger.debug(f"[SPA] Detected SPA shell ({len(visible_text)} chars), trying Playwright: {url}")
                js_data = await self._scrape_page_js(url, domain)
                if js_data:
                    self._js_rendered_count += 1
                    return js_data
                # If JS fallback also fails, continue with whatever we got from static HTML

            data = {
                "emails": {
                    "contact": set(),
                    "marketing": set(),
                    "sales": set(),
                    "support": set(),
                    "press": set(),
                    "hr": set(),
                    "legal": set(),
                    "finance": set(),
                    "other": set()
                },
                "social_links": {},
                "company_info": {},
                "people": [],
                "internal_links": []
            }

            # === 1. Extract emails from page text (with validation) ===
            raw_emails = self.GENERAL_EMAIL_RE.findall(text)
            for email in raw_emails:
                email = email.lower().strip()
                if self._is_valid_email(email, domain):
                    category = self._classify_email(email)
                    data["emails"][category].add(email)

            # === 2. Extract emails from mailto: links (highest confidence) ===
            for mailto in soup.select("a[href^='mailto:']"):
                email = mailto.get("href", "").replace("mailto:", "").split("?")[0].lower().strip()
                if email and self._is_valid_email(email, domain):
                    category = self._classify_email(email)
                    data["emails"][category].add(email)

            # === 3. Extract from structured data (JSON-LD, Schema.org) ===
            structured = self._extract_structured_data(soup)
            for email in structured.get("emails", set()):
                if self._is_valid_email(email, domain):
                    category = self._classify_email(email)
                    data["emails"][category].add(email)
            for platform, url_val in structured.get("social", {}).items():
                if not data["social_links"].get(platform):
                    data["social_links"][platform] = url_val
            for key, val in structured.get("company", {}).items():
                if val and not data["company_info"].get(key):
                    data["company_info"][key] = val
            phones_list = list(structured.get("phones", set()))
            if phones_list:
                data["company_info"]["phones"] = phones_list

            # === 4. Extract social links from all anchor tags ===
            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                if "linkedin.com/company" in href or "linkedin.com/in/" in href:
                    if "linkedin.com/company" in href:
                        data["social_links"]["linkedin"] = href
                elif "twitter.com/" in href or "x.com/" in href:
                    data["social_links"]["twitter"] = href
                elif "facebook.com/" in href:
                    data["social_links"]["facebook"] = href
                elif "instagram.com/" in href:
                    data["social_links"]["instagram"] = href

            # === 5. Extract company name & description ===
            title = soup.find("title")
            if title:
                data["company_info"]["name"] = title.get_text(strip=True).split("|")[0].split("-")[0].strip()

            meta_desc = soup.find("meta", {"name": "description"}) or \
                        soup.find("meta", {"property": "og:description"})
            if meta_desc:
                data["company_info"]["description"] = meta_desc.get("content", "")[:500]

            # === 6. Find internal links (for BFS crawling) ===
            base_url = f"https://{domain}"
            seen_links = set()
            for link in soup.find_all("a", href=True):
                href = link.get("href", "").split("#")[0].split("?")[0].strip()  # Remove fragments & query params
                if not href:
                    continue

                full_url = None
                if href.startswith("/"):
                    full_url = base_url + href
                elif href.startswith("http") and domain in href:
                    full_url = href

                if full_url and full_url not in seen_links and not self._should_skip_url(full_url):
                    seen_links.add(full_url)
                    data["internal_links"].append(full_url)

            # === 7. Look for team members (enhanced patterns) ===
            team_patterns = re.compile(
                r'team|staff|people|leadership|executive|management|founder|board|director',
                re.IGNORECASE
            )
            team_sections = soup.find_all(class_=team_patterns)
            # Also check by section/div IDs
            team_sections += soup.find_all(id=team_patterns)

            for section in team_sections:
                card_patterns = re.compile(r'card|member|person|profile|bio|exec', re.IGNORECASE)
                cards = section.find_all(class_=card_patterns)
                if not cards:
                    # Fallback: look for list items or divs with names
                    cards = section.find_all(["li", "div", "article"])

                for card in cards[:15]:
                    name_elem = card.find(["h2", "h3", "h4", "h5", "strong"])
                    title_elem = card.find(class_=re.compile(r'title|role|position|designation|job', re.IGNORECASE))
                    if not title_elem:
                        # Fallback: look for <p> or <span> after the name
                        title_elem = card.find(["p", "span"], class_=True)

                    if name_elem:
                        name_text = name_elem.get_text(strip=True)
                        # Skip if the "name" is too long (probably not a person name)
                        if len(name_text) > 60 or len(name_text) < 3:
                            continue
                        person = {
                            "name": name_text,
                            "title": title_elem.get_text(strip=True) if title_elem else None,
                        }

                        # Look for LinkedIn profile link
                        linkedin = card.find("a", href=re.compile(r'linkedin\.com/in/', re.I))
                        if linkedin:
                            person["linkedin"] = linkedin.get("href")

                        # Look for person email
                        person_mailto = card.find("a", href=re.compile(r'^mailto:', re.I))
                        if person_mailto:
                            person["email"] = person_mailto.get("href", "").replace("mailto:", "").split("?")[0]

                        data["people"].append(person)

            return data

        except Exception as e:
            logger.debug(f"Page scrape error for {url}: {e}")
            return None

    async def _get_browser(self):
        """Lazily initialize a reusable Playwright browser for SPA rendering."""
        if not HAS_PLAYWRIGHT:
            return None
        if self._browser is None:
            try:
                self._playwright = await async_playwright().start()
                self._browser = await self._playwright.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
                )
                logger.info("[JS] Playwright browser launched for SPA rendering")
            except Exception as e:
                logger.warning(f"[JS] Failed to launch Playwright browser: {e}")
                self._browser = None
                return None
        return self._browser

    async def _scrape_page_js(self, url: str, domain: str) -> Optional[Dict[str, Any]]:
        """
        Fallback: Render JavaScript-heavy pages with headless Chromium.

        Called when _scrape_page() detects an SPA shell (minimal visible text
        + SPA marker divs like #root, #app, #__next, #__nuxt).

        Uses a reusable browser instance to avoid launching per page.
        """
        browser = await self._get_browser()
        if not browser:
            return None

        try:
            page = await browser.new_page(
                user_agent=random.choice(self.USER_AGENTS)
            )
            try:
                await page.goto(url, wait_until="networkidle", timeout=15000)
                html = await page.content()
            finally:
                await page.close()

            if not html or len(html) < 100:
                return None

            # Parse the JS-rendered HTML with the same extraction logic as _scrape_page
            soup = BeautifulSoup(html, "html.parser")

            # Remove script/style tags
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()

            text = soup.get_text(separator=" ")

            data: Dict[str, Any] = {
                "emails": {
                    "contact": set(), "marketing": set(),
                    "sales": set(), "support": set(),
                    "press": set(), "hr": set(),
                    "legal": set(), "finance": set(),
                    "other": set()
                },
                "social_links": {},
                "company_info": {},
                "people": [],
                "internal_links": []
            }

            # 1. Extract emails from text
            raw_emails = self.GENERAL_EMAIL_RE.findall(text)
            for email in raw_emails:
                email = email.lower().strip()
                if self._is_valid_email(email, domain):
                    category = self._classify_email(email)
                    data["emails"][category].add(email)

            # 2. Mailto links
            for mailto in soup.select("a[href^='mailto:']"):
                email = mailto.get("href", "").replace("mailto:", "").split("?")[0].lower().strip()
                if email and self._is_valid_email(email, domain):
                    category = self._classify_email(email)
                    data["emails"][category].add(email)

            # 3. Structured data (JSON-LD + microdata)
            # Re-parse from raw HTML since we decomposed tags above
            full_soup = BeautifulSoup(html, "html.parser")
            structured = self._extract_structured_data(full_soup)
            for email in structured.get("emails", set()):
                if self._is_valid_email(email, domain):
                    category = self._classify_email(email)
                    data["emails"][category].add(email)
            for platform, url_val in structured.get("social", {}).items():
                if not data["social_links"].get(platform):
                    data["social_links"][platform] = url_val
            for key, val in structured.get("company", {}).items():
                if val and not data["company_info"].get(key):
                    data["company_info"][key] = val

            # 4. Social links
            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                if "linkedin.com/company" in href or "linkedin.com/in/" in href:
                    if "linkedin.com/company" in href:
                        data["social_links"]["linkedin"] = href
                elif "twitter.com/" in href or "x.com/" in href:
                    data["social_links"]["twitter"] = href
                elif "facebook.com/" in href:
                    data["social_links"]["facebook"] = href
                elif "instagram.com/" in href:
                    data["social_links"]["instagram"] = href

            # 5. Company name & description
            title = soup.find("title")
            if title:
                data["company_info"]["name"] = title.get_text(strip=True).split("|")[0].split("-")[0].strip()
            meta_desc = soup.find("meta", {"name": "description"}) or \
                        soup.find("meta", {"property": "og:description"})
            if meta_desc:
                data["company_info"]["description"] = meta_desc.get("content", "")[:500]

            # 6. Internal links
            base_url = f"https://{domain}"
            seen_links: Set[str] = set()
            for link in soup.find_all("a", href=True):
                href = link.get("href", "").split("#")[0].split("?")[0].strip()
                if not href:
                    continue
                full_url = None
                if href.startswith("/"):
                    full_url = base_url + href
                elif href.startswith("http") and domain in href:
                    full_url = href
                if full_url and full_url not in seen_links and not self._should_skip_url(full_url):
                    seen_links.add(full_url)
                    data["internal_links"].append(full_url)

            total_emails = sum(len(v) for v in data["emails"].values())
            if total_emails > 0:
                logger.info(f"[JS] Playwright rendered {url}: found {total_emails} emails")

            return data

        except Exception as e:
            logger.debug(f"[JS] Playwright render error for {url}: {e}")
            return None

    async def _extract_emails_from_pdf(self, url: str, domain: str) -> Set[str]:
        """
        Download PDF and extract email addresses from it.

        Privacy policies, annual reports, and legal docs often contain
        real contact emails that aren't visible on HTML pages.

        Limits: max 10MB file, max 20 pages per PDF.
        """
        if not HAS_PDFPLUMBER:
            return set()

        emails: Set[str] = set()
        tmp_path = None
        try:
            # Download PDF (stream to avoid loading huge files into memory)
            response = await self.client.get(url, headers=self.headers)
            if response.status_code != 200:
                return set()

            # Skip very large files (> 10MB)
            if len(response.content) > 10_000_000:
                logger.debug(f"PDF too large ({len(response.content)} bytes), skipping: {url}")
                return set()

            # Write to temp file (pdfplumber needs a file path)
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
                f.write(response.content)
                tmp_path = f.name

            # Extract text from PDF pages and find emails
            with pdfplumber.open(tmp_path) as pdf:
                for page in pdf.pages[:20]:  # Max 20 pages
                    text = page.extract_text() or ""
                    for email in self.GENERAL_EMAIL_RE.findall(text):
                        email_lower = email.lower().strip()
                        if self._is_valid_email(email_lower, domain):
                            emails.add(email_lower)

            if emails:
                logger.info(f"[PDF] Extracted {len(emails)} emails from {url}")

        except Exception as e:
            logger.debug(f"PDF extraction error for {url}: {e}")
        finally:
            # Always clean up temp file
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

        return emails

    async def close(self):
        await self.client.aclose()
        # Clean up Playwright browser if it was launched
        if self._browser:
            try:
                await self._browser.close()
            except Exception as e:
                logger.debug(f"Non-critical error in Playwright browser close: {e}")
            self._browser = None
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception as e:
                logger.debug(f"Non-critical error in Playwright instance stop: {e}")
            self._playwright = None


class ProductHuntScraper:
    """
    FREE ProductHunt Scraper
    Finds new products and their makers
    """

    def __init__(self, timeout: int = 30, shared_client: Optional[httpx.AsyncClient] = None):
        self.timeout = timeout
        self.base_url = "https://www.producthunt.com"
        self.headers = get_headers(context="navigate")
        self.client = shared_client or httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )

    async def search_products(
        self,
        query: str,
        max_results: int = 30
    ) -> List[Dict[str, Any]]:
        """Search ProductHunt for products"""
        products = []

        try:
            search_url = f"{self.base_url}/search"
            params = {"q": query}

            response = await self.client.get(search_url, params=params, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                # Find product cards
                for card in soup.select("[data-test='post-item']")[:max_results]:
                    try:
                        name_elem = card.select_one("[data-test='post-name']")
                        tagline_elem = card.select_one("[data-test='post-tagline']")
                        link_elem = card.select_one("a[href*='/posts/']")

                        if name_elem:
                            product = {
                                "name": name_elem.get_text(strip=True),
                                "tagline": tagline_elem.get_text(strip=True) if tagline_elem else "",
                                "url": self.base_url + link_elem.get("href") if link_elem else None,
                                "source": "producthunt"
                            }
                            products.append(product)
                    except Exception as e:
                        logger.debug(f"Non-critical error in ProductHunt search card parsing: {e}")
                        continue

        except Exception as e:
            logger.error(f"ProductHunt search error: {e}")

        return products

    async def get_today_products(self, max_results: int = 30) -> List[Dict[str, Any]]:
        """Get today's featured products"""
        products = []

        try:
            response = await self.client.get(self.base_url, headers=self.headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                for card in soup.select("[data-test='post-item']")[:max_results]:
                    try:
                        name_elem = card.select_one("[data-test='post-name']")
                        if name_elem:
                            products.append({
                                "name": name_elem.get_text(strip=True),
                                "source": "producthunt"
                            })
                    except Exception as e:
                        logger.debug(f"Non-critical error in ProductHunt today card parsing: {e}")
                        continue

        except Exception as e:
            logger.error(f"ProductHunt today error: {e}")

        return products

    async def close(self):
        await self.client.aclose()


# ============================================
# LAYER 10: MULTI-SOURCE DISCOVERY ENGINES
# ============================================

class WebDiscoveryEngine:
    """
    Discovers companies via web search engines (DuckDuckGo, Bing, SearX, Google Dork).
    Uses WEB_DISCOVERY_QUERIES permutations + site-specific dorks + LinkedIn dorks.
    Returns List[AppData] so results flow into the same Stage 2/3 pipeline as app store results.
    """

    def __init__(self, web_search: 'MobiAdzWebSearch', shared_client: Optional[httpx.AsyncClient] = None):
        self.web_search = web_search
        self.client = shared_client or httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )
        self._owns_client = shared_client is None
        self._seen_domains: Set[str] = set()

    async def discover_companies(
        self,
        category: 'ProductCategory',
        demographic: 'Demographic',
        max_companies: int = 50,
        bloom_filter: Optional[BloomFilter] = None,
    ) -> List['AppData']:
        """
        Run many permutation queries to discover companies via web search.
        Returns AppData objects with store="web_search".
        """
        discovered: List[AppData] = []
        year = str(datetime.now(timezone.utc).year)

        # 1. General web search queries
        queries = WEB_DISCOVERY_QUERIES.get(category, [])
        # 2. Site-specific dork queries
        site_dorks = WEB_DISCOVERY_SITE_DORKS.get(category, [])
        # 3. LinkedIn dork queries
        linkedin_dorks = WEB_DISCOVERY_LINKEDIN_DORKS.get(category, [])

        all_queries = [q.replace("{year}", year) for q in queries]
        all_queries += [q.replace("{year}", year) for q in site_dorks]
        all_queries += [q.replace("{year}", year) for q in linkedin_dorks]

        # Add demographic-specific modifiers
        demo_modifiers = {
            "usa": "US", "europe": "Europe", "uk": "UK", "australia": "Australia",
            "singapore": "Singapore", "east_asia": "Asia", "south_asia": "India",
            "middle_east": "Middle East", "latin_america": "Latin America",
            "africa": "Africa", "southeast_asia": "Southeast Asia", "global": "",
        }
        demo_label = demo_modifiers.get(demographic.value if hasattr(demographic, 'value') else str(demographic), "")
        if demo_label:
            # Add a few demographic-filtered queries
            base_terms = queries[:3] if queries else [category.value]
            for term in base_terms:
                all_queries.append(f'{term.replace("{year}", year)} {demo_label}')

        logger.info(f"[WebDiscovery] {category.value}: running {len(all_queries)} search queries")

        for query in all_queries:
            if len(discovered) >= max_companies:
                break

            try:
                # Search with DuckDuckGo and Bing in parallel
                tasks = []
                if hasattr(self.web_search, 'duckduckgo'):
                    tasks.append(self.web_search.duckduckgo.search_html(query, max_results=15))
                if hasattr(self.web_search, 'bing'):
                    tasks.append(self.web_search.bing.search(query, max_results=15, max_pages=2))

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result_batch in results:
                    if isinstance(result_batch, Exception):
                        continue
                    if not isinstance(result_batch, list):
                        continue

                    for result in result_batch:
                        if len(discovered) >= max_companies:
                            break

                        if not hasattr(result, 'url') or not result.url:
                            continue

                        # Extract domain from search result
                        try:
                            parsed = urlparse(result.url)
                            domain = parsed.netloc.replace("www.", "").lower()
                        except Exception as e:
                            logger.debug(f"Non-critical error in web discovery URL parsing: {e}")
                            continue

                        # Skip aggregator/directory sites — we want actual company sites
                        skip_domains = {
                            "linkedin.com", "twitter.com", "facebook.com", "instagram.com",
                            "youtube.com", "reddit.com", "wikipedia.org", "crunchbase.com",
                            "g2.com", "capterra.com", "glassdoor.com", "indeed.com",
                            "wellfound.com", "techcrunch.com", "producthunt.com",
                            "github.com", "medium.com", "bloomberg.com", "forbes.com",
                            "google.com", "bing.com", "duckduckgo.com", "yahoo.com",
                        }
                        if any(domain.endswith(sd) for sd in skip_domains):
                            # But extract company names mentioned on these pages
                            company_name = self._extract_company_from_title(result)
                            if company_name and company_name.lower() not in self._seen_domains:
                                # Search for this company's actual website
                                website = await self._find_company_website(company_name)
                                if website:
                                    w_domain = urlparse(website).netloc.replace("www.", "").lower()
                                    if w_domain and w_domain not in self._seen_domains:
                                        if bloom_filter and bloom_filter.contains(w_domain):
                                            continue
                                        self._seen_domains.add(w_domain)
                                        if bloom_filter:
                                            bloom_filter.add(w_domain)
                                        app_data = AppData(
                                            app_id=f"web_{w_domain}",
                                            app_name=company_name,
                                            developer_name=company_name,
                                            store="web_search",
                                            store_url=website,
                                            developer_website=website,
                                            category=category.value,
                                            demographic=demographic.value if hasattr(demographic, 'value') else str(demographic),
                                        )
                                        discovered.append(app_data)
                            continue

                        # Direct company website found
                        if domain and domain not in self._seen_domains:
                            if bloom_filter and bloom_filter.contains(domain):
                                continue
                            self._seen_domains.add(domain)
                            if bloom_filter:
                                bloom_filter.add(domain)

                            company_name = self._extract_company_from_title(result) or domain.split(".")[0].title()
                            app_data = AppData(
                                app_id=f"web_{domain}",
                                app_name=company_name,
                                developer_name=company_name,
                                store="web_search",
                                store_url=result.url,
                                developer_website=f"https://{domain}",
                                category=category.value,
                                demographic=demographic.value if hasattr(demographic, 'value') else str(demographic),
                            )
                            discovered.append(app_data)

                await smart_delay(0.5)  # Rate limit between queries

            except Exception as e:
                logger.warning(f"[WebDiscovery] Query failed '{query[:50]}': {e}")
                continue

        logger.info(f"[WebDiscovery] {category.value}: discovered {len(discovered)} companies")
        return discovered

    def _extract_company_from_title(self, result) -> Optional[str]:
        """Extract company name from search result title."""
        title = getattr(result, 'title', '') or ''
        # Remove common suffixes
        for suffix in [" - Crunchbase", " | LinkedIn", " - G2", " - Capterra",
                       " - TechCrunch", " | Product Hunt", " - AngelList",
                       " Company Profile", " Overview", " Reviews", " - Glassdoor",
                       " - Wikipedia", " - Forbes", " | Wellfound"]:
            if suffix in title:
                title = title.split(suffix)[0].strip()
        # Remove leading patterns like "Top 10 SaaS Companies: "
        if ":" in title and len(title.split(":")[0]) < 40:
            # Only use part after colon if it looks like a company list
            pass
        # Clean up
        title = title.strip(" -|")
        if title and len(title) > 2 and len(title) < 100:
            return title
        return None

    async def _find_company_website(self, company_name: str) -> Optional[str]:
        """Quick search to find a company's actual website."""
        try:
            results = await self.web_search.duckduckgo.search_html(
                f'"{company_name}" official website', max_results=5
            )
            if isinstance(results, list):
                skip = {"linkedin.com", "twitter.com", "facebook.com", "crunchbase.com",
                        "g2.com", "wikipedia.org", "glassdoor.com", "indeed.com"}
                for r in results:
                    if hasattr(r, 'url') and r.url:
                        domain = urlparse(r.url).netloc.replace("www.", "").lower()
                        if not any(domain.endswith(s) for s in skip):
                            return r.url
        except Exception as e:
            logger.debug(f"Non-critical error in company website search: {e}")
        return None

    async def close(self):
        if self._owns_client:
            await self.client.aclose()


class JobBoardDiscovery:
    """
    Discovers companies via job boards:
    - RemoteOK JSON API (free, no auth)
    - HackerNews "Who is hiring" threads
    - Web search for Indeed/Glassdoor/Wellfound listings
    Returns List[AppData] with store="job_board".
    """

    def __init__(self, web_search: 'MobiAdzWebSearch', shared_client: Optional[httpx.AsyncClient] = None):
        self.web_search = web_search
        self.client = shared_client or httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )
        self._owns_client = shared_client is None
        self._seen_companies: Set[str] = set()

    async def discover_companies(
        self,
        category: 'ProductCategory',
        max_companies: int = 30,
        bloom_filter: Optional[BloomFilter] = None,
    ) -> List['AppData']:
        """Run all job board sources and merge results."""
        discovered: List[AppData] = []

        # 1. RemoteOK JSON API
        remoteok_results = await self._search_remoteok(category, max_companies // 3)
        discovered.extend(remoteok_results)

        # 2. Web-search job boards (Indeed, Glassdoor, Wellfound)
        job_search_results = await self._search_job_boards_via_web(
            category, max_companies // 3, bloom_filter
        )
        discovered.extend(job_search_results)

        # 3. HackerNews "Who is Hiring"
        hn_results = await self._search_hackernews_hiring(category, max_companies // 3)
        discovered.extend(hn_results)

        logger.info(f"[JobBoardDiscovery] {category.value}: discovered {len(discovered)} companies")
        return discovered

    async def _search_remoteok(self, category: 'ProductCategory', max_results: int) -> List['AppData']:
        """Search RemoteOK free JSON API for companies in this category."""
        discovered: List[AppData] = []
        search_terms = JOB_BOARD_SEARCH_TERMS.get(category, [category.value])

        # RemoteOK tags map
        tag_map = {
            "saas": "saas", "fintech": "fintech", "health_tech": "healthtech",
            "ed_tech": "edtech", "enterprise": "enterprise", "games": "gamedev",
            "ecommerce": "ecommerce", "ads_based": "marketing", "jobs": "hr",
            "recruitment": "recruiting", "startups": "startup",
            "mobile_apps": "mobile", "android_apps": "android", "ios_apps": "ios",
            "social_media": "social", "streaming": "video", "productivity": "productivity",
            "product_based": "product",
        }
        tag = tag_map.get(category.value, category.value)

        try:
            headers = get_headers(context="search_api")
            response = await self.client.get(
                f"https://remoteok.com/api?tag={tag}",
                headers=headers,
                follow_redirects=True,
            )
            if response.status_code == 200:
                data = response.json()
                # First item is metadata, skip it
                jobs = data[1:] if len(data) > 1 else []
                for job in jobs[:max_results * 3]:  # Check more since many will be duplicates
                    if len(discovered) >= max_results:
                        break
                    company = job.get("company", "").strip()
                    if not company or company.lower() in self._seen_companies:
                        continue
                    self._seen_companies.add(company.lower())

                    company_url = job.get("company_url") or job.get("url", "")
                    app_data = AppData(
                        app_id=f"remoteok_{company.lower().replace(' ', '_')}",
                        app_name=f"{company} (via RemoteOK)",
                        developer_name=company,
                        store="job_board",
                        store_url=company_url,
                        developer_website=company_url if company_url and "remoteok" not in company_url else None,
                        category=category.value,
                    )
                    discovered.append(app_data)

        except Exception as e:
            logger.warning(f"[JobBoardDiscovery] RemoteOK error: {e}")

        return discovered

    async def _search_job_boards_via_web(
        self, category: 'ProductCategory', max_results: int,
        bloom_filter: Optional[BloomFilter] = None,
    ) -> List['AppData']:
        """Search Indeed/Glassdoor/Wellfound via web search to find hiring companies."""
        discovered: List[AppData] = []
        search_terms = JOB_BOARD_SEARCH_TERMS.get(category, [category.value])

        for term in search_terms[:3]:
            if len(discovered) >= max_results:
                break

            # Search each job board via Bing/DuckDuckGo
            job_queries = [
                f'site:indeed.com "{term}" company',
                f'site:glassdoor.com "{term}" company',
                f'site:wellfound.com "{term}" startup',
            ]

            for query in job_queries:
                if len(discovered) >= max_results:
                    break
                try:
                    results = await self.web_search.bing.search(query, max_results=10, max_pages=1)
                    if isinstance(results, list):
                        for result in results:
                            if len(discovered) >= max_results:
                                break
                            company_name = self._extract_company_from_job_result(result)
                            if company_name and company_name.lower() not in self._seen_companies:
                                self._seen_companies.add(company_name.lower())
                                app_data = AppData(
                                    app_id=f"jobboard_{company_name.lower().replace(' ', '_')}",
                                    app_name=f"{company_name} (via Job Board)",
                                    developer_name=company_name,
                                    store="job_board",
                                    store_url=getattr(result, 'url', ''),
                                    category=category.value,
                                )
                                discovered.append(app_data)
                    await smart_delay(0.3)
                except Exception as e:
                    logger.warning(f"[JobBoardDiscovery] Web search error for '{query[:50]}': {e}")

        return discovered

    async def _search_hackernews_hiring(self, category: 'ProductCategory', max_results: int) -> List['AppData']:
        """Search HN "Who is Hiring" threads for companies."""
        discovered: List[AppData] = []
        category_terms = JOB_BOARD_SEARCH_TERMS.get(category, [category.value])

        try:
            # Search for recent "Who is Hiring" threads via web
            query = f'site:news.ycombinator.com "Who is hiring" {category_terms[0]}'
            results = await self.web_search.duckduckgo.search_html(query, max_results=5)

            if isinstance(results, list):
                for result in results[:2]:  # Only check top 2 threads
                    if len(discovered) >= max_results:
                        break
                    if not hasattr(result, 'url'):
                        continue

                    # Fetch the HN thread page
                    try:
                        response = await self.client.get(
                            result.url, follow_redirects=True,
                            headers=get_headers(context="navigate"),
                        )
                        if response.status_code == 200:
                            soup = BeautifulSoup(response.text, "html.parser")
                            # HN comments are in .commtext class
                            comments = soup.select(".commtext")
                            for comment in comments[:100]:
                                if len(discovered) >= max_results:
                                    break
                                text = comment.get_text()
                                # First line of HN hiring posts is usually "Company Name | Role | Location"
                                first_line = text.split("\n")[0].strip()
                                parts = [p.strip() for p in first_line.split("|")]
                                if len(parts) >= 2:
                                    company_name = parts[0].strip()
                                    if company_name and len(company_name) > 1 and len(company_name) < 80:
                                        if company_name.lower() not in self._seen_companies:
                                            self._seen_companies.add(company_name.lower())
                                            # Try to extract URL from the comment
                                            links = comment.select("a[href]")
                                            website = None
                                            for link in links:
                                                href = link.get("href", "")
                                                if href and "ycombinator.com" not in href:
                                                    website = href
                                                    break

                                            app_data = AppData(
                                                app_id=f"hn_{company_name.lower().replace(' ', '_')}",
                                                app_name=f"{company_name} (via HN Hiring)",
                                                developer_name=company_name,
                                                store="job_board",
                                                store_url=website or result.url,
                                                developer_website=website,
                                                category=category.value,
                                            )
                                            discovered.append(app_data)
                    except Exception as e:
                        logger.warning(f"[JobBoardDiscovery] HN thread fetch error: {e}")

        except Exception as e:
            logger.warning(f"[JobBoardDiscovery] HN search error: {e}")

        return discovered

    def _extract_company_from_job_result(self, result) -> Optional[str]:
        """Extract company name from job board search result."""
        title = getattr(result, 'title', '') or ''
        snippet = getattr(result, 'snippet', '') or ''

        # Indeed format: "Job Title - Company Name - Location"
        if " - " in title:
            parts = title.split(" - ")
            if len(parts) >= 2:
                # Company is usually the second part
                candidate = parts[1].strip()
                for noise in ["Indeed", "Glassdoor", "Wellfound", "Review", "Salary",
                              "Jobs", "careers", "hiring"]:
                    if noise.lower() in candidate.lower():
                        candidate = ""
                        break
                if candidate and len(candidate) > 1 and len(candidate) < 80:
                    return candidate

        # Glassdoor format: "Company Name - Reviews & Jobs"
        if "glassdoor" in getattr(result, 'url', '').lower():
            name = title.split(" - ")[0].strip() if " - " in title else ""
            if name and len(name) > 1 and len(name) < 80:
                return name

        # Wellfound format: "Company Name | Startup Jobs"
        if "wellfound" in getattr(result, 'url', '').lower():
            name = title.split(" | ")[0].strip() if " | " in title else ""
            if name and len(name) > 1 and len(name) < 80:
                return name

        return None

    async def close(self):
        if self._owns_client:
            await self.client.aclose()


class StartupDatabaseDiscovery:
    """
    Discovers companies via startup databases and directories:
    - ProductHunt topic pages
    - Y Combinator company directory
    - G2/Capterra category pages (via web search)
    - TechCrunch funding articles (via web search)
    Returns List[AppData] with store="startup_db".
    """

    def __init__(self, web_search: 'MobiAdzWebSearch', shared_client: Optional[httpx.AsyncClient] = None):
        self.web_search = web_search
        self.client = shared_client or httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
        )
        self._owns_client = shared_client is None
        self._seen_companies: Set[str] = set()

    async def discover_companies(
        self,
        category: 'ProductCategory',
        max_companies: int = 30,
        bloom_filter: Optional[BloomFilter] = None,
    ) -> List['AppData']:
        """Run all startup database sources and merge results."""
        discovered: List[AppData] = []
        topics = STARTUP_DB_TOPICS.get(category, {})

        # 1. ProductHunt topic page
        ph_results = await self._search_producthunt(
            topics.get("producthunt", category.value), category, max_companies // 4
        )
        discovered.extend(ph_results)

        # 2. Y Combinator directory
        yc_results = await self._search_yc_directory(
            topics.get("yc_industry", "B2B"), category, max_companies // 4
        )
        discovered.extend(yc_results)

        # 3. G2/Capterra via web search
        g2_results = await self._search_review_sites(
            topics.get("g2_category", category.value), category, max_companies // 4
        )
        discovered.extend(g2_results)

        # 4. TechCrunch/VentureBeat funding articles via web search
        tc_results = await self._search_tech_news(category, max_companies // 4)
        discovered.extend(tc_results)

        logger.info(f"[StartupDBDiscovery] {category.value}: discovered {len(discovered)} companies")
        return discovered

    async def _search_producthunt(
        self, topic: str, category: 'ProductCategory', max_results: int
    ) -> List['AppData']:
        """Scrape ProductHunt topic pages for company discovery."""
        discovered: List[AppData] = []

        try:
            url = f"https://www.producthunt.com/topics/{topic}"
            headers = get_headers(context="navigate")
            response = await self.client.get(url, headers=headers, follow_redirects=True)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                # ProductHunt lists products with links
                # Look for product cards/links
                product_links = soup.select('a[href*="/posts/"]')
                seen_slugs = set()

                for link in product_links:
                    if len(discovered) >= max_results:
                        break
                    href = link.get("href", "")
                    slug = href.split("/posts/")[-1].split("?")[0] if "/posts/" in href else ""
                    if not slug or slug in seen_slugs:
                        continue
                    seen_slugs.add(slug)

                    product_name = link.get_text(strip=True)
                    if not product_name or len(product_name) < 2:
                        product_name = slug.replace("-", " ").title()

                    if product_name.lower() in self._seen_companies:
                        continue
                    self._seen_companies.add(product_name.lower())

                    app_data = AppData(
                        app_id=f"ph_{slug}",
                        app_name=product_name,
                        developer_name=product_name,
                        store="startup_db",
                        store_url=f"https://www.producthunt.com{href}" if href.startswith("/") else href,
                        category=category.value,
                    )
                    discovered.append(app_data)

        except Exception as e:
            logger.warning(f"[StartupDBDiscovery] ProductHunt error: {e}")

        return discovered

    async def _search_yc_directory(
        self, industry: str, category: 'ProductCategory', max_results: int
    ) -> List['AppData']:
        """Search Y Combinator company directory."""
        discovered: List[AppData] = []

        try:
            url = f"https://www.ycombinator.com/companies?industry={industry}"
            headers = get_headers(context="navigate")
            response = await self.client.get(url, headers=headers, follow_redirects=True)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                # YC directory lists companies as cards with links
                company_links = soup.select('a[href*="/companies/"]')
                seen_slugs = set()

                for link in company_links:
                    if len(discovered) >= max_results:
                        break
                    href = link.get("href", "")
                    slug = href.split("/companies/")[-1].split("?")[0].split("/")[0] if "/companies/" in href else ""
                    if not slug or slug in seen_slugs or slug == "":
                        continue
                    # Skip navigation links
                    if slug in ("industry", "batch", "region", "top-companies", ""):
                        continue
                    seen_slugs.add(slug)

                    company_name = link.get_text(strip=True)
                    if not company_name or len(company_name) < 2 or len(company_name) > 100:
                        company_name = slug.replace("-", " ").title()

                    if company_name.lower() in self._seen_companies:
                        continue
                    self._seen_companies.add(company_name.lower())

                    app_data = AppData(
                        app_id=f"yc_{slug}",
                        app_name=f"{company_name} (YC)",
                        developer_name=company_name,
                        store="startup_db",
                        store_url=f"https://www.ycombinator.com{href}" if href.startswith("/") else href,
                        category=category.value,
                    )
                    discovered.append(app_data)

        except Exception as e:
            logger.warning(f"[StartupDBDiscovery] YC directory error: {e}")

        return discovered

    async def _search_review_sites(
        self, g2_category: str, category: 'ProductCategory', max_results: int
    ) -> List['AppData']:
        """Discover companies from G2/Capterra via web search."""
        discovered: List[AppData] = []

        queries = [
            f'site:g2.com/products "{g2_category}"',
            f'site:capterra.com "{g2_category}" software',
        ]

        for query in queries:
            if len(discovered) >= max_results:
                break
            try:
                results = await self.web_search.bing.search(query, max_results=10, max_pages=1)
                if isinstance(results, list):
                    for result in results:
                        if len(discovered) >= max_results:
                            break
                        title = getattr(result, 'title', '') or ''
                        # G2 format: "Product Name Reviews 2025 | G2"
                        # Capterra format: "Product Name Reviews - Capterra"
                        company_name = title.split(" Reviews")[0].strip() if " Reviews" in title else ""
                        if not company_name:
                            company_name = title.split(" - ")[0].strip() if " - " in title else ""
                        if not company_name:
                            company_name = title.split(" | ")[0].strip() if " | " in title else ""

                        if company_name and len(company_name) > 1 and len(company_name) < 80:
                            if company_name.lower() not in self._seen_companies:
                                self._seen_companies.add(company_name.lower())
                                app_data = AppData(
                                    app_id=f"g2_{company_name.lower().replace(' ', '_')}",
                                    app_name=f"{company_name} (via G2/Capterra)",
                                    developer_name=company_name,
                                    store="startup_db",
                                    store_url=getattr(result, 'url', ''),
                                    category=category.value,
                                )
                                discovered.append(app_data)
                await smart_delay(0.3)
            except Exception as e:
                logger.warning(f"[StartupDBDiscovery] Review site search error: {e}")

        return discovered

    async def _search_tech_news(self, category: 'ProductCategory', max_results: int) -> List['AppData']:
        """Discover recently funded companies from TechCrunch/VentureBeat."""
        discovered: List[AppData] = []
        year = str(datetime.now(timezone.utc).year)
        cat_name = category.value.replace("_", " ")

        queries = [
            f'site:techcrunch.com "{cat_name}" "raises" "$" million {year}',
            f'site:venturebeat.com "{cat_name}" startup funding {year}',
        ]

        for query in queries:
            if len(discovered) >= max_results:
                break
            try:
                results = await self.web_search.duckduckgo.search_html(query, max_results=10)
                if isinstance(results, list):
                    for result in results:
                        if len(discovered) >= max_results:
                            break
                        title = getattr(result, 'title', '') or ''
                        # TechCrunch format: "CompanyName raises $XM..."
                        company_name = ""
                        if " raises " in title.lower():
                            company_name = title.split(" raises ")[0].strip()
                            company_name = company_name.split(" | ")[-1].strip()
                        elif " lands " in title.lower():
                            company_name = title.split(" lands ")[0].strip()
                        elif " secures " in title.lower():
                            company_name = title.split(" secures ")[0].strip()

                        if company_name and len(company_name) > 1 and len(company_name) < 80:
                            if company_name.lower() not in self._seen_companies:
                                self._seen_companies.add(company_name.lower())
                                app_data = AppData(
                                    app_id=f"news_{company_name.lower().replace(' ', '_')}",
                                    app_name=f"{company_name} (Funded)",
                                    developer_name=company_name,
                                    store="startup_db",
                                    store_url=getattr(result, 'url', ''),
                                    category=category.value,
                                )
                                discovered.append(app_data)
                await smart_delay(0.3)
            except Exception as e:
                logger.warning(f"[StartupDBDiscovery] Tech news search error: {e}")

        return discovered

    async def close(self):
        if self._owns_client:
            await self.client.aclose()


class MobiAdzExtractionEngine:
    """
    TheMobiAdz Main Extraction Engine V2.0 - ULTRA INTEGRATED

    Combines all scrapers + AI/ML components for comprehensive app/company data extraction.
    Supports both FREE and PAID modes with intelligent fallback.

    ULTRA FEATURES:
    - SpaCy NER for entity extraction
    - 50+ email patterns with permutation generator
    - Email verification via MX records
    - Bloom Filter for O(1) deduplication
    - LRU Cache for API responses
    - GitHub, npm, HackerNews extraction
    - DNS/SSL intelligence
    - Wayback Machine historical data
    """

    def __init__(self, config: Optional[MobiAdzConfig] = None):
        self.config = config or MobiAdzConfig()

        # ========== SHARED CONNECTION POOL ==========
        # Single httpx.AsyncClient shared across all scrapers for efficient connection reuse.
        # Transport: retries=3 for ConnectError/ConnectTimeout (not HTTP errors).
        # HTTP/2 multiplexing allows many requests per connection.
        self._shared_client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
            timeout=httpx.Timeout(
                connect=10.0,   # TCP + TLS handshake
                read=30.0,      # Wait for response body
                write=10.0,     # Send request body
                pool=5.0,       # Wait for connection from pool
            ),
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=50,
                keepalive_expiry=60.0,
            ),
        )

        # Initialize FREE scrapers - all share the connection pool
        self.playstore = GooglePlayScraper(timeout=self.config.search_timeout, shared_client=self._shared_client)
        self.appstore = AppStoreScraper(timeout=self.config.search_timeout, shared_client=self._shared_client)
        self.steam = SteamScraper(timeout=self.config.search_timeout, shared_client=self._shared_client)
        self.website_scraper = CompanyWebsiteScraper(
            timeout=self.config.search_timeout,
            max_depth=self.config.website_scrape_depth,
            shared_client=self._shared_client,
        )
        # Inject file cache into website scraper (set after engine init completes below)
        self.producthunt = ProductHuntScraper(timeout=self.config.search_timeout, shared_client=self._shared_client)

        # New store scrapers (Layer 1 expansion)
        self.fdroid = FDroidScraper(timeout=self.config.search_timeout, shared_client=self._shared_client) if self.config.use_fdroid else None
        self.microsoft = MicrosoftStoreScraper(timeout=self.config.search_timeout, shared_client=self._shared_client) if self.config.use_microsoft_store else None
        self.huawei = HuaweiAppGalleryScraper(timeout=self.config.search_timeout, shared_client=self._shared_client) if self.config.use_huawei_appgallery else None

        # ========== ULTRA ENGINE COMPONENTS ==========
        # Advanced Data Structures
        if self.config.use_bloom_filter:
            # Auto-size bloom filters based on expected items (URLs ~10x companies, emails ~5x)
            expected_urls = self.config.max_companies * 40  # ~40 pages per company
            expected_emails = self.config.target_contacts * 5  # ~5x for dedup headroom
            if self.config.bloom_filter_size > 0:
                # Manual override
                self.url_bloom = BloomFilter(size=self.config.bloom_filter_size)
                self.email_bloom = BloomFilter(size=self.config.bloom_filter_size // 2)
            else:
                # Auto-sized (recommended)
                self.url_bloom = BloomFilter(expected_items=expected_urls, fp_rate=self.config.bloom_filter_fp_rate)
                self.email_bloom = BloomFilter(expected_items=expected_emails, fp_rate=self.config.bloom_filter_fp_rate)
        else:
            self.url_bloom = None
            self.email_bloom = None

        if self.config.use_lru_cache:
            self.response_cache = LRUCache(capacity=self.config.lru_cache_capacity)
        else:
            self.response_cache = None

        # File-based HTTP response cache with TTL
        if self.config.use_file_cache:
            self.file_cache = TTLFileCache(ttl_seconds=self.config.file_cache_ttl)
        else:
            self.file_cache = None

        # Per-domain circuit breaker
        if self.config.use_circuit_breaker:
            self.circuit_breaker = CircuitBreaker(
                failure_threshold=self.config.circuit_breaker_threshold,
                cooldown_seconds=self.config.circuit_breaker_cooldown,
            )
        else:
            self.circuit_breaker = None

        # AI/ML Components
        if self.config.use_nlp_extraction:
            self.nlp_extractor = NLPEntityExtractor()
        else:
            self.nlp_extractor = None

        if self.config.use_email_verification:
            self.email_verifier = EmailVerifier()
        else:
            self.email_verifier = None

        # Additional Data Source Scrapers
        if self.config.use_github_extraction:
            self.github_scraper = GitHubOrganizationScraper(timeout=self.config.search_timeout)
        else:
            self.github_scraper = None

        if self.config.use_npm_extraction:
            self.npm_scraper = NPMPackageScraper(timeout=self.config.search_timeout)
        else:
            self.npm_scraper = None

        if self.config.use_hackernews_mentions:
            self.hackernews_scraper = HackerNewsScraper(timeout=self.config.search_timeout)
        else:
            self.hackernews_scraper = None

        if self.config.use_dns_intelligence:
            self.dns_intel = DNSIntelligence()
        else:
            self.dns_intel = None

        if self.config.use_ssl_subdomains:
            self.ssl_intel = SSLCertificateIntelligence(timeout=self.config.search_timeout)
        else:
            self.ssl_intel = None

        if self.config.use_wayback_machine:
            self.wayback_intel = WaybackIntelligence(timeout=self.config.search_timeout)
        else:
            self.wayback_intel = None

        # Paid API clients (initialized on demand)
        self._hunter_client = None
        self._clearbit_client = None

        # NLP initialization flag
        self._nlp_initialized = False

        # ========== OSINT ENGINE ==========
        if self.config.use_osint:
            self.osint_engine = MobiAdzOSINTEngine(
                timeout=self.config.search_timeout,
                brave_api_key=self.config.brave_api_key,
                crunchbase_api_key=self.config.crunchbase_api_key,
                theorg_api_key=self.config.theorg_api_key,
                search_cache_ttl=self.config.search_cache_ttl,
                search_cache_maxsize=self.config.search_cache_maxsize,
            )
        else:
            self.osint_engine = None

        # ========== FREE WEB SEARCH ENGINE ==========
        if self.config.use_free_web_search:
            self.web_search = MobiAdzWebSearch(
                timeout=self.config.search_timeout,
                brave_search_api_key=self.config.brave_search_api_key,
            )
        else:
            self.web_search = None

        # ========== LAYER 10: MULTI-SOURCE DISCOVERY ENGINES ==========
        if self.web_search:
            self.web_discovery = WebDiscoveryEngine(
                web_search=self.web_search, shared_client=self._shared_client
            )
            self.job_board_discovery = JobBoardDiscovery(
                web_search=self.web_search, shared_client=self._shared_client
            )
            self.startup_db_discovery = StartupDatabaseDiscovery(
                web_search=self.web_search, shared_client=self._shared_client
            )
        else:
            self.web_discovery = None
            self.job_board_discovery = None
            self.startup_db_discovery = None

        # Statistics
        self.stats = {
            "apps_found": 0,
            "playstore_apps_found": 0,
            "appstore_apps_found": 0,
            "fdroid_apps_found": 0,
            "microsoft_apps_found": 0,
            "huawei_apps_found": 0,
            "steam_apps_found": 0,
            "progressive_phase": "",
            "companies_found": 0,
            "emails_found": 0,
            "emails_verified": 0,
            "emails_smtp_verified": 0,
            "emails_ms365_verified": 0,
            "emails_bounce_scored": 0,
            "smtp_greylisting_retries": 0,
            "domain_ages_checked": 0,
            "disposable_emails_caught": 0,
            "pages_scraped": 0,
            "api_calls": 0,
            "sources_used": [],
            "bloom_filter_hits": 0,
            "cache_hits": 0,
            "nlp_entities_extracted": 0,
            "email_permutations_generated": 0,
            # OSINT stats
            "osint_leadership_found": 0,
            "osint_employees_found": 0,
            "osint_phones_found": 0,
            "osint_social_profiles_found": 0,
            # Layer 5 OSINT stats
            "osint_sec_officers_found": 0,
            "osint_press_execs_found": 0,
            "osint_blog_authors_found": 0,
            "osint_patent_inventors_found": 0,
            "osint_conference_speakers_found": 0,
            # Web search stats
            "web_search_queries": 0,
            "web_search_results": 0,
            "web_search_emails_found": 0,
            # Layer 7: Web Search Enhancement stats
            "search_cache_hits": 0,
            "search_cache_misses": 0,
            "search_engines_used": [],
            "search_circuit_breaker_trips": 0,
            "search_engine_rotations": 0,
            # ULTRA DEEP V2.0 stats
            "ultra_deep_emails_found": 0,
            "ultra_deep_paid_emails_found": 0,
            "ultra_deep_layers_completed": 0,
            "ultra_deep_multi_engine_hits": 0,
            "ultra_deep_archive_hits": 0,
            "ultra_deep_dns_hits": 0,
            "ultra_deep_whois_hits": 0,
            "ultra_deep_ct_hits": 0,
            "ultra_deep_sitemap_hits": 0,
            "ultra_deep_social_hits": 0,
            "ultra_deep_dev_platform_hits": 0,
            "ultra_deep_job_posting_hits": 0,
            "ultra_deep_press_hits": 0,
            "ultra_deep_startup_db_hits": 0,
            "ultra_deep_smtp_verified": 0,
            # Layer 8: Performance & Data Structures stats
            "file_cache_hits": 0,
            "file_cache_misses": 0,
            "circuit_breaker_trips": 0,
            "circuit_breaker_open_domains": 0,
            "bloom_filter_memory_bytes": 0,
            "priority_queue_used": True,
            "shared_connection_pool": True,
            # Layer 10: Multi-source discovery stats
            "web_search_companies_found": 0,
            "job_board_companies_found": 0,
            "startup_db_companies_found": 0,
            "remoteok_companies": 0,
            "hn_companies": 0,
            "producthunt_companies": 0,
            "yc_companies": 0,
            "g2_capterra_companies": 0,
            "discovery_sources_used": [],
            # Layer 11: Full-Spectrum Contact Discovery OSINT stats
            "osint_dept_contacts_found": 0,
            "osint_dept_emails_found": 0,
            "osint_linkedin_dept_contacts": 0,
            "osint_media_contacts_found": 0,
            "osint_crunchbase_team_found": 0,
            "osint_job_board_contacts_found": 0,
            "osint_podcast_guests_found": 0,
            "osint_wellfound_team_found": 0,
            "osint_email_patterns_detected": 0,
            "start_time": None,
            "end_time": None
        }

        # Progress tracking
        self.progress = {
            "stage": "idle",
            "stage_progress": 0,
            "total_progress": 0,
            "message": "Ready"
        }

        # Live contact callback for real-time updates
        self._live_contact_callback = None
        self._live_contact_counter = 0

        # Cancellation flag - checked at every stage boundary and inside loops
        self._cancelled = False

        # ========== RESILIENT FETCH HELPER ==========
        # Wraps shared client with retry + backoff + circuit breaker
        @retry_with_backoff(max_retries=3, base_delay=1.0, max_delay=15.0)
        async def _resilient_get(url: str, **kwargs) -> httpx.Response:
            """Shared HTTP GET with exponential backoff + decorrelated jitter."""
            return await self._shared_client.get(url, **kwargs)

        self._resilient_get = _resilient_get

        # Inject file cache and circuit breaker into website scraper
        if self.file_cache:
            self.website_scraper._file_cache = self.file_cache
        if self.circuit_breaker:
            self.website_scraper._circuit_breaker = self.circuit_breaker

        # Cross-stage deduplication: prevent same GitHub org/user being searched multiple times
        # across Ultra (Stage 4), OSINT (Stage 5), and Web Search (Stage 5.5) stages
        self._github_searched_orgs: Set[str] = set()
        self._github_searched_users: Set[str] = set()
        self._dns_searched_domains: Set[str] = set()

    def cancel(self):
        """Request cancellation of the running extraction."""
        self._cancelled = True
        logger.info("[ENGINE] Cancellation requested")

    @property
    def is_cancelled(self) -> bool:
        """Check if extraction has been cancelled."""
        return self._cancelled

    def _check_cancelled(self):
        """Raise exception if cancellation was requested. Call at every stage boundary."""
        if self._cancelled:
            raise ExtractionCancelled("Extraction was cancelled by user")

    def set_live_contact_callback(self, callback):
        """Set callback for live contact updates"""
        self._live_contact_callback = callback

    def _emit_live_contact(
        self,
        company_name: str,
        contact_type: str,
        source: str,
        confidence: int = 50,
        app_or_product: str = None,
        email: str = None,
        person_name: str = None,
        playstore_url: str = None,
        website: str = None,
        phone: str = ""
    ):
        """Emit a live contact event to the callback"""
        if self._live_contact_callback:
            self._live_contact_counter += 1
            contact_data = {
                "id": f"live_{self._live_contact_counter}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "company_name": company_name,
                "type": contact_type,
                "source": source,
                "confidence": confidence,
                "app_or_product": app_or_product,
                "email": email,
                "person_name": person_name,
                "playstore_url": playstore_url,
                "website": website,
                "phone": phone
            }
            self._live_contact_callback(contact_data)

    async def _initialize_nlp(self):
        """Initialize NLP components asynchronously."""
        if self.nlp_extractor and not self._nlp_initialized:
            await self.nlp_extractor.initialize()
            self._nlp_initialized = True

    def _update_progress(self, stage: str, stage_progress: int, message: str):
        """Update progress tracking"""
        stages = [
            "discovery", "app_scraping", "company_scraping", "contact_finding",
            "enrichment", "web_search", "ultra_deep", "verification", "complete"
        ]
        stage_idx = stages.index(stage) if stage in stages else 0

        self.progress = {
            "stage": stage,
            "stage_progress": stage_progress,
            "total_progress": int((stage_idx * 100 + stage_progress) / len(stages)),
            "message": message
        }

    async def run_extraction(
        self,
        demographics: Optional[List[Demographic]] = None,
        categories: Optional[List[ProductCategory]] = None
    ) -> List[CompanyContact]:
        """
        Run the full extraction pipeline.

        Args:
            demographics: Target regions
            categories: Product categories to search

        Returns:
            List of CompanyContact objects
        """
        self.stats["start_time"] = datetime.now(timezone.utc).isoformat()

        demographics = demographics or self.config.demographics
        categories = categories or self.config.categories

        all_contacts: List[CompanyContact] = []
        all_apps: List[AppData] = []

        try:
            # Stage 1: MULTI-SOURCE DISCOVERY
            # Routes each category to the appropriate discovery sources:
            # - App stores (Play Store, App Store, Steam, F-Droid, Microsoft, Huawei)
            # - Web search (DuckDuckGo, Bing, SearX with permutation queries)
            # - Job boards (RemoteOK API, HN Who's Hiring, Indeed/Glassdoor/Wellfound)
            # - Startup databases (ProductHunt, YC Directory, G2/Capterra, TechCrunch)
            self._check_cancelled()
            self._update_progress("discovery", 0, "Starting multi-source discovery...")

            # Determine which discovery sources are needed for ALL selected categories
            needs_app_stores = False
            needs_web_search = False
            needs_job_boards = False
            needs_startup_dbs = False
            for cat in categories:
                sources = CATEGORY_DISCOVERY_SOURCES.get(cat, ["app_stores", "web_search"])
                if "app_stores" in sources:
                    needs_app_stores = True
                if "web_search" in sources:
                    needs_web_search = True
                if "job_boards" in sources:
                    needs_job_boards = True
                if "startup_databases" in sources:
                    needs_startup_dbs = True

            # Track which discovery sources we actually use
            sources_used = []

            # === CATEGORY ROUTING (defined once, used in all phases) ===
            PLAY_STORE_CATEGORIES = {
                ProductCategory.MOBILE_APPS, ProductCategory.ANDROID_APPS,
                ProductCategory.ECOMMERCE, ProductCategory.SAAS,
                ProductCategory.FINTECH, ProductCategory.HEALTH_TECH,
                ProductCategory.PRODUCT_BASED, ProductCategory.ADS_BASED,
                ProductCategory.ED_TECH, ProductCategory.SOCIAL_MEDIA,
                ProductCategory.STREAMING, ProductCategory.PRODUCTIVITY,
                ProductCategory.ENTERPRISE,
            }

            APP_STORE_CATEGORIES = {
                ProductCategory.MOBILE_APPS, ProductCategory.IOS_APPS,
                ProductCategory.ECOMMERCE, ProductCategory.SAAS,
                ProductCategory.FINTECH, ProductCategory.HEALTH_TECH,
                ProductCategory.PRODUCT_BASED, ProductCategory.ADS_BASED,
                ProductCategory.ED_TECH, ProductCategory.SOCIAL_MEDIA,
                ProductCategory.STREAMING, ProductCategory.PRODUCTIVITY,
                ProductCategory.ENTERPRISE,
            }

            FDROID_CATEGORIES = {
                ProductCategory.MOBILE_APPS, ProductCategory.ANDROID_APPS,
                ProductCategory.PRODUCTIVITY, ProductCategory.SOCIAL_MEDIA,
                ProductCategory.ED_TECH, ProductCategory.ENTERPRISE,
                ProductCategory.SAAS,
            }

            MICROSOFT_STORE_CATEGORIES = {
                ProductCategory.PRODUCTIVITY, ProductCategory.ENTERPRISE,
                ProductCategory.SAAS, ProductCategory.GAMES,
                ProductCategory.ED_TECH, ProductCategory.STREAMING,
                ProductCategory.ECOMMERCE,
            }

            HUAWEI_CATEGORIES = {
                ProductCategory.MOBILE_APPS, ProductCategory.ANDROID_APPS,
                ProductCategory.ECOMMERCE, ProductCategory.FINTECH,
                ProductCategory.GAMES, ProductCategory.SOCIAL_MEDIA,
                ProductCategory.STREAMING,
            }

            # Huawei only for Asian demographics
            HUAWEI_DEMOGRAPHICS = {
                Demographic.EAST_ASIA, Demographic.SOUTHEAST_ASIA, Demographic.GLOBAL,
            }

            # Track seen app IDs to avoid re-processing across progressive phases
            seen_app_ids: Set[str] = set()

            def _collect_new_apps(found_apps: List[AppData], source: str) -> List[AppData]:
                """Filter out already-seen apps and emit live contacts for new ones"""
                new_apps = []
                for app in found_apps:
                    if app.app_id and app.app_id not in seen_app_ids:
                        seen_app_ids.add(app.app_id)
                        new_apps.append(app)

                        # Determine contact type based on discovery source
                        is_app_store = app.store in ("playstore", "appstore", "steam", "fdroid", "microsoft", "huawei")
                        contact_type = "app" if is_app_store else "company"

                        # Emit live contact
                        self._emit_live_contact(
                            company_name=app.developer_name or "Unknown Developer",
                            contact_type=contact_type,
                            source=source,
                            confidence=40 if is_app_store else 35,
                            app_or_product=app.app_name,
                            playstore_url=app.store_url if app.store == "playstore" else None,
                            website=app.developer_website
                        )

                        # Update per-store stats
                        store_key = f"{app.store}_apps_found"
                        if store_key in self.stats:
                            self.stats[store_key] += 1

                return new_apps

            async def _search_all_stores(
                category: 'ProductCategory',
                keyword: str,
                country: str,
                demographic: 'Demographic',
                max_results: int
            ) -> List[AppData]:
                """Search all applicable stores for a category/keyword/country combination"""
                phase_apps: List[AppData] = []

                # Play Store
                if category in PLAY_STORE_CATEGORIES:
                    try:
                        apps = await self.playstore.search_apps(
                            keyword, country=country, max_results=max_results
                        )
                        phase_apps.extend(_collect_new_apps(apps, "Play Store"))
                    except Exception as e:
                        logger.error(f"Play Store search error: {e}")
                    await smart_delay(self.config.delay_between_requests)

                # App Store
                if category in APP_STORE_CATEGORIES:
                    try:
                        apps = await self.appstore.search_apps(
                            keyword, country=country, max_results=max_results
                        )
                        phase_apps.extend(_collect_new_apps(apps, "App Store"))
                    except Exception as e:
                        logger.error(f"App Store search error: {e}")
                    await smart_delay(self.config.delay_between_requests)

                # Steam (games only)
                if category == ProductCategory.GAMES:
                    try:
                        games = await self.steam.search_games(
                            keyword, max_results=max_results
                        )
                        phase_apps.extend(_collect_new_apps(games, "Steam"))
                    except Exception as e:
                        logger.error(f"Steam search error: {e}")
                    await smart_delay(self.config.delay_between_requests)

                # F-Droid (open source apps)
                if self.fdroid and category in FDROID_CATEGORIES:
                    try:
                        apps = await self.fdroid.search_apps(
                            keyword, max_results=min(max_results, 10)
                        )
                        phase_apps.extend(_collect_new_apps(apps, "F-Droid"))
                    except Exception as e:
                        logger.error(f"F-Droid search error: {e}")
                    await smart_delay(self.config.delay_between_requests)

                # Microsoft Store (desktop/enterprise)
                if self.microsoft and category in MICROSOFT_STORE_CATEGORIES:
                    try:
                        apps = await self.microsoft.search_apps(
                            keyword, country=country, max_results=min(max_results, 10)
                        )
                        phase_apps.extend(_collect_new_apps(apps, "Microsoft Store"))
                    except Exception as e:
                        logger.error(f"Microsoft Store search error: {e}")
                    await smart_delay(self.config.delay_between_requests)

                # Huawei AppGallery (Asian markets)
                if self.huawei and category in HUAWEI_CATEGORIES and demographic in HUAWEI_DEMOGRAPHICS:
                    try:
                        apps = await self.huawei.search_apps(
                            keyword, country=country, max_results=min(max_results, 10)
                        )
                        phase_apps.extend(_collect_new_apps(apps, "Huawei AppGallery"))
                    except Exception as e:
                        logger.error(f"Huawei AppGallery search error: {e}")
                    await smart_delay(self.config.delay_between_requests)

                return phase_apps

            # ============ PROGRESSIVE APP STORE SEARCH ============
            # Only run for categories that use app_stores as a discovery source
            if needs_app_stores:
                sources_used.append("app_stores")

                progressive_phases = [
                    (3, "Top 3", 5),     # (max_results, label, progress_pct)
                    (5, "Top 5", 12),
                    (10, "Top 10", 22),
                    (20, "Top 20", 32),
                ]

                # Filter to only categories that actually use app stores
                app_store_cats = [
                    c for c in categories
                    if "app_stores" in CATEGORY_DISCOVERY_SOURCES.get(c, [])
                ]

                for phase_max, phase_label, progress_pct in progressive_phases:
                    self._check_cancelled()
                    self.stats["progressive_phase"] = phase_label
                    self._update_progress(
                        "discovery", progress_pct,
                        f"App stores: {phase_label} per keyword..."
                    )

                    for category in app_store_cats:
                        self._check_cancelled()
                        keywords = CATEGORY_KEYWORDS.get(category, [category.value])

                        for demographic in demographics:
                            countries = DEMOGRAPHIC_COUNTRIES.get(demographic, ["us"])

                            for country in countries[:2]:
                                self._check_cancelled()

                                for keyword in keywords[:3]:
                                    new_apps = await _search_all_stores(
                                        category, keyword, country, demographic, phase_max
                                    )
                                    all_apps.extend(new_apps)
                                    self.stats["apps_found"] = len(all_apps)

                    logger.info(
                        f"Progressive {phase_label} complete: "
                        f"{len(all_apps)} total apps, {len(seen_app_ids)} unique"
                    )

                # Phase 5: Deep search - ALL keywords, higher count
                self._check_cancelled()
                self.stats["progressive_phase"] = "Deep Search"
                self._update_progress(
                    "discovery", 38,
                    f"App stores: deep search with all keywords..."
                )

                for category in app_store_cats:
                    self._check_cancelled()
                    keywords = CATEGORY_KEYWORDS.get(category, [category.value])

                    for demographic in demographics:
                        countries = DEMOGRAPHIC_COUNTRIES.get(demographic, ["us"])

                        for country in countries[:2]:
                            self._check_cancelled()

                            for keyword in keywords:
                                new_apps = await _search_all_stores(
                                    category, keyword, country, demographic,
                                    self.config.max_apps_per_category
                                )
                                all_apps.extend(new_apps)
                                self.stats["apps_found"] = len(all_apps)

                logger.info(f"App store discovery complete: {len(all_apps)} apps found")

            # ============ WEB SEARCH DISCOVERY ============
            # Discovers companies via DuckDuckGo, Bing, SearX with many query permutations
            if needs_web_search and self.web_discovery:
                self._check_cancelled()
                sources_used.append("web_search")
                self._update_progress(
                    "discovery", 50,
                    f"Web search discovery: searching with permutation queries..."
                )

                web_cats = [
                    c for c in categories
                    if "web_search" in CATEGORY_DISCOVERY_SOURCES.get(c, [])
                ]

                for category in web_cats:
                    self._check_cancelled()
                    for demographic in demographics:
                        try:
                            web_apps = await self.web_discovery.discover_companies(
                                category=category,
                                demographic=demographic,
                                max_companies=self.config.max_companies // 3,
                                bloom_filter=self.url_bloom,
                            )
                            # Collect into all_apps using same dedup
                            new = _collect_new_apps(web_apps, "Web Search")
                            all_apps.extend(new)
                            self.stats["web_search_companies_found"] += len(new)
                            self.stats["apps_found"] = len(all_apps)
                        except Exception as e:
                            logger.warning(f"Web discovery error for {category.value}: {e}")

                logger.info(
                    f"Web search discovery complete: "
                    f"{self.stats['web_search_companies_found']} companies found"
                )

            # ============ JOB BOARD DISCOVERY ============
            # Discovers companies via RemoteOK, HN Hiring, Indeed/Glassdoor/Wellfound
            if needs_job_boards and self.job_board_discovery:
                self._check_cancelled()
                sources_used.append("job_boards")
                self._update_progress(
                    "discovery", 70,
                    f"Job board discovery: searching RemoteOK, HN, Indeed..."
                )

                job_cats = [
                    c for c in categories
                    if "job_boards" in CATEGORY_DISCOVERY_SOURCES.get(c, [])
                ]

                for category in job_cats:
                    self._check_cancelled()
                    try:
                        job_apps = await self.job_board_discovery.discover_companies(
                            category=category,
                            max_companies=self.config.max_companies // 4,
                            bloom_filter=self.url_bloom,
                        )
                        new = _collect_new_apps(job_apps, "Job Board")
                        all_apps.extend(new)
                        self.stats["job_board_companies_found"] += len(new)
                        self.stats["apps_found"] = len(all_apps)
                    except Exception as e:
                        logger.warning(f"Job board discovery error for {category.value}: {e}")

                logger.info(
                    f"Job board discovery complete: "
                    f"{self.stats['job_board_companies_found']} companies found"
                )

            # ============ STARTUP DATABASE DISCOVERY ============
            # Discovers companies via ProductHunt, YC, G2/Capterra, TechCrunch
            if needs_startup_dbs and self.startup_db_discovery:
                self._check_cancelled()
                sources_used.append("startup_databases")
                self._update_progress(
                    "discovery", 85,
                    f"Startup database discovery: ProductHunt, YC, G2..."
                )

                startup_cats = [
                    c for c in categories
                    if "startup_databases" in CATEGORY_DISCOVERY_SOURCES.get(c, [])
                ]

                for category in startup_cats:
                    self._check_cancelled()
                    try:
                        startup_apps = await self.startup_db_discovery.discover_companies(
                            category=category,
                            max_companies=self.config.max_companies // 4,
                            bloom_filter=self.url_bloom,
                        )
                        new = _collect_new_apps(startup_apps, "Startup DB")
                        all_apps.extend(new)
                        self.stats["startup_db_companies_found"] += len(new)
                        self.stats["apps_found"] = len(all_apps)
                    except Exception as e:
                        logger.warning(f"Startup DB discovery error for {category.value}: {e}")

                logger.info(
                    f"Startup database discovery complete: "
                    f"{self.stats['startup_db_companies_found']} companies found"
                )

            # Discovery complete
            self.stats["apps_found"] = len(all_apps)
            self.stats["progressive_phase"] = "Complete"
            self.stats["discovery_sources_used"] = sources_used
            self._update_progress(
                "discovery", 100,
                f"Discovery complete: {len(all_apps)} companies from {', '.join(sources_used)}"
            )

            # Stage 2: Deduplicate and group by developer
            self._check_cancelled()
            self._update_progress("app_scraping", 0, "Processing developers...")

            developers = {}
            for app in all_apps:
                dev_key = app.developer_name.lower() if app.developer_name else app.developer_website
                if dev_key:
                    if dev_key not in developers:
                        # For web/job/startup-discovered companies, use store_url as fallback website
                        website = app.developer_website
                        if not website and app.store in ("web_search", "job_board", "startup_db"):
                            website = app.store_url
                        developers[dev_key] = {
                            "name": app.developer_name,
                            "website": website,
                            "apps": []
                        }
                    developers[dev_key]["apps"].append(app)

            # Stage 3: Scrape company websites - PARALLEL PROCESSING for speed
            self._check_cancelled()
            self._update_progress("company_scraping", 0, f"Pre-resolving DNS for {len(developers)} domains...")

            # DNS prefetch: resolve all target domains in parallel before scraping
            # Saves ~50-100ms per domain by caching DNS results in OS resolver
            prefetcher = DNSPrefetcher(concurrency=30)
            target_domains = []
            for dev_info in developers.values():
                website = dev_info.get("website")
                if website:
                    try:
                        parsed = urlparse(website)
                        domain = parsed.netloc.replace("www.", "") if parsed.netloc else None
                        if domain:
                            target_domains.append(domain)
                    except Exception as e:
                        logger.debug(f"Non-critical error in URL domain parsing for DNS prefetch: {e}")
            if target_domains:
                await prefetcher.prefetch_domains(target_domains)
                # Warm up connections using the shared client's pool
                await prefetcher.warmup_connections(target_domains, client=self._shared_client)

            self._update_progress("company_scraping", 0, f"Scraping {len(developers)} company websites (parallel)...")

            # Per-domain concurrency control: global(15) + per-domain(3)
            # Prevents hammering any single domain while maintaining high throughput
            domain_sem = get_domain_semaphore(global_limit=15, per_domain_limit=3)
            # AIMD adaptive concurrency: starts at 8, grows on success, shrinks on failure
            aimd = AdaptiveConcurrencyController(min_concurrency=2, max_concurrency=20, initial_concurrency=8)
            companies_processed = 0
            total_to_process = min(len(developers), self.config.max_companies)

            async def scrape_single_company(dev_key: str, dev_info: dict) -> Optional[CompanyContact]:
                """Scrape a single company website with AIMD + per-domain rate limiting"""
                nonlocal companies_processed

                # Check cancellation before each company
                if self._cancelled:
                    return None

                website = dev_info.get("website")
                # Acquire AIMD adaptive slot + per-domain semaphore
                domain_key = website or dev_key
                async with aimd.acquire(), domain_sem.acquire(domain_key):
                    if website:
                        try:
                            # Extract domain
                            parsed = urlparse(website)
                            domain = parsed.netloc.replace("www.", "")

                            if domain:
                                # Deep scrape company website
                                company_data = await self.website_scraper.scrape_company(
                                    domain,
                                    max_pages=self.config.max_pages_per_site
                                )

                                self.stats["pages_scraped"] += company_data.get("pages_scraped", 0)

                                # Extract emails from website scrape - all categories
                                emails_dict = company_data.get("emails", {})
                                contact_email = next(iter(emails_dict.get("contact", [])), None)
                                marketing_email = next(iter(emails_dict.get("marketing", [])), None)
                                sales_email = next(iter(emails_dict.get("sales", [])), None)
                                support_email = next(iter(emails_dict.get("support", [])), None)
                                press_email = next(iter(emails_dict.get("press", [])), None)

                                # Promote HR/legal/finance/dev emails to contact_email if empty
                                if not contact_email:
                                    for extra_cat in ["hr", "legal", "finance", "other"]:
                                        extra_emails = emails_dict.get(extra_cat, [])
                                        if extra_emails:
                                            contact_email = next(iter(extra_emails))
                                            break

                                # Also collect ALL other/overflow emails found on the website
                                other_emails = list(emails_dict.get("other", []))
                                for cat_name, cat_emails in emails_dict.items():
                                    if cat_name not in ("contact", "marketing", "sales", "support", "press"):
                                        other_emails.extend(list(cat_emails))

                                # Fallback: If no website emails found, use developer_email from app store page
                                if not contact_email and not marketing_email and not sales_email:
                                    for app in dev_info["apps"]:
                                        if hasattr(app, 'developer_email') and app.developer_email:
                                            contact_email = app.developer_email
                                            break

                                # If still no categorized emails, promote first "other" email
                                if not contact_email and not marketing_email and not sales_email and other_emails:
                                    contact_email = other_emails[0]

                                # Fill empty slots with overflow emails
                                overflow_pool = [e for e in other_emails if e not in (contact_email, marketing_email, sales_email, support_email, press_email)]
                                for overflow_email in overflow_pool[:3]:
                                    if not marketing_email:
                                        marketing_email = overflow_email
                                    elif not sales_email:
                                        sales_email = overflow_email
                                    elif not press_email:
                                        press_email = overflow_email

                                company_name = company_data.get("company_info", {}).get("name") or dev_info["name"]
                                app_name = dev_info["apps"][0].app_name if dev_info["apps"] else None
                                playstore_url = dev_info["apps"][0].store_url if dev_info["apps"] and dev_info["apps"][0].store == "playstore" else None

                                # Capture store email for source attribution below
                                store_email = None
                                for app in dev_info["apps"]:
                                    if hasattr(app, 'developer_email') and app.developer_email:
                                        store_email = app.developer_email
                                        break

                                data_sources = ["website_scrape"]
                                if store_email:
                                    data_sources.append("store_email")

                                # Create contact record
                                contact = CompanyContact(
                                    company_name=company_name,
                                    app_or_product=app_name,
                                    product_category=dev_info["apps"][0].category if dev_info["apps"] else None,
                                    demographic=dev_info["apps"][0].demographic if dev_info["apps"] else None,
                                    company_website=website,
                                    company_domain=domain,
                                    company_description=company_data.get("company_info", {}).get("description"),
                                    company_linkedin=company_data.get("social_links", {}).get("linkedin"),
                                    company_size=company_data.get("company_info", {}).get("size"),
                                    company_industry=company_data.get("company_info", {}).get("industry"),
                                    company_founded=company_data.get("company_info", {}).get("foundingDate"),
                                    company_location=company_data.get("company_info", {}).get("location"),
                                    company_phones=company_data.get("company_info", {}).get("phones", []),
                                    contact_email=contact_email,
                                    marketing_email=marketing_email,
                                    sales_email=sales_email,
                                    support_email=support_email,
                                    press_email=press_email,
                                    people=company_data.get("people", []),
                                    playstore_url=playstore_url,
                                    appstore_url=dev_info["apps"][0].store_url if dev_info["apps"] and dev_info["apps"][0].store == "appstore" else None,
                                    data_sources=data_sources,
                                    confidence_score=self._calculate_confidence(company_data)
                                )

                                # Layer 9: Record which sources found each email
                                # Use page-type tracking for more accurate source attribution
                                email_page_sources = company_data.get("email_page_sources", {})

                                def _get_source_tag(email_addr, fallback="website_scrape"):
                                    """Determine source tag based on which page the email was found on."""
                                    page_info = email_page_sources.get(email_addr)
                                    if not page_info:
                                        return fallback
                                    ptype = page_info.get("page_type", "other")
                                    if ptype in ("contact", "team", "about"):
                                        return "website_contact"  # High confidence: 0.85
                                    return "website_scrape"  # Standard confidence: 0.80

                                if contact_email:
                                    source_tag = "store_email" if (contact_email == store_email) else _get_source_tag(contact_email, "website_contact")
                                    self._record_email_source(contact, "contact_email", source_tag)
                                if marketing_email:
                                    self._record_email_source(contact, "marketing_email", _get_source_tag(marketing_email))
                                if sales_email:
                                    self._record_email_source(contact, "sales_email", _get_source_tag(sales_email))
                                if support_email:
                                    self._record_email_source(contact, "support_email", _get_source_tag(support_email))
                                if press_email:
                                    self._record_email_source(contact, "press_email", _get_source_tag(press_email))

                                # Emit company discovery
                                self._emit_live_contact(
                                    company_name=company_name,
                                    contact_type="company",
                                    source=f"Website ({domain})",
                                    confidence=contact.confidence_score,
                                    app_or_product=app_name,
                                    playstore_url=playstore_url,
                                    website=website
                                )

                                # Emit email discoveries
                                primary_email = contact_email or marketing_email or sales_email
                                if primary_email:
                                    self._emit_live_contact(
                                        company_name=company_name,
                                        contact_type="email",
                                        source=f"Website Scrape ({domain})",
                                        confidence=contact.confidence_score,
                                        app_or_product=app_name,
                                        email=primary_email,
                                        playstore_url=playstore_url,
                                        website=website
                                    )

                                # Emit people/leaders found
                                for person in company_data.get("people", [])[:3]:
                                    person_email = person.get("emails", [None])[0] if person.get("emails") else None
                                    self._emit_live_contact(
                                        company_name=company_name,
                                        contact_type="leadership" if "CEO" in str(person.get("title", "")) or "Founder" in str(person.get("title", "")) else "person",
                                        source=f"Website ({domain})",
                                        confidence=person.get("confidence", 60),
                                        app_or_product=app_name,
                                        person_name=person.get("name"),
                                        email=person_email,
                                        website=website
                                    )

                                companies_processed += 1
                                aimd.record_success()  # AIMD: successful scrape → increase concurrency
                                # Update progress
                                progress = int(companies_processed / total_to_process * 100)
                                self._update_progress("company_scraping", progress, f"Scraped {companies_processed}/{total_to_process}: {domain} (concurrency: {aimd.current_concurrency})")

                                return contact

                        except Exception as e:
                            logger.warning(f"Error processing {dev_key}: {e}")
                            aimd.record_failure()  # AIMD: failed scrape → halve concurrency
                            companies_processed += 1

                    # Create record even without website (use email from Play Store if available)
                    elif dev_info["name"]:
                        # Check if any app has a developer_email from store page
                        store_email = None
                        for app in dev_info["apps"]:
                            if hasattr(app, 'developer_email') and app.developer_email:
                                store_email = app.developer_email
                                break

                        company_name = dev_info["name"]
                        app_name = dev_info["apps"][0].app_name if dev_info["apps"] else None
                        playstore_url = dev_info["apps"][0].store_url if dev_info["apps"] and dev_info["apps"][0].store == "playstore" else None

                        contact = CompanyContact(
                            company_name=company_name,
                            app_or_product=app_name,
                            product_category=dev_info["apps"][0].category if dev_info["apps"] else None,
                            demographic=dev_info["apps"][0].demographic if dev_info["apps"] else None,
                            contact_email=store_email,  # Use email from store page if available
                            playstore_url=playstore_url,
                            appstore_url=dev_info["apps"][0].store_url if dev_info["apps"] and dev_info["apps"][0].store == "appstore" else None,
                            data_sources=["app_store", "store_email"] if store_email else ["app_store"],
                            confidence_score=50 if store_email else 30  # Higher confidence if we have email
                        )

                        # Emit company discovery
                        self._emit_live_contact(
                            company_name=company_name,
                            contact_type="company",
                            source="App Store Page",
                            confidence=contact.confidence_score,
                            app_or_product=app_name,
                            playstore_url=playstore_url
                        )

                        # Emit email if found from store + record source
                        if store_email:
                            self._record_email_source(contact, "contact_email", "store_email")
                            self._emit_live_contact(
                                company_name=company_name,
                                contact_type="email",
                                source="Play Store Developer Info",
                                confidence=50,
                                app_or_product=app_name,
                                email=store_email,
                                playstore_url=playstore_url
                            )

                        companies_processed += 1
                        return contact

                    return None

            # Run all company scrapes in parallel with rate limiting
            # Wrap each task with a 90-second timeout to prevent hanging
            PER_COMPANY_TIMEOUT = 90  # seconds

            async def _timed_scrape(dev_key, dev_info):
                try:
                    return await asyncio.wait_for(
                        scrape_single_company(dev_key, dev_info),
                        timeout=PER_COMPANY_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout scraping {dev_key} after {PER_COMPANY_TIMEOUT}s, skipping")
                    return None

            scrape_tasks = [
                _timed_scrape(dev_key, dev_info)
                for dev_key, dev_info in list(developers.items())[:self.config.max_companies]
            ]

            # Process in batches of 10 for faster progress updates
            batch_size = 10
            for i in range(0, len(scrape_tasks), batch_size):
                self._check_cancelled()
                batch = scrape_tasks[i:i + batch_size]
                results = await asyncio.gather(*batch, return_exceptions=True)

                for result in results:
                    if isinstance(result, CompanyContact):
                        all_contacts.append(result)
                        # Update stats incrementally
                        self.stats["companies_found"] = len(all_contacts)
                        if result.contact_email or result.marketing_email or result.sales_email or result.support_email or result.press_email:
                            self.stats["emails_found"] += 1

                self._update_progress(
                    "company_scraping",
                    int(min(i + batch_size, len(scrape_tasks)) / len(scrape_tasks) * 100),
                    f"Processed batch {i // batch_size + 1}/{(len(scrape_tasks) + batch_size - 1) // batch_size}"
                )

            # ========== STAGE 4: ULTRA EXTRACTION ==========
            self._check_cancelled()
            if self.config.deep_extraction_mode:
                await self._run_ultra_extraction(all_contacts)

            # ========== STAGE 5: DEEP OSINT ==========
            self._check_cancelled()
            if self.config.use_osint:
                await self._run_osint_extraction(all_contacts)

            # ========== STAGE 5.5: FREE WEB SEARCH ENHANCEMENT ==========
            self._check_cancelled()
            if self.config.use_free_web_search:
                self._update_progress("web_search", 0, "Starting FREE web search enhancement...")
                try:
                    await self._web_search_enhancement(all_contacts)
                    self._update_progress("web_search", 100, f"Web search complete: {self.stats.get('web_search_emails_found', 0)} additional emails found")
                except ExtractionCancelled:
                    raise  # Re-raise cancellation
                except Exception as e:
                    logger.warning(f"Web search enhancement failed: {e}")
                    self._update_progress("web_search", 100, f"Web search completed with errors")

            # ========== STAGE 6: ULTRA DEEP SEARCH V2.0 ==========
            self._check_cancelled()
            if self.config.use_ultra_deep_search and ULTRA_DEEP_AVAILABLE:
                self._update_progress("ultra_deep", 0, "Starting ULTRA DEEP Search V2.0...")
                try:
                    await self._run_ultra_deep_extraction(all_contacts)
                    ultra_deep_found = self.stats.get('ultra_deep_emails_found', 0)
                    self._update_progress("ultra_deep", 100, f"ULTRA DEEP complete: {ultra_deep_found} emails via 15+ layers")
                except ExtractionCancelled:
                    raise  # Re-raise cancellation
                except Exception as e:
                    logger.warning(f"ULTRA DEEP extraction failed: {e}")
                    self._update_progress("ultra_deep", 100, f"ULTRA DEEP completed with errors: {str(e)[:50]}")

            # Stage 7: Enrichment (if paid mode or fallback)
            self._check_cancelled()
            if self.config.use_paid_apis:
                await self._enrich_with_paid_apis(all_contacts)

            # Stage 8: Email Verification & Deduplication
            self._check_cancelled()
            if self.config.use_email_verification or self.config.use_fuzzy_matching:
                await self._verify_and_deduplicate(all_contacts)

            # Stage 9: Calculate final stats
            self.stats["companies_found"] = len(all_contacts)
            self.stats["emails_found"] = sum(
                1 for c in all_contacts if c.contact_email or c.marketing_email or c.sales_email or c.support_email or c.press_email
            )
            self.stats["end_time"] = datetime.now(timezone.utc).isoformat()

            # Layer 8 performance stats
            if self.file_cache:
                self.stats["file_cache_hits"] = self.file_cache.stats["hits"]
                self.stats["file_cache_misses"] = self.file_cache.stats["misses"]
            if self.circuit_breaker:
                self.stats["circuit_breaker_trips"] = self.circuit_breaker.total_trips
                self.stats["circuit_breaker_open_domains"] = len(self.circuit_breaker.get_open_domains())
            if self.url_bloom:
                self.stats["bloom_filter_memory_bytes"] = self.url_bloom.memory_bytes + (self.email_bloom.memory_bytes if self.email_bloom else 0)

            # Summary message
            summary = f"[DONE] ULTRA PRO MAX Extraction complete: {len(all_contacts)} companies"
            summary += f", {self.stats['emails_found']} emails"
            if self.stats.get('osint_leadership_found'):
                summary += f", {self.stats['osint_leadership_found']} leadership"
            if self.stats.get('osint_employees_found'):
                summary += f", {self.stats['osint_employees_found']} employees"
            if self.stats.get('web_search_emails_found'):
                summary += f", {self.stats['web_search_emails_found']} web search"
            if self.stats.get('ultra_deep_emails_found'):
                summary += f", {self.stats['ultra_deep_emails_found']} ultra deep"
            if self.stats.get('ultra_deep_paid_emails_found'):
                summary += f", {self.stats['ultra_deep_paid_emails_found']} paid APIs"
            # Layer 5 stats
            l5_total = (self.stats.get('osint_sec_officers_found', 0) +
                        self.stats.get('osint_press_execs_found', 0) +
                        self.stats.get('osint_blog_authors_found', 0) +
                        self.stats.get('osint_patent_inventors_found', 0) +
                        self.stats.get('osint_conference_speakers_found', 0))
            if l5_total:
                summary += f", {l5_total} Layer5 OSINT"

            # Layer 11 Full-Spectrum Contact Discovery stats
            l11_total = sum(
                self.stats.get(f"osint_{k}", 0) for k in [
                    "dept_contacts_found", "linkedin_dept_contacts", "media_contacts_found",
                    "crunchbase_team_found", "job_board_contacts_found",
                    "podcast_guests_found", "wellfound_team_found",
                ]
            )
            if l11_total:
                summary += f", {l11_total} Layer11 Full-Spectrum"

            # Layer 6 stats
            l6_parts = []
            if self.stats.get('emails_ms365_verified'):
                l6_parts.append(f"{self.stats['emails_ms365_verified']} MS365")
            if self.stats.get('emails_bounce_scored'):
                l6_parts.append(f"{self.stats['emails_bounce_scored']} bounce-scored")
            if self.stats.get('disposable_emails_caught'):
                l6_parts.append(f"{self.stats['disposable_emails_caught']} disposable")
            if self.stats.get('domain_ages_checked'):
                l6_parts.append(f"{self.stats['domain_ages_checked']} age-checked")
            if l6_parts:
                summary += f", L6: {', '.join(l6_parts)}"

            # Layer 7 stats
            l7_parts = []
            if self.stats.get('search_cache_hits'):
                l7_parts.append(f"{self.stats['search_cache_hits']} cache-hits")
            if self.stats.get('search_engine_rotations'):
                l7_parts.append(f"{self.stats['search_engine_rotations']} rotations")
            if self.stats.get('search_circuit_breaker_trips'):
                l7_parts.append(f"{self.stats['search_circuit_breaker_trips']} CB-trips")
            if l7_parts:
                summary += f", L7: {', '.join(l7_parts)}"

            self._update_progress("complete", 100, summary)

        except ExtractionCancelled:
            logger.info(f"Extraction cancelled by user. Returning {len(all_contacts)} contacts found so far.")
            self.stats["end_time"] = datetime.now(timezone.utc).isoformat()
            self._update_progress("complete", 100, f"Cancelled - returning {len(all_contacts)} contacts found so far")

        except Exception as e:
            logger.error(f"Extraction error: {e}")
            self.stats["end_time"] = datetime.now(timezone.utc).isoformat()

        # Apply exclusion filters (for re-runs that want to avoid duplicates from prior jobs)
        if self.config.exclude_domains or self.config.exclude_emails:
            exclude_domains_set = set(d.lower() for d in self.config.exclude_domains)
            exclude_emails_set = set(e.lower() for e in self.config.exclude_emails)
            before_count = len(all_contacts)

            filtered = []
            for contact in all_contacts:
                # Skip if domain is excluded
                if contact.company_domain and contact.company_domain.lower() in exclude_domains_set:
                    continue
                # Skip if any email matches exclusion list
                contact_emails = [
                    e for e in [contact.contact_email, contact.marketing_email,
                                contact.sales_email, contact.support_email, contact.press_email] if e
                ]
                if any(e.lower() in exclude_emails_set for e in contact_emails):
                    continue
                filtered.append(contact)

            all_contacts = filtered
            excluded_count = before_count - len(all_contacts)
            if excluded_count > 0:
                logger.info(f"[EXCLUDE] Filtered out {excluded_count} contacts matching exclusion criteria")

        return all_contacts

    async def _enrich_with_paid_apis(self, contacts: List[CompanyContact]):
        """Enrich contacts using paid APIs with free fallback"""
        self._update_progress("enrichment", 0, "Enriching with additional data...")

        for i, contact in enumerate(contacts):
            try:
                # Try Hunter.io if API key provided
                if self.config.hunter_api_key and contact.company_domain:
                    enriched = await self._hunter_enrich(contact.company_domain)
                    if enriched:
                        contact.data_sources.append("hunter.io")
                        # Update emails if found
                        if enriched.get("emails"):
                            for email_data in enriched["emails"]:
                                email = email_data.get("value")
                                email_type = email_data.get("type", "generic")

                                if email_type == "generic" and not contact.contact_email:
                                    contact.contact_email = email
                                elif "marketing" in email_type and not contact.marketing_email:
                                    contact.marketing_email = email
                                elif "sales" in email_type and not contact.sales_email:
                                    contact.sales_email = email

                # Try Clearbit if API key provided
                if self.config.clearbit_api_key and contact.company_domain:
                    enriched = await self._clearbit_enrich(contact.company_domain)
                    if enriched:
                        contact.data_sources.append("clearbit")
                        if not contact.company_size:
                            contact.company_size = enriched.get("metrics", {}).get("employeesRange")
                        if not contact.company_industry:
                            contact.company_industry = enriched.get("category", {}).get("industry")

                progress = int((i + 1) / len(contacts) * 100)
                self._update_progress("enrichment", progress, f"Enriched {i + 1}/{len(contacts)}")

            except Exception as e:
                logger.warning(f"Enrichment error for {contact.company_name}: {e}")

    async def _hunter_enrich(self, domain: str) -> Optional[Dict]:
        """Enrich using Hunter.io API"""
        if not self.config.hunter_api_key:
            return None

        try:
            url = "https://api.hunter.io/v2/domain-search"
            params = {
                "domain": domain,
                "api_key": self.config.hunter_api_key
            }

            async with httpx.AsyncClient(
                    transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
                    timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
                    follow_redirects=True,
                ) as client:
                response = await client.get(url, params=params)

                if response.status_code == 200:
                    self.stats["api_calls"] += 1
                    return response.json().get("data")

        except Exception as e:
            logger.warning(f"Hunter.io API error: {e}")

        return None

    async def _clearbit_enrich(self, domain: str) -> Optional[Dict]:
        """Enrich using Clearbit API"""
        if not self.config.clearbit_api_key:
            return None

        try:
            url = f"https://company.clearbit.com/v2/companies/find?domain={domain}"
            headers = {"Authorization": f"Bearer {self.config.clearbit_api_key}"}

            async with httpx.AsyncClient(
                    transport=httpx.AsyncHTTPTransport(retries=3, http2=True),
                    timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
                    follow_redirects=True,
                ) as client:
                response = await client.get(url, headers=headers)

                if response.status_code == 200:
                    self.stats["api_calls"] += 1
                    return response.json()

        except Exception as e:
            logger.warning(f"Clearbit API error: {e}")

        return None

    def _calculate_confidence(self, company_data: Dict) -> int:
        """Calculate confidence score for extracted data"""
        score = 0

        emails = company_data.get("emails", {})
        if emails.get("contact"):
            score += 25
        if emails.get("marketing"):
            score += 15
        if emails.get("sales"):
            score += 15
        if emails.get("support"):
            score += 10
        if emails.get("press"):
            score += 5
        if emails.get("hr") or emails.get("legal") or emails.get("finance"):
            score += 5

        if company_data.get("social_links", {}).get("linkedin"):
            score += 10

        if company_data.get("company_info", {}).get("name"):
            score += 10

        if company_data.get("people"):
            score += min(len(company_data["people"]) * 3, 10)

        return min(score, 100)

    # ========== ULTRA ENGINE METHODS ==========

    async def _run_ultra_extraction(self, contacts: List[CompanyContact]):
        """
        Run ULTRA extraction for additional data sources.

        This method enhances contacts with:
        - GitHub organization emails
        - npm registry maintainer emails
        - DNS intelligence (MX/TXT records)
        - SSL certificate subdomains
        - Wayback Machine historical emails
        - NLP entity extraction
        - Email permutation generation

        Layer 14: Parallelized with semaphore(10) for ~10x speedup.
        """
        self._update_progress("contact_finding", 0, "Starting ULTRA extraction...")

        # Initialize NLP if needed
        await self._initialize_nlp()

        total = len(contacts)
        completed = [0]  # mutable counter for progress tracking

        sem = asyncio.Semaphore(10)

        async def process_one_ultra(contact):
            async with sem:
                self._check_cancelled()
                try:
                    await asyncio.wait_for(
                        self._ultra_extract_single(contact, total, completed),
                        timeout=120
                    )
                except ExtractionCancelled:
                    raise
                except asyncio.TimeoutError:
                    logger.warning(f"Ultra extraction timeout (120s) for {contact.company_name}, skipping")
                except Exception as e:
                    logger.warning(f"Ultra extraction error for {contact.company_name}: {e}")
                finally:
                    completed[0] += 1
                    progress = int((completed[0] / total) * 100)
                    self._update_progress("contact_finding", progress, f"ULTRA: {completed[0]}/{total} complete")

        tasks = [process_one_ultra(c) for c in contacts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Re-raise cancellation if any task was cancelled
        for r in results:
            if isinstance(r, ExtractionCancelled):
                raise r

        self._update_progress("contact_finding", 100, f"ULTRA extraction complete for {total} companies")

    async def _ultra_extract_single(self, contact: CompanyContact, total: int, completed: list):
        """Process a single contact through ULTRA extraction pipeline."""
        domain = contact.company_domain
        company_name = contact.company_name

        if not domain:
            return

        # 1. Enhanced DNS Intelligence (with cross-stage dedup)
        if self.dns_intel and self.config.use_dns_intelligence and domain not in self._dns_searched_domains:
            self._dns_searched_domains.add(domain)
            try:
                dns_data = await asyncio.wait_for(
                    self.dns_intel.get_domain_intelligence(domain), timeout=30
                )

                # Extract emails from DNS records (TXT, SPF, DMARC rua/ruf)
                dns_emails = dns_data.get("emails_from_txt", [])
                for email in dns_emails[:3]:
                    self._assign_email(contact, "contact_email", email, "dns_intel")
                    if "dns_intel" not in contact.data_sources:
                        contact.data_sources.append("dns_intel")

                # Store primary email provider (Google Workspace, Microsoft 365, etc.)
                if dns_data.get("email_provider"):
                    contact.email_provider = dns_data["email_provider"]

                # Store all detected email providers (MX + SPF combined)
                if dns_data.get("email_providers"):
                    contact.email_providers = dns_data["email_providers"]

                # Check if domain has confirmed email service
                if dns_data.get("has_email_service"):
                    if "dns_verified" not in contact.data_sources:
                        contact.data_sources.append("dns_verified")

                # BIMI presence = strong brand indicator (mature email infrastructure)
                if dns_data.get("bimi", {}).get("has_bimi"):
                    if "bimi_verified" not in contact.data_sources:
                        contact.data_sources.append("bimi_verified")

                # Security gateway detection (Proofpoint, Mimecast, Barracuda)
                if dns_data.get("has_security_gateway"):
                    if "security_gateway" not in contact.data_sources:
                        contact.data_sources.append("security_gateway")

                # Store infrastructure score for later use in confidence calc
                if dns_data.get("infrastructure_score", 0) > 0:
                    contact.dns_infrastructure_score = dns_data["infrastructure_score"]

                # Store mail subdomains from DNS for reference
                spf_providers = dns_data.get("spf_providers", {})
                if spf_providers.get("sending_services"):
                    contact.spf_sending_services = spf_providers["sending_services"]

                logger.debug(
                    f"DNS intel for {domain}: provider={dns_data.get('email_provider')}, "
                    f"providers={dns_data.get('email_providers')}, "
                    f"infra_score={dns_data.get('infrastructure_score', 0)}, "
                    f"bimi={dns_data.get('bimi', {}).get('has_bimi', False)}"
                )

            except Exception as e:
                logger.debug(f"DNS intel error for {domain}: {e}")

        # 2. Enhanced SSL Certificate Subdomains (mail-focused)
        if self.ssl_intel and self.config.use_ssl_subdomains:
            try:
                # Use new mail-specific subdomain filtering
                mail_subdomains = await asyncio.wait_for(
                    self.ssl_intel.get_mail_subdomains(domain), timeout=30
                )

                if mail_subdomains:
                    if "ssl_certs" not in contact.data_sources:
                        contact.data_sources.append("ssl_certs")
                    # Store mail subdomains for later use (pattern detection, verification)
                    contact.mail_subdomains = mail_subdomains[:10]
                    logger.debug(f"SSL mail subdomains for {domain}: {mail_subdomains[:5]}")

            except Exception as e:
                logger.debug(f"SSL intel error for {domain}: {e}")

        # 3. GitHub Organization Search (with cross-stage dedup)
        if self.github_scraper and self.config.use_github_extraction:
            org_name = company_name.lower().replace(" ", "").replace("-", "").replace(".", "")

            if org_name not in self._github_searched_orgs:
                self._github_searched_orgs.add(org_name)  # Mark as searched BEFORE call

                try:
                    org_details = await asyncio.wait_for(
                        self.github_scraper.get_org_details(org_name), timeout=30
                    )

                    if org_details and org_details.get("email"):
                        self._assign_email(contact, "contact_email", org_details["email"], "github_org")
                        contact.data_sources.append("github_org")

                    # Get emails from commits (high confidence)
                    if org_details:
                        member_emails = await asyncio.wait_for(
                            self.github_scraper.get_org_members_emails(org_name, max_members=5), timeout=30
                        )

                        for member in member_emails[:3]:
                            contact.people.append({
                                "name": member.get("username"),
                                "email": member.get("email"),
                                "source": "github_commits",
                                "confidence": 90
                            })

                        if member_emails:
                            contact.data_sources.append("github_commits")
                            self.stats["emails_found"] += len(member_emails)

                except Exception as e:
                    logger.debug(f"GitHub extraction error for {company_name}: {e}")
            else:
                logger.debug(f"[DEDUP] Skipping GitHub org search for '{org_name}' (already searched)")

        # 4. npm Registry Search
        if self.npm_scraper and self.config.use_npm_extraction:
            try:
                packages = await asyncio.wait_for(
                    self.npm_scraper.search_packages(company_name, max_results=3), timeout=30
                )

                for pkg in packages[:2]:
                    if pkg.get("publisher_email"):
                        contact.people.append({
                            "name": pkg.get("publisher"),
                            "email": pkg.get("publisher_email"),
                            "source": "npm_registry",
                            "context": f"npm package: {pkg.get('name')}",
                            "confidence": 95
                        })
                        contact.data_sources.append("npm")
                        break

            except Exception as e:
                logger.debug(f"npm extraction error: {e}")

        # 5. HackerNews Mentions
        if self.hackernews_scraper and self.config.use_hackernews_mentions:
            try:
                mentions = await self.hackernews_scraper.search(company_name, max_results=5)

                if mentions:
                    contact.data_sources.append("hackernews")

            except Exception as e:
                logger.debug(f"HackerNews error: {e}")

        # 6. Wayback Machine Historical Emails
        if self.wayback_intel and self.config.use_wayback_machine:
            try:
                historical_emails = await asyncio.wait_for(
                    self.wayback_intel.get_historical_emails(domain), timeout=30
                )

                for email in historical_emails[:5]:
                    email_lower = email.lower()

                    # Classify email
                    if any(x in email_lower for x in ["marketing", "ads", "pr"]) and not contact.marketing_email:
                        contact.marketing_email = email
                    elif any(x in email_lower for x in ["sales", "business"]) and not contact.sales_email:
                        contact.sales_email = email
                    elif not contact.contact_email:
                        contact.contact_email = email

                if historical_emails:
                    contact.data_sources.append("wayback")
                    self.stats["emails_found"] += len(historical_emails)

            except Exception as e:
                logger.debug(f"Wayback error for {domain}: {e}")

        # 7. Smart Email Pattern Detection & Generation
        if self.config.use_email_permutations and domain:

            # 7a. Check if domain is catch-all (makes SMTP verification useless)
            is_catchall = False
            if self.config.use_catchall_detection:
                try:
                    is_catchall = await self.email_verifier.check_catchall(domain)
                    if is_catchall:
                        contact.domain_is_catchall = True
                        logger.debug(f"Domain {domain} is catch-all - skipping SMTP for permutations")
                except Exception as e:
                    logger.debug(f"Non-critical error in catch-all detection: {e}")

            # 7b. Get domain warmup score for confidence adjustment
            warmup_score = 10  # Default moderate
            if self.config.use_warmup_scoring:
                try:
                    warmup_score = await self.email_verifier.get_domain_warmup_score(domain)
                except Exception as e:
                    logger.debug(f"Non-critical error in domain warmup score fetch: {e}")

            # 7b-2. Layer 15: Compute comprehensive email warmth score
            if self.config.use_warmup_scoring:
                contact.email_warmth_score = await self._compute_warmth_score(contact)

            # 7c. Detect email pattern from FOUND emails (emailhunter approach)
            detected_pattern = None
            if self.config.use_pattern_detection and contact.people:
                emails_with_names = []

                # Collect found email + name pairs from website/store data
                for person in contact.people:
                    person_email = person.get("email")
                    person_name = person.get("name", "")
                    if person_email and person_name and domain in person_email:
                        emails_with_names.append((person_email, person_name))

                # Also check contact's found emails against known people
                for found_email in [contact.contact_email, contact.marketing_email, contact.sales_email]:
                    if found_email and domain in found_email:
                        for person in contact.people:
                            if person.get("name"):
                                pattern = EmailPatternDetector.detect_pattern(
                                    found_email, person["name"], domain
                                )
                                if pattern:
                                    detected_pattern = pattern
                                    break
                        if detected_pattern:
                            break

                if not detected_pattern and emails_with_names:
                    detected_pattern = EmailPatternDetector.detect_from_multiple(
                        emails_with_names, domain
                    )

                if detected_pattern:
                    logger.debug(f"Detected email pattern for {domain}: {detected_pattern}")

            # 7d. Generate emails using detected pattern OR fallback to permutations
            use_smtp = (self.config.use_smtp_verification and not is_catchall)

            if contact.people:
                for person in contact.people[:3]:
                    person_name = person.get("name", "")
                    if not person_name:
                        continue

                    if detected_pattern:
                        # Use detected pattern — high confidence
                        email = EmailPatternDetector.apply_pattern(
                            detected_pattern, person_name, domain
                        )
                        if email:
                            person["possible_emails"] = [email]
                            self.stats["email_permutations_generated"] += 1

                            # Verify via SMTP if available
                            if use_smtp:
                                try:
                                    vresult = await self.email_verifier.verify(
                                        email, smtp_check=True
                                    )
                                    if vresult.get("smtp_exists") is True:
                                        if not contact.contact_email:
                                            contact.contact_email = email
                                        person["email_verified"] = True
                                        self.stats["emails_found"] += 1
                                except Exception as e:
                                    logger.debug(f"Non-critical error in SMTP email verification: {e}")
                    else:
                        # Fallback: generate permutations, optionally SMTP-verify top candidates
                        permutations = EmailPermutationGenerator.generate(person_name, domain)
                        self.stats["email_permutations_generated"] += len(permutations)

                        if use_smtp and permutations:
                            # SMTP-verify top 3 candidates to find real one
                            verified_any = False
                            for perm in permutations[:3]:
                                try:
                                    vresult = await self.email_verifier.verify(
                                        perm["email"], smtp_check=True
                                    )
                                    if vresult.get("smtp_exists") is True:
                                        person["possible_emails"] = [perm["email"]]
                                        person["email_verified"] = True
                                        detected_pattern = perm["pattern"]  # Learn for next person
                                        if not contact.contact_email:
                                            contact.contact_email = perm["email"]
                                        self.stats["emails_found"] += 1
                                        verified_any = True
                                        break
                                except Exception as e:
                                    logger.debug(f"Non-critical error in SMTP permutation verification: {e}")
                                    continue

                            if not verified_any:
                                # None verified — keep top 3 as guesses
                                person["possible_emails"] = [
                                    p["email"] for p in permutations[:3]
                                ]
                        else:
                            # No SMTP available — just use top 3 permutations
                            person["possible_emails"] = [
                                p["email"] for p in permutations[:3]
                            ]

            # 7e. Role-based email discovery via web search
            if self.config.use_role_email_discovery:
                try:
                    role_discovered = await self.email_verifier.discover_role_emails_via_search(
                        domain, self.website_scraper.client
                    )
                    for role_email_data in role_discovered:
                        email = role_email_data["email"]
                        if "marketing" in email and not contact.marketing_email:
                            contact.marketing_email = email
                        elif "sales" in email and not contact.sales_email:
                            contact.sales_email = email
                        elif ("info" in email or "contact" in email) and not contact.contact_email:
                            contact.contact_email = email
                        elif "press" in email or "pr" in email:
                            if not contact.marketing_email:
                                contact.marketing_email = email
                        elif "hr" in email or "careers" in email:
                            pass  # Skip HR-only emails for now
                except Exception as e:
                    logger.debug(f"Non-critical error in role-based email discovery: {e}")
                    role_discovered = []

                # Fallback to generated role emails if web search found nothing
                if not role_discovered:
                    role_emails = EmailPermutationGenerator.generate_role_emails(domain)
                    for role_email in role_emails[:5]:
                        email = role_email["email"]
                        if "marketing" in email and not contact.marketing_email:
                            contact.marketing_email = email
                        elif "sales" in email and not contact.sales_email:
                            contact.sales_email = email
                        elif ("info" in email or "contact" in email) and not contact.contact_email:
                            contact.contact_email = email
            else:
                # Role email discovery disabled — use generated role emails
                role_emails = EmailPermutationGenerator.generate_role_emails(domain)
                for role_email in role_emails[:5]:
                    email = role_email["email"]
                    if "marketing" in email and not contact.marketing_email:
                        contact.marketing_email = email
                    elif "sales" in email and not contact.sales_email:
                        contact.sales_email = email
                    elif ("info" in email or "contact" in email) and not contact.contact_email:
                        contact.contact_email = email

        # 8. NLP Entity Extraction (if text data available)
        if self.nlp_extractor and self.config.use_nlp_extraction and contact.company_description:
            try:
                entities = self.nlp_extractor.extract_entities(contact.company_description)

                # Extract persons
                for person_name in entities.get("persons", [])[:5]:
                    if not any(p.get("name") == person_name for p in contact.people):
                        contact.people.append({
                            "name": person_name,
                            "source": "nlp_extraction"
                        })

                # Extract emails
                for email in entities.get("emails", []):
                    if not contact.contact_email:
                        contact.contact_email = email

                self.stats["nlp_entities_extracted"] += len(entities.get("persons", []))

            except Exception as e:
                logger.debug(f"NLP extraction error: {e}")

        # Recalculate confidence
        contact.confidence_score = self._calculate_ultra_confidence(contact)

        await smart_delay(0.2)  # Rate limiting

    async def _verify_and_deduplicate(self, contacts: List[CompanyContact]):
        """
        Verify emails and deduplicate contacts using fuzzy matching.
        """
        self._update_progress("enrichment", 0, "Verifying emails and deduplicating...")

        # Collect all emails for verification
        emails_to_verify = []

        for contact in contacts:
            for email in [contact.contact_email, contact.marketing_email, contact.sales_email, contact.support_email, contact.press_email]:
                if email and email not in emails_to_verify:
                    emails_to_verify.append(email)

        # Verify emails (Layer 6 enhanced: layered verification pipeline)
        if self.email_verifier and self.config.use_email_verification:
            verified_count = 0
            smtp_verified_count = 0
            ms365_verified_count = 0
            bounce_scored_count = 0
            greylisting_retries = 0
            domain_ages_checked = 0
            disposable_caught = 0

            use_smtp = self.config.use_smtp_verification
            use_ms365 = self.config.use_ms365_verification
            use_domain_age = self.config.use_domain_age_check

            # Configure rate limiter from config
            if hasattr(self.email_verifier, 'rate_limiter'):
                self.email_verifier.rate_limiter.min_delay = self.config.smtp_rate_limit_delay
                self.email_verifier.rate_limiter.max_per_domain = self.config.smtp_max_per_domain

            # Layer 14: Parallel email verification with semaphore(20), cap raised to 500
            verify_sem = asyncio.Semaphore(20)
            emails_capped = emails_to_verify[:500]
            total_to_verify = len(emails_capped)
            verified_idx = [0]  # mutable progress counter

            # Group emails by domain for efficient MX reuse (verification handles caching internally)

            async def verify_one(email):
                async with verify_sem:
                    try:
                        result = await self.email_verifier.verify(
                            email,
                            smtp_check=use_smtp,
                            ms365_check=use_ms365,
                            domain_age_check=use_domain_age
                        )
                        return (email, result)
                    except Exception as e:
                        logger.debug(f"Email verification error for {email}: {e}")
                        return (email, None)
                    finally:
                        verified_idx[0] += 1
                        progress = int(verified_idx[0] / total_to_verify * 50)
                        self._update_progress("enrichment", progress, f"Verified {verified_idx[0]}/{total_to_verify} emails...")

            verify_tasks = [verify_one(email) for email in emails_capped]
            verify_results = await asyncio.gather(*verify_tasks, return_exceptions=True)

            for item in verify_results:
                if isinstance(item, Exception) or item is None:
                    continue
                email, result = item
                if result is None:
                    continue

                if result.get("deliverable"):
                    verified_count += 1

                # Extract all verification fields
                confidence = result.get("confidence", 0)
                mx_valid = result.get("mx_valid", False)
                is_disposable = result.get("is_disposable", False)
                is_role_based = result.get("is_role_based", False)
                smtp_exists = result.get("smtp_exists")
                is_catchall = result.get("is_catchall", False)
                ms365_exists = result.get("ms365_exists")
                domain_age_days = result.get("domain_age_days")
                domain_age_category = result.get("domain_age_category")
                bounce_score = result.get("bounce_score")
                bounce_category = result.get("bounce_category")
                mx_provider = result.get("mx_provider")

                # Track Layer 6 stats
                if is_disposable:
                    disposable_caught += 1
                if ms365_exists is True:
                    ms365_verified_count += 1
                if bounce_score is not None:
                    bounce_scored_count += 1
                if domain_age_days is not None:
                    domain_ages_checked += 1
                if "greylisting_retried" in result.get("verification_layers_passed", []):
                    greylisting_retries += 1

                # Determine verification status with enhanced tiers
                if (smtp_exists is True or ms365_exists is True) and confidence >= 85:
                    verification_status = "smtp_verified"
                    smtp_verified_count += 1
                elif ms365_exists is True and confidence >= 70:
                    verification_status = "ms365_verified"
                elif confidence >= 75:
                    verification_status = "verified"
                elif confidence >= 50:
                    verification_status = "maybe"
                else:
                    verification_status = "not_verified"

                for contact in contacts:
                    if email in (contact.contact_email, contact.marketing_email, contact.sales_email, contact.support_email, contact.press_email):
                        # Store base verification info
                        contact.email_verification_status = verification_status
                        contact.email_verification_confidence = confidence
                        contact.email_mx_valid = mx_valid
                        contact.email_is_disposable = is_disposable
                        contact.email_is_role_based = is_role_based
                        contact.email_smtp_exists = smtp_exists
                        contact.email_is_catchall = is_catchall

                        # Store Layer 6 enhanced fields
                        contact.email_ms365_verified = ms365_exists is True
                        contact.email_bounce_score = bounce_score
                        contact.email_bounce_category = bounce_category
                        contact.email_domain_age_days = domain_age_days
                        contact.email_domain_age_category = domain_age_category
                        contact.email_mx_provider = mx_provider

                        if "verified_emails" not in contact.data_sources:
                            contact.data_sources.append("verified_emails")

                        # Add Layer 6 data source tags
                        if ms365_exists is True and "ms365_verified" not in contact.data_sources:
                            contact.data_sources.append("ms365_verified")
                        if bounce_score is not None and "bounce_scored" not in contact.data_sources:
                            contact.data_sources.append("bounce_scored")

                        # Remove invalid emails (SMTP or MS365 rejected)
                        email_rejected = (
                            smtp_exists is False or
                            ms365_exists is False or
                            is_disposable
                        )
                        if email_rejected:
                            if contact.contact_email == email:
                                contact.contact_email = None
                            if contact.marketing_email == email:
                                contact.marketing_email = None
                            if contact.sales_email == email:
                                contact.sales_email = None
                            if contact.support_email == email:
                                contact.support_email = None
                            if contact.press_email == email:
                                contact.press_email = None

            self.stats["emails_verified"] = verified_count
            self.stats["emails_smtp_verified"] = smtp_verified_count
            self.stats["emails_ms365_verified"] = ms365_verified_count
            self.stats["emails_bounce_scored"] = bounce_scored_count
            self.stats["smtp_greylisting_retries"] = greylisting_retries
            self.stats["domain_ages_checked"] = domain_ages_checked
            self.stats["disposable_emails_caught"] = disposable_caught

        # Fuzzy deduplication — Layer 14: O(n log n) sorted-key approach replaces O(n^2)
        if self.config.use_fuzzy_matching:
            self._update_progress("enrichment", 60, "Removing duplicates...")

            # Pre-compute normalized keys and sort alphabetically
            keyed_contacts = []
            for contact in contacts:
                company_key = contact.company_name.lower() if contact.company_name else ""
                keyed_contacts.append((company_key, contact))
            keyed_contacts.sort(key=lambda x: x[0])

            unique_contacts = []
            seen_companies = {}  # key -> contact (for merging)

            # Sliding window: only compare each key against nearby sorted neighbors
            WINDOW = 10  # nearby keys to compare (covers typos, abbreviations)

            for idx, (company_key, contact) in enumerate(keyed_contacts):
                is_duplicate = False

                # Compare against recent unique entries within window
                check_start = max(0, len(unique_contacts) - WINDOW)
                for j in range(check_start, len(unique_contacts)):
                    seen_key = unique_contacts[j].company_name.lower() if unique_contacts[j].company_name else ""
                    similarity = FuzzyMatcher.similarity(company_key, seen_key)

                    if similarity > 0.85:  # 85% similarity threshold
                        is_duplicate = True
                        self.stats["bloom_filter_hits"] += 1

                        # Merge data into existing contact
                        existing_contact = unique_contacts[j]

                        # Merge emails (all 5 fields)
                        if contact.contact_email and not existing_contact.contact_email:
                            existing_contact.contact_email = contact.contact_email
                        if contact.marketing_email and not existing_contact.marketing_email:
                            existing_contact.marketing_email = contact.marketing_email
                        if contact.sales_email and not existing_contact.sales_email:
                            existing_contact.sales_email = contact.sales_email
                        if contact.support_email and not existing_contact.support_email:
                            existing_contact.support_email = contact.support_email
                        if contact.press_email and not existing_contact.press_email:
                            existing_contact.press_email = contact.press_email

                        # Merge people
                        existing_contact.people.extend(contact.people)

                        # Merge data sources
                        existing_contact.data_sources = list(set(
                            existing_contact.data_sources + contact.data_sources
                        ))

                        break

                if not is_duplicate:
                    unique_contacts.append(contact)

            # Replace contacts list (in-place modification)
            contacts.clear()
            contacts.extend(unique_contacts)

        self._update_progress("enrichment", 100, "Verification and deduplication complete")

    async def _run_osint_extraction(self, contacts: List[CompanyContact]):
        """
        Run Deep OSINT extraction for companies and people.

        This method performs comprehensive Open Source Intelligence gathering:
        - Google dorking for emails and phone numbers
        - LinkedIn public profile scraping for leadership/employees
        - GitHub organization and user email extraction
        - Social media profile discovery
        - Company registry searches (OpenCorporates, SEC EDGAR)
        - Domain intelligence (WHOIS, DNS, subdomains)
        - Email permutation generation for discovered people
        """
        if not self.osint_engine:
            return

        self._update_progress("contact_finding", 50, "Starting Deep OSINT extraction...")

        total = len(contacts)
        completed = [0]  # mutable counter for progress tracking

        sem = asyncio.Semaphore(5)  # Lower concurrency — OSINT is heavier per-contact

        async def process_one_osint(contact):
            async with sem:
                self._check_cancelled()
                try:
                    await asyncio.wait_for(
                        self._osint_extract_single(contact, total, completed),
                        timeout=180
                    )
                except ExtractionCancelled:
                    raise
                except asyncio.TimeoutError:
                    logger.warning(f"OSINT extraction timeout (180s) for {contact.company_name}, skipping")
                except Exception as e:
                    logger.warning(f"OSINT extraction error for {contact.company_name}: {e}")
                finally:
                    completed[0] += 1
                    progress = 50 + int((completed[0] / total) * 50)
                    self._update_progress("contact_finding", progress, f"OSINT: {completed[0]}/{total} complete")

        tasks = [process_one_osint(c) for c in contacts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Re-raise cancellation if any task was cancelled
        for r in results:
            if isinstance(r, ExtractionCancelled):
                raise r

        # Propagate Layer 11 stats from OSINT engine to extraction engine stats
        if self.osint_engine:
            osint_stats = self.osint_engine.get_stats()
            layer11_stat_keys = [
                "dept_contacts_found", "dept_emails_found", "linkedin_dept_contacts",
                "media_contacts_found", "crunchbase_team_found", "job_board_contacts_found",
                "podcast_guests_found", "wellfound_team_found", "email_patterns_detected",
            ]
            for key in layer11_stat_keys:
                self.stats[f"osint_{key}"] = osint_stats.get(key, 0)

        sec_count = self.stats.get('osint_sec_officers_found', 0)
        pr_count = self.stats.get('osint_press_execs_found', 0)
        blog_count = self.stats.get('osint_blog_authors_found', 0)
        patent_count = self.stats.get('osint_patent_inventors_found', 0)
        speaker_count = self.stats.get('osint_conference_speakers_found', 0)
        layer5_total = sec_count + pr_count + blog_count + patent_count + speaker_count

        # Layer 11 totals
        layer11_total = sum(
            self.stats.get(f"osint_{k}", 0) for k in [
                "dept_contacts_found", "linkedin_dept_contacts", "media_contacts_found",
                "crunchbase_team_found", "job_board_contacts_found",
                "podcast_guests_found", "wellfound_team_found",
            ]
        )

        self._update_progress(
            "contact_finding", 100,
            f"OSINT complete: {self.stats['osint_leadership_found']} leaders, "
            f"{self.stats['osint_employees_found']} employees"
            f"{f', {layer5_total} from Layer 5 sources' if layer5_total > 0 else ''}"
            f"{f', {layer11_total} from Layer 11 Full-Spectrum' if layer11_total > 0 else ''}"
        )

    async def _osint_extract_single(self, contact: CompanyContact, total: int, completed: list):
        """Process a single contact through OSINT extraction pipeline."""
        company_name = contact.company_name
        domain = contact.company_domain

        if not company_name:
            return

        # Perform deep company OSINT (pass dedup sets to avoid re-searching GitHub)
        try:
            company_intel = await asyncio.wait_for(
                self.osint_engine.deep_company_osint(
                    company_name=company_name,
                    domain=domain,
                    find_leadership=self.config.osint_find_leadership,
                    find_employees=self.config.osint_find_employees,
                    skip_github_orgs=self._github_searched_orgs,
                    skip_github_users=self._github_searched_users,
                    skip_dns_domains=self._dns_searched_domains
                ),
                timeout=300
            )
        except asyncio.TimeoutError:
            logger.warning(f"OSINT pipeline timed out after 300s for {company_name}")
            company_intel = None

        # Update contact with OSINT data
        if company_intel:
            # Update company info
            if company_intel.description and not contact.company_description:
                contact.company_description = company_intel.description

            if company_intel.headquarters and not contact.company_location:
                contact.company_location = company_intel.headquarters

            if company_intel.founded:
                contact.company_founded = company_intel.founded

            if company_intel.size:
                contact.company_size = company_intel.size

            if company_intel.industry:
                contact.company_industry = company_intel.industry

            # Update social profiles
            if company_intel.linkedin_url and not contact.company_linkedin:
                contact.company_linkedin = company_intel.linkedin_url
                self.stats["osint_social_profiles_found"] += 1

            # Update emails from OSINT - comprehensive mapping
            OSINT_EMAIL_MAP = {
                # Primary field mapping
                "contact": "contact_email", "info": "contact_email",
                "dns": "contact_email", "whois": "contact_email",
                "general": "contact_email", "team": "contact_email",
                "dev": "contact_email", "developer": "contact_email",
                "engineering": "contact_email", "tech": "contact_email",
                "office": "contact_email", "hello": "contact_email",
                "hr": "contact_email", "legal": "contact_email",
                "finance": "contact_email",
                "marketing": "marketing_email", "advertising": "marketing_email",
                "media": "marketing_email", "pr": "marketing_email",
                "affiliate": "marketing_email", "growth": "marketing_email",
                "sales": "sales_email", "business": "sales_email",
                "enterprise": "sales_email", "partnerships": "sales_email",
                "support": "support_email", "help": "support_email",
                "customer": "support_email", "service": "support_email",
                "press": "press_email", "newsroom": "press_email",
            }
            FALLBACK_FIELDS = ["contact_email", "marketing_email", "sales_email", "support_email", "press_email"]

            for email_type, email in company_intel.emails.items():
                if isinstance(email, str):
                    target_field = OSINT_EMAIL_MAP.get(email_type, "contact_email")
                    if not getattr(contact, target_field, None):
                        setattr(contact, target_field, email)
                    else:
                        # Primary slot full, try fallbacks
                        for fb_field in FALLBACK_FIELDS:
                            if fb_field != target_field and not getattr(contact, fb_field, None):
                                setattr(contact, fb_field, email)
                                break
                elif isinstance(email, list):
                    for e in email[:3]:
                        placed = False
                        for fb_field in FALLBACK_FIELDS:
                            if not getattr(contact, fb_field, None):
                                setattr(contact, fb_field, e)
                                placed = True
                                break
                        if not placed:
                            break

            # Update phone numbers
            if company_intel.phones:
                self.stats["osint_phones_found"] += len(company_intel.phones)
                # Store phones in people data or as additional contact info
                for person in contact.people:
                    if "phones" not in person:
                        person["phones"] = company_intel.phones[:2]
                        break

            # Add leadership to people
            for leader in company_intel.leadership[:self.config.osint_max_leadership]:
                # Check if already exists
                existing = next(
                    (p for p in contact.people if p.get("name", "").lower() == leader.name.lower()),
                    None
                )

                if existing:
                    # Update existing person
                    if leader.title and not existing.get("title"):
                        existing["title"] = leader.title
                    if leader.linkedin_url and not existing.get("linkedin"):
                        existing["linkedin"] = leader.linkedin_url
                    if leader.emails:
                        existing["emails"] = list(set(existing.get("emails", []) + leader.emails))
                    existing["sources"] = list(set(existing.get("sources", []) + leader.sources))
                else:
                    # Add new person
                    contact.people.append({
                        "name": leader.name,
                        "title": leader.title,
                        "role": "leadership",
                        "emails": leader.emails[:self.config.osint_max_email_permutations],
                        "linkedin": leader.linkedin_url,
                        "github": leader.github_url,
                        "twitter": leader.twitter_url,
                        "location": leader.location,
                        "sources": leader.sources,
                        "confidence": leader.confidence_score
                    })
                    self.stats["osint_leadership_found"] += 1

            # Add employees to people
            for employee in company_intel.employees[:self.config.osint_max_employees]:
                # Check if already exists
                existing = next(
                    (p for p in contact.people if p.get("name", "").lower() == employee.name.lower()),
                    None
                )

                if existing:
                    # Update existing person
                    if employee.title and not existing.get("title"):
                        existing["title"] = employee.title
                    if employee.linkedin_url and not existing.get("linkedin"):
                        existing["linkedin"] = employee.linkedin_url
                    if employee.github_url and not existing.get("github"):
                        existing["github"] = employee.github_url
                    if employee.emails:
                        existing["emails"] = list(set(existing.get("emails", []) + employee.emails))
                    existing["sources"] = list(set(existing.get("sources", []) + employee.sources))
                else:
                    # Add new person
                    contact.people.append({
                        "name": employee.name,
                        "title": employee.title,
                        "role": "employee",
                        "emails": employee.emails[:self.config.osint_max_email_permutations],
                        "linkedin": employee.linkedin_url,
                        "github": employee.github_url,
                        "twitter": employee.twitter_url,
                        "location": employee.location,
                        "bio": employee.bio,
                        "sources": employee.sources,
                        "confidence": employee.confidence_score
                    })
                    self.stats["osint_employees_found"] += 1

            # Add technologies discovered
            if company_intel.technologies:
                if "technologies" not in contact.data_sources:
                    contact.data_sources.append("technologies")

            # Add subdomains for reference
            if company_intel.subdomains:
                if "subdomains" not in contact.data_sources:
                    contact.data_sources.append("subdomains")

            # Update data sources
            for source in company_intel.sources:
                if source not in contact.data_sources:
                    contact.data_sources.append(f"osint_{source}")

            # Recalculate confidence with OSINT data
            contact.confidence_score = self._calculate_osint_confidence(contact)

        # Rate limiting
        await smart_delay(0.3)

    async def _web_search_enhancement(self, contacts: List[CompanyContact]):
        """
        ULTRA DEEP FREE Web Search Enhancement - NO API Keys Required!
        Uses DuckDuckGo, Bing, SearX with MULTI-PAGE scraping
        and VARIED search prompts for maximum contact discovery
        """
        if not self.web_search:
            return

        # Focus on contacts without emails OR with incomplete data
        contacts_needing_emails = [
            c for c in contacts
            if not c.contact_email and not c.marketing_email and not c.sales_email
        ][:40]  # Increased limit for deeper search

        # Also get contacts with partial data for enrichment
        contacts_needing_enrichment = [
            c for c in contacts
            if c not in contacts_needing_emails and (
                not c.company_linkedin or
                len(c.people) < 2
            )
        ][:20]

        total_to_search = contacts_needing_emails + contacts_needing_enrichment

        if not total_to_search:
            logger.info("All companies have emails, skipping web search enhancement")
            return

        self._update_progress("web_search", 0, f"ULTRA Deep web search for {len(total_to_search)} companies...")

        completed = [0]
        sem = asyncio.Semaphore(8)

        async def process_one_websearch(contact, needs_email):
            async with sem:
                self._check_cancelled()
                try:
                    await asyncio.wait_for(
                        self._web_search_single(contact, needs_email, contacts, len(total_to_search), completed),
                        timeout=90
                    )
                except ExtractionCancelled:
                    raise
                except asyncio.TimeoutError:
                    logger.warning(f"Web search timeout (90s) for {contact.company_name}, skipping")
                except Exception as e:
                    logger.warning(f"Web search error for {contact.company_name}: {e}")
                finally:
                    completed[0] += 1
                    progress = int((completed[0] / len(total_to_search)) * 90)
                    self._update_progress("web_search", progress, f"Web search: {completed[0]}/{len(total_to_search)} complete")

        tasks = [
            process_one_websearch(c, c in contacts_needing_emails)
            for c in total_to_search
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, ExtractionCancelled):
                raise r

        self._update_progress("web_search", 95, f"ULTRA Deep search complete: Found {self.stats['web_search_emails_found']} emails")

    async def _web_search_single(self, contact: CompanyContact, needs_email: bool, all_contacts: List[CompanyContact], total: int, completed: list):
        """Process a single contact through web search enhancement. Short-circuits on email found."""

        def _has_email():
            return contact.contact_email or contact.marketing_email or contact.sales_email

        # ========== PHASE 1: Standard Email Search (Multi-page) ==========
        search_results = await self.web_search.search_company_emails(
            company_name=contact.company_name,
            domain=contact.company_domain,
            max_results=self.config.web_search_max_results,
            max_pages=3  # Scrape 3 pages per search
        )

        self.stats["web_search_queries"] += search_results.get("stats", {}).get("queries_used", 1)
        self.stats["web_search_results"] += search_results.get("stats", {}).get("total_results", 0)

        # Process found emails
        emails_found = search_results.get("emails_found", [])
        emails_categorized = search_results.get("emails_categorized", {})

        if emails_found:
            self._process_found_emails(contact, emails_categorized, "Web Search (Multi-Page)")

        # Task 10: Short-circuit — skip remaining phases if email already found
        if _has_email() and needs_email:
            self._finalize_web_search_contact(contact, all_contacts)
            return

        # ========== PHASE 2: ULTRA Deep Company Contact Search ==========
        if needs_email:
            deep_results = await self.web_search.deep_company_contact_search(
                company_name=contact.company_name,
                domain=contact.company_domain,
                website_url=contact.company_website,
                max_pages=5  # Go deeper - 5 pages per query
            )

            self.stats["web_search_queries"] += deep_results.get("queries_executed", 0)
            self.stats["web_search_results"] += deep_results.get("search_results_count", 0)

            # Process deep search emails
            deep_emails = deep_results.get("emails_categorized", {})
            if deep_emails:
                self._process_found_emails(contact, deep_emails, "ULTRA Deep Web Search")

            # Add social profiles
            social_profiles = deep_results.get("social_profiles", {})
            if social_profiles:
                if social_profiles.get("linkedin_company") and not contact.company_linkedin:
                    contact.company_linkedin = social_profiles["linkedin_company"]
                    contact.data_sources.append("web_search_linkedin")

                # Add LinkedIn people as contacts
                linkedin_people = social_profiles.get("linkedin_people", [])
                for profile_url in linkedin_people[:5]:
                    if not any(p.get("linkedin") == profile_url for p in contact.people):
                        contact.people.append({
                            "linkedin": profile_url,
                            "role": "leadership",
                            "sources": ["deep_web_search"]
                        })
                        self.stats["osint_leadership_found"] += 1

            # Emit discovery for social profiles
            if social_profiles:
                self._emit_live_contact(
                    company_name=contact.company_name,
                    contact_type="social",
                    source="Deep Web Search",
                    confidence=65,
                    website=contact.company_website
                )

            # Short-circuit after Phase 2
            if _has_email():
                self._finalize_web_search_contact(contact, all_contacts)
                return

        # ========== PHASE 3: Leadership Search ==========
        if self.config.osint_find_leadership and len(contact.people) < 3:
            leadership_results = await self.web_search.search_leadership(
                company_name=contact.company_name,
                max_results=30  # More results
            )

            linkedin_profiles = leadership_results.get("linkedin_profiles", [])
            for profile in linkedin_profiles[:5]:
                if isinstance(profile, SearchResult):
                    name = profile.title.split(" - ")[0] if profile.title else ""
                    if name and not any(p.get("name") == name for p in contact.people):
                        contact.people.append({
                            "name": name,
                            "role": "leadership",
                            "linkedin": profile.url,
                            "sources": ["web_search_linkedin"]
                        })
                        self.stats["osint_leadership_found"] += 1
                        self._emit_live_contact(
                            company_name=contact.company_name,
                            contact_type="leadership",
                            source="Web Search LinkedIn",
                            confidence=60,
                            person_name=name
                        )

        # ========== PHASE 4: ULTRA PARALLEL MULTI-ATTEMPT SEARCH ==========
        # For contacts STILL without emails after Phase 1-3, run aggressive parallel search
        still_needs_email = not _has_email()

        if still_needs_email and needs_email:
            try:
                # Run ULTRA parallel search - 8 methods simultaneously
                parallel_results = await self.web_search.parallel_multi_attempt_search(
                    company_name=contact.company_name,
                    domain=contact.company_domain,
                    website_url=contact.company_website,
                    product_name=contact.product_name if hasattr(contact, 'product_name') else None,
                    max_attempts=4
                )

                self.stats["web_search_queries"] += parallel_results.get("total_queries", 8)

                # Process parallel search emails
                parallel_emails = parallel_results.get("emails_categorized", {})
                if parallel_emails:
                    self._process_found_emails(contact, parallel_emails, "ULTRA Parallel Search (8 Methods)")
                    logger.info(f"[HIT] Parallel search found {len(parallel_emails)} emails for {contact.company_name}")

                # Add people found from parallel search
                parallel_people = parallel_results.get("people_found", [])
                for person in parallel_people[:5]:
                    if isinstance(person, dict):
                        name = person.get("name", "")
                        if name and not any(p.get("name") == name for p in contact.people):
                            contact.people.append({
                                "name": name,
                                "role": person.get("role", "contact"),
                                "linkedin": person.get("linkedin"),
                                "github": person.get("github"),
                                "sources": person.get("sources", ["parallel_search"])
                            })
                            self.stats["osint_employees_found"] += 1

                # Add social profiles from parallel search
                parallel_social = parallel_results.get("social_profiles", {})
                if parallel_social:
                    if parallel_social.get("github") and not hasattr(contact, 'company_github'):
                        contact.company_github = parallel_social["github"]
                    if parallel_social.get("producthunt"):
                        if "producthunt" not in contact.data_sources:
                            contact.data_sources.append("producthunt")

                # Add discovered sources to global cache
                discovered = parallel_results.get("discovered_sources", [])
                for source in discovered:
                    self.web_search.discovered_sources.add(source)

            except Exception as e:
                logger.warning(f"Parallel search error for {contact.company_name}: {e}")

            # Short-circuit after Phase 4
            if _has_email():
                self._finalize_web_search_contact(contact, all_contacts)
                return

            # ========== PHASE 4.5: AGGRESSIVE LAST RESORT ==========
            # If STILL no email after parallel search, use aggressive hunt with 4 retries
            try:
                aggressive_results = await self.web_search.aggressive_contact_hunt(
                    company_name=contact.company_name,
                    domain=contact.company_domain,
                    website_url=contact.company_website,
                    retry_count=4
                )

                if aggressive_results.get("success"):
                    aggressive_emails = aggressive_results.get("emails_categorized", {})
                    if aggressive_emails:
                        self._process_found_emails(contact, aggressive_emails, "AGGRESSIVE Hunt (4 Retries)")
                        logger.info(f"[OK] Aggressive hunt found {len(aggressive_emails)} emails for {contact.company_name}")

            except Exception as e:
                logger.warning(f"Aggressive hunt error for {contact.company_name}: {e}")

        self._finalize_web_search_contact(contact, all_contacts)

        # Rate limiting between companies
        await smart_delay(self.config.web_search_delay)

    def _finalize_web_search_contact(self, contact: CompanyContact, all_contacts: List[CompanyContact]):
        """Update data sources and stats after web search for a single contact."""
        if "web_search_deep" not in contact.data_sources:
            contact.data_sources.append("web_search_deep")

        # Update total email count
        self.stats["emails_found"] = sum(1 for c in all_contacts if c.contact_email or c.marketing_email or c.sales_email or c.support_email or c.press_email)

    # ========== ULTRA DEEP SEARCH V2.0 ==========
    async def _run_ultra_deep_extraction(self, contacts: List[CompanyContact]):
        """
        ULTRA DEEP SEARCH V2.0 - Maximum extraction power!

        15+ FREE LAYERS:
        1. Multi-Engine Search (6 engines parallel)
        2. Archive Mining (Wayback, Archive.today, CommonCrawl, Google Cache)
        3. DNS Intelligence (MX, TXT, SPF, DMARC)
        4. WHOIS Intelligence (domain contacts)
        5. Certificate Transparency (subdomain discovery)
        6. Sitemap Mining (hidden pages)
        7. Social Media Discovery (LinkedIn, Twitter, Facebook)
        8. Developer Platforms (GitHub, GitLab, npm, PyPI)
        9. Job Postings (Indeed, Glassdoor, LinkedIn Jobs)
        10. Press Releases (PRNewswire, BusinessWire)
        11. Startup Databases (Crunchbase, AngelList, ProductHunt)
        12. Email Permutation (50+ patterns)
        13. SMTP Verification (free email verification)
        14. Google Cache Mining
        15. Academic/Research Paper Mining

        10+ PAID API INTEGRATIONS:
        1. Hunter.io - Email discovery
        2. Clearbit - Company enrichment
        3. Apollo.io - Contact/leads
        4. RocketReach - Verified emails
        5. Snov.io - Email finder
        6. BuiltWith - Tech stack
        7. Lusha - Contact data
        8. ZoomInfo - B2B intelligence
        9. LeadIQ - Sales intelligence
        10. Cognism - B2B data
        """
        if not ULTRA_DEEP_AVAILABLE:
            logger.warning("ULTRA DEEP Search Engine not available")
            return

        logger.info("[START] ULTRA DEEP V2.0: Starting 15+ layer extraction...")
        self._update_progress("ultra_deep", 5, "Initializing ULTRA DEEP Search Engine V2.0...")

        # Filter contacts that need emails
        contacts_needing_emails = [
            c for c in contacts
            if not c.contact_email and not c.marketing_email and not c.sales_email
        ]

        if not contacts_needing_emails:
            logger.info("All contacts already have emails, skipping ULTRA DEEP")
            return

        logger.info(f"ULTRA DEEP: Processing {len(contacts_needing_emails)} contacts without emails")

        # Build configuration for ULTRA DEEP engine
        ultra_config = {
            'max_concurrent': self.config.ultra_deep_max_concurrent,
            'timeout': self.config.ultra_deep_timeout,
            'retry_count': self.config.ultra_deep_retry_count,
        }

        # Add PAID API keys if available
        if self.config.hunter_io_api_key:
            ultra_config['hunter_api_key'] = self.config.hunter_io_api_key
        if self.config.clearbit_api_key_v2:
            ultra_config['clearbit_api_key'] = self.config.clearbit_api_key_v2
        if self.config.apollo_io_api_key:
            ultra_config['apollo_api_key'] = self.config.apollo_io_api_key
        if self.config.rocketreach_api_key:
            ultra_config['rocketreach_api_key'] = self.config.rocketreach_api_key
        if self.config.snov_io_api_key:
            ultra_config['snov_client_id'] = self.config.snov_io_api_key
            # Use separate secret if provided, otherwise leave blank (will fail auth properly)
            ultra_config['snov_client_secret'] = self.config.snov_io_api_secret or ""
        if self.config.builtwith_api_key:
            ultra_config['builtwith_api_key'] = self.config.builtwith_api_key

        # Initialize ULTRA DEEP engine
        try:
            ultra_deep_engine = UltraDeepSearchEngine(config=ultra_config)
        except Exception as e:
            logger.error(f"Failed to initialize ULTRA DEEP engine: {e}")
            return

        total = len(contacts_needing_emails)

        # Process contacts in batches for efficiency (Layer 14: increased from 5)
        batch_size = 20
        for batch_idx in range(0, total, batch_size):
            batch = contacts_needing_emails[batch_idx:batch_idx + batch_size]
            batch_num = batch_idx // batch_size + 1
            total_batches = (total + batch_size - 1) // batch_size

            self._update_progress(
                "ultra_deep",
                10 + int(80 * batch_idx / total),
                f"[SEARCH] ULTRA DEEP V2.0: Batch {batch_num}/{total_batches} - Processing {len(batch)} contacts..."
            )

            # Process batch concurrently
            tasks = []
            for contact in batch:
                domain = contact.company_domain or self._extract_domain(contact.company_website)
                if domain:
                    tasks.append(self._ultra_deep_process_contact(
                        ultra_deep_engine, contact, domain
                    ))

            if tasks:
                try:
                    await asyncio.gather(*tasks, return_exceptions=True)
                except Exception as e:
                    logger.warning(f"ULTRA DEEP batch error: {e}")

            # Layer 14: removed inter-batch delay (no external rate-limiting concern)

        # Update final stats
        self._update_progress(
            "ultra_deep", 95,
            f"[OK] ULTRA DEEP V2.0 complete: {self.stats['ultra_deep_emails_found']} emails found"
        )

        logger.info(f"ULTRA DEEP V2.0 completed: {self.stats['ultra_deep_emails_found']} emails, "
                   f"{self.stats['ultra_deep_layers_completed']} layers completed")

    async def _ultra_deep_process_contact(
        self,
        engine: 'UltraDeepSearchEngine',
        contact: CompanyContact,
        domain: str
    ):
        """Process a single contact through ULTRA DEEP layers"""
        try:
            # Run ULTRA DEEP extraction
            result = await engine.deep_search_company(
                company_name=contact.company_name,
                domain=domain,
                use_paid=self.config.ultra_deep_use_paid_apis
            )

            if not result:
                return

            # Process all emails found (DeepSearchResult has .emails list)
            for email in result.emails:
                if email and self._is_valid_email(email):
                    self._assign_ultra_deep_email(contact, email, "ultra_deep_search")

            # Process verified emails (higher confidence)
            for email in result.verified_emails:
                if email and self._is_valid_email(email):
                    self._assign_ultra_deep_email(contact, email, "ultra_deep_verified")
                    self.stats['ultra_deep_smtp_verified'] += 1

            # Track people found
            for person in result.people:
                if person.get('email') and person.get('name'):
                    contact.people.append({
                        'name': person['name'],
                        'email': person['email'],
                        'title': person.get('title', ''),
                        'source': 'ultra_deep_v2'
                    })
                    # Also assign person email to contact if available
                    if self._is_valid_email(person['email']):
                        self._assign_ultra_deep_email(contact, person['email'], "ultra_deep_person")

            # Add data source markers
            if result.sources:
                for source in result.sources:
                    if source not in contact.data_sources:
                        contact.data_sources.append(f"ultra_deep_{source}")
                self.stats['ultra_deep_layers_completed'] += len(result.sources)
                # Update layer-specific stats
                for source in result.sources:
                    self._update_layer_stats(source)

            # Update confidence
            if result.confidence_score:
                contact.email_verification_confidence = max(
                    contact.email_verification_confidence,
                    result.confidence_score
                )

            # Store raw data for debugging
            if result.raw_data:
                # Check for paid API data to track separately
                for api_name in ['hunter', 'clearbit', 'apollo', 'rocketreach', 'snov', 'builtwith']:
                    if api_name in result.raw_data:
                        self.stats['ultra_deep_paid_emails_found'] += 1

        except Exception as e:
            logger.warning(f"ULTRA DEEP error for {contact.company_name}: {e}")

    def _assign_ultra_deep_email(self, contact: CompanyContact, email: str, source: str):
        """Assign email from ULTRA DEEP to appropriate contact field"""
        email_lower = email.lower()
        assigned = False

        # Categorize and assign
        if any(p in email_lower for p in ['marketing', 'promo', 'ads', 'growth', 'media']):
            if not contact.marketing_email:
                contact.marketing_email = email
                assigned = True
        elif any(p in email_lower for p in ['sales', 'business', 'partner', 'deals']):
            if not contact.sales_email:
                contact.sales_email = email
                assigned = True
        elif any(p in email_lower for p in ['press', 'pr@', 'media@', 'news']):
            if not contact.press_email:
                contact.press_email = email
                assigned = True
        elif any(p in email_lower for p in ['support', 'help', 'service', 'care']):
            if not contact.support_email:
                contact.support_email = email
                assigned = True
        elif not contact.contact_email:
            contact.contact_email = email
            assigned = True

        if assigned:
            self.stats['ultra_deep_emails_found'] += 1
            if source not in contact.data_sources:
                contact.data_sources.append(source)

            # Emit live update
            self._emit_live_contact(
                company_name=contact.company_name,
                contact_type="email",
                source=source,
                confidence=75,  # ULTRA DEEP emails are high confidence
                email=email
            )

    def _update_layer_stats(self, layer_name: str):
        """Update stats for specific ULTRA DEEP layer"""
        layer_stat_map = {
            'multi_engine': 'ultra_deep_multi_engine_hits',
            'archive': 'ultra_deep_archive_hits',
            'dns': 'ultra_deep_dns_hits',
            'whois': 'ultra_deep_whois_hits',
            'ct': 'ultra_deep_ct_hits',
            'certificate': 'ultra_deep_ct_hits',
            'sitemap': 'ultra_deep_sitemap_hits',
            'social': 'ultra_deep_social_hits',
            'developer': 'ultra_deep_dev_platform_hits',
            'github': 'ultra_deep_dev_platform_hits',
            'job': 'ultra_deep_job_posting_hits',
            'press': 'ultra_deep_press_hits',
            'startup': 'ultra_deep_startup_db_hits',
        }

        for key, stat_name in layer_stat_map.items():
            if key in layer_name.lower():
                self.stats[stat_name] = self.stats.get(stat_name, 0) + 1
                break

    def _extract_domain(self, url: Optional[str]) -> Optional[str]:
        """Extract registered domain from URL using tldextract (handles subdomains and ccTLDs)."""
        if not url:
            return None
        try:
            ext = _tld_extractor(url if url.startswith('http') else f'https://{url}')
            if ext.domain and ext.suffix:
                return f"{ext.domain}.{ext.suffix}".lower()
            # Fallback for bare domains / IPs
            parsed = urlparse(url if url.startswith('http') else f'https://{url}')
            domain = parsed.netloc or parsed.path.split('/')[0]
            return domain.lower().replace('www.', '')
        except Exception as e:
            logger.debug(f"Non-critical error in domain extraction: {e}")
            return None

    async def _compute_warmth_score(self, contact: CompanyContact) -> int:
        """
        Layer 15: Compute email warm-up score based on domain infrastructure.
        Scores 0-100 based on SPF, DKIM, DMARC, domain age, MX, provider, catch-all.
        """
        score = 0
        domain = contact.company_domain

        if not domain:
            return 0

        try:
            # Get DNS-based warmup score from email verifier (SPF +20, DKIM +15, DMARC +15, MX +10)
            if self.email_verifier:
                dns_score = await self.email_verifier.get_domain_warmup_score(domain)
                # Map 0-20 verifier score to SPF/DKIM/DMARC/MX components
                # verifier gives 5 each for MX, SPF, DMARC, DKIM = 20 max
                # We remap: MX=+10, SPF=+20, DMARC=+15, DKIM=+15 = 60 max
                score += dns_score * 3  # 0-60

            # Domain age bonus (via WHOIS if available)
            domain_age_months = getattr(contact, '_domain_age_months', None)
            if domain_age_months is not None:
                if domain_age_months > 18:
                    score += 20
                elif domain_age_months >= 6:
                    score += 10

            # Provider reputation bonus
            if contact.email_mx_valid:
                primary_email = contact.contact_email or contact.marketing_email or contact.sales_email
                if primary_email:
                    email_domain = primary_email.split('@')[-1] if '@' in primary_email else ''
                    reputable_providers = ('google.com', 'googlemail.com', 'outlook.com',
                                          'microsoft.com', 'fastmail.com', 'protonmail.com')
                    # Check MX for known providers
                    for provider in reputable_providers:
                        if provider in email_domain:
                            score += 10
                            break

            # Catch-all penalty
            if contact.domain_is_catchall:
                score -= 10

        except Exception as e:
            logger.debug(f"Non-critical error in warmth score computation: {e}")

        return max(0, min(score, 100))

    # Noise domains and prefixes for engine-level email validation
    _ENGINE_NOISE_DOMAINS = {
        "example.com", "example.org", "example.net", "test.com",
        "email.com", "domain.com", "company.com", "website.com",
        "yourcompany.com", "yourdomain.com", "sentry.io",
        "schema.org", "w3.org", "googleapis.com", "gstatic.com",
    }
    _ENGINE_USELESS_PREFIXES = {
        "noreply", "no-reply", "no_reply", "donotreply", "do-not-reply",
        "mailer-daemon", "postmaster", "hostmaster", "webmaster",
        "abuse", "spam", "bounce", "daemon", "root", "nobody",
        "example", "test", "testing", "autoresponder", "auto-reply",
        "unsubscribe", "remove", "optout",
    }

    def _is_valid_email(self, email: str) -> bool:
        """Validate email format and filter out obviously invalid/useless emails."""
        if not email or not isinstance(email, str):
            return False
        email = email.strip().lower()
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email):
            return False
        local_part, email_domain = email.split("@", 1)
        # Skip image/asset/CSS/JS artifacts
        if any(ext in email_domain for ext in [".png", ".jpg", ".jpeg", ".svg", ".gif", ".css", ".js", ".woff", ".woff2", ".ttf", ".eot", ".ico", ".webp"]):
            return False
        if any(x in local_part for x in ["webpack", "sentry", "chunk", "module", "0x", "data-", "font-", "icon-", "img-", "static", "bundle", "vendor"]):
            return False
        # Skip noise domains
        if email_domain in self._ENGINE_NOISE_DOMAINS:
            return False
        # Skip useless prefixes for cold outreach
        if local_part in self._ENGINE_USELESS_PREFIXES:
            return False
        # Skip placeholder/template emails
        if any(p in email for p in ["your-email", "youremail", "user@", "name@", "email@", "someone@", "changeme@", "placeholder@", "john.doe@", "jane.doe@"]):
            return False
        return True

    @staticmethod
    def _record_email_source(contact: CompanyContact, field_name: str, source: str):
        """
        Layer 9: Track which sources found each email for cross-source corroboration.
        E.g. contact.email_sources["contact_email"] = ["website_contact", "dns_verified"]
        """
        if not hasattr(contact, 'email_sources') or contact.email_sources is None:
            contact.email_sources = {}
        if field_name not in contact.email_sources:
            contact.email_sources[field_name] = []
        if source not in contact.email_sources[field_name]:
            contact.email_sources[field_name].append(source)

    def _assign_email(self, contact: CompanyContact, field_name: str, email: str, source: str) -> bool:
        """
        Layer 9: Assign email to field + record source. Returns True if email was assigned.
        If field already has the same email, just adds source for corroboration.
        If field has a different email, returns False (caller should try fallback).
        """
        current = getattr(contact, field_name, None)
        if current is None:
            setattr(contact, field_name, email)
            self._record_email_source(contact, field_name, source)
            return True
        elif current == email:
            # Same email — add source for corroboration boost
            self._record_email_source(contact, field_name, source)
            return True
        return False  # Field occupied by different email

    def _process_found_emails(self, contact: CompanyContact, emails_categorized: Dict[str, str], source: str):
        """Helper to process and assign found emails to contact.

        Maps all categories to the 5 contact email fields:
        - contact_email: general, other, hr, legal, finance, dev/team (catch-all)
        - marketing_email: marketing
        - sales_email: sales
        - support_email: support
        - press_email: press

        If a slot is already filled, tries to fill empty slots as fallback.
        Layer 9: Records source for each assigned email for cross-source corroboration.
        """
        # Map categories to contact fields (primary and fallback assignments)
        CATEGORY_TO_FIELD = {
            "general": "contact_email",
            "other": "contact_email",
            "hr": "contact_email",
            "legal": "contact_email",
            "finance": "contact_email",
            "marketing": "marketing_email",
            "sales": "sales_email",
            "support": "support_email",
            "press": "press_email",
        }

        # Fallback order: if primary slot is full, try these
        FALLBACK_FIELDS = ["contact_email", "marketing_email", "sales_email", "support_email", "press_email"]

        for email, category in emails_categorized.items():
            added = False
            assigned_field = None

            # Try primary field for this category
            primary_field = CATEGORY_TO_FIELD.get(category, "contact_email")
            if not getattr(contact, primary_field, None):
                setattr(contact, primary_field, email)
                assigned_field = primary_field
                added = True
            elif getattr(contact, primary_field) == email:
                # Same email already in slot — just add source for corroboration
                self._record_email_source(contact, primary_field, source)
                continue
            else:
                # Primary slot is full - try to fill any empty slot as fallback
                for fallback_field in FALLBACK_FIELDS:
                    if fallback_field != primary_field and not getattr(contact, fallback_field, None):
                        setattr(contact, fallback_field, email)
                        assigned_field = fallback_field
                        added = True
                        break

            if added and assigned_field:
                # Layer 9: Record source for this email
                self._record_email_source(contact, assigned_field, source)
                self.stats["web_search_emails_found"] += 1
                self._emit_live_contact(
                    company_name=contact.company_name,
                    contact_type="email",
                    source=source,
                    confidence=65 if category in ["marketing", "sales", "press"] else 55,
                    email=email
                )

    def _calculate_osint_confidence(self, contact: CompanyContact) -> int:
        """Backward compat wrapper → unified confidence."""
        return self._calculate_unified_confidence(contact)

    def _calculate_ultra_confidence(self, contact: CompanyContact) -> int:
        """Backward compat wrapper → unified confidence."""
        return self._calculate_unified_confidence(contact)

    def _calculate_unified_confidence(self, contact: CompanyContact) -> int:
        """
        LAYER 9: Unified source-weighted confidence scoring.

        Replaces the old flat additive system with a multi-factor weighted formula:
          1. Source reliability (35%) — weighted by how trustworthy each source is
          2. Cross-source corroboration (25%) — 2+ sources finding same email = big boost
          3. Freshness decay (15%) — emails lose confidence over time
          4. Domain reputation (15%) — SPF/DKIM/DMARC/MX/age/catchall
          5. Role engagement (10%) — hr@=0.65, info@=0.20, personal=1.0

        Score range: 0-100. Tiers:
          90-100: Verified/Safe (<2% bounce)
          70-89:  High Confidence (<5% bounce)
          40-69:  Medium (verify before sending)
          1-39:   Low (do not send)
          0:      Invalid
        """
        # ========== 1. SOURCE RELIABILITY SCORE (0-1) ==========
        # For each email field, get the max source reliability from its sources
        email_fields = ["contact_email", "marketing_email", "sales_email", "support_email", "press_email"]
        email_source_weights = []  # Collect all per-email source weights

        for field in email_fields:
            email_val = getattr(contact, field, None)
            if not email_val:
                continue
            # Get sources for this email
            field_sources = (contact.email_sources or {}).get(field, [])
            if field_sources:
                # Best source reliability for this email
                best_weight = max(SOURCE_RELIABILITY.get(s, 0.30) for s in field_sources)
                email_source_weights.append(best_weight)
            else:
                # No source tracking — fall back to data_sources
                best_weight = 0.30  # Default: unknown source
                for ds in contact.data_sources:
                    w = SOURCE_RELIABILITY.get(ds, 0.0)
                    if w > best_weight:
                        best_weight = w
                email_source_weights.append(best_weight)

        if email_source_weights:
            source_score = sum(email_source_weights) / len(email_source_weights)
        else:
            source_score = 0.15  # No emails found at all

        # Bonus for data completeness (linkedin, description, people)
        completeness_bonus = 0.0
        if contact.company_linkedin:
            completeness_bonus += 0.05
        if contact.company_description:
            completeness_bonus += 0.03
        if contact.people:
            completeness_bonus += min(len(contact.people) * 0.02, 0.08)
        source_score = min(source_score + completeness_bonus, 1.0)

        # ========== 2. CROSS-SOURCE CORROBORATION (0-1) ==========
        # If same email found by 2+ independent sources → boost via complement product
        corroboration_score = 0.0
        corroboration_count = 0

        for field in email_fields:
            email_val = getattr(contact, field, None)
            if not email_val:
                continue
            field_sources = (contact.email_sources or {}).get(field, [])
            if len(field_sources) >= 2:
                # P(valid) = 1 - product(1 - p_i) for independent sources
                complement = 1.0
                for s in field_sources:
                    w = SOURCE_RELIABILITY.get(s, 0.30)
                    complement *= (1.0 - w)
                corroborated = min(1.0 - complement, 0.99)
                corroboration_score += corroborated
                corroboration_count += 1
            elif len(field_sources) == 1:
                corroboration_score += SOURCE_RELIABILITY.get(field_sources[0], 0.30)
                corroboration_count += 1
            else:
                corroboration_score += 0.25
                corroboration_count += 1

        if corroboration_count > 0:
            corroboration_score /= corroboration_count
        else:
            corroboration_score = 0.15

        # ========== 3. FRESHNESS DECAY (0-1) ==========
        freshness_score = 1.0  # Default: just extracted
        if hasattr(contact, 'last_verified_at') and contact.last_verified_at:
            try:
                verified_dt = datetime.fromisoformat(contact.last_verified_at)
                days_since = (datetime.now(timezone.utc) - verified_dt).days
                freshness_score = math.exp(-FRESHNESS_DECAY_LAMBDA * max(days_since, 0))
            except (ValueError, TypeError):
                freshness_score = 0.8  # Can't parse — assume somewhat fresh
        elif hasattr(contact, 'extracted_at') and contact.extracted_at:
            try:
                extracted_dt = datetime.fromisoformat(contact.extracted_at)
                days_since = (datetime.now(timezone.utc) - extracted_dt).days
                # Unverified emails decay faster (1.5x lambda)
                freshness_score = math.exp(-FRESHNESS_DECAY_LAMBDA * 1.5 * max(days_since, 0))
            except (ValueError, TypeError):
                freshness_score = 0.9

        # Store for external access
        contact.email_freshness_score = round(freshness_score, 3)

        # ========== 4. DOMAIN REPUTATION (0-1) ==========
        domain_score = 0.5  # Neutral default

        # Verification status bonuses
        verification_status = getattr(contact, 'email_verification_status', 'not_verified')
        if verification_status == "smtp_verified":
            domain_score += 0.25
        elif verification_status == "ms365_verified":
            domain_score += 0.20
        elif verification_status == "verified":
            domain_score += 0.15

        # MX validity
        if contact.email_mx_valid:
            domain_score += 0.10

        # Domain age
        age_cat = getattr(contact, 'email_domain_age_category', None)
        if age_cat == "mature":
            domain_score += 0.08
        elif age_cat == "established":
            domain_score += 0.05
        elif age_cat == "new":
            domain_score -= 0.10

        # MX provider quality
        mx_provider = getattr(contact, 'email_mx_provider', None)
        if mx_provider in ("google", "microsoft"):
            domain_score += 0.08
        elif mx_provider in ("zoho", "protonmail", "fastmail", "icloud"):
            domain_score += 0.05

        # Catch-all penalty
        if getattr(contact, 'domain_is_catchall', False):
            domain_score -= 0.10

        # Disposable email penalty
        if contact.email_is_disposable:
            domain_score -= 0.20

        # BIMI/security gateway bonuses
        if "bimi_verified" in contact.data_sources:
            domain_score += 0.05
        if "security_gateway" in contact.data_sources:
            domain_score += 0.03

        # DNS infrastructure score
        infra_score = getattr(contact, 'dns_infrastructure_score', 0)
        if infra_score >= 70:
            domain_score += 0.08
        elif infra_score >= 40:
            domain_score += 0.03

        # Bounce score integration
        bounce = getattr(contact, 'email_bounce_score', None)
        if bounce is not None:
            if bounce >= 80:
                domain_score += 0.08
            elif bounce >= 50:
                domain_score += 0.03
            elif bounce < 30:
                domain_score -= 0.10

        domain_score = max(0.0, min(domain_score, 1.0))

        # Store domain reputation
        contact.domain_reputation_score = int(domain_score * 100)

        # ========== 5. ROLE ENGAGEMENT SCORE (0-1) ==========
        # Score the "best" email by role engagement (personal names = 1.0)
        role_scores = []
        for field in email_fields:
            email_val = getattr(contact, field, None)
            if not email_val:
                continue
            local_part = email_val.split("@")[0].lower()
            # Check if it's a role address
            role_score = 1.0  # Default: personal address (best)
            for role_prefix, engagement in ROLE_ENGAGEMENT.items():
                if local_part == role_prefix or local_part.startswith(role_prefix + ".") or local_part.startswith(role_prefix + "_"):
                    role_score = engagement
                    break
            role_scores.append(role_score)

        if role_scores:
            # Use the best role engagement score (max, not avg)
            role_engagement = max(role_scores)
        else:
            role_engagement = 0.10  # No emails at all

        contact.role_engagement_score = round(role_engagement, 2)

        # ========== 6. EMAIL WARMTH SCORE (0-1) ==========
        warmth_normalized = contact.email_warmth_score / 100.0 if contact.email_warmth_score else 0.0

        # ========== COMBINE ALL FACTORS ==========
        # Weighted combination: source + corroboration + freshness + domain + role + warmth
        raw_score = (
            source_score * 0.33 +
            corroboration_score * 0.24 +
            freshness_score * 0.14 +
            domain_score * 0.14 +
            role_engagement * 0.10 +
            warmth_normalized * 0.05
        )

        # Scale to 0-100
        final = int(round(raw_score * 100))
        return max(0, min(final, 99))  # Cap at 99 - never 100% certain

    def get_stats(self) -> Dict[str, Any]:
        """Get extraction statistics"""
        return self.stats

    def get_progress(self) -> Dict[str, Any]:
        """Get current progress"""
        return self.progress

    async def close(self):
        """Close all scrapers, Ultra engine, and OSINT engine components (fault-tolerant)"""
        # Harvest Layer 7 stats before closing (non-network, safe)
        if self.osint_engine:
            try:
                if hasattr(self.osint_engine, 'search_engine') and self.osint_engine.search_engine:
                    se = self.osint_engine.search_engine
                    self.stats["search_cache_hits"] = getattr(se, 'cache_hits', 0)
                    self.stats["search_cache_misses"] = getattr(se, 'cache_misses', 0)
                    if hasattr(se, 'rotator'):
                        rotator_stats = se.rotator.get_stats()
                        self.stats["search_engine_rotations"] = rotator_stats.get("rotation_count", 0)
                        self.stats["search_circuit_breaker_trips"] = rotator_stats.get("circuit_breaker_trips", 0)
                        self.stats["search_engines_used"] = [
                            e for e, c in rotator_stats.get("success_counts", {}).items() if c > 0
                        ]
            except Exception as e:
                logger.debug(f"Non-critical error in OSINT engine stats harvesting: {e}")

        # Collect all close tasks — fault-tolerant: one failure won't block others
        close_tasks = [
            self.playstore.close(),
            self.appstore.close(),
            self.steam.close(),
            self.website_scraper.close(),
            self.producthunt.close(),
        ]
        if self.fdroid:
            close_tasks.append(self.fdroid.close())
        if self.microsoft:
            close_tasks.append(self.microsoft.close())
        if self.huawei:
            close_tasks.append(self.huawei.close())
        if self.github_scraper:
            close_tasks.append(self.github_scraper.close())
        if self.npm_scraper:
            close_tasks.append(self.npm_scraper.close())
        if self.hackernews_scraper:
            close_tasks.append(self.hackernews_scraper.close())
        if self.ssl_intel:
            close_tasks.append(self.ssl_intel.close())
        if self.wayback_intel:
            close_tasks.append(self.wayback_intel.close())
        if self.osint_engine:
            close_tasks.append(self.osint_engine.close())
        if self.email_verifier and hasattr(self.email_verifier, 'close'):
            close_tasks.append(self.email_verifier.close())

        results = await asyncio.gather(*close_tasks, return_exceptions=True)
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                logger.warning(f"Error closing resource {i}: {r}")

        logger.info("MobiAdz ULTRA+OSINT Extraction Engine V2.0 closed")


# Quick start functions
async def quick_mobiadz_extraction(
    demographics: List[str],
    categories: List[str],
    use_paid: bool = False,
    max_companies: int = 100
) -> List[Dict[str, Any]]:
    """
    Quick start function for MobiAdz extraction.

    Example:
        results = await quick_mobiadz_extraction(
            demographics=["usa", "europe"],
            categories=["mobile_apps", "games"],
            max_companies=50
        )
    """
    # Convert string inputs to enums
    demo_enums = [Demographic(d) for d in demographics if d in [e.value for e in Demographic]]
    cat_enums = [ProductCategory(c) for c in categories if c in [e.value for e in ProductCategory]]

    config = MobiAdzConfig(
        demographics=demo_enums or [Demographic.USA],
        categories=cat_enums or [ProductCategory.MOBILE_APPS],
        max_companies=max_companies,
        use_paid_apis=use_paid
    )

    engine = MobiAdzExtractionEngine(config)

    try:
        contacts = await engine.run_extraction()

        # Convert to dicts
        return [
            {
                "company_name": c.company_name,
                "app_or_product": c.app_or_product,
                "product_category": c.product_category,
                "demographic": c.demographic,
                "company_website": c.company_website,
                "contact_email": c.contact_email,
                "marketing_email": c.marketing_email,
                "sales_email": c.sales_email,
                "support_email": c.support_email,
                "company_linkedin": c.company_linkedin,
                "playstore_url": c.playstore_url,
                "appstore_url": c.appstore_url,
                "people": c.people,
                "confidence_score": c.confidence_score,
                "data_sources": c.data_sources
            }
            for c in contacts
        ]
    finally:
        await engine.close()
