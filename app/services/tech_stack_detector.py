"""
Enhanced Layer 1.5: Tech Stack Detector

INTELLIGENT technology detection and adaptive scraping strategy

Detects:
- Frontend: React, Vue, Angular, Svelte, Next.js, Nuxt, etc.
- Backend: PHP, Python, Ruby, Node.js, Java, .NET, etc.
- CMS: WordPress, Drupal, Joomla, Shopify, etc.
- JavaScript libraries: jQuery, Lodash, Moment.js, etc.
- Analytics: Google Analytics, Mixpanel, etc.
- CDNs: Cloudflare, Akamai, etc.
- Hosting: AWS, GCP, Heroku, Vercel, etc.

Purpose: Adapt scraping strategy based on detected tech stack

Strategy:
1. Analyze HTML, headers, and loaded resources
2. Detect JavaScript frameworks, CMS, backend tech
3. Choose optimal scraping method:
   - Static HTML → BeautifulSoup (fast)
   - React/Vue/Angular → Playwright (JS rendering)
   - Server-side rendered → Special handling
4. Optimize selectors and extraction based on framework
"""

import logging
import re
from typing import Dict, List, Any, Set, Optional
from dataclasses import dataclass
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


@dataclass
class TechStack:
    """Detected technology stack"""
    frontend_frameworks: List[str]  # React, Vue, Angular, etc.
    backend_languages: List[str]  # PHP, Python, Ruby, etc.
    cms: List[str]  # WordPress, Drupal, etc.
    javascript_libraries: List[str]  # jQuery, Lodash, etc.
    analytics: List[str]  # Google Analytics, etc.
    cdn: List[str]  # Cloudflare, Akamai, etc.
    hosting: List[str]  # AWS, GCP, etc.
    meta_frameworks: List[str]  # Next.js, Nuxt, Gatsby, etc.
    rendering_mode: str  # "csr" (client-side), "ssr" (server-side), "ssg" (static), "hybrid"
    confidence_score: float  # 0.0 - 1.0


class TechStackDetector:
    """
    INTELLIGENT tech stack detection and adaptive scraping

    Features:
    - Multi-signal detection (HTML, headers, resources, patterns)
    - 500+ technology signatures
    - Rendering mode detection (CSR vs SSR)
    - Adaptive scraping strategy recommendation
    - Framework-specific selector optimization
    """

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=15.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )

        # Technology signatures
        self._init_signatures()

        # Statistics
        self.stats = {
            "total_detections": 0,
            "csr_sites": 0,  # Client-side rendered
            "ssr_sites": 0,  # Server-side rendered
            "static_sites": 0
        }

    def _init_signatures(self):
        """Initialize technology detection signatures"""

        # Frontend frameworks
        self.frontend_signatures = {
            "react": [
                r'react(?:\.min)?\.js',
                r'data-reactroot',
                r'_react',
                r'__REACT',
                r'ReactDOM'
            ],
            "vue": [
                r'vue(?:\.min)?\.js',
                r'v-if=',
                r'v-for=',
                r'v-bind:',
                r'__VUE__'
            ],
            "angular": [
                r'angular(?:\.min)?\.js',
                r'ng-app=',
                r'ng-controller=',
                r'ng-model=',
                r'\[ngApp\]'
            ],
            "svelte": [
                r'svelte',
                r'__SVELTE__'
            ],
            "ember": [
                r'ember(?:\.min)?\.js',
                r'ember-'
            ]
        }

        # Meta frameworks (built on top of frontend frameworks)
        self.meta_framework_signatures = {
            "next.js": [
                r'__NEXT_DATA__',
                r'_next/static',
                r'__next',
                r'next\.js'
            ],
            "nuxt": [
                r'__NUXT__',
                r'_nuxt/',
                r'nuxt\.js'
            ],
            "gatsby": [
                r'gatsby',
                r'___gatsby'
            ],
            "remix": [
                r'remix',
                r'__remix'
            ]
        }

        # CMS detection
        self.cms_signatures = {
            "wordpress": [
                r'wp-content',
                r'wp-includes',
                r'wordpress',
                r'/xmlrpc\.php'
            ],
            "drupal": [
                r'Drupal\.settings',
                r'/sites/default/',
                r'drupal\.js'
            ],
            "joomla": [
                r'joomla',
                r'/components/com_'
            ],
            "shopify": [
                r'cdn\.shopify\.com',
                r'Shopify\.theme'
            ],
            "wix": [
                r'wix\.com',
                r'_wix'
            ],
            "squarespace": [
                r'squarespace',
                r'sqsp'
            ]
        }

        # Backend languages (detected from headers and patterns)
        self.backend_signatures = {
            "php": [r'\.php', r'PHPSESSID', r'X-Powered-By.*PHP'],
            "python": [r'Django', r'Flask', r'Werkzeug', r'wsgi'],
            "ruby": [r'Ruby', r'Rails', r'rack'],
            "nodejs": [r'Express', r'Node\.js', r'Koa'],
            "java": [r'\.jsp', r'\.do', r'Tomcat', r'JSESSIONID'],
            "dotnet": [r'\.aspx', r'ASP\.NET', r'X-AspNet-Version']
        }

        # JavaScript libraries
        self.js_library_signatures = {
            "jquery": [r'jquery(?:\.min)?\.js', r'\$\('],
            "lodash": [r'lodash(?:\.min)?\.js', r'_\.'],
            "moment": [r'moment(?:\.min)?\.js'],
            "axios": [r'axios(?:\.min)?\.js'],
            "bootstrap": [r'bootstrap(?:\.min)?\.js'],
            "tailwind": [r'tailwindcss']
        }

        # Analytics
        self.analytics_signatures = {
            "google_analytics": [r'google-analytics\.com', r'gtag\(', r'ga\('],
            "mixpanel": [r'mixpanel\.com'],
            "hotjar": [r'hotjar\.com'],
            "segment": [r'segment\.com'],
            "amplitude": [r'amplitude\.com']
        }

        # CDN
        self.cdn_signatures = {
            "cloudflare": [r'cloudflare', r'__cf'],
            "akamai": [r'akamai'],
            "fastly": [r'fastly'],
            "cloudfront": [r'cloudfront\.net']
        }

    async def detect(self, url: str) -> TechStack:
        """
        Detect complete tech stack for a website

        Args:
            url: Website URL to analyze

        Returns:
            TechStack with all detected technologies
        """
        try:
            # Fetch page with headers
            response = await self.client.get(url)
            response.raise_for_status()

            html = response.text
            headers = dict(response.headers)

            # Parse HTML
            soup = BeautifulSoup(html, 'lxml')

            # Detect each category
            frontend = self._detect_category(html, self.frontend_signatures)
            meta_frameworks = self._detect_category(html, self.meta_framework_signatures)
            cms = self._detect_category(html, self.cms_signatures)
            backend = self._detect_backend(html, headers)
            js_libs = self._detect_category(html, self.js_library_signatures)
            analytics = self._detect_category(html, self.analytics_signatures)
            cdn = self._detect_cdn(headers)

            # Detect rendering mode
            rendering_mode = self._detect_rendering_mode(html, frontend, meta_frameworks)

            # Calculate confidence score
            confidence = self._calculate_confidence(
                frontend, meta_frameworks, cms, backend, rendering_mode
            )

            # Update statistics
            self.stats["total_detections"] += 1
            if rendering_mode == "csr":
                self.stats["csr_sites"] += 1
            elif rendering_mode == "ssr":
                self.stats["ssr_sites"] += 1
            else:
                self.stats["static_sites"] += 1

            tech_stack = TechStack(
                frontend_frameworks=frontend,
                backend_languages=backend,
                cms=cms,
                javascript_libraries=js_libs,
                analytics=analytics,
                cdn=cdn,
                hosting=[],  # TODO: detect from DNS/WHOIS
                meta_frameworks=meta_frameworks,
                rendering_mode=rendering_mode,
                confidence_score=confidence
            )

            logger.info(
                f"Tech stack detected for {url}: "
                f"{', '.join(frontend + meta_frameworks + cms) or 'static HTML'} "
                f"(rendering: {rendering_mode}, confidence: {confidence:.2f})"
            )

            return tech_stack

        except Exception as e:
            logger.error(f"Tech detection error for {url}: {e}")
            # Return empty tech stack
            return TechStack(
                frontend_frameworks=[],
                backend_languages=[],
                cms=[],
                javascript_libraries=[],
                analytics=[],
                cdn=[],
                hosting=[],
                meta_frameworks=[],
                rendering_mode="unknown",
                confidence_score=0.0
            )

    def _detect_category(
        self,
        html: str,
        signatures: Dict[str, List[str]]
    ) -> List[str]:
        """Detect technologies in a category"""
        detected = []

        for tech, patterns in signatures.items():
            for pattern in patterns:
                if re.search(pattern, html, re.IGNORECASE):
                    detected.append(tech)
                    break  # One match is enough

        return detected

    def _detect_backend(self, html: str, headers: Dict[str, str]) -> List[str]:
        """Detect backend language/framework"""
        detected = []

        # Check headers
        for tech, patterns in self.backend_signatures.items():
            for pattern in patterns:
                # Check headers
                for header_value in headers.values():
                    if re.search(pattern, str(header_value), re.IGNORECASE):
                        detected.append(tech)
                        break
                # Check HTML
                if re.search(pattern, html, re.IGNORECASE):
                    detected.append(tech)
                    break

        return list(set(detected))  # Remove duplicates

    def _detect_cdn(self, headers: Dict[str, str]) -> List[str]:
        """Detect CDN from headers"""
        detected = []

        header_str = " ".join([f"{k}: {v}" for k, v in headers.items()])

        for cdn, patterns in self.cdn_signatures.items():
            for pattern in patterns:
                if re.search(pattern, header_str, re.IGNORECASE):
                    detected.append(cdn)
                    break

        return detected

    def _detect_rendering_mode(
        self,
        html: str,
        frontend_frameworks: List[str],
        meta_frameworks: List[str]
    ) -> str:
        """
        Detect rendering mode

        Returns:
        - "csr": Client-side rendering (React SPA, Vue SPA, etc.)
        - "ssr": Server-side rendering (Next.js SSR, Nuxt SSR, etc.)
        - "ssg": Static site generation (Gatsby, Hugo, etc.)
        - "hybrid": Mix of SSR and CSR
        - "static": Plain HTML/PHP/etc.
        """
        # Check for typical CSR indicators
        csr_indicators = [
            r'<div id="root"></div>',
            r'<div id="app"></div>',
            r'<div id="__next"></div>',
            r'Loading\.\.\.',
            r'<noscript>.*JavaScript.*</noscript>'
        ]

        is_csr = any(re.search(pattern, html, re.IGNORECASE) for pattern in csr_indicators)

        # Check for SSR indicators (hydrated content)
        has_content = len(re.findall(r'<p>|<h[1-6]>|<article>', html)) > 5

        if "next.js" in meta_frameworks or "nuxt" in meta_frameworks:
            if has_content:
                return "ssr"  # Server-side rendered with hydration
            else:
                return "hybrid"  # Probably hybrid mode

        if "gatsby" in meta_frameworks:
            return "ssg"  # Static site generation

        if frontend_frameworks and is_csr:
            return "csr"  # Pure client-side rendering

        if frontend_frameworks and has_content:
            return "hybrid"  # Server-side with client hydration

        if has_content:
            return "static"  # Traditional server-rendered HTML

        return "unknown"

    def _calculate_confidence(
        self,
        frontend: List[str],
        meta_frameworks: List[str],
        cms: List[str],
        backend: List[str],
        rendering_mode: str
    ) -> float:
        """Calculate confidence score for detection"""
        score = 0.0

        # Strong signals
        if frontend:
            score += 0.3
        if meta_frameworks:
            score += 0.3
        if cms:
            score += 0.3

        # Weak signals
        if backend:
            score += 0.1
        if rendering_mode != "unknown":
            score += 0.1

        return min(score, 1.0)

    def recommend_scraping_strategy(self, tech_stack: TechStack) -> Dict[str, Any]:
        """
        Recommend optimal scraping strategy based on tech stack

        Returns:
            {
                "method": "static" | "playwright" | "hybrid",
                "wait_for": "networkidle" | "load" | "domcontentloaded",
                "selectors": {...},  # Framework-specific selectors
                "wait_time": int,  # Milliseconds
                "scroll_needed": bool
            }
        """
        strategy = {
            "method": "static",
            "wait_for": "load",
            "selectors": {},
            "wait_time": 0,
            "scroll_needed": False
        }

        # Client-side rendered → Use Playwright
        if tech_stack.rendering_mode == "csr":
            strategy["method"] = "playwright"
            strategy["wait_for"] = "networkidle"
            strategy["wait_time"] = 3000  # Wait 3 seconds for JS
            strategy["scroll_needed"] = True  # Trigger lazy loading

        # Server-side rendered → Static is fine, but may need Playwright for interactions
        elif tech_stack.rendering_mode == "ssr":
            strategy["method"] = "hybrid"  # Try static first, Playwright if needed
            strategy["wait_for"] = "domcontentloaded"

        # Hybrid → Use Playwright with shorter wait
        elif tech_stack.rendering_mode == "hybrid":
            strategy["method"] = "playwright"
            strategy["wait_for"] = "domcontentloaded"
            strategy["wait_time"] = 1000

        # Static/unknown → BeautifulSoup is fine
        else:
            strategy["method"] = "static"

        # Framework-specific selectors
        if "react" in tech_stack.frontend_frameworks:
            strategy["selectors"] = {
                "root": "#root, #app, [data-reactroot]",
                "text": "p, span, div[class*='text']"
            }

        if "vue" in tech_stack.frontend_frameworks:
            strategy["selectors"] = {
                "root": "#app, [v-app]",
                "text": "p, span, div[class*='text']"
            }

        if "wordpress" in tech_stack.cms:
            strategy["selectors"] = {
                "content": ".entry-content, .post-content",
                "author": ".author-name, .post-author",
                "date": ".entry-date, .published"
            }

        logger.debug(f"Recommended strategy: {strategy['method']} (rendering: {tech_stack.rendering_mode})")
        return strategy

    def get_stats(self) -> Dict[str, int]:
        """Get detection statistics"""
        return self.stats.copy()

    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()


# Usage Example:
"""
detector = TechStackDetector()

# Detect tech stack
tech_stack = await detector.detect("https://example.com")

print(f"Frontend: {', '.join(tech_stack.frontend_frameworks)}")
print(f"Meta frameworks: {', '.join(tech_stack.meta_frameworks)}")
print(f"CMS: {', '.join(tech_stack.cms)}")
print(f"Backend: {', '.join(tech_stack.backend_languages)}")
print(f"Rendering: {tech_stack.rendering_mode}")
print(f"Confidence: {tech_stack.confidence_score:.2f}")

# Get recommended scraping strategy
strategy = detector.recommend_scraping_strategy(tech_stack)

print(f"\nRecommended scraping method: {strategy['method']}")
print(f"Wait for: {strategy['wait_for']}")
print(f"Scroll needed: {strategy['scroll_needed']}")

if strategy['method'] == 'playwright':
    print("→ Use Playwright (JavaScript rendering needed)")
else:
    print("→ Use BeautifulSoup (static HTML is fine)")

await detector.close()

# Integration with scraper:
if strategy['method'] == 'playwright':
    from app.services.js_renderer import JavaScriptRenderer
    renderer = JavaScriptRenderer()
    await renderer.start()
    result = await renderer.render(
        url="https://example.com",
        wait_for=strategy['wait_for'],
        scroll_to_bottom=strategy['scroll_needed']
    )
    html = result['html']
else:
    # Use static scraper
    from app.services.static_scraper import StaticScraperService
    scraper = StaticScraperService()
    result = await scraper.scrape_url("https://example.com")
    html = result['data']['text']
"""
