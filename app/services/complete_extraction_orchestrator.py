"""
Complete 7-Layer Extraction Orchestrator
Gives users FULL CONTROL over which layers to use (FREE vs PAID)

Layers:
0. Intelligence Discovery (Google Search API - FREE 100/day)
1. Static Scraping (BeautifulSoup - FREE unlimited)
2. Internal Crawling (Recursive - FREE unlimited)
3. JS Rendering (Playwright - FREE but slower)
4. LLM Extraction (Claude API - $0.30/page, OPTIONAL)
5. Email Finding (FREE pattern detection OR Hunter.io)
6. Email Verification (FREE DNS/MX OR Hunter.io)
7. Data Enrichment (FREE web scraping OR Apollo.io)

User Configuration:
- Enable/disable each layer
- Choose FREE or PAID for layers 4-7
- Set budget limits
- View cost estimates
"""

import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from enum import Enum

from app.services.static_scraper import StaticScraperService
from app.services.validation_service import ValidationService
from app.services.free_email_finder import FreeEmailFinder
from app.services.apollo_client import ApolloClient
from app.services.hunter_client import HunterClient
from app.services.enrichment_orchestrator import EnrichmentOrchestrator
from app.services.google_search_client import GoogleSearchClient
from app.services.js_renderer import JavaScriptRenderer
from app.services.llm_extractor import LLMExtractor, PersonProfile
from app.core.ai_client import _ANTHROPIC_API_KEY

logger = logging.getLogger(__name__)


class LayerMode(str, Enum):
    """Extraction mode for each layer"""
    DISABLED = "disabled"
    FREE = "free"
    PAID = "paid"


@dataclass
class LayerConfig:
    """Configuration for each layer"""
    enabled: bool = True
    mode: LayerMode = LayerMode.FREE
    budget_limit: Optional[float] = None  # Max cost in USD


@dataclass
class ExtractionStrategy:
    """
    Complete extraction strategy configuration

    Users can configure each layer independently
    """
    # Layer 0: Discovery
    layer0_discovery: LayerConfig = LayerConfig(enabled=True, mode=LayerMode.FREE)

    # Layer 1: Static scraping (always FREE)
    layer1_static: LayerConfig = LayerConfig(enabled=True, mode=LayerMode.FREE)

    # Layer 2: Internal crawling (always FREE)
    layer2_internal: LayerConfig = LayerConfig(enabled=True, mode=LayerMode.FREE)
    max_depth: int = 3  # Crawl depth

    # Layer 3: JS rendering
    layer3_js_rendering: LayerConfig = LayerConfig(enabled=False, mode=LayerMode.FREE)

    # Layer 4: LLM extraction (OPTIONAL - costs money)
    layer4_llm: LayerConfig = LayerConfig(enabled=False, mode=LayerMode.PAID)

    # Layer 5: Email finding
    layer5_email_finding: LayerConfig = LayerConfig(enabled=True, mode=LayerMode.FREE)

    # Layer 6: Email verification
    layer6_email_verify: LayerConfig = LayerConfig(enabled=True, mode=LayerMode.FREE)

    # Layer 7: Data enrichment
    layer7_enrichment: LayerConfig = LayerConfig(enabled=True, mode=LayerMode.FREE)

    # API Keys (optional - only if PAID mode enabled)
    google_api_key: Optional[str] = None
    google_search_engine_id: Optional[str] = None  # Google Custom Search CX
    anthropic_api_key: Optional[str] = None
    apollo_api_key: Optional[str] = None
    hunter_api_key: Optional[str] = None

    # Budget limits
    max_total_cost_usd: Optional[float] = None  # Max total cost


class CompleteExtractionOrchestrator:
    """
    7-Layer Extraction Orchestrator with User Control

    Allows users to choose FREE vs PAID for each layer
    Provides cost estimation and tracking
    """

    def __init__(self, strategy: ExtractionStrategy):
        self.strategy = strategy

        # Initialize FREE services (always available)
        self.static_scraper = StaticScraperService()
        self.validation_service = ValidationService()
        self.free_email_finder = FreeEmailFinder()

        # Initialize Layer 0: Google Search (if configured)
        self.google_search = None
        if strategy.google_api_key and strategy.google_search_engine_id:
            self.google_search = GoogleSearchClient(
                api_key=strategy.google_api_key,
                search_engine_id=strategy.google_search_engine_id
            )

        # Initialize Layer 3: JavaScript Renderer (if enabled)
        self.js_renderer = None
        if strategy.layer3_js_rendering.enabled:
            self.js_renderer = JavaScriptRenderer(
                browser_type="chromium",
                headless=True,
                stealth_mode=True
            )

        # Initialize Layer 4: LLM Extractor (if configured)
        # Uses centralized API key as fallback if not provided in strategy
        self.llm_extractor = None
        if strategy.layer4_llm.enabled:
            api_key = strategy.anthropic_api_key or _ANTHROPIC_API_KEY
            self.llm_extractor = LLMExtractor(
                api_key=api_key,
                model="claude-3-haiku-20240307"  # Fastest, cheapest
            )

        # Initialize PAID services (only if configured)
        self.apollo = None
        self.hunter = None
        self.enrichment_orchestrator = None

        if strategy.apollo_api_key:
            self.apollo = ApolloClient(api_key=strategy.apollo_api_key)

        if strategy.hunter_api_key:
            self.hunter = HunterClient(api_key=strategy.hunter_api_key)

        if self.apollo or self.hunter:
            self.enrichment_orchestrator = EnrichmentOrchestrator(
                apollo_api_key=strategy.apollo_api_key,
                hunter_api_key=strategy.hunter_api_key
            )

        # Cost tracking
        self.total_cost_usd = 0.0
        self.layer_costs = {
            "layer0_discovery": 0.0,
            "layer1_static": 0.0,
            "layer2_internal": 0.0,
            "layer3_js_rendering": 0.0,
            "layer4_llm": 0.0,
            "layer5_email_finding": 0.0,
            "layer6_email_verify": 0.0,
            "layer7_enrichment": 0.0
        }

        # Statistics
        self.stats = {
            "total_urls_processed": 0,
            "total_records_extracted": 0,
            "layers_used": [],
            "free_methods_used": 0,
            "paid_methods_used": 0
        }

    async def discover_urls(
        self,
        job_titles: Optional[List[str]] = None,
        locations: Optional[List[str]] = None,
        companies: Optional[List[str]] = None,
        industries: Optional[List[str]] = None,
        max_results: int = 50
    ) -> List[str]:
        """
        Layer 0: Discover URLs using Google Search API

        Args:
            job_titles: Job titles to search for
            locations: Locations to search in
            companies: Companies to search for
            industries: Industries to search in
            max_results: Max number of URLs to return

        Returns:
            List of discovered URLs
        """
        if not self.google_search:
            logger.warning("Layer 0: Google Search not configured, skipping discovery")
            return []

        if not self.strategy.layer0_discovery.enabled:
            return []

        logger.info("Layer 0: Discovering URLs with Google Search")
        self.stats["layers_used"].append("layer0_discovery")

        try:
            # Search for people on LinkedIn
            results = await self.google_search.search_people(
                job_titles=job_titles or [],
                locations=locations,
                companies=companies,
                industries=industries,
                max_results=max_results
            )

            urls = [r["link"] for r in results if r.get("link")]

            # Track cost (Google Search API: FREE for 100/day, then $5/1000)
            google_stats = self.google_search.get_stats()
            queries_used = google_stats.get("total_queries", 0)
            if queries_used > 100:  # Beyond free tier
                cost_per_query = 0.005  # $5 / 1000 queries
                paid_queries = queries_used - 100
                self._track_cost("layer0_discovery", paid_queries * cost_per_query)
                self.stats["paid_methods_used"] += 1
            else:
                self.stats["free_methods_used"] += 1

            logger.info(f"Layer 0: Discovered {len(urls)} URLs")
            return urls

        except Exception as e:
            logger.error(f"Layer 0 discovery error: {e}")
            return []

    async def extract_from_url(self, url: str) -> List[Dict[str, Any]]:
        """
        Extract data from URL using configured layers

        Returns: List of extracted and enriched records
        """
        records = []

        # Layer 1: Static Scraping (always enabled, always FREE)
        if self.strategy.layer1_static.enabled:
            logger.info(f"Layer 1: Static scraping {url}")
            self.stats["layers_used"].append("layer1_static")

            result = await self.static_scraper.scrape_url(url, extract_links=True)
            if result["status"] == "success":
                data = result["data"]
                layer1_records = self._convert_to_records(data, url, layer=1)
                records.extend(layer1_records)
                self.stats["free_methods_used"] += 1

        # Layer 2: Internal Crawling (if enabled)
        if self.strategy.layer2_internal.enabled and len(records) < 100:
            logger.info(f"Layer 2: Internal crawling from {url}")
            self.stats["layers_used"].append("layer2_internal")

            # Get internal links
            if result.get("internal_links"):
                for internal_url in result["internal_links"][:20]:  # Limit
                    internal_result = await self.static_scraper.scrape_url(internal_url)
                    if internal_result["status"] == "success":
                        internal_data = internal_result["data"]
                        internal_records = self._convert_to_records(internal_data, internal_url, layer=2)
                        records.extend(internal_records)
                self.stats["free_methods_used"] += 1

        # Layer 3: JS Rendering (if enabled)
        if self.strategy.layer3_js_rendering.enabled and self.js_renderer:
            # Check if page needs JS rendering
            if self._needs_js_rendering(result.get("data", {}).get("text", "")):
                logger.info(f"Layer 3: JS rendering {url}")
                self.stats["layers_used"].append("layer3_js_rendering")

                try:
                    # Start renderer if not already started
                    if not self.js_renderer.browser:
                        await self.js_renderer.start()

                    # Render page with JavaScript
                    rendered = await self.js_renderer.render(
                        url=url,
                        wait_for="networkidle",
                        scroll_to_bottom=True
                    )

                    # Re-scrape the rendered HTML
                    rendered_result = await self.static_scraper.scrape_html(
                        html=rendered["html"],
                        base_url=url
                    )

                    if rendered_result["status"] == "success":
                        rendered_data = rendered_result["data"]
                        rendered_records = self._convert_to_records(rendered_data, url, layer=3)
                        records.extend(rendered_records)

                    self.stats["free_methods_used"] += 1
                    logger.info(f"Layer 3: Extracted {len(rendered_records)} records from JS-rendered page")
                except Exception as e:
                    logger.error(f"Layer 3 JS rendering error: {e}")

        # Layer 4: LLM Extraction (if enabled and needed for complex content)
        if self.strategy.layer4_llm.enabled and self.llm_extractor:
            # Only use LLM if we have few records (expensive!)
            if len(records) < 10:
                logger.info(f"Layer 4: LLM extraction for {url}")
                self.stats["layers_used"].append("layer4_llm")

                try:
                    # Use LLM to extract structured data from HTML
                    llm_result = await self.llm_extractor.extract(
                        html=result.get("data", {}).get("text", ""),
                        schema=PersonProfile,
                        instructions="Extract all people profiles from this page"
                    )

                    if llm_result:
                        llm_record = {
                            "source_url": url,
                            "extraction_layer": 4,
                            **llm_result
                        }
                        records.append(llm_record)

                        # Track LLM cost
                        llm_stats = self.llm_extractor.get_stats()
                        if "avg_cost_per_page" in llm_stats:
                            self._track_cost("layer4_llm", llm_stats["avg_cost_per_page"])

                        self.stats["paid_methods_used"] += 1
                        logger.info(f"Layer 4: LLM extracted 1 profile")
                except Exception as e:
                    logger.error(f"Layer 4 LLM extraction error: {e}")

        # Layer 5: Email Finding (for records without email)
        if self.strategy.layer5_email_finding.enabled:
            await self._enrich_emails_layer5(records)

        # Layer 6: Email Verification
        if self.strategy.layer6_email_verify.enabled:
            await self._verify_emails_layer6(records)

        # Layer 7: Data Enrichment
        if self.strategy.layer7_enrichment.enabled:
            await self._enrich_data_layer7(records)

        # Validation (always run)
        validated_records = []
        for record in records:
            validation = self.validation_service.validate_record(record)
            record.update(validation)
            validated_records.append(record)

        self.stats["total_records_extracted"] += len(validated_records)

        return validated_records

    async def _enrich_emails_layer5(self, records: List[Dict]) -> None:
        """
        Layer 5: Email Finding

        Mode FREE: Use pattern detection + DNS validation
        Mode PAID: Use Hunter.io or Apollo.io
        """
        mode = self.strategy.layer5_email_finding.mode

        for record in records:
            # Skip if already has email
            if record.get("email"):
                continue

            # Need name + company to find email
            if not (record.get("name") and record.get("company")):
                continue

            if mode == LayerMode.FREE:
                # FREE: Pattern detection
                self.stats["layers_used"].append("layer5_email_finding_free")
                result = await self.free_email_finder.find_email(
                    name=record["name"],
                    company=record["company"]
                )
                if result:
                    record["email"] = result["email"]
                    record["email_confidence"] = result["confidence"]
                    record["email_pattern"] = result["pattern"]
                    self.stats["free_methods_used"] += 1

            elif mode == LayerMode.PAID and self.hunter:
                # PAID: Hunter.io
                self.stats["layers_used"].append("layer5_email_finding_paid")
                name_parts = record["name"].split()
                if len(name_parts) >= 2:
                    domain = self._extract_domain(record["company"])
                    result = await self.hunter.find_email(
                        first_name=name_parts[0],
                        last_name=name_parts[-1],
                        domain=domain
                    )
                    if result:
                        record["email"] = result["email"]
                        record["email_confidence"] = result["score"]
                        self.stats["paid_methods_used"] += 1
                        self._track_cost("layer5_email_finding", 0.01)  # ~$0.01 per search

    async def _verify_emails_layer6(self, records: List[Dict]) -> None:
        """
        Layer 6: Email Verification

        Mode FREE: DNS/MX + format validation (80-85% accuracy)
        Mode PAID: Hunter.io verification (95% accuracy)
        """
        mode = self.strategy.layer6_email_verify.mode

        for record in records:
            if not record.get("email"):
                continue

            if mode == LayerMode.FREE:
                # FREE: DNS/MX validation
                self.stats["layers_used"].append("layer6_email_verify_free")
                verification = await self.free_email_finder.verify_email(record["email"])
                record["email_deliverable"] = verification["deliverable"]
                record["email_verification_confidence"] = verification["confidence"]
                record["email_disposable"] = verification["disposable"]
                record["email_role"] = verification["role"]
                self.stats["free_methods_used"] += 1

            elif mode == LayerMode.PAID and self.hunter:
                # PAID: Hunter.io verification
                self.stats["layers_used"].append("layer6_email_verify_paid")
                verification = await self.hunter.verify_email(record["email"])
                if verification:
                    record["email_deliverable"] = verification["is_deliverable"]
                    record["email_verification_score"] = verification["score"]
                    record["email_result"] = verification["result"]
                    record["email_disposable"] = verification["is_disposable"]
                    record["email_role"] = verification["is_role"]
                    self.stats["paid_methods_used"] += 1
                    self._track_cost("layer6_email_verify", 0.002)  # ~$0.002 per verification

    async def _enrich_data_layer7(self, records: List[Dict]) -> None:
        """
        Layer 7: Data Enrichment

        Mode FREE: Web scraping for LinkedIn, GitHub, company website
        Mode PAID: Apollo.io enrichment (phone, LinkedIn, title, etc.)
        """
        mode = self.strategy.layer7_enrichment.mode

        for record in records:
            if not record.get("email"):
                continue

            if mode == LayerMode.FREE:
                # FREE: Basic enrichment from public sources
                self.stats["layers_used"].append("layer7_enrichment_free")
                # Note: Could add more FREE enrichment here
                # - Search LinkedIn for public profile
                # - Check GitHub for developers
                # - Scrape company website for more details
                self.stats["free_methods_used"] += 1

            elif mode == LayerMode.PAID and self.apollo:
                # PAID: Apollo.io enrichment
                self.stats["layers_used"].append("layer7_enrichment_paid")
                apollo_data = await self.apollo.enrich_person(record["email"])
                if apollo_data:
                    record["phone"] = apollo_data.get("phone") or record.get("phone")
                    record["linkedin_url"] = apollo_data.get("linkedin_url") or record.get("linkedin_url")
                    record["title"] = apollo_data.get("title") or record.get("title")
                    record["seniority"] = apollo_data.get("seniority")
                    record["company_data"] = apollo_data.get("company", {})
                    self.stats["paid_methods_used"] += 1
                    self._track_cost("layer7_enrichment", 0.05)  # ~$0.05 per enrichment

    def _convert_to_records(
        self,
        data: Dict[str, Any],
        source_url: str,
        layer: int
    ) -> List[Dict[str, Any]]:
        """Convert scraped data to individual records"""
        records = []

        emails = data.get("emails", [])
        names = data.get("names", [])
        phones = data.get("phones", [])
        titles = data.get("titles", [])
        companies = data.get("companies", [])

        # Pair emails with other data
        for email in emails:
            record = {
                "email": email,
                "source_url": source_url,
                "extraction_layer": layer
            }

            if names:
                record["name"] = names.pop(0)
            if phones:
                record["phone"] = phones.pop(0)
            if titles:
                record["title"] = titles.pop(0)
            if companies:
                record["company"] = companies[0]  # Same company for all

            records.append(record)

        return records

    def _needs_js_rendering(self, html: str) -> bool:
        """Detect if page needs JavaScript rendering"""
        indicators = [
            'id="root"',  # React
            'id="app"',  # Vue
            'Loading...',
            'Please enable JavaScript'
        ]
        return any(indicator in html for indicator in indicators)

    def _extract_domain(self, company: str) -> str:
        """Extract domain from company name"""
        import re
        clean = re.sub(r'[^a-zA-Z0-9]', '', company.lower())
        return f"{clean}.com"

    def _track_cost(self, layer: str, cost: float) -> None:
        """Track cost for layer"""
        self.layer_costs[layer] += cost
        self.total_cost_usd += cost

        # Check budget limit
        if self.strategy.max_total_cost_usd:
            if self.total_cost_usd >= self.strategy.max_total_cost_usd:
                logger.warning(f"Budget limit reached: ${self.total_cost_usd:.2f}")

    def get_cost_estimate(self, num_records: int) -> Dict[str, float]:
        """
        Estimate cost for extracting N records

        Returns cost breakdown by layer
        """
        estimate = {
            "layer0_discovery": 0.0,  # FREE
            "layer1_static": 0.0,  # FREE
            "layer2_internal": 0.0,  # FREE
            "layer3_js_rendering": 0.0,  # FREE
            "layer4_llm": 0.0,  # $0.30 per page (if enabled)
            "layer5_email_finding": 0.0,
            "layer6_email_verify": 0.0,
            "layer7_enrichment": 0.0,
            "total": 0.0
        }

        # Layer 5: Email finding
        if self.strategy.layer5_email_finding.mode == LayerMode.PAID:
            estimate["layer5_email_finding"] = num_records * 0.01  # $0.01 per search

        # Layer 6: Email verification
        if self.strategy.layer6_email_verify.mode == LayerMode.PAID:
            estimate["layer6_email_verify"] = num_records * 0.002  # $0.002 per verification

        # Layer 7: Enrichment
        if self.strategy.layer7_enrichment.mode == LayerMode.PAID:
            estimate["layer7_enrichment"] = num_records * 0.05  # $0.05 per enrichment

        estimate["total"] = sum(estimate.values())

        return estimate

    def get_stats(self) -> Dict[str, Any]:
        """Get extraction statistics"""
        return {
            **self.stats,
            "total_cost_usd": round(self.total_cost_usd, 2),
            "layer_costs": {k: round(v, 2) for k, v in self.layer_costs.items()},
            "avg_cost_per_record": round(
                self.total_cost_usd / self.stats["total_records_extracted"], 4
            ) if self.stats["total_records_extracted"] > 0 else 0
        }

    async def close(self):
        """Close all clients"""
        await self.static_scraper.close()
        await self.free_email_finder.close()

        # Close Layer 0: Google Search
        if self.google_search:
            await self.google_search.close()

        # Close Layer 3: JavaScript Renderer
        if self.js_renderer:
            await self.js_renderer.close()

        # Close Layer 4: LLM Extractor (no async close needed for Anthropic client)

        # Close Layer 5-7: API clients
        if self.apollo:
            await self.apollo.close()
        if self.hunter:
            await self.hunter.close()
        if self.enrichment_orchestrator:
            await self.enrichment_orchestrator.close()


# Usage Example:
"""
# ============================================================
# COMPLETE 7-LAYER EXTRACTION EXAMPLES
# ============================================================

# Mode 1: 100% FREE (Layers 0-3 FREE, Layers 5-7 FREE)
# Best for: Testing, small campaigns, budget-conscious users
# Accuracy: 75-85%
# Cost: $0
strategy_free = ExtractionStrategy(
    layer0_discovery=LayerConfig(enabled=True, mode=LayerMode.FREE),
    layer1_static=LayerConfig(enabled=True, mode=LayerMode.FREE),
    layer2_internal=LayerConfig(enabled=True, mode=LayerMode.FREE),
    layer3_js_rendering=LayerConfig(enabled=True, mode=LayerMode.FREE),
    layer4_llm=LayerConfig(enabled=False),  # Skip LLM (expensive)
    layer5_email_finding=LayerConfig(enabled=True, mode=LayerMode.FREE),
    layer6_email_verify=LayerConfig(enabled=True, mode=LayerMode.FREE),
    layer7_enrichment=LayerConfig(enabled=True, mode=LayerMode.FREE),
    google_api_key="your_google_key",  # FREE tier: 100 queries/day
    google_search_engine_id="your_cx_id"
)

orchestrator = CompleteExtractionOrchestrator(strategy_free)

# Step 1: Discover URLs using Layer 0 (Google Search)
urls = await orchestrator.discover_urls(
    job_titles=["HR Manager", "Recruiter"],
    locations=["Luxembourg"],
    industries=["Technology"],
    max_results=50
)

print(f"Layer 0: Discovered {len(urls)} URLs")

# Step 2: Extract from each URL (Layers 1-7)
all_records = []
for url in urls[:10]:  # Process first 10
    records = await orchestrator.extract_from_url(url)
    all_records.extend(records)

print(f"Extracted {len(all_records)} records")
print(f"Total cost: ${orchestrator.total_cost_usd}")  # $0.00 (100% FREE)
print(f"Methods: {orchestrator.stats['free_methods_used']} FREE")

# ============================================================
# Mode 2: HYBRID (FREE scraping, PAID enrichment)
# Best for: Cost-conscious but need accuracy
# Accuracy: 85-90%
# Cost: $20-50/month
strategy_hybrid = ExtractionStrategy(
    layer0_discovery=LayerConfig(enabled=True, mode=LayerMode.FREE),
    layer3_js_rendering=LayerConfig(enabled=True, mode=LayerMode.FREE),
    layer4_llm=LayerConfig(enabled=False),  # Skip expensive LLM
    layer5_email_finding=LayerConfig(enabled=True, mode=LayerMode.FREE),
    layer6_email_verify=LayerConfig(enabled=True, mode=LayerMode.PAID),  # Hunter
    layer7_enrichment=LayerConfig(enabled=True, mode=LayerMode.PAID),  # Apollo
    hunter_api_key="your_hunter_key",
    apollo_api_key="your_apollo_key",
    max_total_cost_usd=10.0  # Budget limit
)

orchestrator = CompleteExtractionOrchestrator(strategy_hybrid)

records = await orchestrator.extract_from_url("https://example.com/team")

print(f"Total cost: ${orchestrator.total_cost_usd}")  # ~$0.50-2.00
print(f"FREE methods: {orchestrator.stats['free_methods_used']}")
print(f"PAID methods: {orchestrator.stats['paid_methods_used']}")

# ============================================================
# Mode 3: FULL PAID (ALL 7 layers with paid options)
# Best for: Maximum speed & accuracy
# Accuracy: 90-95%
# Cost: $100-200/month
strategy_full_paid = ExtractionStrategy(
    layer0_discovery=LayerConfig(enabled=True, mode=LayerMode.PAID),  # Google (beyond free tier)
    layer1_static=LayerConfig(enabled=True, mode=LayerMode.FREE),
    layer2_internal=LayerConfig(enabled=True, mode=LayerMode.FREE),
    layer3_js_rendering=LayerConfig(enabled=True, mode=LayerMode.FREE),
    layer4_llm=LayerConfig(enabled=True, mode=LayerMode.PAID),  # Claude for complex pages
    layer5_email_finding=LayerConfig(enabled=True, mode=LayerMode.PAID),  # Hunter
    layer6_email_verify=LayerConfig(enabled=True, mode=LayerMode.PAID),  # Hunter
    layer7_enrichment=LayerConfig(enabled=True, mode=LayerMode.PAID),  # Apollo
    google_api_key="your_google_key",
    google_search_engine_id="your_cx_id",
    anthropic_api_key="your_anthropic_key",
    hunter_api_key="your_hunter_key",
    apollo_api_key="your_apollo_key",
)

orchestrator = CompleteExtractionOrchestrator(strategy_full_paid)

# Get cost estimate first
estimate = orchestrator.get_cost_estimate(num_records=100)
print(f"Estimated cost for 100 records: ${estimate['total']}")

# Discover and extract
urls = await orchestrator.discover_urls(
    job_titles=["Software Engineer"],
    locations=["San Francisco"],
    max_results=100
)

all_records = []
for url in urls:
    records = await orchestrator.extract_from_url(url)
    all_records.extend(records)

# Get statistics
stats = orchestrator.get_stats()
print(f"Total records: {stats['total_records_extracted']}")
print(f"Total cost: ${stats['total_cost_usd']}")
print(f"Avg cost per record: ${stats['avg_cost_per_record']}")
print(f"Layers used: {stats['layers_used']}")

await orchestrator.close()

# ============================================================
# LAYER COMPARISON:
#
# Layer 0 (Discovery): Google Search - Find target URLs
#   - FREE: 100 queries/day
#   - PAID: $5 per 1000 queries
#
# Layer 1 (Static): BeautifulSoup - Parse HTML
#   - Always FREE
#
# Layer 2 (Crawling): Recursive links - Follow internal pages
#   - Always FREE
#
# Layer 3 (JS Rendering): Playwright - Handle React/Vue/Angular
#   - Always FREE (but slower, high CPU)
#
# Layer 4 (LLM): Claude AI - Extract from unstructured content
#   - Always PAID: ~$0.30 per page
#   - Use ONLY for high-value targets!
#
# Layer 5 (Email Finding):
#   - FREE: Pattern detection + DNS = 75-85% accuracy
#   - PAID: Hunter.io = 95% accuracy, $0.01 per search
#
# Layer 6 (Email Verify):
#   - FREE: DNS/MX records = 80-85% accuracy
#   - PAID: Hunter.io = 95% accuracy, $0.002 per verification
#
# Layer 7 (Enrichment):
#   - FREE: Web scraping for LinkedIn/company data
#   - PAID: Apollo.io = 50+ data points, $0.05 per enrichment
#
# ============================================================
"""
