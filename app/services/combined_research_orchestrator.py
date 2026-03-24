"""
Combined Research Orchestrator

Coordinates ultra-deep research on both:
1. Company Intelligence (via UltraCompanyIntelligence)
2. Person Intelligence (via UltraPersonIntelligence)

Runs research in parallel for maximum speed, then combines results
into a unified intelligence report for either job applications or
marketing/sales outreach.
"""

import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from app.services.ultra_company_intelligence import UltraCompanyIntelligence
from app.services.ultra_person_intelligence import UltraPersonIntelligence

logger = logging.getLogger(__name__)


class CombinedResearchOrchestrator:
    """
    God-Tier Combined Research Orchestrator

    Performs parallel deep research on:
    - Company (tech stack, culture, projects, pain points, buying signals)
    - Person (work history, education, achievements, online presence)

    Mode-specific focus:
    - Job Mode: Career opportunities, hiring signals, team culture
    - Market Mode: Business opportunities, decision making authority, pain points
    """

    def __init__(self):
        self.company_researcher = UltraCompanyIntelligence()
        self.person_researcher = UltraPersonIntelligence()

    async def research_recipient(
        self,
        recipient_name: str,
        recipient_email: Optional[str],
        recipient_title: Optional[str],
        recipient_location: Optional[str],
        company_name: str,
        company_website: str,
        mode: str = "job"  # "job" or "market"
    ) -> Dict[str, Any]:
        """
        Comprehensive research on recipient and their company

        Args:
            recipient_name: Full name of the recipient
            recipient_email: Email address (optional)
            recipient_title: Job title (optional)
            recipient_location: Location (optional)
            company_name: Company name
            company_website: Company website URL
            mode: Research mode - "job" (career) or "market" (business/sales)

        Returns:
            Combined intelligence report with both company and person data
        """
        logger.info(
            f"🚀 [COMBINED RESEARCH] Starting {mode} research for "
            f"{recipient_name} at {company_name}"
        )

        start_time = datetime.now(timezone.utc)

        # Run both research tasks in parallel for speed
        company_task = self.company_researcher.research_company(
            company_name=company_name,
            website=company_website,
            mode=mode
        )

        person_task = self.person_researcher.research_person(
            name=recipient_name,
            company=company_name,
            email=recipient_email,
            title=recipient_title,
            location=recipient_location,
            mode=mode
        )

        # Wait for both to complete
        company_intelligence, person_intelligence = await asyncio.gather(
            company_task,
            person_task,
            return_exceptions=True
        )

        # Handle errors gracefully
        if isinstance(company_intelligence, Exception):
            logger.error(f"Company research failed: {company_intelligence}")
            company_intelligence = None

        if isinstance(person_intelligence, Exception):
            logger.error(f"Person research failed: {person_intelligence}")
            person_intelligence = None

        # Calculate combined confidence
        company_confidence = (
            company_intelligence.confidence_score
            if company_intelligence else 0
        )
        person_confidence = (
            person_intelligence.get("confidence_score", 0)
            if person_intelligence else 0
        )
        combined_confidence = (company_confidence + person_confidence) / 2

        # Build combined report
        report = {
            "recipient": {
                "name": recipient_name,
                "email": recipient_email,
                "title": recipient_title,
                "location": recipient_location,
            },
            "company": {
                "name": company_name,
                "website": company_website,
            },
            "research_mode": mode,
            "company_intelligence": (
                company_intelligence.to_dict()
                if company_intelligence else {}
            ),
            "person_intelligence": person_intelligence or {},
            "combined_confidence_score": round(combined_confidence, 1),
            "key_insights": self._generate_key_insights(
                company_intelligence,
                person_intelligence,
                mode
            ),
            "research_duration_seconds": (
                datetime.now(timezone.utc) - start_time
            ).total_seconds(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        logger.info(
            f"✅ [COMBINED RESEARCH] Research complete for {recipient_name}: "
            f"{combined_confidence:.1f}% confidence in "
            f"{report['research_duration_seconds']:.1f}s"
        )

        return report

    def _generate_key_insights(
        self,
        company_intelligence,
        person_intelligence: Optional[Dict],
        mode: str
    ) -> Dict[str, Any]:
        """Generate actionable insights from research data"""

        insights = {
            "summary": [],
            "opportunities": [],
            "talking_points": [],
            "warnings": []
        }

        if not company_intelligence and not person_intelligence:
            insights["warnings"].append("Limited data available - research incomplete")
            return insights

        # Company insights
        if company_intelligence:
            # Tech stack
            if company_intelligence.tech_stack:
                tech_count = len(company_intelligence.tech_stack)
                insights["summary"].append(f"Company uses {tech_count} technologies")
                insights["talking_points"].extend(
                    company_intelligence.tech_stack[:5]
                )

            # Job mode specific
            if mode == "job":
                if company_intelligence.job_openings:
                    job_count = len(company_intelligence.job_openings)
                    insights["summary"].append(f"{job_count} active job openings")
                    insights["opportunities"].append(
                        f"Company is actively hiring for {job_count} positions"
                    )

                if company_intelligence.company_culture:
                    insights["talking_points"].append(
                        f"Culture: {company_intelligence.company_culture[:100]}"
                    )

            # Market mode specific
            if mode == "market":
                if company_intelligence.pain_points:
                    pain_count = len(company_intelligence.pain_points)
                    insights["summary"].append(f"{pain_count} potential pain points identified")
                    insights["opportunities"].extend(
                        company_intelligence.pain_points[:3]
                    )

                if company_intelligence.buying_signals:
                    signal_count = len(company_intelligence.buying_signals)
                    insights["summary"].append(f"{signal_count} buying signals detected")
                    insights["opportunities"].extend(
                        company_intelligence.buying_signals[:3]
                    )

                if company_intelligence.recent_initiatives:
                    insights["talking_points"].extend(
                        company_intelligence.recent_initiatives[:3]
                    )

        # Person insights
        if person_intelligence:
            sources_found = person_intelligence.get("sources_found", 0)
            insights["summary"].append(
                f"Found person data in {sources_found}/14 sources"
            )

            # Professional networks
            prof_networks = person_intelligence.get("professional_networks", {})

            if prof_networks.get("linkedin", {}).get("found"):
                insights["talking_points"].append("Active on LinkedIn")

            if prof_networks.get("github", {}).get("found"):
                github_data = prof_networks["github"]["profile"]
                repo_count = github_data.get("public_repos", 0)
                if repo_count > 0:
                    insights["talking_points"].append(
                        f"Open source contributor ({repo_count} repos)"
                    )

            # Academic background
            academic = person_intelligence.get("academic", {})

            if academic.get("google_scholar", {}).get("found"):
                paper_count = academic["google_scholar"].get("count", 0)
                if paper_count > 0:
                    insights["talking_points"].append(
                        f"Published {paper_count} academic papers"
                    )

            if academic.get("patents", {}).get("found"):
                patent_count = academic["patents"].get("count", 0)
                if patent_count > 0:
                    insights["talking_points"].append(
                        f"Holds {patent_count} patents"
                    )

            # Media presence
            media = person_intelligence.get("media_mentions", {})

            if media.get("news", {}).get("found"):
                news_count = media["news"].get("count", 0)
                if news_count > 0:
                    insights["summary"].append(
                        f"Featured in {news_count} news articles"
                    )

        # Confidence warnings
        if company_intelligence:
            if company_intelligence.confidence_score < 40:
                insights["warnings"].append(
                    "Low company data confidence - limited information available"
                )

        if person_intelligence:
            if person_intelligence.get("confidence_score", 0) < 40:
                insights["warnings"].append(
                    "Low person data confidence - limited online presence"
                )

        return insights

    async def close(self):
        """Close all HTTP clients"""
        await self.person_researcher.close()
        # Company researcher uses context manager, no close needed


# Usage Example:
"""
orchestrator = CombinedResearchOrchestrator()

# Job application research
report = await orchestrator.research_recipient(
    recipient_name="John Doe",
    recipient_email="john.doe@techcorp.com",
    recipient_title="Engineering Manager",
    recipient_location="San Francisco, CA",
    company_name="Tech Corp",
    company_website="https://techcorp.com",
    mode="job"
)

print(f"Combined Confidence: {report['combined_confidence_score']}/100")
print(f"Key Insights: {report['key_insights']['summary']}")
print(f"Opportunities: {report['key_insights']['opportunities']}")
print(f"Talking Points: {report['key_insights']['talking_points']}")

# Access company data
company = report['company_intelligence']
print(f"Tech Stack: {company['tech_stack']}")
print(f"Job Openings: {len(company['job_openings'])}")

# Access person data
person = report['person_intelligence']
if person['professional_networks']['linkedin']['found']:
    print("LinkedIn profile found!")

await orchestrator.close()


# Marketing/Sales research
report = await orchestrator.research_recipient(
    recipient_name="Jane Smith",
    recipient_email="jane.smith@enterprise.com",
    recipient_title="VP of Engineering",
    recipient_location="New York, NY",
    company_name="Enterprise Corp",
    company_website="https://enterprise.com",
    mode="market"
)

# Access market intelligence
company = report['company_intelligence']
print(f"Pain Points: {company['pain_points']}")
print(f"Buying Signals: {company['buying_signals']}")
print(f"Recent Initiatives: {company['recent_initiatives']}")
print(f"Budget Indicators: {company['budget_indicators']}")

# Access person influence
person = report['person_intelligence']
print(f"Person Confidence: {person['confidence_score']}/100")
print(f"Sources Found: {person['sources_found']}/14")
"""
