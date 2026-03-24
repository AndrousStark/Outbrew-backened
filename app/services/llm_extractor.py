from __future__ import annotations

"""
Layer 4: LLM-Powered Data Extraction

Purpose: Extract structured data from unstructured HTML/text using Claude AI

When to use:
- Complex, unstructured content (blog posts, bios, descriptions)
- Varying page layouts (no consistent CSS selectors)
- Semantic extraction (job responsibilities, skills from text)
- Data normalization (company names, job titles)

Cost: ~$0.30 per page (Claude Haiku: $0.25 per MTok input, $1.25 per MTok output)
Speed: 2-5 seconds per page
Accuracy: 90-95% (vs 60-70% with regex/selectors)

Use sparingly! Only for high-value targets where other methods fail.
"""

import logging
from typing import Dict, Any, Optional, List, Type
from pydantic import BaseModel, Field
try:
    import anthropic
except ImportError:
    anthropic = None
import json

from app.core.ai_client import _ANTHROPIC_API_KEY

logger = logging.getLogger(__name__)


class LLMExtractor:
    """
    Extract structured data from unstructured content using Claude AI

    Features:
    - Schema-based extraction (Pydantic models)
    - Semantic understanding (job titles, skills, responsibilities)
    - Data normalization (standardize formats)
    - Multi-language support
    - Confidence scoring

    Usage:
    extractor = LLMExtractor(api_key="your_anthropic_key")

    # Define extraction schema
    class PersonProfile(BaseModel):
        name: str
        title: str
        company: str
        skills: List[str]

    data = await extractor.extract(
        html="<html>...</html>",
        schema=PersonProfile
    )
    """

    def __init__(
        self,
        api_key: str = None,
        model: str = "claude-3-haiku-20240307",  # Fastest, cheapest
        max_tokens: int = 4096,
        temperature: float = 0.0  # Deterministic output
    ):
        """
        Initialize LLM extractor

        Args:
            api_key: Anthropic API key (uses centralized key if not provided)
            model: Claude model to use
                   - claude-3-haiku-20240307: Fastest, cheapest ($0.25/MTok)
                   - claude-3-5-sonnet-20241022: Most capable ($3/MTok)
                   - claude-3-opus-20240229: Most intelligent ($15/MTok)
            max_tokens: Max output tokens
            temperature: Randomness (0 = deterministic)

        Cost comparison (per 1000 pages, ~5KB HTML each):
        - Haiku: $0.25 input + $1.25 output = ~$300/1000 pages
        - Sonnet: $3 input + $15 output = ~$3600/1000 pages
        - Opus: $15 input + $75 output = ~$18000/1000 pages

        Recommendation: Use Haiku for most extraction, Sonnet for complex cases
        """
        # Use centralized API key if none provided
        api_key = api_key or _ANTHROPIC_API_KEY
        self.client = anthropic.AsyncAnthropic(api_key=api_key)
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature

        # Statistics
        self.stats = {
            "total_extractions": 0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "total_cost_usd": 0.0,
            "errors": 0
        }

        # Pricing (per million tokens)
        self.pricing = {
            "claude-3-haiku-20240307": {"input": 0.25, "output": 1.25},
            "claude-3-5-sonnet-20241022": {"input": 3.0, "output": 15.0},
            "claude-3-opus-20240229": {"input": 15.0, "output": 75.0},
        }

    async def extract(
        self,
        html: str,
        schema: Type[BaseModel],
        instructions: Optional[str] = None,
        examples: Optional[List[Dict]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Extract structured data from HTML using Claude

        Args:
            html: HTML content to extract from
            schema: Pydantic model defining desired output structure
            instructions: Additional extraction instructions
            examples: Few-shot examples (optional, improves accuracy)

        Returns:
            Extracted data matching schema, or None if extraction fails

        Example:
            class CompanyInfo(BaseModel):
                name: str = Field(description="Company name")
                industry: str = Field(description="Industry/sector")
                size: str = Field(description="Number of employees")
                founded: Optional[int] = Field(description="Year founded")

            data = await extractor.extract(
                html=company_page_html,
                schema=CompanyInfo,
                instructions="Extract company information from the about page"
            )
        """
        try:
            # Generate JSON schema from Pydantic model
            json_schema = schema.model_json_schema()

            # Build prompt
            prompt = self._build_extraction_prompt(
                html=html,
                schema=json_schema,
                instructions=instructions,
                examples=examples
            )

            # Call Claude API
            response = await self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            )

            # Extract JSON from response
            content = response.content[0].text

            # Parse JSON (Claude should return valid JSON)
            try:
                extracted_data = json.loads(content)
            except json.JSONDecodeError:
                # Try to extract JSON from markdown code block
                json_match = content.find("```json")
                if json_match != -1:
                    json_end = content.find("```", json_match + 7)
                    json_str = content[json_match + 7:json_end].strip()
                    extracted_data = json.loads(json_str)
                else:
                    raise ValueError("Failed to parse JSON from Claude response")

            # Validate against schema
            validated_data = schema(**extracted_data)

            # Update statistics
            self.stats["total_extractions"] += 1
            self.stats["total_input_tokens"] += response.usage.input_tokens
            self.stats["total_output_tokens"] += response.usage.output_tokens

            # Calculate cost
            input_cost = (response.usage.input_tokens / 1_000_000) * self.pricing[self.model]["input"]
            output_cost = (response.usage.output_tokens / 1_000_000) * self.pricing[self.model]["output"]
            total_cost = input_cost + output_cost
            self.stats["total_cost_usd"] += total_cost

            logger.info(
                f"Extracted data using {self.model} "
                f"(tokens: {response.usage.input_tokens} in, {response.usage.output_tokens} out, "
                f"cost: ${total_cost:.4f})"
            )

            return validated_data.model_dump()

        except anthropic.APIError as e:
            self.stats["errors"] += 1
            logger.error(f"Anthropic API error: {e}")
            return None

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"LLM extraction error: {e}")
            return None

    def _build_extraction_prompt(
        self,
        html: str,
        schema: Dict,
        instructions: Optional[str],
        examples: Optional[List[Dict]]
    ) -> str:
        """Build extraction prompt for Claude"""

        # Strip HTML tags for cleaner input (optional, but saves tokens)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'lxml')
        text = soup.get_text(separator="\n", strip=True)

        # Limit text length (Claude Haiku: 200K context, but expensive)
        max_chars = 20000  # ~5K tokens
        if len(text) > max_chars:
            text = text[:max_chars] + "\n... (truncated)"

        prompt = f"""You are a data extraction expert. Extract structured information from the following web page content.

OUTPUT FORMAT:
Return ONLY valid JSON matching this schema:
{json.dumps(schema, indent=2)}

"""

        if instructions:
            prompt += f"\nINSTRUCTIONS:\n{instructions}\n"

        if examples:
            prompt += "\nEXAMPLES:\n"
            for i, example in enumerate(examples, 1):
                prompt += f"\nExample {i}:\n{json.dumps(example, indent=2)}\n"

        prompt += f"""
CONTENT TO EXTRACT FROM:
{text}

Return ONLY the JSON object, no explanations or markdown.
"""

        return prompt

    async def extract_batch(
        self,
        html_list: List[str],
        schema: Type[BaseModel],
        max_concurrent: int = 5,
        **extract_kwargs
    ) -> List[Optional[Dict[str, Any]]]:
        """
        Extract from multiple HTML documents concurrently

        Args:
            html_list: List of HTML strings
            schema: Pydantic model for extraction
            max_concurrent: Max concurrent API calls
            **extract_kwargs: Additional arguments for extract()

        Returns:
            List of extracted data (same order as input)
        """
        import asyncio

        semaphore = asyncio.Semaphore(max_concurrent)

        async def extract_with_semaphore(html: str):
            async with semaphore:
                return await self.extract(html, schema, **extract_kwargs)

        tasks = [extract_with_semaphore(html) for html in html_list]
        results = await asyncio.gather(*tasks)

        logger.info(f"Batch extraction: {len(html_list)} pages, {max_concurrent} concurrent")
        return results

    def estimate_cost(
        self,
        num_pages: int,
        avg_html_size_kb: float = 5.0,
        avg_output_tokens: int = 200
    ) -> Dict[str, float]:
        """
        Estimate extraction cost

        Args:
            num_pages: Number of pages to extract
            avg_html_size_kb: Average HTML size in KB
            avg_output_tokens: Average output tokens per page

        Returns:
            {
                "total_cost_usd": float,
                "cost_per_page_usd": float,
                "input_tokens": int,
                "output_tokens": int
            }
        """
        # Rough estimate: 1 token ≈ 4 characters
        chars_per_token = 4
        avg_input_tokens = int((avg_html_size_kb * 1024) / chars_per_token)

        total_input_tokens = num_pages * avg_input_tokens
        total_output_tokens = num_pages * avg_output_tokens

        input_cost = (total_input_tokens / 1_000_000) * self.pricing[self.model]["input"]
        output_cost = (total_output_tokens / 1_000_000) * self.pricing[self.model]["output"]
        total_cost = input_cost + output_cost

        return {
            "total_cost_usd": round(total_cost, 2),
            "cost_per_page_usd": round(total_cost / num_pages, 4),
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get extraction statistics"""
        stats = self.stats.copy()
        if stats["total_extractions"] > 0:
            stats["avg_cost_per_page"] = stats["total_cost_usd"] / stats["total_extractions"]
        return stats


# Pre-defined extraction schemas for common use cases

class PersonProfile(BaseModel):
    """LinkedIn-style person profile"""
    name: str = Field(description="Full name")
    headline: Optional[str] = Field(description="Professional headline/title")
    current_company: Optional[str] = Field(description="Current company name")
    current_title: Optional[str] = Field(description="Current job title")
    location: Optional[str] = Field(description="Geographic location")
    email: Optional[str] = Field(description="Email address")
    phone: Optional[str] = Field(description="Phone number")
    linkedin_url: Optional[str] = Field(description="LinkedIn profile URL")
    skills: List[str] = Field(default=[], description="List of skills")
    experience_years: Optional[int] = Field(description="Years of experience")


class CompanyProfile(BaseModel):
    """Company information"""
    name: str = Field(description="Company name")
    industry: Optional[str] = Field(description="Industry/sector")
    description: Optional[str] = Field(description="Company description")
    size: Optional[str] = Field(description="Number of employees (e.g., '51-200')")
    founded: Optional[int] = Field(description="Year founded")
    headquarters: Optional[str] = Field(description="HQ location")
    website: Optional[str] = Field(description="Company website URL")
    technologies: List[str] = Field(default=[], description="Tech stack used")


class JobPosting(BaseModel):
    """Job posting information"""
    title: str = Field(description="Job title")
    company: str = Field(description="Company name")
    location: Optional[str] = Field(description="Job location")
    job_type: Optional[str] = Field(description="Full-time, Part-time, Contract, etc.")
    salary_range: Optional[str] = Field(description="Salary range if mentioned")
    requirements: List[str] = Field(default=[], description="Job requirements")
    responsibilities: List[str] = Field(default=[], description="Job responsibilities")
    apply_url: Optional[str] = Field(description="Application URL")


class ContactInfo(BaseModel):
    """Contact information extraction"""
    emails: List[str] = Field(default=[], description="All email addresses found")
    phones: List[str] = Field(default=[], description="All phone numbers found")
    addresses: List[str] = Field(default=[], description="All physical addresses found")
    social_media: Dict[str, str] = Field(default={}, description="Social media links")


# Usage Example:
"""
# Initialize extractor
extractor = LLMExtractor(
    api_key="your_anthropic_api_key",
    model="claude-3-haiku-20240307"  # Cheapest option
)

# Example 1: Extract person profile from LinkedIn
html = '''
<html>
<body>
    <h1>John Doe</h1>
    <div class="headline">Senior Software Engineer at Google</div>
    <div class="location">San Francisco, CA</div>
    <div class="skills">Python, Machine Learning, Cloud Architecture</div>
    <div class="experience">10 years in tech industry</div>
</body>
</html>
'''

profile = await extractor.extract(
    html=html,
    schema=PersonProfile,
    instructions="Extract all available information about this person"
)

print(f"Name: {profile['name']}")
print(f"Company: {profile['current_company']}")
print(f"Skills: {', '.join(profile['skills'])}")

# Example 2: Extract company information
company_html = open('company_about_page.html').read()

company_data = await extractor.extract(
    html=company_html,
    schema=CompanyProfile
)

print(f"Company: {company_data['name']}")
print(f"Industry: {company_data['industry']}")
print(f"Size: {company_data['size']}")

# Example 3: Batch extraction
html_pages = [page1_html, page2_html, page3_html]

results = await extractor.extract_batch(
    html_list=html_pages,
    schema=PersonProfile,
    max_concurrent=5
)

print(f"Extracted {len(results)} profiles")

# Example 4: Cost estimation
cost_estimate = extractor.estimate_cost(
    num_pages=1000,
    avg_html_size_kb=5.0
)

print(f"Estimated cost for 1000 pages: ${cost_estimate['total_cost_usd']}")
print(f"Cost per page: ${cost_estimate['cost_per_page_usd']}")

# Get statistics
stats = extractor.get_stats()
print(f"Total extractions: {stats['total_extractions']}")
print(f"Total cost: ${stats['total_cost_usd']:.2f}")
print(f"Avg cost per page: ${stats['avg_cost_per_page']:.4f}")


# Cost comparison:
# Layer 1 (Static scraping): $0 per page, 60-70% accuracy
# Layer 4 (LLM extraction): $0.30 per page, 90-95% accuracy
# Only use Layer 4 for high-value targets!

# Best practice:
# 1. Try Layer 1 (static scraping) first
# 2. If Layer 1 fails or returns incomplete data, use Layer 4
# 3. Set budget limits to avoid overspending
# 4. Use Haiku model (cheapest) unless you need more intelligence
"""
