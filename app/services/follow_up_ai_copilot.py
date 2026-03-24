from __future__ import annotations

"""
Follow-Up AI Copilot Service - ULTRA Follow-Up System V2.0

AI-powered sequence generation and content optimization using GPT-4.

Features:
- Natural language sequence generation
- Personalized email content creation
- Spintax generation for deliverability
- Sequence improvement suggestions
- A/B variant generation

Author: Metaminds AI
Version: 2.0.0
"""

import logging
import json
import re
import random
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum

try:
    import anthropic
except ImportError:
    anthropic = None

from app.core.config import settings
from app.core.ai_client import _ANTHROPIC_API_KEY
from app.models.follow_up import (
    FollowUpTone,
    FollowUpStrategy,
    FOLLOW_UP_TEMPLATES,
    DEFAULT_SEQUENCE_PRESETS,
)

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

# Claude Model Configuration (switched from GPT-4 to Claude)
DEFAULT_MODEL = "claude-sonnet-4-20250514"  # Latest Claude Sonnet
FALLBACK_MODEL = "claude-3-haiku-20240307"  # Cheaper fallback

# Token limits
MAX_TOKENS_SEQUENCE = 2000
MAX_TOKENS_EMAIL = 1000
MAX_TOKENS_SUGGESTIONS = 1500

# Temperature settings
TEMPERATURE_SEQUENCE = 0.7  # Creative for sequence structure
TEMPERATURE_EMAIL = 0.6  # Balanced for email content
TEMPERATURE_SUGGESTIONS = 0.5  # More focused for suggestions

# Pricing (per 1M tokens) for cost tracking - Claude models
PRICING = {
    "claude-sonnet-4-20250514": {"input": 3.0, "output": 15.0},
    "claude-3-5-sonnet-20241022": {"input": 3.0, "output": 15.0},
    "claude-3-haiku-20240307": {"input": 0.25, "output": 1.25},
}


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class GeneratedEmail:
    """AI-generated email content"""
    subject: str
    body: str
    body_html: str
    tone: str
    strategy: str
    spintax_subject: Optional[str] = None
    spintax_body: Optional[str] = None
    predicted_open_rate: Optional[float] = None
    tokens_used: int = 0
    model_used: str = ""


@dataclass
class GeneratedStep:
    """AI-generated sequence step"""
    step_number: int
    delay_days: int
    delay_hours: int = 0
    strategy: str = "soft_bump"
    tone: str = "professional"
    subject_template: str = ""
    body_template: str = ""
    include_original_context: bool = True
    include_call_to_action: bool = True


@dataclass
class GeneratedSequence:
    """AI-generated complete sequence"""
    name: str
    description: str
    steps: List[GeneratedStep]
    stop_on_reply: bool = True
    respect_business_hours: bool = True
    total_duration_days: int = 0
    tokens_used: int = 0
    model_used: str = ""


@dataclass
class SequenceSuggestion:
    """AI-generated improvement suggestion"""
    suggestion_type: str  # timing, content, strategy, tone
    priority: str  # high, medium, low
    title: str
    description: str
    current_value: Optional[str] = None
    suggested_value: Optional[str] = None
    expected_impact: Optional[str] = None


@dataclass
class CopilotContext:
    """Context for AI generation"""
    candidate_name: Optional[str] = None
    candidate_title: Optional[str] = None
    candidate_skills: List[str] = field(default_factory=list)
    candidate_experience_years: Optional[int] = None
    target_company: Optional[str] = None
    target_role: Optional[str] = None
    target_industry: Optional[str] = None
    original_email_subject: Optional[str] = None
    original_email_preview: Optional[str] = None
    previous_interactions: List[str] = field(default_factory=list)


# ============================================================================
# PROMPTS
# ============================================================================

SEQUENCE_GENERATION_PROMPT = """You are an expert cold email copywriter and sales strategist. Generate a follow-up email sequence based on the user's requirements.

USER REQUEST:
{user_request}

CONTEXT:
- Candidate: {candidate_name} ({candidate_title})
- Target Company: {target_company}
- Target Role: {target_role}
- Industry: {target_industry}
- Skills: {skills}

REQUIREMENTS:
1. Create {num_steps} follow-up emails
2. Tone: {preferred_tone}
3. Goal: {goal}
4. Each email should have a clear purpose and call-to-action
5. Use appropriate delays between emails (typically 2-5 days)
6. End with a "breakup" email to close the loop professionally

OUTPUT FORMAT (JSON):
{{
    "name": "Sequence name",
    "description": "Brief description of the sequence",
    "steps": [
        {{
            "step_number": 1,
            "delay_days": 2,
            "strategy": "soft_bump|add_value|social_proof|question|breakup",
            "tone": "professional|friendly|persistent|value_add|breakup",
            "subject_template": "Subject line with {{name}} {{company}} placeholders",
            "body_template": "Email body with placeholders..."
        }}
    ]
}}

Generate a high-converting sequence:"""

EMAIL_GENERATION_PROMPT = """You are an expert cold email copywriter. Generate a single follow-up email based on the context.

CONTEXT:
- Recipient: {recipient_name} at {company}
- Role: {role}
- Original Email Subject: {original_subject}
- Days Since Last Contact: {days_since}
- Step Number: {step_number} of {total_steps}
- Strategy: {strategy}
- Tone: {tone}

PREVIOUS EMAILS IN SEQUENCE:
{previous_emails}

CANDIDATE INFO:
- Name: {candidate_name}
- Title: {candidate_title}
- Key Skills: {skills}
- Value Proposition: {value_prop}

REQUIREMENTS:
1. Keep it concise (under 150 words)
2. Reference the original email naturally
3. Provide value, not just "checking in"
4. Clear call-to-action
5. Sound human, not templated
6. Strategy-specific approach:
   - soft_bump: Gentle reminder, show continued interest
   - add_value: Share relevant insight, article, or project
   - social_proof: Mention achievement or testimonial
   - question: Ask engaging question about their work
   - breakup: Final professional close, leave door open

OUTPUT FORMAT (JSON):
{{
    "subject": "Email subject line",
    "body": "Plain text email body",
    "body_html": "HTML formatted email body"
}}

Generate the email:"""

SUGGESTIONS_PROMPT = """You are an email marketing optimization expert. Analyze this follow-up sequence and provide improvement suggestions.

SEQUENCE:
Name: {sequence_name}
Description: {sequence_description}

STEPS:
{steps_detail}

PERFORMANCE DATA (if available):
- Open Rate: {open_rate}%
- Reply Rate: {reply_rate}%
- Best Performing Step: {best_step}

Analyze and provide 3-5 specific, actionable suggestions to improve this sequence.

OUTPUT FORMAT (JSON):
{{
    "suggestions": [
        {{
            "suggestion_type": "timing|content|strategy|tone",
            "priority": "high|medium|low",
            "title": "Short title",
            "description": "Detailed explanation",
            "current_value": "What it is now",
            "suggested_value": "What it should be",
            "expected_impact": "Estimated improvement"
        }}
    ]
}}

Provide your analysis:"""

SPINTAX_GENERATION_PROMPT = """Convert this email content into spintax format for deliverability. Create 3-5 natural variations for key phrases while maintaining the same meaning and tone.

ORIGINAL CONTENT:
Subject: {subject}
Body: {body}

SPINTAX RULES:
1. Format: {{option1|option2|option3}}
2. Only vary phrases that can be naturally expressed multiple ways
3. Maintain professional tone across all variations
4. Keep placeholders like {{name}}, {{company}} unchanged
5. Don't over-spin - focus on greetings, transitions, and CTAs

OUTPUT FORMAT (JSON):
{{
    "spintax_subject": "Subject with {{variations|options}}",
    "spintax_body": "Body with {{natural|organic}} variations"
}}

Generate spintax:"""


# ============================================================================
# MAIN SERVICE CLASS
# ============================================================================

class FollowUpAICopilot:
    """
    AI Copilot for follow-up sequence generation and optimization.

    Uses GPT-4 to:
    - Generate complete sequences from natural language
    - Create personalized email content
    - Suggest improvements to existing sequences
    - Generate spintax for deliverability

    Usage:
        copilot = FollowUpAICopilot()

        # Generate a sequence
        sequence = await copilot.generate_sequence(
            user_request="Create a 4-email sequence for a software engineer...",
            context=CopilotContext(candidate_name="John", ...)
        )

        # Generate a single email
        email = await copilot.generate_email(
            context=CopilotContext(...),
            step_number=2,
            strategy="add_value",
            tone="friendly"
        )
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = DEFAULT_MODEL,
    ):
        """
        Initialize the AI Copilot.

        Args:
            api_key: Anthropic API key (uses centralized key if not provided)
            model: Claude model to use
        """
        self.api_key = api_key or _ANTHROPIC_API_KEY
        self.model = model
        self.client = anthropic.AsyncAnthropic(api_key=self.api_key)

        # Statistics tracking
        self.stats = {
            "total_requests": 0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "total_cost_usd": 0.0,
            "sequences_generated": 0,
            "emails_generated": 0,
            "errors": 0,
        }

        logger.info(f"[FollowUpAICopilot] Initialized with Claude model: {model}")

    # ========================================================================
    # HELPER METHODS FOR CLAUDE API
    # ========================================================================

    async def _call_claude(
        self,
        prompt: str,
        system_prompt: str,
        max_tokens: int = 1024,
    ) -> tuple[str, dict]:
        """
        Make a Claude API call and return content and usage stats.

        Args:
            prompt: User prompt
            system_prompt: System instructions
            max_tokens: Max response tokens

        Returns:
            tuple: (response_content, usage_dict)
        """
        response = await self.client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            system=system_prompt + " Always respond with valid JSON only.",
            messages=[
                {"role": "user", "content": prompt}
            ]
        )

        content = response.content[0].text
        usage = {
            "input_tokens": response.usage.input_tokens,
            "output_tokens": response.usage.output_tokens,
            "total_tokens": response.usage.input_tokens + response.usage.output_tokens,
        }

        return content, usage

    # ========================================================================
    # SEQUENCE GENERATION
    # ========================================================================

    async def generate_sequence(
        self,
        user_request: str,
        context: CopilotContext,
        num_steps: int = 4,
        preferred_tone: str = "professional",
        goal: str = "schedule a call or get a response",
    ) -> GeneratedSequence:
        """
        Generate a complete follow-up sequence from natural language.

        Args:
            user_request: Natural language description of desired sequence
            context: Candidate and target context
            num_steps: Number of follow-up emails (3-6 recommended)
            preferred_tone: Overall tone for the sequence
            goal: What the sequence should achieve

        Returns:
            GeneratedSequence with all steps and content
        """
        logger.info(f"[FollowUpAICopilot] Generating sequence: {user_request[:50]}...")

        try:
            prompt = SEQUENCE_GENERATION_PROMPT.format(
                user_request=user_request,
                candidate_name=context.candidate_name or "the candidate",
                candidate_title=context.candidate_title or "professional",
                target_company=context.target_company or "the company",
                target_role=context.target_role or "the position",
                target_industry=context.target_industry or "their industry",
                skills=", ".join(context.candidate_skills[:5]) if context.candidate_skills else "relevant skills",
                num_steps=num_steps,
                preferred_tone=preferred_tone,
                goal=goal,
            )

            content, usage = await self._call_claude(
                prompt=prompt,
                system_prompt="You are an expert email copywriter.",
                max_tokens=MAX_TOKENS_SEQUENCE,
            )

            # Parse response
            data = json.loads(content)

            # Build sequence
            steps = []
            total_days = 0

            for step_data in data.get("steps", []):
                delay = step_data.get("delay_days", 2)
                total_days += delay

                step = GeneratedStep(
                    step_number=step_data.get("step_number", len(steps) + 1),
                    delay_days=delay,
                    strategy=step_data.get("strategy", "soft_bump"),
                    tone=step_data.get("tone", preferred_tone),
                    subject_template=step_data.get("subject_template", ""),
                    body_template=step_data.get("body_template", ""),
                )
                steps.append(step)

            sequence = GeneratedSequence(
                name=data.get("name", f"AI Generated Sequence - {datetime.now().strftime('%Y%m%d')}"),
                description=data.get("description", user_request[:200]),
                steps=steps,
                total_duration_days=total_days,
                tokens_used=usage["total_tokens"],
                model_used=self.model,
            )

            # Update stats
            self._update_stats_dict(usage)
            self.stats["sequences_generated"] += 1

            logger.info(f"[FollowUpAICopilot] Generated sequence with {len(steps)} steps")
            return sequence

        except json.JSONDecodeError as e:
            logger.error(f"[FollowUpAICopilot] JSON parse error: {e}")
            self.stats["errors"] += 1
            raise ValueError(f"Failed to parse AI response: {e}")
        except Exception as e:
            logger.error(f"[FollowUpAICopilot] Generation error: {e}")
            self.stats["errors"] += 1
            raise

    # ========================================================================
    # EMAIL GENERATION
    # ========================================================================

    async def generate_email(
        self,
        context: CopilotContext,
        step_number: int,
        total_steps: int,
        strategy: str = "soft_bump",
        tone: str = "professional",
        recipient_name: str = "",
        company: str = "",
        role: str = "",
        days_since_last: int = 3,
        previous_emails: List[str] = None,
        value_proposition: str = "",
        include_spintax: bool = True,
    ) -> GeneratedEmail:
        """
        Generate a single follow-up email.

        Args:
            context: Candidate context
            step_number: Current step in sequence
            total_steps: Total steps in sequence
            strategy: Email strategy (soft_bump, add_value, etc.)
            tone: Email tone
            recipient_name: Name of recipient
            company: Company name
            role: Target role
            days_since_last: Days since last email
            previous_emails: Previous email subjects/previews
            value_proposition: What value to highlight
            include_spintax: Generate spintax variants

        Returns:
            GeneratedEmail with content and metadata
        """
        logger.info(f"[FollowUpAICopilot] Generating email step {step_number}/{total_steps}")

        try:
            # Format previous emails
            prev_emails_str = ""
            if previous_emails:
                prev_emails_str = "\n".join([f"- {e}" for e in previous_emails[-3:]])

            prompt = EMAIL_GENERATION_PROMPT.format(
                recipient_name=recipient_name or "the recipient",
                company=company or context.target_company or "the company",
                role=role or context.target_role or "the position",
                original_subject=context.original_email_subject or "Previous outreach",
                days_since=days_since_last,
                step_number=step_number,
                total_steps=total_steps,
                strategy=strategy,
                tone=tone,
                previous_emails=prev_emails_str or "None",
                candidate_name=context.candidate_name or "the candidate",
                candidate_title=context.candidate_title or "",
                skills=", ".join(context.candidate_skills[:5]) if context.candidate_skills else "",
                value_prop=value_proposition or "relevant experience and skills",
            )

            content, usage = await self._call_claude(
                prompt=prompt,
                system_prompt="You are an expert cold email copywriter.",
                max_tokens=MAX_TOKENS_EMAIL,
            )

            data = json.loads(content)

            email = GeneratedEmail(
                subject=data.get("subject", "Following up"),
                body=data.get("body", ""),
                body_html=data.get("body_html", data.get("body", "")),
                tone=tone,
                strategy=strategy,
                tokens_used=usage["total_tokens"],
                model_used=self.model,
            )

            # Generate spintax if requested
            if include_spintax and email.body:
                spintax = await self._generate_spintax(email.subject, email.body)
                email.spintax_subject = spintax.get("spintax_subject")
                email.spintax_body = spintax.get("spintax_body")

            # Predict open rate (simple heuristic for now)
            email.predicted_open_rate = self._predict_open_rate(email.subject)

            # Update stats
            self._update_stats_dict(usage)
            self.stats["emails_generated"] += 1

            logger.info(f"[FollowUpAICopilot] Generated email: {email.subject[:50]}...")
            return email

        except Exception as e:
            logger.error(f"[FollowUpAICopilot] Email generation error: {e}")
            self.stats["errors"] += 1
            raise

    # ========================================================================
    # SUGGESTIONS
    # ========================================================================

    async def suggest_improvements(
        self,
        sequence_name: str,
        sequence_description: str,
        steps: List[Dict[str, Any]],
        open_rate: Optional[float] = None,
        reply_rate: Optional[float] = None,
        best_performing_step: Optional[int] = None,
    ) -> List[SequenceSuggestion]:
        """
        Analyze a sequence and suggest improvements.

        Args:
            sequence_name: Name of the sequence
            sequence_description: Description
            steps: List of step configurations
            open_rate: Current open rate percentage
            reply_rate: Current reply rate percentage
            best_performing_step: Which step performs best

        Returns:
            List of improvement suggestions
        """
        logger.info(f"[FollowUpAICopilot] Analyzing sequence: {sequence_name}")

        try:
            # Format steps detail
            steps_detail = ""
            for i, step in enumerate(steps, 1):
                steps_detail += f"""
Step {i}:
  - Delay: {step.get('delay_days', 0)} days
  - Strategy: {step.get('strategy', 'unknown')}
  - Tone: {step.get('tone', 'unknown')}
  - Subject: {step.get('subject_template', 'N/A')[:50]}...
"""

            prompt = SUGGESTIONS_PROMPT.format(
                sequence_name=sequence_name,
                sequence_description=sequence_description,
                steps_detail=steps_detail,
                open_rate=open_rate or "N/A",
                reply_rate=reply_rate or "N/A",
                best_step=best_performing_step or "N/A",
            )

            content, usage = await self._call_claude(
                prompt=prompt,
                system_prompt="You are an email optimization expert.",
                max_tokens=MAX_TOKENS_SUGGESTIONS,
            )

            data = json.loads(content)

            suggestions = []
            for s in data.get("suggestions", []):
                suggestions.append(SequenceSuggestion(
                    suggestion_type=s.get("suggestion_type", "content"),
                    priority=s.get("priority", "medium"),
                    title=s.get("title", "Suggestion"),
                    description=s.get("description", ""),
                    current_value=s.get("current_value"),
                    suggested_value=s.get("suggested_value"),
                    expected_impact=s.get("expected_impact"),
                ))

            # Update stats
            self._update_stats_dict(usage)

            logger.info(f"[FollowUpAICopilot] Generated {len(suggestions)} suggestions")
            return suggestions

        except Exception as e:
            logger.error(f"[FollowUpAICopilot] Suggestions error: {e}")
            self.stats["errors"] += 1
            raise

    # ========================================================================
    # SPINTAX GENERATION
    # ========================================================================

    async def _generate_spintax(
        self,
        subject: str,
        body: str,
    ) -> Dict[str, str]:
        """Generate spintax variants for email content."""
        try:
            prompt = SPINTAX_GENERATION_PROMPT.format(
                subject=subject,
                body=body,
            )

            content, _ = await self._call_claude(
                prompt=prompt,
                system_prompt="Generate spintax.",
                max_tokens=MAX_TOKENS_EMAIL,
            )

            return json.loads(content)

        except Exception as e:
            logger.warning(f"[FollowUpAICopilot] Spintax generation failed: {e}")
            return {"spintax_subject": subject, "spintax_body": body}

    # ========================================================================
    # A/B VARIANT GENERATION
    # ========================================================================

    async def generate_ab_variants(
        self,
        original_subject: str,
        original_body: str,
        num_variants: int = 3,
    ) -> List[Dict[str, str]]:
        """
        Generate A/B test variants for an email.

        Args:
            original_subject: Original subject line
            original_body: Original body content
            num_variants: Number of variants to generate

        Returns:
            List of variant dicts with subject and body
        """
        logger.info(f"[FollowUpAICopilot] Generating {num_variants} A/B variants")

        try:
            prompt = f"""Create {num_variants} A/B test variants for this email. Each variant should test a different approach while maintaining the core message.

ORIGINAL:
Subject: {original_subject}
Body: {original_body}

Generate {num_variants} distinct variants. Vary:
- Subject line approach (question, statement, personalized, curiosity)
- Opening hook
- Call-to-action style

OUTPUT FORMAT (JSON):
{{
    "variants": [
        {{"subject": "...", "body": "...", "hypothesis": "What this tests"}}
    ]
}}

Generate variants:"""

            content, _ = await self._call_claude(
                prompt=prompt,
                system_prompt="Generate email variants.",
                max_tokens=MAX_TOKENS_SEQUENCE,
            )

            data = json.loads(content)

            return data.get("variants", [])

        except Exception as e:
            logger.error(f"[FollowUpAICopilot] A/B variant generation failed: {e}")
            return []

    # ========================================================================
    # UTILITY METHODS
    # ========================================================================

    def _predict_open_rate(self, subject: str) -> float:
        """
        Simple heuristic to predict open rate based on subject line characteristics.
        (To be replaced with ML model in future)
        """
        score = 50.0  # Base score

        # Positive factors
        if len(subject) < 50:
            score += 5  # Short subjects perform better
        if "?" in subject:
            score += 3  # Questions engage
        if any(word in subject.lower() for word in ["quick", "follow", "re:"]):
            score += 5  # Follow-up indicators
        if "{" in subject:
            score += 5  # Personalization

        # Negative factors
        if subject.isupper():
            score -= 10  # ALL CAPS is spammy
        if "!!!" in subject or "???" in subject:
            score -= 5
        if len(subject) > 70:
            score -= 5  # Too long

        return min(max(score, 10), 90)  # Clamp between 10-90

    def _update_stats(self, usage) -> None:
        """Update usage statistics (legacy OpenAI format)."""
        self.stats["total_requests"] += 1
        self.stats["total_input_tokens"] += getattr(usage, 'prompt_tokens', 0)
        self.stats["total_output_tokens"] += getattr(usage, 'completion_tokens', 0)

        # Calculate cost
        pricing = PRICING.get(self.model, PRICING["claude-3-haiku-20240307"])
        input_cost = (getattr(usage, 'prompt_tokens', 0) / 1_000_000) * pricing["input"]
        output_cost = (getattr(usage, 'completion_tokens', 0) / 1_000_000) * pricing["output"]
        self.stats["total_cost_usd"] += input_cost + output_cost

    def _update_stats_dict(self, usage: dict) -> None:
        """Update usage statistics from dict (Claude format)."""
        self.stats["total_requests"] += 1
        self.stats["total_input_tokens"] += usage.get("input_tokens", 0)
        self.stats["total_output_tokens"] += usage.get("output_tokens", 0)

        # Calculate cost
        pricing = PRICING.get(self.model, PRICING["claude-3-haiku-20240307"])
        input_cost = (usage.get("input_tokens", 0) / 1_000_000) * pricing["input"]
        output_cost = (usage.get("output_tokens", 0) / 1_000_000) * pricing["output"]
        self.stats["total_cost_usd"] += input_cost + output_cost

    def get_stats(self) -> Dict[str, Any]:
        """Get usage statistics."""
        return {
            **self.stats,
            "model": self.model,
        }

    def render_spintax(self, text: str) -> str:
        """
        Render spintax by randomly selecting options.

        Example: "{Hello|Hi|Hey} {there|}" -> "Hi there"
        """
        pattern = r'\{([^{}]+)\}'

        def replace_match(match):
            options = match.group(1).split('|')
            return random.choice(options)

        return re.sub(pattern, replace_match, text)


# ============================================================================
# FACTORY FUNCTION
# ============================================================================

def get_follow_up_ai_copilot(
    api_key: Optional[str] = None,
    model: str = DEFAULT_MODEL,
) -> FollowUpAICopilot:
    """
    Factory function to create FollowUpAICopilot instance.

    Args:
        api_key: OpenAI API key (uses settings if not provided)
        model: GPT model to use

    Returns:
        Configured FollowUpAICopilot instance
    """
    return FollowUpAICopilot(api_key=api_key, model=model)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "FollowUpAICopilot",
    "get_follow_up_ai_copilot",
    "GeneratedEmail",
    "GeneratedStep",
    "GeneratedSequence",
    "SequenceSuggestion",
    "CopilotContext",
]
