"""
Centralized AI Client for all AI features.

All AI services should import from here to use the Anthropic API.
This ensures a single source of truth for the API key and client configuration.

Usage:
    from app.core.ai_client import get_ai_client, ai_generate

    # Get the client directly
    client = get_ai_client()
    response = client.messages.create(...)

    # Or use the helper function
    response = ai_generate("Your prompt here", max_tokens=500)
"""

import logging
import os
from functools import lru_cache
from typing import Optional, List, Dict, Any

try:
    import anthropic
except ImportError:
    anthropic = None

from app.core.config import settings

logger = logging.getLogger(__name__)

# API key from environment variable
_ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")


@lru_cache()
def get_ai_client() -> anthropic.Anthropic:
    """
    Get the centralized Anthropic AI client.

    This function is cached, so it returns the same client instance
    across all calls, reducing overhead.

    Returns:
        anthropic.Anthropic: Configured Anthropic client
    """
    logger.info("[AI-CLIENT] Initializing Anthropic client")
    return anthropic.Anthropic(api_key=_ANTHROPIC_API_KEY)


def ai_generate(
    prompt: str,
    max_tokens: int = 1024,
    model: str = "claude-sonnet-4-20250514",
    system: Optional[str] = None,
    temperature: float = 0.7,
) -> str:
    """
    Generate AI response using Claude.

    Args:
        prompt: The user prompt
        max_tokens: Maximum tokens in response (default 1024)
        model: Claude model to use (default claude-sonnet-4-20250514)
        system: Optional system prompt
        temperature: Creativity level 0-1 (default 0.7)

    Returns:
        str: Generated text response
    """
    try:
        client = get_ai_client()

        messages = [{"role": "user", "content": prompt}]

        kwargs = {
            "model": model,
            "max_tokens": max_tokens,
            "messages": messages,
        }

        if system:
            kwargs["system"] = system

        # Note: temperature is not directly supported in newer API
        # Using top_p for similar effect if needed

        response = client.messages.create(**kwargs)

        return response.content[0].text

    except Exception as e:
        logger.error(f"[AI-CLIENT] Generation error: {e}")
        raise


def ai_generate_with_history(
    messages: List[Dict[str, str]],
    max_tokens: int = 1024,
    model: str = "claude-sonnet-4-20250514",
    system: Optional[str] = None,
) -> str:
    """
    Generate AI response with conversation history.

    Args:
        messages: List of {"role": "user"|"assistant", "content": "..."}
        max_tokens: Maximum tokens in response
        model: Claude model to use
        system: Optional system prompt

    Returns:
        str: Generated text response
    """
    try:
        client = get_ai_client()

        kwargs = {
            "model": model,
            "max_tokens": max_tokens,
            "messages": messages,
        }

        if system:
            kwargs["system"] = system

        response = client.messages.create(**kwargs)

        return response.content[0].text

    except Exception as e:
        logger.error(f"[AI-CLIENT] Generation error: {e}")
        raise


def ai_analyze_email(
    email_content: str,
    analysis_type: str = "sentiment",
) -> Dict[str, Any]:
    """
    Analyze email content using AI.

    Args:
        email_content: The email text to analyze
        analysis_type: Type of analysis (sentiment, intent, urgency)

    Returns:
        dict: Analysis results
    """
    prompts = {
        "sentiment": f"""Analyze the sentiment of this email. Return JSON with:
- sentiment: "positive", "negative", or "neutral"
- confidence: 0-100
- key_phrases: list of important phrases

Email:
{email_content}

Return only valid JSON.""",

        "intent": f"""Analyze the intent of this email. Return JSON with:
- primary_intent: main purpose (inquiry, complaint, request, etc.)
- urgency: "low", "medium", "high"
- action_required: boolean
- suggested_response_type: brief description

Email:
{email_content}

Return only valid JSON.""",

        "urgency": f"""Analyze the urgency of this email. Return JSON with:
- urgency_level: 1-10
- deadline_mentioned: boolean
- requires_immediate_action: boolean
- reasoning: brief explanation

Email:
{email_content}

Return only valid JSON.""",
    }

    prompt = prompts.get(analysis_type, prompts["sentiment"])

    try:
        response = ai_generate(prompt, max_tokens=500, model="claude-3-haiku-20240307")

        # Parse JSON response
        import json
        # Clean response if needed
        response = response.strip()
        if response.startswith("```json"):
            response = response[7:]
        if response.startswith("```"):
            response = response[3:]
        if response.endswith("```"):
            response = response[:-3]

        return json.loads(response.strip())

    except Exception as e:
        logger.error(f"[AI-CLIENT] Email analysis error: {e}")
        return {"error": str(e)}


# Export commonly used items
__all__ = [
    "get_ai_client",
    "ai_generate",
    "ai_generate_with_history",
    "ai_analyze_email",
    "_ANTHROPIC_API_KEY",
]
