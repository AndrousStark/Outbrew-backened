"""
Advanced Email Template Engine

Features:
- Jinja2-based template rendering
- Custom filters for formatting
- Variable validation and type checking
- Fallback values for missing variables
- HTML sanitization with bleach
- Preview generation with sample data
- Thread-safe singleton pattern
"""

import re
import html
import bleach
import threading
from datetime import date, datetime
from typing import Dict, Any, List, Optional, Set
from jinja2 import Environment, BaseLoader, UndefinedError, TemplateSyntaxError, Undefined
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)

# Security limits to prevent DoS
MAX_TEMPLATE_LENGTH = 100000  # 100KB max template size
MAX_CONTEXT_ITEMS = 500  # Max number of context variables

# Allowed HTML tags and attributes for sanitization
ALLOWED_TAGS = [
    'p', 'br', 'strong', 'em', 'b', 'i', 'u', 'a', 'ul', 'ol', 'li',
    'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'span', 'div', 'table', 'tr',
    'td', 'th', 'thead', 'tbody', 'blockquote', 'pre', 'code', 'hr'
]
ALLOWED_ATTRIBUTES = {
    '*': ['style', 'class'],
    'a': ['href', 'title', 'target'],
    'img': ['src', 'alt', 'width', 'height'],
    'table': ['border', 'cellpadding', 'cellspacing', 'width'],
    'td': ['colspan', 'rowspan', 'width', 'align', 'valign'],
    'th': ['colspan', 'rowspan', 'width', 'align', 'valign'],
}


class SilentUndefined(Undefined):
    """Custom Undefined that returns empty string instead of raising error."""

    def _fail_with_undefined_error(self, *args, **kwargs):
        return ""

    def __str__(self):
        return ""

    def __iter__(self):
        return iter([])

    def __bool__(self):
        return False


class TemplateVariable(BaseModel):
    """Schema for template variable metadata."""
    name: str
    description: str
    example: str
    required: bool = False
    category: str = "general"


class TemplateEngine:
    """
    High-performance Email Template Engine

    Supports:
    - {{ variable }} - Direct variable substitution
    - {{ variable | filter }} - Filtered variables
    - {% if condition %} - Conditionals
    - {% for item in list %} - Loops
    """

    # Available template variables with metadata
    AVAILABLE_VARIABLES: List[TemplateVariable] = [
        # Recruiter variables
        TemplateVariable(
            name="recruiter_name",
            description="Name of the recruiter",
            example="John Smith",
            required=True,
            category="recruiter"
        ),
        TemplateVariable(
            name="recruiter_title",
            description="Job title of the recruiter",
            example="Senior HR Manager",
            category="recruiter"
        ),
        TemplateVariable(
            name="recruiter_email",
            description="Email address of the recruiter",
            example="john.smith@company.com",
            category="recruiter"
        ),

        # Company variables
        TemplateVariable(
            name="company_name",
            description="Name of the company",
            example="Tech Corp Inc.",
            required=True,
            category="company"
        ),
        TemplateVariable(
            name="company_industry",
            description="Industry of the company",
            example="Technology",
            category="company"
        ),
        TemplateVariable(
            name="company_website",
            description="Company website URL",
            example="https://techcorp.com",
            category="company"
        ),

        # Position variables
        TemplateVariable(
            name="position_title",
            description="Title of the position",
            example="Senior Software Engineer",
            category="position"
        ),
        TemplateVariable(
            name="position_level",
            description="Level of the position",
            example="Senior",
            category="position"
        ),
        TemplateVariable(
            name="position_country",
            description="Country where the position is located",
            example="United States",
            category="position"
        ),
        TemplateVariable(
            name="position_city",
            description="City where the position is located",
            example="San Francisco",
            category="position"
        ),

        # Candidate variables
        TemplateVariable(
            name="candidate_name",
            description="Full name of the candidate",
            example="Pragya Pandey",
            required=True,
            category="candidate"
        ),
        TemplateVariable(
            name="candidate_first_name",
            description="First name of the candidate",
            example="Pragya",
            category="candidate"
        ),
        TemplateVariable(
            name="candidate_email",
            description="Email of the candidate",
            example="pragya@email.com",
            category="candidate"
        ),
        TemplateVariable(
            name="candidate_title",
            description="Professional title of the candidate",
            example="Full Stack Developer",
            category="candidate"
        ),

        # Date variables
        TemplateVariable(
            name="current_date",
            description="Today's date (formatted)",
            example="January 1, 2026",
            category="date"
        ),
        TemplateVariable(
            name="current_year",
            description="Current year",
            example="2026",
            category="date"
        ),

        # Application variables
        TemplateVariable(
            name="job_posting_url",
            description="URL of the job posting",
            example="https://company.com/careers/123",
            category="application"
        ),
        TemplateVariable(
            name="application_id",
            description="Unique application ID",
            example="APP-12345",
            category="application"
        ),

        # Recipient variables (for Group Campaigns)
        TemplateVariable(
            name="recipient_name",
            description="Full name of the recipient",
            example="Jane Smith",
            category="recipient"
        ),
        TemplateVariable(
            name="recipient_first_name",
            description="First name of the recipient",
            example="Jane",
            category="recipient"
        ),
        TemplateVariable(
            name="recipient_last_name",
            description="Last name of the recipient",
            example="Smith",
            category="recipient"
        ),
        TemplateVariable(
            name="recipient_email",
            description="Email address of the recipient",
            example="jane.smith@company.com",
            category="recipient"
        ),
        TemplateVariable(
            name="recipient_company",
            description="Company name of the recipient",
            example="TechStart Inc.",
            category="recipient"
        ),
        TemplateVariable(
            name="recipient_position",
            description="Position/title of the recipient",
            example="Senior Recruiter",
            category="recipient"
        ),
        TemplateVariable(
            name="recipient_country",
            description="Country of the recipient",
            example="United States",
            category="recipient"
        ),
        TemplateVariable(
            name="recipient_language",
            description="Preferred language of the recipient",
            example="en",
            category="recipient"
        ),
    ]

    def __init__(self, strict_mode: bool = False):
        """
        Initialize the template engine.

        Args:
            strict_mode: If True, raise error on undefined variables.
                        If False, replace with empty string.
        """
        self.strict_mode = strict_mode

        # Create Jinja2 environment with custom settings
        self.env = Environment(
            loader=BaseLoader(),
            undefined=Undefined if strict_mode else SilentUndefined,
            autoescape=True,  # Auto-escape HTML for security
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Add custom filters
        self._register_filters()

    def _register_filters(self):
        """Register custom Jinja2 filters."""

        def title_case(value: str) -> str:
            """Convert to title case."""
            return str(value).title() if value else ""

        def upper(value: str) -> str:
            """Convert to uppercase."""
            return str(value).upper() if value else ""

        def lower(value: str) -> str:
            """Convert to lowercase."""
            return str(value).lower() if value else ""

        def default(value: Any, default_value: str = "") -> str:
            """Return default if value is empty."""
            return str(value) if value else default_value

        def truncate(value: str, length: int = 50, suffix: str = "...") -> str:
            """Truncate string to specified length."""
            if not value:
                return ""
            value = str(value)
            if len(value) <= length:
                return value
            return value[:length - len(suffix)] + suffix

        def date_format(value: Any, format_str: str = "%B %d, %Y") -> str:
            """Format a date."""
            if isinstance(value, str):
                try:
                    value = datetime.fromisoformat(value)
                except ValueError:
                    return str(value)
            if isinstance(value, (date, datetime)):
                return value.strftime(format_str)
            return str(value) if value else ""

        def first_name(value: str) -> str:
            """Extract first name from full name."""
            if not value:
                return ""
            return str(value).split()[0]

        def last_name(value: str) -> str:
            """Extract last name from full name."""
            if not value:
                return ""
            parts = str(value).split()
            return parts[-1] if len(parts) > 1 else ""

        def initials(value: str) -> str:
            """Get initials from name."""
            if not value:
                return ""
            return "".join(word[0].upper() for word in str(value).split() if word)

        # Register all filters
        self.env.filters["title"] = title_case
        self.env.filters["upper"] = upper
        self.env.filters["lower"] = lower
        self.env.filters["default"] = default
        self.env.filters["truncate"] = truncate
        self.env.filters["date_format"] = date_format
        self.env.filters["first_name"] = first_name
        self.env.filters["last_name"] = last_name
        self.env.filters["initials"] = initials

    def render(
        self,
        template_str: str,
        context: Dict[str, Any],
        validate: bool = True
    ) -> str:
        """
        Render a template string with the given context.

        Args:
            template_str: Jinja2 template string
            context: Dictionary of variables
            validate: Whether to validate required variables

        Returns:
            Rendered string

        Raises:
            ValueError: If validation fails or template syntax is invalid
        """
        if not template_str:
            logger.debug("[TemplateEngine] Empty template string, returning empty")
            return ""

        # Security check: Validate template length to prevent DoS
        if len(template_str) > MAX_TEMPLATE_LENGTH:
            logger.warning(f"[TemplateEngine] Template too large: {len(template_str)} chars (max: {MAX_TEMPLATE_LENGTH})")
            raise ValueError(f"Template exceeds maximum length of {MAX_TEMPLATE_LENGTH} characters")

        # Security check: Validate context size
        if len(context) > MAX_CONTEXT_ITEMS:
            logger.warning(f"[TemplateEngine] Context too large: {len(context)} items (max: {MAX_CONTEXT_ITEMS})")
            raise ValueError(f"Context exceeds maximum of {MAX_CONTEXT_ITEMS} items")

        logger.debug(f"[TemplateEngine] Rendering template ({len(template_str)} chars) with {len(context)} context vars")

        # Add default context values
        enriched_context = self._enrich_context(context)

        # Validate if required
        if validate:
            missing = self._validate_required_variables(template_str, enriched_context)
            if missing and self.strict_mode:
                logger.warning(f"[TemplateEngine] Missing required variables: {missing}")
                raise ValueError(f"Missing required variables: {', '.join(missing)}")

        try:
            template = self.env.from_string(template_str)
            result = template.render(**enriched_context)
            logger.debug(f"[TemplateEngine] Rendered successfully ({len(result)} chars output)")
            return result

        except TemplateSyntaxError as e:
            logger.error(f"[TemplateEngine] Template syntax error: {e}")
            raise ValueError(f"Invalid template syntax: {e}")

        except UndefinedError as e:
            logger.error(f"[TemplateEngine] Undefined variable: {e}")
            raise ValueError(f"Missing variable: {e}")

        except Exception as e:
            logger.error(f"[TemplateEngine] Unexpected error during rendering: {e}")
            raise ValueError(f"Template rendering failed: {e}")

    def _enrich_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Add default/computed values to context."""
        enriched = dict(context)

        # Add date variables if not present
        today = date.today()
        enriched.setdefault("current_date", today.strftime("%B %d, %Y"))
        enriched.setdefault("current_year", str(today.year))

        # Extract first name if full name is provided
        if "candidate_name" in enriched and "candidate_first_name" not in enriched:
            full_name = enriched["candidate_name"]
            if full_name:
                enriched["candidate_first_name"] = str(full_name).split()[0]

        # Set default fallbacks
        enriched.setdefault("recruiter_name", "Hiring Manager")
        enriched.setdefault("position_title", "Open Position")

        return enriched

    def _validate_required_variables(
        self,
        template_str: str,
        context: Dict[str, Any]
    ) -> List[str]:
        """Check for missing required variables."""
        # Extract variable names from template
        used_vars = self.extract_variables(template_str)

        # Get required variable names
        required_vars = {v.name for v in self.AVAILABLE_VARIABLES if v.required}

        # Find missing required variables that are used in template
        missing = []
        for var in used_vars:
            if var in required_vars and not context.get(var):
                missing.append(var)

        return missing

    def extract_variables(self, template_str: str) -> Set[str]:
        """Extract all variable names used in a template."""
        if not template_str:
            return set()

        # Match {{ variable }} and {{ variable | filter }}
        pattern = r'\{\{\s*(\w+)(?:\s*\|[^}]*)?\s*\}\}'
        matches = re.findall(pattern, template_str)

        # Also match {% if variable %} and similar
        control_pattern = r'\{%\s*(?:if|elif|for)\s+(\w+)'
        control_matches = re.findall(control_pattern, template_str)

        return set(matches + control_matches)

    def preview(
        self,
        template_str: str,
        custom_context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Generate a preview with sample data.

        Args:
            template_str: Template to preview
            custom_context: Optional custom values to override samples

        Returns:
            Rendered preview string
        """
        # Build sample context from variable metadata
        sample_context = {
            var.name: var.example
            for var in self.AVAILABLE_VARIABLES
        }

        # Override with custom values if provided
        if custom_context:
            sample_context.update(custom_context)

        return self.render(template_str, sample_context, validate=False)

    def get_available_variables(
        self,
        category: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get list of available variables, optionally filtered by category."""
        variables = self.AVAILABLE_VARIABLES

        if category:
            variables = [v for v in variables if v.category == category]

        return [v.model_dump() for v in variables]

    @staticmethod
    def build_application_context(
        application,
        candidate,
        company
    ) -> Dict[str, Any]:
        """
        Build template context from application, candidate, and company objects.

        Args:
            application: Application model instance
            candidate: Candidate model instance
            company: Company model instance

        Returns:
            Dictionary with all available context variables
        """
        context = {
            # Recruiter info
            "recruiter_name": application.recruiter_name or "Hiring Manager",
            "recruiter_email": application.recruiter_email,

            # Company info
            "company_name": company.name if company else (application.company_name or ""),
            "company_industry": getattr(company, "industry", "") if company else "",
            "company_website": getattr(company, "website_url", "") if company else "",

            # Position info
            "position_title": application.position_title or "Open Position",
            "position_level": getattr(application, "position_level", "") or "",
            "position_country": getattr(application, "position_country", "") or "",

            # Candidate info
            "candidate_name": candidate.full_name,
            "candidate_first_name": candidate.full_name.split()[0] if candidate.full_name else "",
            "candidate_email": candidate.email,
            "candidate_title": getattr(candidate, "title", "") or "",

            # Application info
            "job_posting_url": getattr(application, "job_posting_url", "") or "",
            "application_id": f"APP-{application.id:05d}" if application.id else "",

            # Date info (auto-added in enrich_context)
        }

        return context

    @staticmethod
    def build_recipient_context(
        recipient,
        candidate
    ) -> Dict[str, Any]:
        """
        Build template context from recipient and candidate objects (for Group Campaigns).

        Args:
            recipient: Recipient model instance
            candidate: Candidate model instance

        Returns:
            Dictionary with all available context variables for recipient-based emails
        """
        # Extract recipient name parts
        recipient_name = recipient.name or ""
        name_parts = recipient_name.split() if recipient_name else []
        first_name = name_parts[0] if name_parts else ""
        last_name = name_parts[-1] if len(name_parts) > 1 else ""

        context = {
            # Recipient info (primary for group campaigns)
            "recipient_name": recipient.name or "",
            "recipient_first_name": first_name,
            "recipient_last_name": last_name,
            "recipient_email": recipient.email,
            "recipient_company": recipient.company or "",
            "recipient_position": recipient.position or "",
            "recipient_country": recipient.country or "",
            "recipient_language": recipient.language or "en",

            # Candidate info (sender)
            "candidate_name": candidate.full_name,
            "candidate_first_name": candidate.full_name.split()[0] if candidate.full_name else "",
            "candidate_email": candidate.email,
            "candidate_title": getattr(candidate, "title", "") or "",

            # Legacy aliases for backward compatibility
            # (these map recipient to recruiter for existing templates)
            "recruiter_name": recipient.name or "Hiring Manager",
            "recruiter_email": recipient.email,
            "company_name": recipient.company or "",
            "position_title": recipient.position or "Open Position",

            # Date info (auto-added in enrich_context)
        }

        return context


# Thread-safe singleton instance for application-wide use
_template_engine_lock = threading.Lock()
_template_engine_instance = None


def get_template_engine(strict_mode: bool = False) -> TemplateEngine:
    """
    Get the thread-safe singleton template engine instance.

    Args:
        strict_mode: If True, raise error on undefined variables (only used on first call)

    Returns:
        TemplateEngine singleton instance
    """
    global _template_engine_instance

    if _template_engine_instance is None:
        with _template_engine_lock:
            # Double-check locking pattern
            if _template_engine_instance is None:
                logger.info("[TemplateEngine] Creating singleton template engine instance")
                _template_engine_instance = TemplateEngine(strict_mode=strict_mode)

    return _template_engine_instance


# Legacy alias for backwards compatibility
template_engine = get_template_engine(strict_mode=False)


def sanitize_html(html_content: str) -> str:
    """
    Sanitize HTML content to prevent XSS attacks.

    Args:
        html_content: Raw HTML content to sanitize

    Returns:
        Sanitized HTML string safe for rendering
    """
    if not html_content:
        return ""

    logger.debug("[TemplateEngine] Sanitizing HTML content")
    sanitized = bleach.clean(
        html_content,
        tags=ALLOWED_TAGS,
        attributes=ALLOWED_ATTRIBUTES,
        strip=True
    )
    logger.debug(f"[TemplateEngine] Sanitized {len(html_content)} chars to {len(sanitized)} chars")
    return sanitized


def render_email_template(
    subject_template: str,
    body_template: str,
    context: Dict[str, Any],
    sanitize_output: bool = True
) -> Dict[str, str]:
    """
    Convenience function to render both subject and body.

    Args:
        subject_template: Subject line template
        body_template: Email body HTML template
        context: Template variables
        sanitize_output: Whether to sanitize the rendered HTML output

    Returns:
        Dictionary with 'subject' and 'body' keys
    """
    subject = template_engine.render(subject_template, context)
    body = template_engine.render(body_template, context)

    # Sanitize output HTML to prevent XSS
    if sanitize_output:
        body = sanitize_html(body)

    logger.debug(f"[TemplateEngine] Rendered email template - subject: {len(subject)} chars, body: {len(body)} chars")

    return {
        "subject": subject,
        "body": body
    }
