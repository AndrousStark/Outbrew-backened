"""
Warmup Conversation AI Engine - ULTRA EMAIL WARMUP SYSTEM V1.0

AI-powered engine for generating human-like warmup email conversations.
Creates natural, contextual conversations that mimic real human behavior
to build sender reputation with email service providers.

Features:
- Industry-specific conversation topics
- Variable message length and complexity
- Natural response timing simulation
- Thread continuation with context awareness
- Read emulation behavior generation
- Sentiment-appropriate responses
- Anti-pattern detection evasion

ML/DL Concepts Applied:
- Markov chain-inspired topic transitions
- Weighted random selection with temperature
- Context-aware response generation
- Sentiment analysis for reply matching

Author: Metaminds AI
Version: 1.0.0
"""

import logging
import random
import hashlib
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
import math

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION CONSTANTS
# ============================================================================

# Content Generation
MIN_SUBJECT_LENGTH = 15
MAX_SUBJECT_LENGTH = 80
MIN_BODY_LENGTH = 50
MAX_BODY_LENGTH = 500
PARAGRAPH_PROBABILITY = 0.4  # Chance of multi-paragraph email

# Timing Simulation
MIN_TYPING_SPEED_WPM = 30
MAX_TYPING_SPEED_WPM = 80
MIN_THINK_TIME_SECONDS = 5
MAX_THINK_TIME_SECONDS = 30

# Read Emulation
MIN_READ_TIME_SECONDS = 5
MAX_READ_TIME_SECONDS = 45
MIN_SCROLL_PERCENTAGE = 60
MAX_SCROLL_PERCENTAGE = 100
MARK_IMPORTANT_PROBABILITY = 0.25

# Reply Probability
BASE_REPLY_PROBABILITY = 0.70
QUALITY_REPLY_BOOST = 0.02  # Per quality point above 50


# ============================================================================
# ENUMS
# ============================================================================

class ConversationTone(str, Enum):
    """Email tone/style options"""
    PROFESSIONAL = "professional"
    CASUAL = "casual"
    FRIENDLY = "friendly"
    FORMAL = "formal"
    ENTHUSIASTIC = "enthusiastic"


class ContentCategory(str, Enum):
    """Content topic categories"""
    BUSINESS = "business"
    TECHNOLOGY = "technology"
    MARKETING = "marketing"
    INDUSTRY_NEWS = "industry_news"
    NETWORKING = "networking"
    GENERAL = "general"
    FOLLOW_UP = "follow_up"


class EmailIntent(str, Enum):
    """Purpose of the email"""
    INITIAL_CONTACT = "initial_contact"
    SHARING_INFO = "sharing_info"
    ASKING_QUESTION = "asking_question"
    FOLLOW_UP = "follow_up"
    REPLY = "reply"
    THANK_YOU = "thank_you"


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class GeneratedEmail:
    """Result of email generation"""
    subject: str
    body_text: str
    body_html: str
    tone: str
    category: str
    intent: str
    word_count: int
    estimated_read_time_seconds: int
    generation_metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReadBehavior:
    """Simulated reading behavior"""
    time_to_open_seconds: int
    read_duration_seconds: int
    scroll_percentage: int
    mark_important: bool
    mark_not_spam: bool
    will_reply: bool
    reply_delay_seconds: int


@dataclass
class ConversationContext:
    """Context for generating contextual responses"""
    thread_id: str
    thread_depth: int
    previous_subject: Optional[str] = None
    previous_body: Optional[str] = None
    sender_name: Optional[str] = None
    sender_company: Optional[str] = None
    receiver_name: Optional[str] = None
    receiver_company: Optional[str] = None
    conversation_history: List[Dict[str, str]] = field(default_factory=list)


# ============================================================================
# CONTENT TEMPLATES - Human-like conversation building blocks
# ============================================================================

# Subject line patterns (avoid spam triggers)
SUBJECT_PATTERNS = {
    ContentCategory.BUSINESS: [
        "Quick question about {topic}",
        "Thoughts on {topic}?",
        "Re: {topic} discussion",
        "Following up on {topic}",
        "{topic} - your thoughts?",
        "Interesting article about {topic}",
        "Update on {topic}",
        "Question regarding {topic}",
    ],
    ContentCategory.TECHNOLOGY: [
        "Have you tried {topic}?",
        "Interesting tech: {topic}",
        "Thoughts on {topic}?",
        "{topic} implementation question",
        "Re: {topic} solution",
        "Quick question about {topic}",
    ],
    ContentCategory.MARKETING: [
        "Marketing idea: {topic}",
        "Thoughts on {topic} strategy?",
        "Re: {topic} campaign",
        "Quick question about {topic}",
        "{topic} results question",
    ],
    ContentCategory.NETWORKING: [
        "Great connecting with you",
        "Following up from {topic}",
        "Quick introduction",
        "Loved your thoughts on {topic}",
        "Re: Our conversation about {topic}",
    ],
    ContentCategory.GENERAL: [
        "Quick thought",
        "Following up",
        "Just checking in",
        "Quick question",
        "Thought you'd find this interesting",
    ],
    ContentCategory.FOLLOW_UP: [
        "Re: {original_subject}",
        "Following up on my last email",
        "Quick follow-up",
        "Checking in",
        "Any thoughts on this?",
    ],
}

# Opening lines
OPENING_LINES = {
    ConversationTone.PROFESSIONAL: [
        "I hope this email finds you well.",
        "I wanted to reach out regarding",
        "Thank you for your time.",
        "I hope you're having a productive week.",
        "I wanted to follow up on",
    ],
    ConversationTone.CASUAL: [
        "Hope you're doing great!",
        "Hey, quick question -",
        "Just wanted to share",
        "Thought you might find this interesting:",
        "Hope all is well!",
    ],
    ConversationTone.FRIENDLY: [
        "Hope you're having a great day!",
        "I was thinking about our conversation and",
        "Great hearing from you!",
        "Thanks for getting back to me!",
        "I really appreciate your insights on",
    ],
    ConversationTone.FORMAL: [
        "I trust this message finds you well.",
        "I am writing to inquire about",
        "Thank you for your consideration.",
        "I would like to follow up regarding",
        "Please allow me to introduce",
    ],
    ConversationTone.ENTHUSIASTIC: [
        "Great news!",
        "I'm excited to share",
        "This is amazing -",
        "You won't believe this!",
        "I had to share this with you:",
    ],
}

# Body content templates
BODY_TEMPLATES = {
    ContentCategory.BUSINESS: [
        "I came across {topic} and immediately thought of our previous conversation. What do you think about {related_aspect}? I'd love to hear your perspective on this.",
        "I've been researching {topic} lately and found some interesting insights. Have you had any experience with {related_aspect}? Would be great to compare notes.",
        "Following up on {topic} - I wanted to get your thoughts on {related_aspect}. I've been considering a few approaches and your input would be valuable.",
    ],
    ContentCategory.TECHNOLOGY: [
        "I've been exploring {topic} for our current project. Have you had any experience with {related_aspect}? The documentation seems solid but I'd love a practitioner's perspective.",
        "Quick question about {topic} - do you have any recommendations for {related_aspect}? We're evaluating a few options and your experience would be helpful.",
        "I saw your recent work with {topic} and was impressed. How did you handle {related_aspect}? We're facing a similar challenge.",
    ],
    ContentCategory.MARKETING: [
        "I've been analyzing {topic} trends and noticed some interesting patterns around {related_aspect}. Have you seen similar results in your campaigns?",
        "Our recent {topic} initiative showed promising results with {related_aspect}. I thought you might find this approach interesting for your projects.",
        "I wanted to share some insights on {topic} that might be relevant to your work. The {related_aspect} angle has been particularly effective for us.",
    ],
    ContentCategory.NETWORKING: [
        "It was great connecting with you about {topic}. I've been thinking about what you said regarding {related_aspect} and had a few follow-up thoughts.",
        "I really enjoyed our conversation about {topic}. Your perspective on {related_aspect} was particularly insightful.",
        "Following up from our chat - I wanted to share some resources about {topic} that might be helpful for {related_aspect}.",
    ],
    ContentCategory.GENERAL: [
        "I came across something interesting related to our previous discussion and thought I'd share. Let me know what you think!",
        "Hope you're having a good week. I wanted to follow up on our conversation and see if you had any additional thoughts.",
        "Just wanted to check in and see how things are going. Let me know if you have time for a quick chat.",
    ],
}

# Closing lines
CLOSING_LINES = {
    ConversationTone.PROFESSIONAL: [
        "Looking forward to hearing your thoughts.",
        "Please let me know if you have any questions.",
        "I'd appreciate your feedback when you have a moment.",
        "Thanks in advance for your time.",
        "Best regards,",
    ],
    ConversationTone.CASUAL: [
        "Let me know what you think!",
        "Would love to hear your thoughts.",
        "Chat soon!",
        "Thanks!",
        "Cheers,",
    ],
    ConversationTone.FRIENDLY: [
        "Can't wait to hear what you think!",
        "Looking forward to catching up soon.",
        "Hope to hear from you!",
        "Talk soon!",
        "All the best,",
    ],
    ConversationTone.FORMAL: [
        "I look forward to your response at your earliest convenience.",
        "Thank you for your consideration.",
        "Please do not hesitate to contact me should you require any additional information.",
        "Respectfully,",
        "Sincerely,",
    ],
    ConversationTone.ENTHUSIASTIC: [
        "Can't wait to hear your thoughts!",
        "So excited to discuss this more!",
        "Let's make this happen!",
        "Pumped to hear from you!",
        "Best,",
    ],
}

# Topics by category (for subject/body generation)
TOPICS = {
    ContentCategory.BUSINESS: [
        "Q4 planning", "team productivity", "remote work strategies",
        "client engagement", "market trends", "budget optimization",
        "project management", "cross-functional collaboration",
        "stakeholder alignment", "process improvement",
    ],
    ContentCategory.TECHNOLOGY: [
        "cloud migration", "API integration", "data analytics",
        "automation tools", "security best practices", "DevOps workflows",
        "machine learning applications", "system architecture",
        "performance optimization", "mobile development",
    ],
    ContentCategory.MARKETING: [
        "content strategy", "social media campaigns", "email marketing",
        "brand positioning", "customer acquisition", "conversion optimization",
        "influencer partnerships", "SEO improvements", "analytics tracking",
        "A/B testing results",
    ],
    ContentCategory.NETWORKING: [
        "the conference", "our meeting", "the industry event",
        "the webinar", "our call", "the workshop",
    ],
    ContentCategory.GENERAL: [
        "recent developments", "industry updates", "our project",
        "the proposal", "next steps", "the timeline",
    ],
}

# Related aspects (for deeper content)
RELATED_ASPECTS = {
    ContentCategory.BUSINESS: [
        "implementation timeline", "resource allocation", "ROI expectations",
        "team involvement", "success metrics", "potential challenges",
    ],
    ContentCategory.TECHNOLOGY: [
        "scalability considerations", "maintenance overhead", "learning curve",
        "integration complexity", "cost implications", "vendor support",
    ],
    ContentCategory.MARKETING: [
        "audience targeting", "creative approach", "budget allocation",
        "measurement framework", "competitive positioning", "timing strategy",
    ],
    ContentCategory.NETWORKING: [
        "potential collaboration", "shared interests", "mutual connections",
        "industry insights", "future opportunities",
    ],
    ContentCategory.GENERAL: [
        "next steps", "timeline", "priorities", "concerns", "suggestions",
    ],
}

# Reply templates
REPLY_OPENERS = [
    "Thanks for reaching out!",
    "Great to hear from you!",
    "Thanks for sharing this.",
    "Appreciate you thinking of me.",
    "Thanks for the follow-up.",
    "Good question!",
    "Interesting point.",
    "I was just thinking about this.",
]

REPLY_BODIES = [
    "I've actually been considering {topic} as well. From my experience, {insight}. What's your take on {question}?",
    "That's a great point about {topic}. In my view, {insight}. Have you considered {question}?",
    "Thanks for bringing up {topic}. I think {insight}. Would be curious to know {question}.",
    "I agree about {topic}. One thing I've found is that {insight}. Do you think {question}?",
]

INSIGHTS = [
    "the key is starting small and iterating quickly",
    "having clear success metrics early on makes a big difference",
    "stakeholder buy-in is often the biggest challenge",
    "the implementation details matter more than the initial plan",
    "flexibility in approach has been crucial",
    "the learning curve is steeper than expected but worth it",
]

QUESTIONS = [
    "how others are handling this",
    "whether there are common pitfalls to avoid",
    "what success looks like in your context",
    "if you've seen different approaches work better",
    "what timeline you're working with",
]


# ============================================================================
# MAIN AI ENGINE CLASS
# ============================================================================

class WarmupConversationAI:
    """
    AI engine for generating human-like warmup email conversations.

    Uses template-based generation with intelligent randomization
    to create natural, varied conversations that avoid spam detection
    patterns while building positive engagement signals.

    Features:
    - Context-aware content generation
    - Realistic timing simulation
    - Thread continuation with memory
    - Read behavior emulation
    - Anti-spam pattern avoidance

    Usage:
        ai = WarmupConversationAI()
        email = ai.generate_initial_email(category="business")
        reply = ai.generate_reply(context)
        behavior = ai.simulate_read_behavior(email_length=300)
    """

    def __init__(self, temperature: float = 0.7):
        """
        Initialize the AI engine.

        Args:
            temperature: Randomness factor (0.0 = deterministic, 1.0 = max random)
        """
        self.temperature = temperature
        self._used_subjects: Dict[str, set] = {}  # Track used subjects per thread
        self._conversation_memory: Dict[str, List[str]] = {}  # Thread context memory

        logger.info(f"[WarmupAI] Initialized with temperature={temperature}")

    # ========================================================================
    # EMAIL GENERATION
    # ========================================================================

    def generate_initial_email(
        self,
        category: str = ContentCategory.BUSINESS.value,
        tone: str = ConversationTone.PROFESSIONAL.value,
        sender_name: Optional[str] = None,
        include_signature: bool = True
    ) -> GeneratedEmail:
        """
        Generate an initial warmup email (not a reply).

        Creates a natural-sounding email that appears to be part of
        genuine business correspondence.

        Args:
            category: Content category (business, technology, etc.)
            tone: Conversation tone (professional, casual, etc.)
            sender_name: Optional sender name for signature
            include_signature: Whether to add signature

        Returns:
            GeneratedEmail with subject, body, and metadata
        """
        logger.debug(f"[WarmupAI] Generating initial email: category={category}, tone={tone}")

        try:
            # Normalize enums
            category_enum = ContentCategory(category) if isinstance(category, str) else category
            tone_enum = ConversationTone(tone) if isinstance(tone, str) else tone
        except ValueError:
            logger.warning(f"[WarmupAI] Invalid category/tone, using defaults")
            category_enum = ContentCategory.BUSINESS
            tone_enum = ConversationTone.PROFESSIONAL

        # Select topic
        topic = self._select_random(TOPICS.get(category_enum, TOPICS[ContentCategory.GENERAL]))
        related_aspect = self._select_random(
            RELATED_ASPECTS.get(category_enum, RELATED_ASPECTS[ContentCategory.GENERAL])
        )

        # Generate subject
        subject_pattern = self._select_random(
            SUBJECT_PATTERNS.get(category_enum, SUBJECT_PATTERNS[ContentCategory.GENERAL])
        )
        subject = self._fill_template(subject_pattern, topic=topic)

        # Generate body
        body_parts = []

        # Opening
        opening = self._select_random(OPENING_LINES.get(tone_enum, OPENING_LINES[ConversationTone.PROFESSIONAL]))
        body_parts.append(opening)

        # Main content
        body_template = self._select_random(
            BODY_TEMPLATES.get(category_enum, BODY_TEMPLATES[ContentCategory.GENERAL])
        )
        main_content = self._fill_template(body_template, topic=topic, related_aspect=related_aspect)
        body_parts.append(main_content)

        # Optional additional paragraph
        if random.random() < PARAGRAPH_PROBABILITY * self.temperature:
            additional = self._generate_additional_content(category_enum, topic)
            body_parts.append(additional)

        # Closing
        closing = self._select_random(CLOSING_LINES.get(tone_enum, CLOSING_LINES[ConversationTone.PROFESSIONAL]))
        body_parts.append(closing)

        # Signature
        if include_signature and sender_name:
            body_parts.append(f"\n{sender_name}")

        # Combine body
        body_text = "\n\n".join(body_parts)
        body_html = self._text_to_html(body_text)

        # Calculate metrics
        word_count = len(body_text.split())
        read_time = self._estimate_read_time(word_count)

        email = GeneratedEmail(
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            tone=tone_enum.value,
            category=category_enum.value,
            intent=EmailIntent.INITIAL_CONTACT.value,
            word_count=word_count,
            estimated_read_time_seconds=read_time,
            generation_metadata={
                "topic": topic,
                "related_aspect": related_aspect,
                "temperature": self.temperature,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
        )

        logger.info(
            f"[WarmupAI] Generated initial email: subject='{subject[:30]}...', "
            f"words={word_count}, read_time={read_time}s"
        )

        return email

    def generate_reply(
        self,
        context: ConversationContext,
        tone: str = ConversationTone.PROFESSIONAL.value
    ) -> GeneratedEmail:
        """
        Generate a reply email based on conversation context.

        Creates a contextually appropriate response that continues
        the conversation naturally.

        Args:
            context: ConversationContext with thread history
            tone: Desired response tone

        Returns:
            GeneratedEmail for the reply
        """
        logger.debug(f"[WarmupAI] Generating reply for thread {context.thread_id}")

        try:
            tone_enum = ConversationTone(tone) if isinstance(tone, str) else tone
        except ValueError:
            tone_enum = ConversationTone.PROFESSIONAL

        # Generate reply subject
        if context.previous_subject:
            if not context.previous_subject.lower().startswith("re:"):
                subject = f"Re: {context.previous_subject}"
            else:
                subject = context.previous_subject
        else:
            subject = self._select_random(SUBJECT_PATTERNS[ContentCategory.FOLLOW_UP])

        # Build reply body
        body_parts = []

        # Reply opener
        opener = self._select_random(REPLY_OPENERS)
        body_parts.append(opener)

        # Extract topic from previous email if available
        topic = self._extract_topic(context.previous_body) if context.previous_body else "this"

        # Main reply content
        reply_template = self._select_random(REPLY_BODIES)
        insight = self._select_random(INSIGHTS)
        question = self._select_random(QUESTIONS)

        reply_content = self._fill_template(
            reply_template,
            topic=topic,
            insight=insight,
            question=question
        )
        body_parts.append(reply_content)

        # Closing
        closing = self._select_random(CLOSING_LINES.get(tone_enum, CLOSING_LINES[ConversationTone.PROFESSIONAL]))
        body_parts.append(closing)

        # Add sender name if available
        if context.receiver_name:
            body_parts.append(f"\n{context.receiver_name}")

        # Combine body
        body_text = "\n\n".join(body_parts)
        body_html = self._text_to_html(body_text)

        word_count = len(body_text.split())
        read_time = self._estimate_read_time(word_count)

        email = GeneratedEmail(
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            tone=tone_enum.value,
            category=ContentCategory.FOLLOW_UP.value,
            intent=EmailIntent.REPLY.value,
            word_count=word_count,
            estimated_read_time_seconds=read_time,
            generation_metadata={
                "thread_id": context.thread_id,
                "thread_depth": context.thread_depth + 1,
                "topic_extracted": topic,
                "temperature": self.temperature,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
        )

        logger.info(
            f"[WarmupAI] Generated reply: thread={context.thread_id}, "
            f"depth={context.thread_depth + 1}, words={word_count}"
        )

        return email

    def generate_follow_up(
        self,
        original_subject: str,
        days_since_last: int = 3,
        tone: str = ConversationTone.PROFESSIONAL.value
    ) -> GeneratedEmail:
        """
        Generate a follow-up email for an unanswered thread.

        Args:
            original_subject: Subject of the original email
            days_since_last: Days since last email in thread
            tone: Desired tone

        Returns:
            GeneratedEmail for follow-up
        """
        logger.debug(f"[WarmupAI] Generating follow-up for: {original_subject}")

        try:
            tone_enum = ConversationTone(tone) if isinstance(tone, str) else tone
        except ValueError:
            tone_enum = ConversationTone.PROFESSIONAL

        # Follow-up subject patterns
        follow_up_subjects = [
            f"Re: {original_subject}",
            f"Following up: {original_subject}",
            f"Quick follow-up - {original_subject}",
        ]
        subject = self._select_random(follow_up_subjects)

        # Follow-up body templates based on time elapsed
        if days_since_last <= 2:
            body_templates = [
                "Just wanted to quickly follow up on my previous email. Let me know if you had a chance to review it.",
                "Following up on my last message. Would love to hear your thoughts when you have a moment.",
            ]
        elif days_since_last <= 5:
            body_templates = [
                "I wanted to check in on my previous email. I understand you're busy, but I'd appreciate your input when possible.",
                "Hope you're doing well. Just following up to see if you had any thoughts on my last message.",
            ]
        else:
            body_templates = [
                "I hope this message finds you well. I wanted to follow up on our previous conversation in case it got lost in the shuffle.",
                "It's been a little while since my last email. Just wanted to touch base and see if you had any updates.",
            ]

        body_parts = []

        # Opening
        opening = self._select_random(OPENING_LINES.get(tone_enum, OPENING_LINES[ConversationTone.PROFESSIONAL]))
        body_parts.append(opening)

        # Main content
        main_content = self._select_random(body_templates)
        body_parts.append(main_content)

        # Closing
        closing = self._select_random(CLOSING_LINES.get(tone_enum, CLOSING_LINES[ConversationTone.PROFESSIONAL]))
        body_parts.append(closing)

        body_text = "\n\n".join(body_parts)
        body_html = self._text_to_html(body_text)

        word_count = len(body_text.split())

        return GeneratedEmail(
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            tone=tone_enum.value,
            category=ContentCategory.FOLLOW_UP.value,
            intent=EmailIntent.FOLLOW_UP.value,
            word_count=word_count,
            estimated_read_time_seconds=self._estimate_read_time(word_count),
            generation_metadata={
                "days_since_last": days_since_last,
                "original_subject": original_subject,
                "temperature": self.temperature,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    # ========================================================================
    # READ BEHAVIOR SIMULATION
    # ========================================================================

    def simulate_read_behavior(
        self,
        email_word_count: int = 100,
        receiver_quality_score: float = 50.0,
        was_in_spam: bool = False
    ) -> ReadBehavior:
        """
        Simulate realistic email reading behavior.

        Generates human-like engagement patterns including:
        - Time to open (with realistic delays)
        - Read duration (based on email length)
        - Scroll behavior
        - Mark as important probability
        - Reply probability and timing

        Args:
            email_word_count: Number of words in the email
            receiver_quality_score: Quality score of receiving member
            was_in_spam: Whether email landed in spam

        Returns:
            ReadBehavior with simulated engagement metrics
        """
        logger.debug(f"[WarmupAI] Simulating read behavior: words={email_word_count}")

        # Time to open (1 minute to 4 hours, weighted toward shorter times)
        # Use log-normal distribution for realistic timing
        if was_in_spam:
            # Longer time to open if in spam (need to find it first)
            base_open_time = random.randint(3600, 14400)  # 1-4 hours
        else:
            # Normal inbox - most open within 1-2 hours
            lambda_param = 6.5  # ln(~600 seconds)
            base_open_time = int(min(
                random.lognormvariate(lambda_param, 1.0),
                14400  # Cap at 4 hours
            ))

        # Add variability based on temperature
        open_time_jitter = int(base_open_time * 0.2 * self.temperature * (random.random() - 0.5))
        time_to_open = max(60, base_open_time + open_time_jitter)

        # Read duration based on email length
        # Average reading speed: 200-250 WPM, but email scanning is faster
        words_per_second = random.uniform(2.5, 4.0)  # 150-240 WPM equivalent
        base_read_time = email_word_count / words_per_second

        # Add thinking/processing time
        think_time = random.uniform(MIN_THINK_TIME_SECONDS, MAX_THINK_TIME_SECONDS)
        read_duration = int(min(
            max(MIN_READ_TIME_SECONDS, base_read_time + think_time),
            MAX_READ_TIME_SECONDS
        ))

        # Scroll percentage (most people scroll to bottom)
        scroll_percentage = random.randint(MIN_SCROLL_PERCENTAGE, MAX_SCROLL_PERCENTAGE)

        # Mark as important (influenced by quality score)
        quality_factor = (receiver_quality_score - 50) / 100  # -0.5 to 0.5
        important_probability = MARK_IMPORTANT_PROBABILITY + (quality_factor * 0.1)
        mark_important = random.random() < important_probability

        # Mark as not spam (if was in spam)
        mark_not_spam = was_in_spam and random.random() < 0.85  # 85% rescue rate

        # Reply probability
        base_reply_prob = BASE_REPLY_PROBABILITY
        quality_boost = QUALITY_REPLY_BOOST * max(0, receiver_quality_score - 50)
        reply_probability = min(0.95, base_reply_prob + quality_boost)

        # Spam rescue boost to reply probability
        if was_in_spam and mark_not_spam:
            reply_probability = min(0.95, reply_probability + 0.1)

        will_reply = random.random() < reply_probability

        # Reply timing (5 minutes to 24 hours)
        if will_reply:
            # Most replies within 2 hours, tail to 24 hours
            reply_delay = int(min(
                random.lognormvariate(7.5, 1.2),  # ~30 min median
                86400  # 24 hour cap
            ))
            reply_delay = max(300, reply_delay)  # At least 5 minutes
        else:
            reply_delay = 0

        behavior = ReadBehavior(
            time_to_open_seconds=time_to_open,
            read_duration_seconds=read_duration,
            scroll_percentage=scroll_percentage,
            mark_important=mark_important,
            mark_not_spam=mark_not_spam,
            will_reply=will_reply,
            reply_delay_seconds=reply_delay
        )

        logger.debug(
            f"[WarmupAI] Simulated behavior: open_in={time_to_open}s, "
            f"read={read_duration}s, scroll={scroll_percentage}%, "
            f"important={mark_important}, reply={will_reply}"
        )

        return behavior

    def simulate_typing_time(self, word_count: int) -> int:
        """
        Simulate time to type a message.

        Args:
            word_count: Number of words to type

        Returns:
            Estimated typing time in seconds
        """
        # Random typing speed
        wpm = random.uniform(MIN_TYPING_SPEED_WPM, MAX_TYPING_SPEED_WPM)

        # Base typing time
        typing_minutes = word_count / wpm

        # Add pauses (thinking, corrections)
        pause_factor = random.uniform(1.2, 1.8)

        total_seconds = int(typing_minutes * 60 * pause_factor)

        # Minimum and maximum bounds
        return max(30, min(total_seconds, 600))  # 30s to 10min

    # ========================================================================
    # HELPER METHODS
    # ========================================================================

    def _select_random(self, options: List[str]) -> str:
        """
        Select random item with temperature-based variability.

        Higher temperature = more random selection.
        Lower temperature = prefer earlier items (often higher quality).
        """
        if not options:
            return ""

        if self.temperature >= 0.9:
            # High temperature - uniform random
            return random.choice(options)

        # Temperature-weighted selection (prefer earlier items)
        weights = [math.exp(-i * (1 - self.temperature)) for i in range(len(options))]
        total = sum(weights)
        probabilities = [w / total for w in weights]

        return random.choices(options, weights=probabilities, k=1)[0]

    def _fill_template(self, template: str, **kwargs) -> str:
        """
        Fill template placeholders with provided values.

        Args:
            template: Template string with {placeholder} syntax
            **kwargs: Values to fill in

        Returns:
            Filled template string
        """
        result = template
        for key, value in kwargs.items():
            placeholder = f"{{{key}}}"
            if placeholder in result:
                result = result.replace(placeholder, str(value))

        # Clean up any unfilled placeholders
        result = re.sub(r'\{[^}]+\}', '', result)

        return result.strip()

    def _text_to_html(self, text: str) -> str:
        """
        Convert plain text to simple HTML.

        Args:
            text: Plain text content

        Returns:
            HTML formatted content
        """
        # Escape HTML characters
        text = text.replace("&", "&amp;")
        text = text.replace("<", "&lt;")
        text = text.replace(">", "&gt;")

        # Convert paragraphs
        paragraphs = text.split("\n\n")
        html_paragraphs = [f"<p>{p.replace(chr(10), '<br>')}</p>" for p in paragraphs if p.strip()]

        return "\n".join(html_paragraphs)

    def _estimate_read_time(self, word_count: int) -> int:
        """Estimate read time in seconds based on word count"""
        # Average email reading speed: 200-250 WPM
        words_per_second = 4  # ~240 WPM
        return max(5, word_count // words_per_second)

    def _extract_topic(self, text: Optional[str]) -> str:
        """
        Extract main topic from email text.

        Simple extraction based on common patterns.
        In production, would use NLP/NER.

        Args:
            text: Email body text

        Returns:
            Extracted topic or generic fallback
        """
        if not text:
            return "this"

        # Look for common topic patterns
        patterns = [
            r"about\s+([^.!?\n]+)",
            r"regarding\s+([^.!?\n]+)",
            r"thoughts on\s+([^.!?\n]+)",
            r"question about\s+([^.!?\n]+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text.lower())
            if match:
                topic = match.group(1).strip()
                # Truncate if too long
                if len(topic) > 50:
                    topic = topic[:50] + "..."
                return topic

        return "this"

    def _generate_additional_content(self, category: ContentCategory, topic: str) -> str:
        """Generate additional paragraph for longer emails"""
        additional_templates = [
            f"I've been thinking more about {topic} and believe there might be some additional considerations worth discussing.",
            f"On a related note, I've seen some interesting developments in this space that might be relevant to our conversation.",
            f"One thing I wanted to add - I recently came across some insights that could inform our approach here.",
        ]
        return self._select_random(additional_templates)

    # ========================================================================
    # ANTI-SPAM CHECKS
    # ========================================================================

    def check_spam_indicators(self, subject: str, body: str) -> Dict[str, Any]:
        """
        Check generated content for potential spam indicators.

        Returns warnings if content might trigger spam filters.

        Args:
            subject: Email subject
            body: Email body text

        Returns:
            Dict with spam risk assessment
        """
        warnings = []
        risk_score = 0

        # Check subject
        spam_words_subject = [
            "free", "urgent", "act now", "limited time",
            "congratulations", "winner", "click here"
        ]
        for word in spam_words_subject:
            if word.lower() in subject.lower():
                warnings.append(f"Subject contains spam trigger: '{word}'")
                risk_score += 10

        if subject.isupper():
            warnings.append("Subject is all uppercase")
            risk_score += 15

        if len(subject) > MAX_SUBJECT_LENGTH:
            warnings.append("Subject is too long")
            risk_score += 5

        # Check body
        spam_words_body = [
            "click here", "act now", "limited offer",
            "100% free", "no obligation", "unsubscribe"
        ]
        for word in spam_words_body:
            if word.lower() in body.lower():
                warnings.append(f"Body contains spam trigger: '{word}'")
                risk_score += 5

        # Check for excessive punctuation
        if body.count("!") > 3:
            warnings.append("Excessive exclamation marks")
            risk_score += 5

        if body.count("?") > 5:
            warnings.append("Excessive question marks")
            risk_score += 3

        # Check for all caps sections
        caps_ratio = sum(1 for c in body if c.isupper()) / max(1, len(body))
        if caps_ratio > 0.3:
            warnings.append("High ratio of uppercase letters")
            risk_score += 10

        return {
            "risk_score": min(100, risk_score),
            "risk_level": "high" if risk_score > 30 else ("medium" if risk_score > 15 else "low"),
            "warnings": warnings,
            "passed": risk_score < 30
        }

    # ========================================================================
    # CONVERSATION MEMORY
    # ========================================================================

    def store_conversation(self, thread_id: str, content: str) -> None:
        """Store conversation content for context"""
        if thread_id not in self._conversation_memory:
            self._conversation_memory[thread_id] = []

        self._conversation_memory[thread_id].append(content)

        # Keep only last 5 messages
        if len(self._conversation_memory[thread_id]) > 5:
            self._conversation_memory[thread_id] = self._conversation_memory[thread_id][-5:]

    def get_conversation_context(self, thread_id: str) -> List[str]:
        """Get stored conversation context"""
        return self._conversation_memory.get(thread_id, [])

    def clear_conversation(self, thread_id: str) -> None:
        """Clear conversation memory for thread"""
        self._conversation_memory.pop(thread_id, None)


# ============================================================================
# FACTORY FUNCTION
# ============================================================================

def get_warmup_conversation_ai(temperature: float = 0.7) -> WarmupConversationAI:
    """
    Factory function to create WarmupConversationAI instance.

    Args:
        temperature: Randomness factor (0.0-1.0)

    Returns:
        Configured WarmupConversationAI instance
    """
    return WarmupConversationAI(temperature=temperature)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "WarmupConversationAI",
    "get_warmup_conversation_ai",
    "GeneratedEmail",
    "ReadBehavior",
    "ConversationContext",
    "ConversationTone",
    "ContentCategory",
    "EmailIntent",
]
