"""
Follow-Up ML Models - AI/ML Intelligence for Email Follow-Up System

ULTRA Follow-Up System V2.0 - Enterprise AI/ML Models

Features:
- ML prediction caching (reply probability, optimal send time)
- AI-generated content tracking
- Send time analytics for optimization
- Conditional branching for behavioral sequences
- Intent detection for auto-response
"""

from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, Boolean,
    Text, Float, JSON, Enum as SQLEnum, UniqueConstraint, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime
from enum import Enum
from typing import Dict, List, Any, Optional

from app.core.database import Base


# ============= ENUMS =============

class BranchConditionType(str, Enum):
    """Condition types for sequence branching"""
    OPENED = "opened"                    # Email was opened
    NOT_OPENED = "not_opened"            # Email not opened after X hours
    CLICKED = "clicked"                  # Link was clicked
    REPLIED = "replied"                  # Recipient replied
    REPLIED_POSITIVE = "replied_positive"  # Positive reply detected
    REPLIED_NEGATIVE = "replied_negative"  # Negative reply detected
    REPLIED_OBJECTION = "replied_objection"  # Objection detected
    BOUNCED = "bounced"                  # Email bounced
    OUT_OF_OFFICE = "out_of_office"      # OOO auto-reply detected
    IGNORED = "ignored"                  # No engagement after full wait period
    CUSTOM = "custom"                    # Custom condition (keyword, sentiment)


class IntentType(str, Enum):
    """Detected intent types from replies"""
    INTERESTED = "interested"            # Positive response, wants to proceed
    CURIOUS = "curious"                  # Needs more information
    OBJECTION = "objection"              # Has concerns or objections
    NOT_INTERESTED = "not_interested"    # Declined, not a fit
    OUT_OF_OFFICE = "out_of_office"      # Auto-reply, person away
    QUESTION = "question"                # Asking a question
    MEETING_REQUEST = "meeting_request"  # Wants to schedule a call/meeting
    FORWARD = "forward"                  # Will forward to someone else
    UNSUBSCRIBE = "unsubscribe"          # Wants to stop receiving emails
    UNKNOWN = "unknown"                  # Could not classify


class AIModelType(str, Enum):
    """AI models used for content generation"""
    GPT4 = "gpt-4"
    GPT4_TURBO = "gpt-4-turbo"
    GPT4O = "gpt-4o"
    GPT4O_MINI = "gpt-4o-mini"
    GPT35_TURBO = "gpt-3.5-turbo"
    CLAUDE_OPUS = "claude-opus"
    CLAUDE_SONNET = "claude-sonnet"
    CLAUDE_HAIKU = "claude-haiku"
    CUSTOM = "custom"


class PredictionConfidence(str, Enum):
    """Confidence levels for ML predictions"""
    HIGH = "high"          # > 85% confidence, can auto-apply
    MEDIUM = "medium"      # 60-85% confidence, suggest but confirm
    LOW = "low"            # < 60% confidence, manual review needed


# ============= ML PREDICTION MODELS =============

class FollowUpPrediction(Base):
    """
    ML prediction cache for follow-up campaigns.

    Stores predictions for:
    - Reply probability (0-100%)
    - Optimal send time (hour/day)
    - Features used for prediction (for explainability)
    """
    __tablename__ = "follow_up_predictions"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign keys
    campaign_id = Column(Integer, ForeignKey("follow_up_campaigns.id", ondelete="CASCADE"), nullable=True, index=True)
    email_id = Column(Integer, ForeignKey("follow_up_emails.id", ondelete="CASCADE"), nullable=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Reply probability prediction
    reply_probability = Column(Float, nullable=False)  # 0.0 - 1.0
    reply_probability_confidence = Column(SQLEnum(PredictionConfidence), default=PredictionConfidence.MEDIUM)

    # Optimal send time prediction
    optimal_send_hour = Column(Integer)  # 0-23 (hour of day)
    optimal_send_day = Column(String(15))  # monday, tuesday, etc.
    optimal_send_timezone = Column(String(50), default="UTC")
    send_time_confidence = Column(SQLEnum(PredictionConfidence), default=PredictionConfidence.MEDIUM)

    # Priority score (for prioritizing which campaigns to send first)
    priority_score = Column(Integer, default=50)  # 0-100, higher = more important

    # Features used for prediction (for explainability)
    features_json = Column(JSON, default=dict)
    """
    {
        "recipient_domain": "company.com",
        "recipient_seniority": "manager",
        "email_count": 2,
        "previous_opens": 1,
        "previous_replies": 0,
        "industry": "technology",
        "company_size": "medium",
        "time_since_last_email_hours": 72,
        "original_email_personalization_score": 0.85,
        "subject_line_length": 45,
        "body_word_count": 150
    }
    """

    # Model info
    model_version = Column(String(50))  # "v1.0", "v2.0"
    model_type = Column(String(100))  # "xgboost", "gradient_boosting"

    # Prediction accuracy tracking (updated when actual outcome known)
    actual_replied = Column(Boolean)
    actual_reply_hours = Column(Float)  # Hours until reply (if replied)
    prediction_accurate = Column(Boolean)  # Was prediction correct?

    # Auto-apply status
    auto_applied = Column(Boolean, default=False)  # Was this prediction auto-applied?
    auto_apply_reason = Column(String(200))  # Why it was/wasn't auto-applied

    # Timestamps
    predicted_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)  # Predictions should be recalculated periodically
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    campaign = relationship("FollowUpCampaign", backref="predictions")
    candidate = relationship("Candidate", backref="follow_up_predictions")

    __table_args__ = (
        Index('ix_prediction_campaign_created', 'campaign_id', 'predicted_at'),
    )

    def to_dict(self) -> dict:
        """Convert to dictionary for API responses"""
        return {
            "id": self.id,
            "campaign_id": self.campaign_id,
            "email_id": self.email_id,
            "reply_probability": round(self.reply_probability * 100, 1),  # as percentage
            "reply_probability_confidence": self.reply_probability_confidence.value if self.reply_probability_confidence else None,
            "optimal_send_hour": self.optimal_send_hour,
            "optimal_send_day": self.optimal_send_day,
            "optimal_send_timezone": self.optimal_send_timezone,
            "send_time_confidence": self.send_time_confidence.value if self.send_time_confidence else None,
            "priority_score": self.priority_score,
            "auto_applied": self.auto_applied,
            "predicted_at": self.predicted_at.isoformat() if self.predicted_at else None,
        }


# ============= AI CONTENT GENERATION MODELS =============

class AIGeneratedContent(Base):
    """
    Track AI-generated content for sequences and emails.

    Stores:
    - Prompts used for generation
    - Generated content (subject, body)
    - Model used and tokens consumed
    - User edits and final content
    """
    __tablename__ = "ai_generated_content"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign keys
    campaign_id = Column(Integer, ForeignKey("follow_up_campaigns.id", ondelete="CASCADE"), nullable=True, index=True)
    sequence_id = Column(Integer, ForeignKey("follow_up_sequences.id", ondelete="CASCADE"), nullable=True)
    email_id = Column(Integer, ForeignKey("follow_up_emails.id", ondelete="CASCADE"), nullable=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Step info (if generating for a specific step)
    step_number = Column(Integer)

    # Generation prompt
    prompt_used = Column(Text, nullable=False)
    system_prompt = Column(Text)  # System prompt (if separate)

    # Context provided to AI
    context_json = Column(JSON, default=dict)
    """
    {
        "candidate_name": "John Doe",
        "company_name": "Tech Corp",
        "position": "Software Engineer",
        "original_email_subject": "...",
        "original_email_body": "...",
        "recipient_info": {...},
        "tone": "professional",
        "strategy": "add_value"
    }
    """

    # Generated content
    generated_subject = Column(String(500))
    generated_body = Column(Text)
    generated_html = Column(Text)  # HTML version if generated

    # Spintax (if generated with variations)
    spintax_subject = Column(Text)  # Subject with spintax: {Hi|Hello|Hey}
    spintax_body = Column(Text)     # Body with spintax

    # A/B variants (if multiple versions generated)
    ab_variants = Column(JSON, default=list)
    """
    [
        {"subject": "...", "body": "...", "predicted_open_rate": 0.25},
        {"subject": "...", "body": "...", "predicted_open_rate": 0.23}
    ]
    """

    # Model info
    model_used = Column(SQLEnum(AIModelType), default=AIModelType.GPT4O)
    model_version = Column(String(50))  # "gpt-4-0613"

    # Token usage
    tokens_prompt = Column(Integer, default=0)
    tokens_completion = Column(Integer, default=0)
    tokens_total = Column(Integer, default=0)

    # Cost tracking
    cost_usd = Column(Float, default=0.0)

    # Quality metrics
    quality_score = Column(Float)  # 0-1, AI self-assessment
    personalization_score = Column(Float)  # 0-1, how personalized is the content
    readability_score = Column(Float)  # 0-1, Flesch-Kincaid or similar

    # User interaction
    user_edited = Column(Boolean, default=False)
    user_edit_percentage = Column(Float, default=0.0)  # % of content changed
    final_subject = Column(String(500))  # After user edits
    final_body = Column(Text)

    # Feedback
    user_rating = Column(Integer)  # 1-5 stars
    user_feedback = Column(Text)  # Free-form feedback

    # Performance tracking (if sent)
    was_sent = Column(Boolean, default=False)
    open_rate = Column(Float)  # Actual open rate if sent
    reply_received = Column(Boolean, default=False)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    campaign = relationship("FollowUpCampaign", backref="ai_generated_content")
    sequence = relationship("FollowUpSequence", backref="ai_generated_content")
    candidate = relationship("Candidate", backref="ai_generated_content")

    def to_dict(self) -> dict:
        """Convert to dictionary for API responses"""
        return {
            "id": self.id,
            "step_number": self.step_number,
            "generated_subject": self.generated_subject,
            "generated_body": self.generated_body,
            "model_used": self.model_used.value if self.model_used else None,
            "tokens_total": self.tokens_total,
            "cost_usd": round(self.cost_usd, 4) if self.cost_usd else 0,
            "quality_score": round(self.quality_score, 2) if self.quality_score else None,
            "user_edited": self.user_edited,
            "final_subject": self.final_subject,
            "final_body": self.final_body,
            "ab_variants": self.ab_variants,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


# ============= SEND TIME OPTIMIZATION MODELS =============

class SendTimeAnalytics(Base):
    """
    Analytics for send time optimization.

    Tracks engagement patterns by:
    - Day of week
    - Hour of day
    - Recipient domain/industry

    Used to predict optimal send times for future emails.
    """
    __tablename__ = "send_time_analytics"

    id = Column(Integer, primary_key=True, index=True)

    # Scope
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)
    recipient_domain = Column(String(255), index=True)  # company.com
    recipient_industry = Column(String(100))  # Technology, Finance, etc.

    # Time window
    day_of_week = Column(Integer, nullable=False)  # 0=Monday, 6=Sunday
    hour_of_day = Column(Integer, nullable=False)  # 0-23
    timezone = Column(String(50), default="UTC")

    # Aggregated stats
    emails_sent = Column(Integer, default=0)
    emails_opened = Column(Integer, default=0)
    emails_clicked = Column(Integer, default=0)
    emails_replied = Column(Integer, default=0)

    # Calculated rates
    open_rate = Column(Float, default=0.0)
    click_rate = Column(Float, default=0.0)
    reply_rate = Column(Float, default=0.0)

    # Timing metrics
    avg_time_to_open_hours = Column(Float)  # Average hours until opened
    avg_time_to_reply_hours = Column(Float)  # Average hours until reply

    # Confidence
    sample_size = Column(Integer, default=0)
    confidence_level = Column(Float, default=0.0)  # 0-1, based on sample size

    # Last updated
    last_email_at = Column(DateTime)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    candidate = relationship("Candidate", backref="send_time_analytics")

    __table_args__ = (
        UniqueConstraint('candidate_id', 'recipient_domain', 'day_of_week', 'hour_of_day',
                        name='unique_send_time_window'),
        Index('ix_send_time_domain_day_hour', 'recipient_domain', 'day_of_week', 'hour_of_day'),
    )

    def calculate_rates(self):
        """Recalculate rates based on current counts"""
        if self.emails_sent > 0:
            self.open_rate = self.emails_opened / self.emails_sent
            self.click_rate = self.emails_clicked / self.emails_sent
            self.reply_rate = self.emails_replied / self.emails_sent
            self.sample_size = self.emails_sent
            # Confidence increases with sample size (simplified)
            self.confidence_level = min(1.0, self.emails_sent / 100)

    def to_dict(self) -> dict:
        """Convert to dictionary for API responses"""
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        return {
            "id": self.id,
            "recipient_domain": self.recipient_domain,
            "day_of_week": self.day_of_week,
            "day_name": day_names[self.day_of_week] if 0 <= self.day_of_week <= 6 else None,
            "hour_of_day": self.hour_of_day,
            "emails_sent": self.emails_sent,
            "open_rate": round(self.open_rate * 100, 1) if self.open_rate else 0,
            "reply_rate": round(self.reply_rate * 100, 1) if self.reply_rate else 0,
            "avg_time_to_open_hours": round(self.avg_time_to_open_hours, 1) if self.avg_time_to_open_hours else None,
            "confidence_level": round(self.confidence_level * 100, 0) if self.confidence_level else 0,
        }


# ============= BEHAVIORAL BRANCHING MODELS =============

class SequenceBranch(Base):
    """
    Conditional branches for behavioral follow-up sequences.

    The killer feature from Smartlead - allows sequences to branch
    based on recipient behavior:

    Examples:
    - IF opened BUT NOT replied → Send value-add sequence
    - IF clicked link → Send case study sequence
    - IF replied with objection → Send objection handling sequence
    - IF out-of-office → Pause and resume after return date
    """
    __tablename__ = "sequence_branches"

    id = Column(Integer, primary_key=True, index=True)

    # Source (where the branch originates)
    source_step_id = Column(Integer, ForeignKey("follow_up_steps.id", ondelete="CASCADE"), nullable=False, index=True)

    # Condition
    condition_type = Column(SQLEnum(BranchConditionType), nullable=False)
    condition_operator = Column(String(20), default="equals")  # equals, contains, greater_than, sentiment_is
    condition_value = Column(Text)  # Optional value for comparison (keyword, sentiment, etc.)

    # Wait time before evaluating condition
    wait_hours = Column(Integer, default=24)  # Hours to wait before checking condition

    # Target (where to branch to)
    target_step_id = Column(Integer, ForeignKey("follow_up_steps.id", ondelete="SET NULL"), nullable=True)
    target_sequence_id = Column(Integer, ForeignKey("follow_up_sequences.id", ondelete="SET NULL"), nullable=True)

    # Delay before executing target
    delay_hours = Column(Integer, default=0)  # Additional delay before sending target

    # Branch metadata
    name = Column(String(200))  # Friendly name: "Opened but didn't reply"
    description = Column(Text)
    priority = Column(Integer, default=0)  # Higher priority branches evaluated first

    # Status
    is_active = Column(Boolean, default=True)

    # Stats
    times_triggered = Column(Integer, default=0)
    times_converted = Column(Integer, default=0)  # Led to reply
    conversion_rate = Column(Float, default=0.0)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    source_step = relationship("FollowUpStep", foreign_keys=[source_step_id], backref="outgoing_branches")
    target_step = relationship("FollowUpStep", foreign_keys=[target_step_id], backref="incoming_branches")
    target_sequence = relationship("FollowUpSequence", backref="incoming_branches")

    __table_args__ = (
        Index('ix_branch_source_condition', 'source_step_id', 'condition_type'),
    )

    def to_dict(self) -> dict:
        """Convert to dictionary for API responses"""
        return {
            "id": self.id,
            "source_step_id": self.source_step_id,
            "condition_type": self.condition_type.value if self.condition_type else None,
            "condition_operator": self.condition_operator,
            "condition_value": self.condition_value,
            "wait_hours": self.wait_hours,
            "target_step_id": self.target_step_id,
            "target_sequence_id": self.target_sequence_id,
            "delay_hours": self.delay_hours,
            "name": self.name,
            "is_active": self.is_active,
            "times_triggered": self.times_triggered,
            "conversion_rate": round(self.conversion_rate * 100, 1) if self.conversion_rate else 0,
        }


# ============= INTENT DETECTION MODELS =============

class ReplyIntent(Base):
    """
    Intent detection results from reply analysis.

    When a recipient replies, NLP analyzes the content to detect:
    - Intent (interested, objection, OOO, etc.)
    - Sentiment
    - Key information (return date for OOO, meeting times, etc.)

    This enables:
    - Automatic sequence branching
    - AI reply generation
    - Analytics and reporting
    """
    __tablename__ = "reply_intents"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign keys
    email_id = Column(Integer, ForeignKey("follow_up_emails.id", ondelete="CASCADE"), nullable=True, index=True)
    campaign_id = Column(Integer, ForeignKey("follow_up_campaigns.id", ondelete="CASCADE"), nullable=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Reply content
    reply_subject = Column(String(500))
    reply_content = Column(Text, nullable=False)
    reply_content_cleaned = Column(Text)  # Without signatures, quoted text

    # Detected intent
    detected_intent = Column(SQLEnum(IntentType), nullable=False)
    intent_confidence = Column(Float, nullable=False)  # 0-1

    # Secondary intents (if multiple detected)
    secondary_intents = Column(JSON, default=list)
    """
    [
        {"intent": "question", "confidence": 0.75},
        {"intent": "interested", "confidence": 0.60}
    ]
    """

    # Sentiment analysis
    sentiment_score = Column(Float)  # -1 (negative) to +1 (positive)
    sentiment_label = Column(String(20))  # negative, neutral, positive

    # Extracted data (context-dependent on intent)
    extracted_data = Column(JSON, default=dict)
    """
    For OUT_OF_OFFICE:
    {
        "return_date": "2025-02-10",
        "backup_contact": "jane@company.com",
        "auto_reply_detected": true
    }

    For MEETING_REQUEST:
    {
        "suggested_times": ["Monday 2pm", "Tuesday 10am"],
        "meeting_type": "call",
        "duration_minutes": 30
    }

    For OBJECTION:
    {
        "objection_type": "budget",
        "objection_text": "We don't have budget right now"
    }
    """

    # Keywords detected
    keywords_detected = Column(JSON, default=list)  # ["budget", "not interested", "call me"]

    # Model info
    model_used = Column(String(100))  # "gpt-4", "bert-intent-classifier"
    processing_time_ms = Column(Integer)

    # Action taken (if any)
    action_triggered = Column(String(100))  # "branch_to_objection_sequence", "pause_campaign"
    action_timestamp = Column(DateTime)

    # Human review
    human_reviewed = Column(Boolean, default=False)
    human_corrected_intent = Column(SQLEnum(IntentType))
    reviewer_notes = Column(Text)

    # Timestamps
    detected_at = Column(DateTime, default=datetime.utcnow)
    reply_received_at = Column(DateTime)

    # Relationships
    campaign = relationship("FollowUpCampaign", backref="reply_intents")
    candidate = relationship("Candidate", backref="reply_intents")

    __table_args__ = (
        Index('ix_reply_intent_campaign_intent', 'campaign_id', 'detected_intent'),
    )

    def to_dict(self) -> dict:
        """Convert to dictionary for API responses"""
        return {
            "id": self.id,
            "email_id": self.email_id,
            "campaign_id": self.campaign_id,
            "detected_intent": self.detected_intent.value if self.detected_intent else None,
            "intent_confidence": round(self.intent_confidence * 100, 1) if self.intent_confidence else 0,
            "secondary_intents": self.secondary_intents,
            "sentiment_score": round(self.sentiment_score, 2) if self.sentiment_score else None,
            "sentiment_label": self.sentiment_label,
            "extracted_data": self.extracted_data,
            "keywords_detected": self.keywords_detected,
            "action_triggered": self.action_triggered,
            "human_reviewed": self.human_reviewed,
            "detected_at": self.detected_at.isoformat() if self.detected_at else None,
        }

    @property
    def is_positive(self) -> bool:
        """Check if intent indicates positive engagement"""
        positive_intents = {IntentType.INTERESTED, IntentType.MEETING_REQUEST, IntentType.CURIOUS}
        return self.detected_intent in positive_intents

    @property
    def requires_action(self) -> bool:
        """Check if intent requires follow-up action"""
        action_intents = {
            IntentType.INTERESTED, IntentType.MEETING_REQUEST,
            IntentType.QUESTION, IntentType.OBJECTION
        }
        return self.detected_intent in action_intents

    @property
    def should_stop_sequence(self) -> bool:
        """Check if intent indicates sequence should stop"""
        stop_intents = {
            IntentType.NOT_INTERESTED, IntentType.UNSUBSCRIBE,
            IntentType.INTERESTED, IntentType.MEETING_REQUEST
        }
        return self.detected_intent in stop_intents
