"""
Database Configuration
Supports both SQLite (for local testing) and PostgreSQL (for production)
"""
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from typing import Generator

from app.core.config import settings

logger = logging.getLogger(__name__)

# Database connection timeout in seconds
DB_CONNECT_TIMEOUT = 30

# Use SQLite for local development (no PostgreSQL required)
# DATABASE_URL format: sqlite:///./outbrew.db
# For PostgreSQL: postgresql://user:password@localhost/dbname

# Determine database URL
if settings.DATABASE_URL:
    # Direct connection string (Neon, Railway, etc.)
    database_url = settings.DATABASE_URL
    logger.info("[DATABASE] Using DATABASE_URL connection string")
elif settings.POSTGRES_SERVER and settings.POSTGRES_SERVER != "localhost":
    # PostgreSQL from individual vars
    database_url = settings.database_url
    logger.info("[DATABASE] Using PostgreSQL database")
else:
    # SQLite (default for local development)
    database_url = "sqlite:///./outbrew.db"
    logger.info(f"[DATABASE] Using SQLite database: {database_url}")

# Connection pooling configuration
# SQLite doesn't support pooling the same way as PostgreSQL
if "sqlite" in database_url:
    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        connect_args={
            "check_same_thread": False,
            "timeout": DB_CONNECT_TIMEOUT  # SQLite connection timeout
        },
        echo=settings.DEBUG,
    )
    logger.info("[DATABASE] SQLite engine created (no connection pooling)")
else:
    # PostgreSQL with proper connection pooling
    pg_connect_args = {"connect_timeout": DB_CONNECT_TIMEOUT}
    # Neon and other serverless PostgreSQL require SSL
    if settings.DATABASE_URL and ("neon.tech" in settings.DATABASE_URL or "sslmode=require" in settings.DATABASE_URL):
        pg_connect_args["sslmode"] = "require"
        pool_size = 5  # Neon free tier has limited connections
        max_overflow = 5
        logger.info("[DATABASE] Neon PostgreSQL detected — using SSL + smaller pool")
    else:
        pool_size = 10
        max_overflow = 20

    engine = create_engine(
        database_url,
        pool_pre_ping=True,  # Verify connections before use
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=30,  # Seconds to wait for a connection from pool
        pool_recycle=1800,  # Recycle connections after 30 minutes
        connect_args=pg_connect_args,
        echo=settings.DEBUG,
    )
    logger.info(f"[DATABASE] PostgreSQL engine created with pool_size={pool_size}, max_overflow={max_overflow}, connect_timeout={DB_CONNECT_TIMEOUT}s")

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def import_models():
    """Import all models to register them with SQLAlchemy Base"""
    # Import all models so SQLAlchemy can detect them
    from app.models.candidate import Candidate  # noqa: F401
    from app.models.company import Company  # noqa: F401
    from app.models.application import Application  # noqa: F401
    from app.models.email_log import EmailLog  # noqa: F401
    from app.models.resume import ResumeVersion  # noqa: F401
    from app.models.email_template import EmailTemplate  # noqa: F401
    from app.models.application_history import ApplicationHistory, ApplicationNote, ApplicationAttachment  # noqa: F401
    from app.models.email_warming import EmailWarmingConfig, EmailWarmingDailyLog  # noqa: F401
    from app.models.rate_limiting import RateLimitConfig, RateLimitUsageLog  # noqa: F401
    from app.models.notification import Notification  # noqa: F401
    from app.models.scheduled_email import ScheduledEmail, SendTimePreference  # noqa: F401
    from app.models.warmup_health import WarmupHealthScore, WarmupHealthAlert, DomainReputation, WarmupMilestone  # noqa: F401
    from app.models.company_intelligence import (  # noqa: F401
        CompanyProject, CompanyResearchCache, SkillMatch,
        PersonalizedEmailDraft, CandidateSkillProfile
    )
    from app.models.follow_up import (  # noqa: F401
        FollowUpSequence, FollowUpStep, FollowUpCampaign,
        FollowUpEmail, FollowUpLog, CandidateProfile
    )
    from app.models.follow_up_ml import (  # noqa: F401
        FollowUpPrediction, SendTimeAnalytics, AIGeneratedContent,
        SequenceBranch, ReplyIntent
    )
    from app.models.email_inbox import (  # noqa: F401
        EmailAccount, EmailMessage, EmailThread, StorageQuota
    )
    from app.models.template_marketplace import (  # noqa: F401
        PublicTemplate, TemplateRating, TemplateReview,
        TemplateUsageReport, TemplateFavorite, TemplateCollection
    )
    from app.models.documents import (  # noqa: F401
        ParsedResume, CompanyInfoDoc
    )
    # Extraction models
    from app.models.extraction import (  # noqa: F401
        ExtractionJob, ExtractionResult, ExtractionProgress, ExtractionTemplate
    )
    # Recipient/Group models
    from app.models.recipient import Recipient  # noqa: F401
    from app.models.recipient_group import RecipientGroup  # noqa: F401
    from app.models.group_recipient import GroupRecipient  # noqa: F401
    from app.models.group_campaign import GroupCampaign  # noqa: F401
    from app.models.group_campaign_recipient import GroupCampaignRecipient  # noqa: F401
    # Warmup Pool models
    from app.models.warmup_pool import (  # noqa: F401
        WarmupPoolMember, WarmupConversation, InboxPlacementTest,
        BlacklistStatus, WarmupSchedule
    )
    # Enrichment models
    from app.models.enrichment_job import EnrichmentJob  # noqa: F401
    # MobiAdz Extraction models
    from app.models.mobiadz_job import MobiAdzJob, MobiAdzContact  # noqa: F401
    # Notification Preferences
    from app.models.notification_preference import NotificationPreference  # noqa: F401
    # Merge History
    from app.models.merge_history import MergeHistory  # noqa: F401
    # Audit Logs
    from app.models.audit_log import AuditLog  # noqa: F401
    # User Sessions
    from app.models.user_session import UserSession  # noqa: F401
    # Password Reset Tokens
    from app.models.password_reset import PasswordResetToken  # noqa: F401


def get_database_session() -> Generator[Session, None, None]:
    """Get database session with proper error handling"""
    database = None
    try:
        database = SessionLocal()
        logger.debug("[DATABASE] Session created")
        yield database
    except OperationalError as e:
        logger.error(f"[DATABASE] Connection error: {e}")
        raise
    except SQLAlchemyError as e:
        logger.error(f"[DATABASE] SQLAlchemy error: {e}")
        raise
    finally:
        if database:
            database.close()
            logger.debug("[DATABASE] Session closed")


# Alias for convenience (used in API endpoints)
def get_db() -> Generator[Session, None, None]:
    """Get database session (alias for get_database_session)"""
    database = None
    try:
        database = SessionLocal()
        yield database
    except OperationalError as e:
        logger.error(f"[DATABASE] Connection error in get_db: {e}")
        raise
    except SQLAlchemyError as e:
        logger.error(f"[DATABASE] SQLAlchemy error in get_db: {e}")
        raise
    finally:
        if database:
            database.close()


def check_database_health() -> dict:
    """
    Check database connectivity and health.

    Returns:
        dict with 'healthy' bool and 'message' string
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.debug("[DATABASE] Health check passed")
        return {"healthy": True, "message": "Database connection successful"}
    except OperationalError as e:
        logger.error(f"[DATABASE] Health check failed: {e}")
        return {"healthy": False, "message": f"Database connection failed: {str(e)}"}
    except Exception as e:
        logger.error(f"[DATABASE] Health check error: {e}")
        return {"healthy": False, "message": f"Database error: {str(e)}"}


def init_db():
    """Initialize database - creates all tables"""
    logger.info("[DATABASE] Importing models...")
    import_models()
    logger.info("[DATABASE] Creating database tables...")
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("[DATABASE] Database tables created successfully!")
    except SQLAlchemyError as e:
        logger.error(f"[DATABASE] Failed to create tables: {e}")
        raise
