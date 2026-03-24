"""
Application Configuration Management
"""
from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings"""

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="ignore"
    )

    # Application
    PROJECT_NAME: str = "Outbrew API"
    VERSION: str = "1.0.0"
    DESCRIPTION: str = """
## Outbrew — AI-Powered Cold Email Platform API

Brew outreach that actually converts. A comprehensive outreach management system with intelligent email automation.

### Features:
- **Campaign Management**: Create, track, and manage outreach campaigns
- **Email Automation**: Send personalized cold emails with rate limiting
- **Email Warming**: Gradual domain reputation building
- **Contact Extraction**: AI-powered lead discovery and enrichment
- **Template System**: Customizable email templates with variable substitution
- **Analytics**: Real-time statistics and performance tracking
- **Notifications**: In-app notification system

### Authentication:
All protected endpoints require a JWT Bearer token.
Obtain a token via `/api/v1/auth/login` or `/api/v1/auth/login/json`.

### Rate Limiting:
- Login: 5 attempts per minute
- Registration: 3 per minute
- Password change: 3 per hour
- API reads: 100 per minute
- API writes: 30 per minute
"""
    ENVIRONMENT: str = "development"
    DEBUG: bool = False

    # API
    API_V1_PREFIX: str = "/api/v1"
    BASE_URL: str = "http://localhost:8000"  # Public base URL for absolute links (emails, unsubscribe)

    # CORS - stored as string, converted to list when accessed
    CORS_ORIGINS: str = "http://localhost:3000,http://localhost:3001,http://localhost:3002,https://metaminds.store"

    def get_cors_origins(self) -> List[str]:
        """Parse CORS origins from string to list"""
        if isinstance(self.CORS_ORIGINS, str):
            return [origin.strip() for origin in self.CORS_ORIGINS.split(",")]
        return self.CORS_ORIGINS

    # Database
    DATABASE_URL: str = ""  # Full connection string (Neon, etc.) — takes priority
    POSTGRES_SERVER: str = ""
    POSTGRES_USER: str = ""
    POSTGRES_PASSWORD: str = ""
    POSTGRES_DB: str = ""
    POSTGRES_PORT: int = 5432

    @property
    def database_url(self) -> str:
        # Prefer DATABASE_URL if set (Neon, Railway, etc.)
        if self.DATABASE_URL:
            return self.DATABASE_URL
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # Security
    SECRET_KEY: str = ""
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30  # Short-lived access tokens (30 minutes)
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7     # Long-lived refresh tokens (7 days)
    ENCRYPTION_KEY: str = ""  # Separate key for data encryption; falls back to SECRET_KEY
    WEBHOOK_SECRET: str = ""  # Secret for authenticating incoming webhooks
    ALLOWED_REDIRECT_DOMAINS: str = ""  # Comma-separated allowlist for click-tracking redirects

    # Redis (for caching, distributed rate limiting, and token blacklist)
    REDIS_URL: str = ""  # Redis connection URL (empty = use in-memory for dev)
    REDIS_ENABLED: bool = False  # Enable/disable Redis caching
    REDIS_CACHE_TTL_DEFAULT: int = 3600  # Default cache TTL in seconds (1 hour)
    REDIS_CACHE_TTL_SHORT: int = 300  # Short cache TTL (5 minutes)
    REDIS_CACHE_TTL_LONG: int = 86400  # Long cache TTL (24 hours)

    # Email - Pragya
    PRAGYA_EMAIL: str = "pragyapandey2709@gmail.com"
    PRAGYA_PASSWORD: str = "bicu canf ksgd swzo"
    PRAGYA_RESUME_PATH: str = "resumes/Pragya_Pandey_Resume.pdf"

    # Email - Aniruddh
    ANIRUDDH_EMAIL: str = "atreyaniruddh@gmail.com"
    ANIRUDDH_PASSWORD: str = "your-password-here"
    ANIRUDDH_RESUME_PATH: str = "resumes/ANIRUDDH_ATREY.pdf"

    # SMTP
    SMTP_HOST: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USE_TLS: bool = True

    # AI - API Key from environment
    ANTHROPIC_API_KEY: str = ""
    OPENAI_API_KEY: str = ""

    # Data Enrichment APIs (Layer 5)
    APOLLO_API_KEY: str = ""  # Apollo.io for people search & enrichment
    HUNTER_API_KEY: str = ""  # Hunter.io for email verification & finding
    CLEARBIT_API_KEY: str = ""  # Clearbit for company/person enrichment
    PROXYCURL_API_KEY: str = ""  # Proxycurl for legal LinkedIn scraping
    BUILTWITH_API_KEY: str = ""  # BuiltWith for tech stack detection

    # Pagination
    DEFAULT_PAGE_SIZE: int = 20
    MAX_PAGE_SIZE: int = 100

    # File Storage & Document Management
    STORAGE_BASE_PATH: str = "storage"
    EXPORTS_DIR: str = "exports"  # Directory for extraction exports
    MAX_FILE_SIZE_MB: int = 10  # Maximum file size per upload
    MAX_STORAGE_QUOTA_MB: int = 500  # Default quota per user
    ALLOWED_RESUME_EXTENSIONS: List[str] = [".pdf", ".doc", ".docx"]
    ALLOWED_ATTACHMENT_EXTENSIONS: List[str] = [".pdf", ".doc", ".docx", ".png", ".jpg", ".jpeg", ".txt", ".zip"]

    @property
    def max_file_size_bytes(self) -> int:
        """Convert MB to bytes"""
        return self.MAX_FILE_SIZE_MB * 1024 * 1024

    @property
    def max_storage_quota_bytes(self) -> int:
        """Convert MB to bytes"""
        return self.MAX_STORAGE_QUOTA_MB * 1024 * 1024


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
