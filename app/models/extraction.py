"""
ULTRA PRO MAX EXTRACTION ENGINE - Database Models
GOD-TIER data extraction system with multi-layer scraping
"""

from sqlalchemy import (
    Column, Integer, String, Text, Float, Boolean, DateTime, JSON,
    ForeignKey, Enum as SQLEnum
)
from sqlalchemy.orm import relationship
from datetime import datetime
import enum

from app.core.database import Base


class SectorEnum(str, enum.Enum):
    """Target sectors for data extraction"""
    CLIENTS = "clients"
    COMPANIES = "companies"
    RECRUITERS = "recruiters"
    CUSTOMERS = "customers"


class JobStatusEnum(str, enum.Enum):
    """Extraction job status"""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ExtractionStageEnum(str, enum.Enum):
    """Multi-layer extraction stages"""
    DISCOVERY = "discovery"           # Layer 0: Finding sources
    FETCHING = "fetching"             # Layer 1: Static page scraping
    RENDERING = "rendering"           # Layer 3: JavaScript rendering
    PARSING = "parsing"               # Extracting structured data
    ENRICHMENT = "enrichment"         # Layer 4: API enrichment
    VALIDATION = "validation"         # Data quality checks
    STORAGE = "storage"               # Saving to database


class ExtractionJob(Base):
    """
    Main extraction job tracking
    Manages the complete lifecycle of a data extraction operation
    """
    __tablename__ = "extraction_jobs"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id"), nullable=False, index=True)

    # Job Configuration
    sector = Column(SQLEnum(SectorEnum), nullable=False, index=True)
    status = Column(SQLEnum(JobStatusEnum), default=JobStatusEnum.PENDING, index=True)

    # Sources & Filters (JSON for flexibility)
    sources = Column(JSON, nullable=False)  # {"urls": [...], "files": [...], "directories": [...]}
    filters = Column(JSON, default={})      # {"job_titles": [...], "locations": [...], "industry": ...}
    options = Column(JSON, default={})      # {"depth": 3, "follow_external": false, "use_playwright": true, "rate_limit": 10}

    # Progress Tracking
    total_sources = Column(Integer, default=0)
    processed_sources = Column(Integer, default=0)
    total_records = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    duplicate_count = Column(Integer, default=0)

    # Timing
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    estimated_completion = Column(DateTime, nullable=True)
    duration_seconds = Column(Integer, default=0)

    # Results
    result_file_path = Column(String(500), nullable=True)  # Path to exported Excel file

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    candidate = relationship("Candidate", back_populates="extraction_jobs")
    results = relationship("ExtractionResult", back_populates="job", cascade="all, delete-orphan")
    progress_updates = relationship("ExtractionProgress", back_populates="job", cascade="all, delete-orphan")


class ExtractionResult(Base):
    """
    Individual extracted records
    Flexible schema to handle different data types (clients, companies, recruiters, customers)
    """
    __tablename__ = "extraction_results"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("extraction_jobs.id"), nullable=False, index=True)

    # Extracted Data (flexible JSON schema)
    data = Column(JSON, nullable=False)  # {"name": "...", "email": "...", "title": "...", "company": "...", ...}

    # Metadata
    source_url = Column(String(1000), nullable=True)  # Origin URL
    extraction_layer = Column(Integer, default=1)     # Which layer extracted this (1-7)

    # Quality Metrics
    quality_score = Column(Float, default=0.0)        # 0.0 - 1.0
    confidence_score = Column(Float, default=0.0)     # 0.0 - 1.0
    completeness_score = Column(Float, default=0.0)   # 0.0 - 1.0 (% of fields populated)

    # Flags
    is_duplicate = Column(Boolean, default=False)
    is_validated = Column(Boolean, default=False)
    enriched_via_api = Column(Boolean, default=False)

    # API Enrichment
    api_source = Column(String(100), nullable=True)   # "apollo", "hunter", "clearbit", etc.
    api_enrichment_data = Column(JSON, default={})

    # Timestamps
    extracted_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    job = relationship("ExtractionJob", back_populates="results")


class ExtractionProgress(Base):
    """
    Real-time progress tracking for SSE streaming
    Stores granular progress updates for live dashboard
    """
    __tablename__ = "extraction_progress"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("extraction_jobs.id"), nullable=False, index=True)

    # Progress Details
    stage = Column(SQLEnum(ExtractionStageEnum), nullable=False)
    message = Column(Text, nullable=False)
    progress_percent = Column(Float, default=0.0)  # 0.0 - 100.0

    # Current Operation
    current_source = Column(String(1000), nullable=True)  # URL/file being processed
    current_layer = Column(Integer, default=1)            # Current extraction layer

    # Metrics
    records_extracted = Column(Integer, default=0)
    records_validated = Column(Integer, default=0)
    errors_encountered = Column(Integer, default=0)

    # Network Metrics (for throughput visualization)
    requests_made = Column(Integer, default=0)
    bytes_downloaded = Column(Integer, default=0)
    avg_response_time_ms = Column(Float, default=0.0)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    # Relationships
    job = relationship("ExtractionJob", back_populates="progress_updates")


class ExtractionTemplate(Base):
    """
    Saved extraction templates for reuse
    Users can save and share extraction configurations
    """
    __tablename__ = "extraction_templates"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id"), nullable=False, index=True)

    # Template Details
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    sector = Column(SQLEnum(SectorEnum), nullable=False)

    # Configuration
    filters = Column(JSON, default={})
    options = Column(JSON, default={})

    # Sharing
    is_public = Column(Boolean, default=False)
    usage_count = Column(Integer, default=0)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    candidate = relationship("Candidate", back_populates="extraction_templates")
