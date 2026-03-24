"""
ULTRA PRO MAX EXTRACTION ENGINE - Pydantic Schemas
GOD-TIER API request/response models for data extraction
"""

from typing import List, Dict, Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum


# ============================================================================
# ENUMS
# ============================================================================

class SectorType(str, Enum):
    """Target sectors for data extraction"""
    CLIENTS = "clients"
    COMPANIES = "companies"
    RECRUITERS = "recruiters"
    CUSTOMERS = "customers"


class JobStatus(str, Enum):
    """Extraction job status"""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ExtractionStage(str, Enum):
    """Multi-layer extraction stages"""
    DISCOVERY = "discovery"
    FETCHING = "fetching"
    RENDERING = "rendering"
    PARSING = "parsing"
    ENRICHMENT = "enrichment"
    VALIDATION = "validation"
    STORAGE = "storage"


class ExportFormat(str, Enum):
    """Export file formats"""
    EXCEL = "excel"
    CSV = "csv"
    JSON = "json"
    GOOGLE_SHEETS = "google_sheets"


class ExtractionMode(str, Enum):
    """Extraction mode - FREE or PAID"""
    FREE = "free"
    PAID = "paid"


# ============================================================================
# DEMOGRAPHICS CONFIGURATION SCHEMAS
# ============================================================================

class DemographicsConfiguration(BaseModel):
    """Demographics targeting configuration"""
    regions: List[str] = Field(default_factory=list, description="Geographic regions")
    company_sizes: List[str] = Field(default_factory=list, description="Company size ranges")
    industries: List[str] = Field(default_factory=list, description="Target industries")
    custom_keywords: List[str] = Field(default_factory=list, description="Custom search keywords")


# ============================================================================
# API LAYER CONFIGURATION SCHEMAS (PAID MODE)
# ============================================================================

class APIProviderConfig(BaseModel):
    """Configuration for a single API provider"""
    provider: str = Field(..., description="Provider ID: google_custom_search, hunter, apollo, etc.")
    api_key: Optional[str] = Field(default=None, description="API key (stored securely)")
    enabled: bool = Field(default=True, description="Whether this layer is enabled")


class APILayerConfiguration(BaseModel):
    """API configuration for each extraction layer (PAID mode)"""
    search_discovery: Optional[APIProviderConfig] = None
    contact_database: Optional[APIProviderConfig] = None
    email_verification: Optional[APIProviderConfig] = None
    company_enrichment: Optional[APIProviderConfig] = None
    social_data: Optional[APIProviderConfig] = None
    ai_extraction: Optional[APIProviderConfig] = None
    document_parsing: Optional[APIProviderConfig] = None
    captcha_solving: Optional[APIProviderConfig] = None


# ============================================================================
# SOURCE CONFIGURATION SCHEMAS
# ============================================================================

class URLSource(BaseModel):
    """URLs to scrape"""
    urls: List[str] = Field(..., description="List of URLs to extract data from")

    @validator('urls')
    def validate_urls(cls, v):
        if not v:
            raise ValueError("At least one URL is required")
        return v


class FileSource(BaseModel):
    """Files to parse"""
    file_paths: List[str] = Field(..., description="Paths to CSV/Excel files")


class DirectorySource(BaseModel):
    """API directories to query"""
    service: str = Field(..., description="apollo, hunter, crunchbase, yellow_pages, etc.")
    query: Dict[str, Any] = Field(..., description="Directory-specific query parameters")


class SourceConfiguration(BaseModel):
    """Complete source configuration"""
    urls: Optional[List[str]] = Field(default=None, description="URLs to scrape")
    files: Optional[List[str]] = Field(default=None, description="File paths to import")
    directories: Optional[List[DirectorySource]] = Field(default=None, description="API directories to query")

    # Note: Validation moved to CreateExtractionJobRequest to allow empty sources in FREE mode


# ============================================================================
# FILTER SCHEMAS
# ============================================================================

class BasicFilters(BaseModel):
    """Basic search filters"""
    job_titles: Optional[List[str]] = Field(default=None, description="Target job titles")
    locations: Optional[List[str]] = Field(default=None, description="Target locations/cities")
    industries: Optional[List[str]] = Field(default=None, description="Target industries")
    company_sizes: Optional[List[str]] = Field(default=None, description="1-10, 11-50, 51-200, etc.")


class AdvancedFilters(BaseModel):
    """Advanced search filters"""
    revenue_ranges: Optional[List[str]] = Field(default=None, description="Revenue ranges")
    tech_stack: Optional[List[str]] = Field(default=None, description="Required technologies")
    funding_status: Optional[List[str]] = Field(default=None, description="Funded, bootstrapped, etc.")
    keywords: Optional[List[str]] = Field(default=None, description="Additional keywords")


class FilterConfiguration(BaseModel):
    """Complete filter configuration"""
    basic: Optional[BasicFilters] = None
    advanced: Optional[AdvancedFilters] = None


# ============================================================================
# OPTIONS SCHEMAS
# ============================================================================

class ExtractionOptions(BaseModel):
    """Extraction behavior options"""
    depth: int = Field(default=3, ge=1, le=7, description="Scraping depth (1-7 layers)")
    follow_external: bool = Field(default=False, description="Follow external domain links")
    use_playwright: bool = Field(default=True, description="Use headless browser for JS rendering")
    rate_limit: int = Field(default=10, ge=1, le=100, description="Requests per second per domain")
    max_records: Optional[int] = Field(default=5000, description="Maximum records to extract")
    timeout_seconds: int = Field(default=300, description="Overall extraction timeout")
    use_proxies: bool = Field(default=False, description="Use proxy rotation")
    respect_robots_txt: bool = Field(default=True, description="Respect robots.txt rules")


# ============================================================================
# CREATE JOB REQUEST/RESPONSE
# ============================================================================

class CreateExtractionJobRequest(BaseModel):
    """Request to create extraction job"""
    sector: SectorType = Field(..., description="Target sector")
    mode: ExtractionMode = Field(default=ExtractionMode.FREE, description="Extraction mode: FREE or PAID")
    demographics: Optional[DemographicsConfiguration] = Field(default=None, description="Target demographics")
    api_config: Optional[APILayerConfiguration] = Field(default=None, description="API configuration (PAID mode only)")
    sources: Optional[SourceConfiguration] = Field(default=None, description="Data sources (optional for FREE mode)")
    filters: Optional[FilterConfiguration] = Field(default=None, description="Search filters")
    options: Optional[ExtractionOptions] = Field(default_factory=ExtractionOptions, description="Extraction options")

    @validator('sources')
    def validate_sources(cls, v, values):
        """Sources optional for FREE mode, required for PAID mode if no api_config"""
        mode = values.get('mode', ExtractionMode.FREE)
        api_config = values.get('api_config')
        if mode == ExtractionMode.PAID and not v and not api_config:
            raise ValueError("Either sources or api_config required for PAID mode")
        return v


class ExtractionJobResponse(BaseModel):
    """Extraction job summary"""
    id: int
    sector: SectorType
    mode: Optional[ExtractionMode] = ExtractionMode.FREE
    status: JobStatus
    demographics: Optional[Dict[str, Any]] = None
    api_config: Optional[Dict[str, Any]] = None
    sources: Optional[Dict[str, Any]] = None
    filters: Optional[Dict[str, Any]] = None
    options: Optional[Dict[str, Any]] = None

    # Progress
    total_sources: int
    processed_sources: int
    total_records: int
    success_count: int
    error_count: int
    duplicate_count: int

    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    estimated_completion: Optional[datetime] = None
    duration_seconds: int

    # Results
    result_file_path: Optional[str] = None

    # Metadata
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ============================================================================
# PROGRESS UPDATE SCHEMAS (SSE)
# ============================================================================

class ProgressUpdate(BaseModel):
    """Real-time progress update (sent via SSE)"""
    job_id: int
    stage: ExtractionStage
    message: str
    progress_percent: float = Field(..., ge=0, le=100)

    # Current operation
    current_source: Optional[str] = None
    current_layer: int = 1

    # Metrics
    records_extracted: int = 0
    records_validated: int = 0
    errors_encountered: int = 0

    # Network metrics
    requests_made: int = 0
    bytes_downloaded: int = 0
    avg_response_time_ms: float = 0.0

    # ETA
    estimated_time_remaining: Optional[int] = None  # seconds

    # Type
    type: str = "progress"  # or "complete", "error"


# ============================================================================
# EXTRACTION RESULT SCHEMAS
# ============================================================================

class ExtractionResultData(BaseModel):
    """Flexible extracted data (varies by sector)"""
    # Common fields
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    company: Optional[str] = None
    title: Optional[str] = None
    location: Optional[str] = None
    linkedin_url: Optional[str] = None

    # Additional fields stored in extra
    extra: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional extracted fields")

    class Config:
        extra = "allow"  # Allow additional fields


class ExtractionResultResponse(BaseModel):
    """Single extraction result"""
    id: int
    job_id: int
    data: Dict[str, Any]  # Flexible schema
    source_url: Optional[str] = None
    extraction_layer: int

    # Quality
    quality_score: float = Field(..., ge=0, le=1)
    confidence_score: float = Field(..., ge=0, le=1)
    completeness_score: float = Field(..., ge=0, le=1)

    # Flags
    is_duplicate: bool
    is_validated: bool
    enriched_via_api: bool
    api_source: Optional[str] = None

    extracted_at: datetime

    class Config:
        from_attributes = True


class PaginatedResultsResponse(BaseModel):
    """Paginated extraction results"""
    job_id: int
    results: List[ExtractionResultResponse]
    total: int
    page: int
    page_size: int
    has_more: bool


# ============================================================================
# EXPORT SCHEMAS
# ============================================================================

class ExportRequest(BaseModel):
    """Request to export results"""
    format: ExportFormat = Field(..., description="Export format")
    include_metadata: bool = Field(default=True, description="Include quality scores and metadata")
    filters: Optional[Dict[str, Any]] = Field(default=None, description="Filter which results to export")


class ExportResponse(BaseModel):
    """Export result"""
    file_path: str
    format: ExportFormat
    record_count: int
    file_size_bytes: int
    download_url: Optional[str] = None


# ============================================================================
# INTEGRATION SCHEMAS
# ============================================================================

class ImportToRecipientsRequest(BaseModel):
    """Request to import results to Recipients table"""
    job_id: int
    create_group: bool = Field(default=True, description="Create recipient group")
    group_name: Optional[str] = Field(default=None, description="Group name (auto-generated if None)")
    filter_duplicates: bool = Field(default=True, description="Skip duplicates")


class ImportToRecipientsResponse(BaseModel):
    """Import result"""
    imported_count: int
    skipped_count: int
    group_id: Optional[int] = None
    group_name: Optional[str] = None
    errors: List[str] = []


# ============================================================================
# JOB CONTROL SCHEMAS
# ============================================================================

class JobControlResponse(BaseModel):
    """Response for pause/resume/stop/cancel actions"""
    job_id: int
    status: JobStatus
    message: str


# ============================================================================
# EXTRACTION TEMPLATE SCHEMAS
# ============================================================================

class CreateExtractionTemplateRequest(BaseModel):
    """Save extraction configuration as template"""
    name: str = Field(..., max_length=200)
    description: Optional[str] = None
    sector: SectorType
    filters: Dict[str, Any]
    options: Dict[str, Any]
    is_public: bool = Field(default=False, description="Share with community")


class ExtractionTemplateResponse(BaseModel):
    """Extraction template"""
    id: int
    name: str
    description: Optional[str] = None
    sector: SectorType
    filters: Dict[str, Any]
    options: Dict[str, Any]
    is_public: bool
    usage_count: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ============================================================================
# APOLLO.IO / HUNTER.IO API SCHEMAS
# ============================================================================

class ApolloSearchRequest(BaseModel):
    """Apollo.io people search request"""
    job_titles: List[str]
    locations: List[str]
    company_sizes: Optional[List[str]] = None
    industries: Optional[List[str]] = None
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=100)


class HunterVerifyRequest(BaseModel):
    """Hunter.io email verification request"""
    email: str = Field(..., description="Email to verify")


class HunterVerifyResponse(BaseModel):
    """Hunter.io verification result"""
    email: str
    is_valid: bool
    is_disposable: bool
    is_role: bool
    is_free: bool
    score: int = Field(..., ge=0, le=100)


# ============================================================================
# STATISTICS & ANALYTICS SCHEMAS
# ============================================================================

class JobStatistics(BaseModel):
    """Extraction job statistics"""
    total_jobs: int
    running_jobs: int
    completed_jobs: int
    failed_jobs: int
    total_records_extracted: int
    avg_extraction_time_seconds: float
    success_rate: float = Field(..., ge=0, le=1)


class LayerPerformance(BaseModel):
    """Performance by extraction layer"""
    layer: int
    records_extracted: int
    avg_time_ms: float
    success_rate: float


class ExtractionAnalytics(BaseModel):
    """Comprehensive analytics"""
    statistics: JobStatistics
    layer_performance: List[LayerPerformance]
    top_sources: List[Dict[str, Any]]  # URLs with most records
    quality_distribution: Dict[str, int]  # high/medium/low counts
