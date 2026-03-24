"""
TheMobiAdz Extraction API Endpoints V2.0

API for extracting app/game/e-commerce company data.
FIXED: Database persistence - results survive server restarts.
Uses in-memory cache for running jobs (speed) + SQLite/PostgreSQL for completed jobs (persistence).
"""

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from app.core.rate_limiter import limiter
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from collections import deque
from datetime import datetime, timezone
from sqlalchemy.orm import Session
import asyncio
import logging
import uuid

from app.services.mobiadz_extraction_engine import (
    MobiAdzExtractionEngine,
    MobiAdzConfig,
    Demographic,
    ProductCategory,
    DEMOGRAPHIC_COUNTRIES,
    CATEGORY_KEYWORDS,
    ExtractionCancelled,
)
from app.core.database import SessionLocal
from app.models.mobiadz_job import MobiAdzJob, MobiAdzContact, MobiAdzDomainBounceHistory
from app.api.dependencies import get_current_candidate
from app.models.candidate import Candidate

logger = logging.getLogger(__name__)

router = APIRouter()

# === IN-MEMORY CACHE for running jobs (speed during real-time updates) ===
# Only stores actively-running jobs. Completed jobs are in the database.
active_jobs: Dict[str, Dict[str, Any]] = {}

# === ENGINE REFERENCES for running jobs (needed for cancellation) ===
active_engines: Dict[str, MobiAdzExtractionEngine] = {}

# Memory cleanup delay: seconds to keep completed jobs in memory before evicting
_MEMORY_CLEANUP_DELAY_SECONDS = 300  # 5 minutes


async def _schedule_memory_cleanup(job_id: str):
    """Remove completed/failed job from memory after a delay.
    The job is already persisted in the database, so this only
    frees in-memory resources while allowing final polling."""
    await asyncio.sleep(_MEMORY_CLEANUP_DELAY_SECONDS)
    if job_id in active_jobs:
        status = active_jobs[job_id].get("status", "")
        if status in ("completed", "failed", "cancelled"):
            del active_jobs[job_id]
            logger.debug(f"[MEMORY] Evicted completed job {job_id[:8]} from memory")


def _get_db_session() -> Session:
    """Get a standalone database session (for background tasks)."""
    return SessionLocal()


def _persist_job_to_db(job_id: str, job_data: Dict[str, Any]):
    """Persist a job and its results to the database."""
    db = _get_db_session()
    try:
        # Check if job already exists in DB
        existing = db.query(MobiAdzJob).filter(MobiAdzJob.job_id == job_id).first()

        if existing:
            # Update existing job
            existing.status = job_data.get("status", existing.status)
            existing.progress = job_data.get("progress", existing.progress)
            existing.stats = job_data.get("stats", existing.stats)
            existing.live_contacts = list(job_data.get("live_contacts", existing.live_contacts or []))
            existing.config = job_data.get("config", existing.config)
            if job_data.get("completed_at"):
                existing.completed_at = datetime.fromisoformat(job_data["completed_at"])
        else:
            # Create new job
            db_job = MobiAdzJob(
                job_id=job_id,
                status=job_data.get("status", "pending"),
                config=job_data.get("config", {}),
                progress=job_data.get("progress", {}),
                stats=job_data.get("stats", {}),
                live_contacts=list(job_data.get("live_contacts", [])),
                created_at=datetime.fromisoformat(job_data["created_at"]) if job_data.get("created_at") else datetime.now(timezone.utc),
                completed_at=datetime.fromisoformat(job_data["completed_at"]) if job_data.get("completed_at") else None,
            )
            db.add(db_job)
            db.flush()  # Get the ID

        # Persist results as individual contacts
        results = job_data.get("results", [])
        if results:
            # Clear old contacts for this job (in case of re-persist)
            db.query(MobiAdzContact).filter(MobiAdzContact.job_id == job_id).delete()

            for result in results:
                contact = MobiAdzContact(
                    job_id=job_id,
                    company_name=result.get("company_name", "Unknown"),
                    app_or_product=result.get("app_or_product"),
                    product_category=result.get("product_category"),
                    demographic=result.get("demographic"),
                    company_website=result.get("company_website"),
                    company_domain=result.get("company_domain"),
                    company_description=result.get("company_description"),
                    company_linkedin=result.get("company_linkedin"),
                    company_size=result.get("company_size"),
                    company_industry=result.get("company_industry"),
                    company_founded=result.get("company_founded"),
                    company_location=result.get("company_location"),
                    company_phones=result.get("company_phones", []),
                    contact_email=result.get("contact_email"),
                    marketing_email=result.get("marketing_email"),
                    sales_email=result.get("sales_email"),
                    support_email=result.get("support_email"),
                    press_email=result.get("press_email"),
                    playstore_url=result.get("playstore_url"),
                    appstore_url=result.get("appstore_url"),
                    people=result.get("people", []),
                    confidence_score=result.get("confidence_score", 0),
                    data_sources=result.get("data_sources", []),
                    # Email verification fields (Layer 6)
                    email_verification_status=result.get("email_verification_status", "not_verified"),
                    email_verification_confidence=result.get("email_verification_confidence", 0),
                    email_mx_valid=result.get("email_mx_valid", False),
                    email_is_disposable=result.get("email_is_disposable", False),
                    email_is_role_based=result.get("email_is_role_based", False),
                    # Layer 9 fields
                    email_sources=result.get("email_sources", {}),
                    role_engagement_score=result.get("role_engagement_score", 0.5),
                    domain_reputation_score=result.get("domain_reputation_score", 0),
                    email_freshness_score=result.get("email_freshness_score", 1.0),
                    # Layer 15 fields
                    email_warmth_score=result.get("email_warmth_score", 0),
                    domain_is_catchall=result.get("domain_is_catchall", False),
                )
                db.add(contact)

        db.commit()
        logger.info(f"[MOBIADZ-DB] Persisted job {job_id[:8]} with {len(results)} contacts")

    except Exception as e:
        db.rollback()
        logger.error(f"[MOBIADZ-DB] Failed to persist job {job_id[:8]}: {e}")
    finally:
        db.close()


def _load_job_from_db(job_id: str) -> Optional[Dict[str, Any]]:
    """Load a job from the database (for completed/historical jobs)."""
    db = _get_db_session()
    try:
        db_job = db.query(MobiAdzJob).filter(MobiAdzJob.job_id == job_id).first()
        if not db_job:
            return None

        # Load contacts
        contacts = db.query(MobiAdzContact).filter(MobiAdzContact.job_id == job_id).all()
        results = [c.to_dict() for c in contacts]

        return {
            "status": db_job.status,
            "progress": db_job.progress or {},
            "stats": db_job.stats or {},
            "results": results,
            "live_contacts": db_job.live_contacts or [],
            "created_at": db_job.created_at.isoformat() if db_job.created_at else None,
            "completed_at": db_job.completed_at.isoformat() if db_job.completed_at else None,
            "config": db_job.config or {},
        }
    except Exception as e:
        logger.error(f"[MOBIADZ-DB] Failed to load job {job_id[:8]}: {e}")
        return None
    finally:
        db.close()


def _get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Get a job from memory cache first, then fall back to database."""
    # 1. Check in-memory cache (for running jobs)
    if job_id in active_jobs:
        return active_jobs[job_id]

    # 2. Fall back to database (for completed/historical jobs)
    return _load_job_from_db(job_id)


# ==================== Pydantic Models ====================

class DemographicOption(BaseModel):
    """Demographic option for frontend"""
    value: str
    label: str
    countries: List[str]


class CategoryOption(BaseModel):
    """Category option for frontend"""
    value: str
    label: str
    keywords: List[str]


class ExtractionRequest(BaseModel):
    """Request to start extraction"""
    demographics: List[str] = Field(..., description="List of demographics to target")
    categories: List[str] = Field(..., description="List of product categories")
    use_paid_apis: bool = Field(False, description="Use paid APIs for enrichment")
    max_companies: int = Field(100, ge=10, le=5000)
    max_apps_per_category: int = Field(50, ge=10, le=500)
    website_scrape_depth: int = Field(8, ge=1, le=15)
    target_contacts: int = Field(1000, ge=100, le=5000, description="Target number of contacts to find")

    # Deduplication options
    exclude_previous_job_id: Optional[str] = Field(None, description="Exclude contacts from a previous job")
    exclude_domains: List[str] = Field(default_factory=list, description="Domains to exclude")
    exclude_emails: List[str] = Field(default_factory=list, description="Emails to exclude")

    # Advanced options
    enable_deep_osint: bool = Field(True, description="Enable deep OSINT research")
    enable_email_verification: bool = Field(True, description="Enable MX record verification")
    enable_social_scraping: bool = Field(True, description="Enable social media scraping")

    # API keys for paid mode
    hunter_api_key: Optional[str] = Field(None, description="Hunter.io API key for email discovery")
    clearbit_api_key: Optional[str] = Field(None, description="Clearbit API key for company enrichment")
    apollo_api_key: Optional[str] = Field(None, description="Apollo.io API key for contacts")


class RerunRequest(BaseModel):
    """Request to rerun a job with same or modified settings"""
    mode: str = Field("same", description="same, same_exclude_found, or new")
    demographics: Optional[List[str]] = None
    categories: Optional[List[str]] = None
    max_companies: Optional[int] = None


class LiveContact(BaseModel):
    """Live contact discovered during extraction"""
    id: str
    timestamp: str
    company_name: str
    app_or_product: Optional[str] = None
    email: Optional[str] = None
    person_name: Optional[str] = None
    type: str
    source: str
    confidence: int = 0
    playstore_url: Optional[str] = None
    website: Optional[str] = None


class ExtractionJobResponse(BaseModel):
    """Extraction job status"""
    job_id: str
    status: str
    progress: Dict[str, Any]
    stats: Dict[str, Any]
    created_at: str
    completed_at: Optional[str] = None
    results_count: int = 0
    live_contacts: List[LiveContact] = []


class ExtractionResult(BaseModel):
    """Single extraction result"""
    company_name: str
    app_or_product: Optional[str] = None
    product_category: Optional[str] = None
    demographic: Optional[str] = None
    company_website: Optional[str] = None
    company_domain: Optional[str] = None
    company_description: Optional[str] = None
    company_linkedin: Optional[str] = None
    company_size: Optional[str] = None
    company_industry: Optional[str] = None
    company_founded: Optional[str] = None
    company_location: Optional[str] = None
    company_phones: List[str] = []
    contact_email: Optional[str] = None
    marketing_email: Optional[str] = None
    sales_email: Optional[str] = None
    support_email: Optional[str] = None
    press_email: Optional[str] = None
    playstore_url: Optional[str] = None
    appstore_url: Optional[str] = None
    people: List[Dict[str, Any]] = []
    confidence_score: int = 0
    data_sources: List[str] = []
    # Email verification fields (Layer 6)
    email_verification_status: str = "not_verified"
    email_verification_confidence: int = 0
    email_mx_valid: bool = False
    email_is_disposable: bool = False
    email_is_role_based: bool = False
    # Layer 9 fields
    email_sources: Dict[str, Any] = {}
    role_engagement_score: float = 0.5
    domain_reputation_score: int = 0
    email_freshness_score: float = 1.0
    last_verified_at: Optional[str] = None
    # Layer 15 fields
    email_warmth_score: int = 0
    domain_is_catchall: bool = False


# ==================== API Endpoints ====================

@router.get("/demographics", response_model=List[DemographicOption])
async def get_demographics():
    """Get available demographics for selection"""
    demographics = []
    for demo in Demographic:
        demographics.append(DemographicOption(
            value=demo.value,
            label=demo.name.replace("_", " ").title(),
            countries=DEMOGRAPHIC_COUNTRIES.get(demo, [])
        ))
    return demographics


@router.get("/categories", response_model=List[CategoryOption])
async def get_categories():
    """Get available product categories for selection"""
    categories = []
    for cat in ProductCategory:
        categories.append(CategoryOption(
            value=cat.value,
            label=cat.name.replace("_", " ").title(),
            keywords=CATEGORY_KEYWORDS.get(cat, [cat.value])
        ))
    return categories


@router.post("/extract", response_model=ExtractionJobResponse)
@limiter.limit("5/minute")
async def start_extraction(
    request: Request,
    extraction_request: ExtractionRequest,
    background_tasks: BackgroundTasks,
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Start a new extraction job"""
    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    config = {
        "demographics": extraction_request.demographics,
        "categories": extraction_request.categories,
        "use_paid_apis": extraction_request.use_paid_apis,
        "max_companies": extraction_request.max_companies,
        "max_apps_per_category": extraction_request.max_apps_per_category,
        "website_scrape_depth": extraction_request.website_scrape_depth,
        "target_contacts": extraction_request.target_contacts,
        "enable_deep_osint": extraction_request.enable_deep_osint,
        "enable_email_verification": extraction_request.enable_email_verification,
        "enable_social_scraping": extraction_request.enable_social_scraping,
    }

    default_stats = {
        "apps_found": 0, "companies_found": 0, "emails_found": 0,
        "emails_verified": 0, "pages_scraped": 0, "api_calls": 0,
        "bloom_filter_hits": 0, "cache_hits": 0,
        "nlp_entities_extracted": 0, "email_permutations_generated": 0,
        "osint_leadership_found": 0, "osint_employees_found": 0,
        "osint_phones_found": 0, "osint_social_profiles_found": 0,
    }

    default_progress = {
        "stage": "initializing", "stage_progress": 0,
        "total_progress": 0, "message": "Starting...",
    }

    # Store in memory cache (for real-time updates during extraction)
    # Use deque for live_contacts: atomic append + automatic maxlen trimming (race-safe)
    active_jobs[job_id] = {
        "status": "pending",
        "progress": default_progress,
        "stats": default_stats,
        "results": [],
        "live_contacts": deque(maxlen=5000),
        "created_at": now,
        "completed_at": None,
        "config": config,
    }

    # Also persist initial state to DB
    _persist_job_to_db(job_id, active_jobs[job_id])

    # Start extraction in background
    background_tasks.add_task(run_extraction_job, job_id, extraction_request)

    return ExtractionJobResponse(
        job_id=job_id,
        status="pending",
        progress=default_progress,
        stats={},
        created_at=now,
    )


async def run_extraction_job(job_id: str, request: ExtractionRequest):
    """Background task to run extraction - persists results to database."""
    try:
        active_jobs[job_id]["status"] = "running"

        # Convert strings to enums
        demographics = []
        for d in request.demographics:
            try:
                demographics.append(Demographic(d))
            except ValueError:
                pass

        categories = []
        for c in request.categories:
            try:
                categories.append(ProductCategory(c))
            except ValueError:
                pass

        if not demographics:
            demographics = [Demographic.USA]
        if not categories:
            categories = [ProductCategory.MOBILE_APPS]

        # Build exclude lists (from request + from previous job if specified)
        exclude_domains = list(request.exclude_domains) if request.exclude_domains else []
        exclude_emails = list(request.exclude_emails) if request.exclude_emails else []

        # If exclude_previous_job_id is specified, load that job's results and add to exclusions
        if request.exclude_previous_job_id:
            prev_job = _get_job(request.exclude_previous_job_id)
            if prev_job:
                prev_results = prev_job.get("results", [])
                if not prev_results:
                    # Load from DB if not in memory
                    db_data = _load_job_from_db(request.exclude_previous_job_id)
                    if db_data:
                        prev_results = db_data.get("results", [])
                for result in prev_results:
                    if result.get("company_domain"):
                        exclude_domains.append(result["company_domain"])
                    for email_field in ["contact_email", "marketing_email", "sales_email", "support_email", "press_email"]:
                        if result.get(email_field):
                            exclude_emails.append(result[email_field])

        # Create config
        config = MobiAdzConfig(
            demographics=demographics,
            categories=categories,
            max_companies=request.max_companies,
            max_apps_per_category=request.max_apps_per_category,
            website_scrape_depth=request.website_scrape_depth,
            target_contacts=request.target_contacts,
            use_paid_apis=request.use_paid_apis,
            exclude_domains=exclude_domains,
            exclude_emails=exclude_emails,
            hunter_api_key=request.hunter_api_key,
            clearbit_api_key=request.clearbit_api_key,
            apollo_api_key=request.apollo_api_key,
        )

        engine = MobiAdzExtractionEngine(config)

        # Store engine reference for cancellation
        active_engines[job_id] = engine

        # Callback for live contacts (race-safe: deque.append is atomic in CPython,
        # and maxlen auto-evicts oldest entries without read-modify-write)
        def on_live_contact(contact_data: dict):
            """Add live contact to the feed. Uses deque(maxlen=5000) for thread safety."""
            try:
                active_jobs[job_id]["live_contacts"].append(contact_data)
            except KeyError:
                pass  # Job evicted from memory — safe to ignore

        engine.set_live_contact_callback(on_live_contact)

        # Progress updater - also propagates cancellation from API to engine
        async def update_progress():
            while active_jobs.get(job_id, {}).get("status") == "running":
                if job_id in active_jobs:
                    active_jobs[job_id]["progress"] = engine.get_progress()
                    active_jobs[job_id]["stats"] = engine.get_stats()
                # Check if API set status to cancelled, propagate to engine
                if active_jobs.get(job_id, {}).get("status") == "cancelled":
                    engine.cancel()
                    break
                await asyncio.sleep(1)

        progress_task = asyncio.create_task(update_progress())

        try:
            contacts = await engine.run_extraction()

            # Store results in memory
            results = [
                {
                    "company_name": c.company_name,
                    "app_or_product": c.app_or_product,
                    "product_category": c.product_category,
                    "demographic": c.demographic,
                    "company_website": c.company_website,
                    "company_domain": c.company_domain,
                    "company_description": c.company_description,
                    "company_linkedin": c.company_linkedin,
                    "company_size": c.company_size,
                    "company_industry": c.company_industry,
                    "company_founded": c.company_founded,
                    "company_location": c.company_location,
                    "company_phones": c.company_phones or [],
                    "contact_email": c.contact_email,
                    "marketing_email": c.marketing_email,
                    "sales_email": c.sales_email,
                    "support_email": c.support_email,
                    "press_email": c.press_email,
                    "playstore_url": c.playstore_url,
                    "appstore_url": c.appstore_url,
                    "people": c.people,
                    "confidence_score": c.confidence_score,
                    "data_sources": c.data_sources,
                    # Email verification fields (Layer 6)
                    "email_verification_status": getattr(c, 'email_verification_status', 'not_verified'),
                    "email_verification_confidence": getattr(c, 'email_verification_confidence', 0),
                    "email_mx_valid": getattr(c, 'email_mx_valid', False),
                    "email_is_disposable": getattr(c, 'email_is_disposable', False),
                    "email_is_role_based": getattr(c, 'email_is_role_based', False),
                    # Layer 9 fields
                    "email_sources": getattr(c, 'email_sources', {}),
                    "role_engagement_score": getattr(c, 'role_engagement_score', 0.5),
                    "domain_reputation_score": getattr(c, 'domain_reputation_score', 0),
                    "email_freshness_score": getattr(c, 'email_freshness_score', 1.0),
                    "last_verified_at": getattr(c, 'last_verified_at', None),
                    # Layer 15 fields
                    "email_warmth_score": getattr(c, 'email_warmth_score', 0),
                    "domain_is_catchall": getattr(c, 'domain_is_catchall', False),
                }
                for c in contacts
            ]

            active_jobs[job_id]["results"] = results
            if active_jobs[job_id].get("status") != "cancelled":
                active_jobs[job_id]["status"] = "completed"
            active_jobs[job_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
            active_jobs[job_id]["stats"] = engine.get_stats()

            # === PERSIST TO DATABASE ===
            _persist_job_to_db(job_id, active_jobs[job_id])

            # Store count before clearing (so get_job_status can report it)
            active_jobs[job_id]["results_count"] = len(results)
            # Remove large results from memory immediately (they're in the DB now)
            active_jobs[job_id]["results"] = []  # DB has them
            logger.info(f"[MOBIADZ] Job {job_id[:8]} results offloaded to database ({len(results)} contacts)")

            # Schedule full memory cleanup after delay (allows final status polls)
            asyncio.create_task(_schedule_memory_cleanup(job_id))

        finally:
            progress_task.cancel()
            try:
                await progress_task
            except asyncio.CancelledError:
                pass
            await engine.close()
            # Clean up engine reference
            active_engines.pop(job_id, None)

    except Exception as e:
        logger.error(f"Extraction job {job_id} failed: {e}")
        if job_id in active_jobs:
            active_jobs[job_id]["status"] = "failed"
            active_jobs[job_id]["stats"]["error"] = str(e)
            # Persist failed state too
            _persist_job_to_db(job_id, active_jobs[job_id])
            # Schedule memory cleanup for failed job
            asyncio.create_task(_schedule_memory_cleanup(job_id))
        # Clean up engine reference
        active_engines.pop(job_id, None)


@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str, current_candidate: Candidate = Depends(get_current_candidate)):
    """Get job status with live contacts - checks memory then database."""
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Convert deque to list for JSON serialization
    live_contacts = list(job.get("live_contacts", []))

    return {
        "job_id": job_id,
        "status": job["status"],
        "progress": job.get("progress", {}),
        "stats": job.get("stats", {}),
        "created_at": job.get("created_at"),
        "completed_at": job.get("completed_at"),
        "results_count": job.get("results_count", len(job.get("results", []))),
        "live_contacts": live_contacts,
    }


@router.get("/jobs/{job_id}/results", response_model=List[ExtractionResult])
async def get_job_results(
    job_id: str,
    page: int = 1,
    limit: int = 50,
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Get extraction results - loads from database if not in memory."""
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job["status"] not in ["completed", "running", "failed", "cancelled"]:
        raise HTTPException(status_code=400, detail="Job not ready")

    # Validate pagination
    if page < 1:
        page = 1
    if limit < 1 or limit > 500:
        limit = min(max(limit, 1), 500)

    results = job.get("results", [])

    # If results are empty in memory, load from database
    if not results and job["status"] in ["completed", "failed"]:
        db_data = _load_job_from_db(job_id)
        if db_data:
            results = db_data.get("results", [])

    # Pagination
    start = (page - 1) * limit
    end = start + limit
    return results[start:end]


@router.delete("/jobs/{job_id}")
async def cancel_job(job_id: str, current_candidate: Candidate = Depends(get_current_candidate)):
    """Cancel a running job - immediately signals the engine to stop."""
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job_id in active_jobs and active_jobs[job_id]["status"] == "running":
        # 1. Set status flag (picked up by progress updater)
        active_jobs[job_id]["status"] = "cancelled"

        # 2. Directly signal the engine to stop (immediate effect)
        engine = active_engines.get(job_id)
        if engine:
            engine.cancel()
            logger.info(f"[MOBIADZ] Cancellation signal sent to engine for job {job_id[:8]}")

        # 3. Persist cancelled state
        _persist_job_to_db(job_id, active_jobs[job_id])

    return {"message": "Job cancelled"}


@router.post("/jobs/{job_id}/delete")
async def delete_job(job_id: str, current_candidate: Candidate = Depends(get_current_candidate)):
    """Permanently delete a job and all its contacts from history."""
    # 1. If running, cancel first
    if job_id in active_jobs and active_jobs[job_id].get("status") == "running":
        engine = active_engines.get(job_id)
        if engine:
            engine.cancel()
        active_jobs[job_id]["status"] = "cancelled"

    # 2. Remove from in-memory store
    if job_id in active_jobs:
        del active_jobs[job_id]
    if job_id in active_engines:
        del active_engines[job_id]

    # 3. Remove from database
    try:
        db = _get_db_session()
        # Delete contacts first (foreign key)
        db.query(MobiAdzContact).filter(MobiAdzContact.job_id == job_id).delete()
        db.query(MobiAdzJob).filter(MobiAdzJob.job_id == job_id).delete()
        db.commit()
        db.close()
        logger.info(f"[MOBIADZ] Deleted job {job_id[:8]} and all contacts from database")
    except Exception as e:
        logger.error(f"[MOBIADZ] Failed to delete job {job_id[:8]} from database: {e}")

    return {"message": "Job deleted"}


@router.post("/jobs/{job_id}/export")
async def export_results(
    job_id: str,
    format: str = "json",
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Export results to specified format"""
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    results = job.get("results", [])

    # Load from DB if not in memory
    if not results:
        db_data = _load_job_from_db(job_id)
        if db_data:
            results = db_data.get("results", [])

    if format == "csv":
        import csv
        import io

        output = io.StringIO()
        if results:
            writer = csv.DictWriter(output, fieldnames=results[0].keys())
            writer.writeheader()
            for row in results:
                row_copy = row.copy()
                row_copy["people"] = str(row_copy.get("people", []))
                row_copy["data_sources"] = ", ".join(row_copy.get("data_sources", []))
                row_copy["email_sources"] = str(row_copy.get("email_sources", {}))
                row_copy["company_phones"] = ", ".join(row_copy.get("company_phones", []) or [])
                writer.writerow(row_copy)

        return {"content": output.getvalue(), "format": "csv"}

    return {"results": results, "format": "json"}


class QuickExtractRequest(BaseModel):
    """Request for quick extraction"""
    demographics: List[str] = Field(..., description="List of demographics")
    categories: List[str] = Field(..., description="List of categories")
    max_results: int = Field(20, ge=5, le=100, description="Max results to return")


@router.post("/quick-extract")
@limiter.limit("10/minute")
async def quick_extract(request: Request, quick_request: QuickExtractRequest, current_candidate: Candidate = Depends(get_current_candidate)):
    """Quick synchronous extraction (limited results)"""
    from app.services.mobiadz_extraction_engine import quick_mobiadz_extraction

    try:
        results = await quick_mobiadz_extraction(
            demographics=quick_request.demographics,
            categories=quick_request.categories,
            max_companies=quick_request.max_results,
        )
        return {"success": True, "count": len(results), "results": results}
    except Exception as e:
        logger.error(f"Quick extraction failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs")
async def list_jobs(current_candidate: Candidate = Depends(get_current_candidate)):
    """List all extraction jobs - from memory + database."""
    jobs = []
    seen_job_ids = set()

    # 1. Active (in-memory) jobs
    for job_id, job in active_jobs.items():
        seen_job_ids.add(job_id)
        config = job.get("config", {})
        stats = job.get("stats", {})
        progress = job.get("progress", {})
        jobs.append({
            "job_id": job_id,
            "status": job["status"],
            "progress": progress.get("total_progress", 0),
            "results_count": job.get("results_count", len(job.get("results", []))),
            "emails_found": stats.get("emails_found", 0),
            "created_at": job.get("created_at"),
            "completed_at": job.get("completed_at"),
            "demographics": config.get("demographics", []),
            "categories": config.get("categories", []),
            "config": config,
            "stats": stats,
        })

    # 2. Historical (database) jobs not in memory
    try:
        from sqlalchemy import func
        db = _get_db_session()
        # Single query with contact count subquery (avoids N+1)
        contact_counts = db.query(
            MobiAdzContact.job_id,
            func.count(MobiAdzContact.id).label("cnt")
        ).group_by(MobiAdzContact.job_id).subquery()

        db_jobs = db.query(MobiAdzJob).order_by(MobiAdzJob.created_at.desc()).limit(50).all()
        # Build lookup from subquery
        count_rows = db.query(contact_counts.c.job_id, contact_counts.c.cnt).all()
        count_map = {row[0]: row[1] for row in count_rows}

        for db_job in db_jobs:
            if db_job.job_id not in seen_job_ids:
                config = db_job.config or {}
                stats = db_job.stats or {}
                progress = db_job.progress or {}
                results_count = count_map.get(db_job.job_id, 0)
                jobs.append({
                    "job_id": db_job.job_id,
                    "status": db_job.status,
                    "progress": progress.get("total_progress", 100 if db_job.status == "completed" else 0),
                    "results_count": results_count,
                    "emails_found": stats.get("emails_found", 0),
                    "created_at": db_job.created_at.isoformat() if db_job.created_at else None,
                    "completed_at": db_job.completed_at.isoformat() if db_job.completed_at else None,
                    "demographics": config.get("demographics", []),
                    "categories": config.get("categories", []),
                    "config": config,
                    "stats": stats,
                })
        db.close()
    except Exception as e:
        logger.error(f"[MOBIADZ-DB] Failed to list DB jobs: {e}")

    # Sort by created_at descending
    jobs.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return {"jobs": jobs, "total": len(jobs)}


@router.post("/jobs/{job_id}/rerun")
async def rerun_job(
    job_id: str,
    request: RerunRequest,
    background_tasks: BackgroundTasks,
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """Rerun a job with same, modified, or new settings"""
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    original_config = job.get("config", {})

    # Determine exclusions based on mode
    exclude_domains = []
    exclude_emails = []

    if request.mode == "same_exclude_found":
        results = job.get("results", [])
        if not results:
            db_data = _load_job_from_db(job_id)
            if db_data:
                results = db_data.get("results", [])
        for result in results:
            if result.get("company_domain"):
                exclude_domains.append(result["company_domain"])
            for email_field in ["contact_email", "marketing_email", "sales_email", "support_email", "press_email"]:
                if result.get(email_field):
                    exclude_emails.append(result[email_field])

    if request.mode in ["same", "same_exclude_found"]:
        new_request = ExtractionRequest(
            demographics=original_config.get("demographics", ["usa"]),
            categories=original_config.get("categories", ["mobile_apps"]),
            use_paid_apis=original_config.get("use_paid_apis", False),
            max_companies=original_config.get("max_companies", 100),
            max_apps_per_category=original_config.get("max_apps_per_category", 50),
            website_scrape_depth=original_config.get("website_scrape_depth", 8),
            exclude_domains=exclude_domains,
            exclude_emails=exclude_emails,
            enable_deep_osint=original_config.get("enable_deep_osint", True),
            enable_email_verification=original_config.get("enable_email_verification", True),
            enable_social_scraping=original_config.get("enable_social_scraping", True),
        )
    else:
        new_request = ExtractionRequest(
            demographics=request.demographics or original_config.get("demographics", ["usa"]),
            categories=request.categories or original_config.get("categories", ["mobile_apps"]),
            use_paid_apis=original_config.get("use_paid_apis", False),
            max_companies=request.max_companies or original_config.get("max_companies", 100),
            max_apps_per_category=original_config.get("max_apps_per_category", 50),
            website_scrape_depth=original_config.get("website_scrape_depth", 8),
            enable_deep_osint=True,
            enable_email_verification=True,
            enable_social_scraping=True,
        )

    return await start_extraction(new_request, background_tasks)


@router.get("/jobs/{job_id}/config")
async def get_job_config(job_id: str, current_candidate: Candidate = Depends(get_current_candidate)):
    """Get the configuration used for a job (for rerun)"""
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    results = job.get("results", [])
    if not results:
        db_data = _load_job_from_db(job_id)
        if db_data:
            results = db_data.get("results", [])

    return {
        "job_id": job_id,
        "config": job.get("config", {}),
        "status": job["status"],
        "created_at": job.get("created_at"),
        "completed_at": job.get("completed_at"),
        "results_count": len(results),
    }


@router.get("/stats/summary")
async def get_extraction_stats(current_candidate: Candidate = Depends(get_current_candidate)):
    """Get overall extraction statistics - combines memory + database."""
    # In-memory stats
    mem_total = len(active_jobs)
    mem_completed = sum(1 for j in active_jobs.values() if j["status"] == "completed")
    mem_running = sum(1 for j in active_jobs.values() if j["status"] == "running")

    # Database stats
    db_total = 0
    db_completed = 0
    db_contacts = 0
    db_emails = 0
    try:
        db = _get_db_session()
        db_total = db.query(MobiAdzJob).count()
        db_completed = db.query(MobiAdzJob).filter(MobiAdzJob.status == "completed").count()
        db_contacts = db.query(MobiAdzContact).count()
        db_emails = db.query(MobiAdzContact).filter(
            MobiAdzContact.contact_email.isnot(None)
        ).count()
        db.close()
    except Exception as e:
        logger.error(f"[MOBIADZ-STATS] Failed to query database stats: {e}")

    # db_total includes persisted jobs; mem_running counts active-only (not in DB yet)
    return {
        "total_jobs": db_total + mem_running,
        "completed_jobs": db_completed,
        "running_jobs": mem_running,
        "total_contacts_extracted": db_contacts,
        "total_emails_found": db_emails,
        "jobs_with_results": db_completed,
    }


# === BOUNCE HISTORY ENDPOINTS ===


class BounceUpdateRequest(BaseModel):
    domain: str
    hard_bounces: int = 0
    soft_bounces: int = 0
    successful_deliveries: int = 0


@router.get("/bounce-history")
async def get_bounce_history(limit: int = 50, offset: int = 0, current_candidate: Candidate = Depends(get_current_candidate)):
    """Get domain bounce history records for deliverability tracking."""
    try:
        db = _get_db_session()
        query = db.query(MobiAdzDomainBounceHistory).order_by(
            MobiAdzDomainBounceHistory.last_updated.desc()
        )
        total = query.count()
        records = query.offset(offset).limit(min(limit, 200)).all()
        result = [r.to_dict() for r in records]
        db.close()
        return {"records": result, "total": total}
    except Exception as e:
        logger.error(f"[MOBIADZ-BOUNCE] Failed to fetch bounce history: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch bounce history")


@router.get("/bounce-history/{domain}")
async def get_domain_bounce(domain: str, current_candidate: Candidate = Depends(get_current_candidate)):
    """Get bounce history for a specific domain."""
    try:
        db = _get_db_session()
        record = db.query(MobiAdzDomainBounceHistory).filter(
            MobiAdzDomainBounceHistory.domain == domain
        ).first()
        db.close()
        if not record:
            raise HTTPException(status_code=404, detail=f"No bounce history for {domain}")
        return record.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[MOBIADZ-BOUNCE] Failed to fetch bounce for {domain}: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch bounce history")


@router.post("/bounce-history")
async def update_bounce_history(request: BounceUpdateRequest, current_candidate: Candidate = Depends(get_current_candidate)):
    """Update bounce stats for a domain (called after email campaigns)."""
    try:
        db = _get_db_session()
        record = db.query(MobiAdzDomainBounceHistory).filter(
            MobiAdzDomainBounceHistory.domain == request.domain
        ).first()

        if not record:
            record = MobiAdzDomainBounceHistory(domain=request.domain)
            db.add(record)

        record.total_emails_sent += (
            request.hard_bounces + request.soft_bounces + request.successful_deliveries
        )
        record.hard_bounces += request.hard_bounces
        record.soft_bounces += request.soft_bounces
        record.successful_deliveries += request.successful_deliveries

        if request.hard_bounces > 0 or request.soft_bounces > 0:
            record.last_bounce_at = datetime.now(timezone.utc)

        record.update_rates()
        db.commit()

        result = record.to_dict()
        db.close()
        return result
    except Exception as e:
        logger.error(f"[MOBIADZ-BOUNCE] Failed to update bounce for {request.domain}: {e}")
        raise HTTPException(status_code=500, detail="Failed to update bounce history")


@router.get("/bounce-history/summary/stats")
async def get_bounce_summary(current_candidate: Candidate = Depends(get_current_candidate)):
    """Get aggregate bounce statistics across all domains."""
    try:
        db = _get_db_session()
        from sqlalchemy import func
        stats = db.query(
            func.count(MobiAdzDomainBounceHistory.id).label("total_domains"),
            func.sum(MobiAdzDomainBounceHistory.total_emails_sent).label("total_sent"),
            func.sum(MobiAdzDomainBounceHistory.hard_bounces).label("total_hard"),
            func.sum(MobiAdzDomainBounceHistory.soft_bounces).label("total_soft"),
            func.sum(MobiAdzDomainBounceHistory.successful_deliveries).label("total_delivered"),
        ).first()

        problematic = db.query(MobiAdzDomainBounceHistory).filter(
            MobiAdzDomainBounceHistory.weighted_bounce_rate > 0.05
        ).count()

        db.close()
        return {
            "total_domains_tracked": stats.total_domains or 0,
            "total_emails_sent": stats.total_sent or 0,
            "total_hard_bounces": stats.total_hard or 0,
            "total_soft_bounces": stats.total_soft or 0,
            "total_delivered": stats.total_delivered or 0,
            "problematic_domains": problematic,
        }
    except Exception as e:
        logger.error(f"[MOBIADZ-BOUNCE] Failed to get bounce summary: {e}")
        raise HTTPException(status_code=500, detail="Failed to get bounce summary")
