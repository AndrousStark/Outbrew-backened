"""
Recipients Endpoints

API endpoints for managing recipients (contacts) in the Recipient Groups feature.

Features:
- CRUD operations for recipients
- Advanced search and filtering
- CSV bulk import
- Engagement tracking
- Statistics
"""

import asyncio
import html as html_mod
import json
import os
import re as re_mod
import smtplib
import ssl
from datetime import datetime, timedelta, timezone
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status, UploadFile, File
from app.core.rate_limiter import limiter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.ext.asyncio import AsyncSession
import sqlalchemy
from typing import List, Optional, Dict, Any
import logging

from app.core.encryption import decrypt_value
from app.models.application import Application, ApplicationStatusEnum
from app.models.company import Company
from app.models.email_log import EmailLog, EmailStatusEnum
from app.models.resume import ResumeVersion

from app.core.database_async import get_async_db, AsyncSessionLocal
from app.core.auth import get_current_candidate
from app.models.candidate import Candidate
from app.models.documents import ParsedResume as ParsedResumeModel
from app.models.enrichment_job import EnrichmentJob, EnrichmentJobStatus
from app.models.merge_history import MergeHistory, MergeStrategyEnum
from app.repositories.recipient import AsyncRecipientRepository
from app.repositories.application_async import AsyncApplicationRepository
from app.repositories.company_async import AsyncCompanyRepository
from app.schemas.recipient import (
    RecipientCreate,
    RecipientUpdate,
    RecipientResponse,
    RecipientListResponse,
    RecipientSearchRequest,
    RecipientCSVImportRequest,
    RecipientCSVImportResponse,
    RecipientStatistics,
)
from app.services.intelligent_csv_parser import IntelligentCSVParser
from app.services.ultra_company_intelligence import UltraCompanyIntelligence
from app.services.ultra_email_generator import UltraEmailGenerator
from app.services.resume_parser import IntelligentResumeParser
from app.services.combined_research_orchestrator import CombinedResearchOrchestrator
from app.services.enrichment_orchestrator import (
    EnrichmentOrchestrator,
    calculate_backoff_delay,
)
from app.services.entity_resolution import (
    EntityResolutionService,
    Entity,
    ResolvedEntity,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# NOTE: Job tracking is now persisted in EnrichmentJob database model.


# ==================== LIST & SEARCH ENDPOINTS ====================


@router.get("/", response_model=RecipientListResponse)
async def list_recipients(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    search_term: Optional[str] = None,
    company: Optional[str] = None,
    country: Optional[str] = None,
    is_active: Optional[bool] = True,
    include_unsubscribed: bool = False,
    order_by: str = Query("created_at", regex="^(created_at|name|company|email|engagement_score|position|country)$"),
    order_desc: bool = True,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    List recipients with pagination and filtering.

    Query Parameters:
    - page: Page number (1-indexed)
    - page_size: Results per page (1-100)
    - search_term: Search across name, email, company, position
    - company: Filter by company
    - country: Filter by country
    - is_active: Filter active recipients
    - include_unsubscribed: Include unsubscribed recipients
    - order_by: Sort field (created_at, name, company, engagement_score)
    - order_desc: Sort descending

    Returns:
        Paginated list of recipients
    """
    logger.info(
        f"📋 [RECIPIENTS] Listing recipients for candidate {current_candidate.id} "
        f"(page {page}, search: {search_term})"
    )

    repo = AsyncRecipientRepository(db)

    # Calculate skip
    skip = (page - 1) * page_size

    # Search recipients
    recipients, total = await repo.search_recipients(
        candidate_id=current_candidate.id,
        search_term=search_term,
        company=company,
        country=country,
        is_active=is_active,
        unsubscribed=not include_unsubscribed,
        skip=skip,
        limit=page_size,
        order_by=order_by,
        order_desc=order_desc,
    )

    # Calculate total pages
    total_pages = (total + page_size - 1) // page_size

    logger.info(f"✅ [RECIPIENTS] Found {len(recipients)} recipients (total: {total})")

    return RecipientListResponse(
        recipients=[RecipientResponse.model_validate(r) for r in recipients],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


@router.post("/search", response_model=RecipientListResponse)
async def search_recipients(
    search_request: RecipientSearchRequest,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Advanced recipient search with POST body.

    This endpoint accepts complex search criteria in the request body.

    Request Body:
        RecipientSearchRequest with search filters

    Returns:
        Paginated list of matching recipients
    """
    logger.info(f"🔍 [RECIPIENTS] Search request for candidate {current_candidate.id}")

    repo = AsyncRecipientRepository(db)

    # Calculate skip
    skip = (search_request.page - 1) * search_request.page_size

    # Search recipients
    recipients, total = await repo.search_recipients(
        candidate_id=current_candidate.id,
        search_term=search_request.search_term,
        company=search_request.company,
        tags=search_request.tags,
        country=search_request.country,
        is_active=search_request.is_active,
        unsubscribed=search_request.include_unsubscribed,
        skip=skip,
        limit=search_request.page_size,
        order_by=search_request.order_by,
        order_desc=search_request.order_desc,
    )

    # Calculate total pages
    total_pages = (total + search_request.page_size - 1) // search_request.page_size

    logger.info(f"✅ [RECIPIENTS] Search complete: {len(recipients)}/{total} results")

    return RecipientListResponse(
        recipients=[RecipientResponse.model_validate(r) for r in recipients],
        total=total,
        page=search_request.page,
        page_size=search_request.page_size,
        total_pages=total_pages,
    )


# ==================== SINGLE RECIPIENT ENDPOINTS ====================


@router.get("/{recipient_id}", response_model=RecipientResponse)
async def get_recipient(
    recipient_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get a single recipient by ID.

    Path Parameters:
    - recipient_id: Recipient ID

    Returns:
        Recipient details

    Raises:
        404: Recipient not found or doesn't belong to candidate
    """
    logger.info(f"🔍 [RECIPIENTS] Fetching recipient {recipient_id}")

    repo = AsyncRecipientRepository(db)

    recipient = await repo.get_by_id(recipient_id)

    if not recipient:
        logger.warning(f"⚠️  [RECIPIENTS] Recipient {recipient_id} not found")
        raise HTTPException(status_code=404, detail="Recipient not found")

    # Ensure recipient belongs to current candidate (multi-tenant isolation)
    if recipient.candidate_id != current_candidate.id:
        logger.warning(
            f"⚠️  [RECIPIENTS] Access denied: recipient {recipient_id} "
            f"belongs to candidate {recipient.candidate_id}, "
            f"not {current_candidate.id}"
        )
        raise HTTPException(status_code=404, detail="Recipient not found")

    logger.info(f"✅ [RECIPIENTS] Fetched recipient {recipient_id}")

    return RecipientResponse.model_validate(recipient)


@router.get("/{recipient_id}/with-groups", response_model=RecipientResponse)
async def get_recipient_with_groups(
    recipient_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get recipient with group memberships and campaign history.

    Path Parameters:
    - recipient_id: Recipient ID

    Returns:
        Recipient details with related data

    Raises:
        404: Recipient not found
    """
    logger.info(f"🔍 [RECIPIENTS] Fetching recipient {recipient_id} with groups")

    repo = AsyncRecipientRepository(db)

    recipient = await repo.get_with_groups(recipient_id)

    if not recipient:
        logger.warning(f"⚠️  [RECIPIENTS] Recipient {recipient_id} not found")
        raise HTTPException(status_code=404, detail="Recipient not found")

    # Multi-tenant isolation
    if recipient.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Recipient not found")

    logger.info(f"✅ [RECIPIENTS] Fetched recipient {recipient_id} with relations")

    return RecipientResponse.model_validate(recipient)


# ==================== CREATE ENDPOINTS ====================


@router.post("/", response_model=RecipientResponse, status_code=status.HTTP_201_CREATED)
async def create_recipient(
    recipient_data: RecipientCreate,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Create a new recipient.

    Request Body:
        RecipientCreate schema

    Returns:
        Created recipient

    Raises:
        400: Duplicate email or validation error
    """
    logger.info(
        f"➕ [RECIPIENTS] Creating recipient {recipient_data.email} "
        f"for candidate {current_candidate.id}"
    )

    repo = AsyncRecipientRepository(db)

    # Check for duplicate email (case-insensitive)
    normalized_email = recipient_data.email.strip().lower()
    existing = await repo.get_by_email(current_candidate.id, normalized_email)
    if existing:
        logger.warning(
            f"⚠️  [RECIPIENTS] Duplicate email: {recipient_data.email} "
            f"(existing ID: {existing.id})"
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Recipient with email '{normalized_email}' already exists",
        )

    # Create recipient (store email normalized to lowercase)
    recipient_dict = recipient_data.model_dump()
    recipient_dict["email"] = normalized_email
    recipient_dict["candidate_id"] = current_candidate.id

    recipient = await repo.create(recipient_dict)

    logger.info(f"✅ [RECIPIENTS] Created recipient {recipient.id} ({recipient.email})")

    return RecipientResponse.model_validate(recipient)


# ==================== UPDATE ENDPOINTS ====================


@router.patch("/{recipient_id}", response_model=RecipientResponse)
async def update_recipient(
    recipient_id: int,
    recipient_data: RecipientUpdate,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Update an existing recipient.

    Path Parameters:
    - recipient_id: Recipient ID

    Request Body:
        RecipientUpdate schema (partial update)

    Returns:
        Updated recipient

    Raises:
        404: Recipient not found
    """
    logger.info(f"✏️  [RECIPIENTS] Updating recipient {recipient_id}")

    repo = AsyncRecipientRepository(db)

    # Get existing recipient
    recipient = await repo.get_by_id(recipient_id)

    if not recipient:
        logger.warning(f"⚠️  [RECIPIENTS] Recipient {recipient_id} not found")
        raise HTTPException(status_code=404, detail="Recipient not found")

    # Multi-tenant isolation
    if recipient.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Recipient not found")

    # Update recipient
    update_dict = recipient_data.model_dump(exclude_unset=True)

    if not update_dict:
        # No fields to update
        return RecipientResponse.model_validate(recipient)

    updated_recipient = await repo.update(recipient_id, update_dict)

    logger.info(f"✅ [RECIPIENTS] Updated recipient {recipient_id}")

    return RecipientResponse.model_validate(updated_recipient)


# ==================== DELETE ENDPOINTS ====================


@router.delete("/{recipient_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_recipient(
    recipient_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Soft delete a recipient.

    Path Parameters:
    - recipient_id: Recipient ID

    Returns:
        204 No Content on success

    Raises:
        404: Recipient not found
    """
    logger.info(f"🗑️  [RECIPIENTS] Deleting recipient {recipient_id}")

    repo = AsyncRecipientRepository(db)

    # Get existing recipient
    recipient = await repo.get_by_id(recipient_id)

    if not recipient:
        logger.warning(f"⚠️  [RECIPIENTS] Recipient {recipient_id} not found")
        raise HTTPException(status_code=404, detail="Recipient not found")

    # Multi-tenant isolation
    if recipient.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Recipient not found")

    # Soft delete
    success = await repo.soft_delete(recipient_id)

    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete recipient")

    logger.info(f"✅ [RECIPIENTS] Deleted recipient {recipient_id}")

    return None


# ==================== BULK OPERATIONS ====================


@router.post("/import-csv/preview")
async def preview_csv_import(
    file: UploadFile = File(...),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Preview CSV import with intelligent parsing and validation.

    Returns:
    - Column mappings (detected columns with confidence scores)
    - Validation results (valid rows, invalid rows, duplicates)
    - Sample data (first 10 rows)
    - Country detection and guidance

    Does NOT save to database - just returns preview for user confirmation.
    """
    try:
        # Validate file type
        if not file.filename or not (
            file.filename.endswith(".csv")
            or file.filename.endswith(".xlsx")
            or file.filename.endswith(".xls")
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid file type. Supported formats: CSV, XLSX, XLS",
            )

        # Read file content
        file_content = await file.read()

        logger.info(
            f"[CSV Preview] Processing file for candidate {current_candidate.id}: {file.filename}"
        )

        # Parse with intelligent CSV parser
        parser = IntelligentCSVParser(fuzzy_threshold=70)
        result = parser.parse_file(file_content, file.filename)

        # Convert to dict for JSON response
        result_dict = result.to_dict()

        # Add sample rows (first 10)
        sample_rows = (
            result_dict["recipients"][:10]
            if len(result_dict["recipients"]) > 10
            else result_dict["recipients"]
        )

        logger.info(
            f"[CSV Preview] Parsed successfully. "
            f"Valid: {result.valid_rows}, Invalid: {result.invalid_rows}, "
            f"Country: {result.detected_country} ({result.country_confidence:.0f}%)"
        )

        return {
            "success": True,
            "filename": file.filename,
            "total_rows": result.total_rows,
            "valid_rows": result.valid_rows,
            "invalid_rows": result.invalid_rows,
            "column_mappings": [
                {
                    "detected_column": m.detected_column,
                    "mapped_to": m.mapped_to,
                    "confidence": m.confidence,
                }
                for m in result.column_mappings
            ],
            "detected_country": result.detected_country,
            "country_confidence": result.country_confidence,
            "country_guidance": parser.get_country_guidance(result.detected_country)
            if result.detected_country
            else None,
            "sample_rows": sample_rows,
            "warnings": result.warnings,
            "summary": {
                "valid_emails": len(
                    [
                        r
                        for r in result_dict["recipients"]
                        if r["is_valid"] and r["email"]
                    ]
                ),
                "missing_emails": len(
                    [r for r in result_dict["recipients"] if not r["email"]]
                ),
                "duplicates": len(
                    [
                        r
                        for r in result_dict["recipients"]
                        if not r["is_valid"] and "duplicate" in str(r)
                    ]
                ),
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[CSV Preview] Error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to preview CSV",
        )


@router.post("/import-csv", response_model=RecipientCSVImportResponse)
@limiter.limit("10/minute")
async def import_recipients_csv(
    request: Request,
    import_data: RecipientCSVImportRequest,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Bulk import recipients from CSV content.

    CSV Format:
        email,name,company,position,country,language,tags

    Example:
        john@example.com,John Doe,Acme Corp,Engineer,USA,en,tech,senior
        jane@example.com,Jane Smith,TechCo,Manager,Canada,en,management

    Request Body:
        RecipientCSVImportRequest with CSV content

    Returns:
        Import statistics (created, skipped, errors)
    """
    logger.info(
        f"📥 [RECIPIENTS] CSV import started for candidate {current_candidate.id}"
    )

    repo = AsyncRecipientRepository(db)

    # Bulk import
    result = await repo.bulk_create_from_csv(
        candidate_id=current_candidate.id,
        csv_content=import_data.csv_content,
        source=import_data.source,
        skip_duplicates=import_data.skip_duplicates,
    )

    logger.info(
        f"✅ [RECIPIENTS] CSV import complete: "
        f"{result['created']} created, {result['skipped']} skipped, "
        f"{result['errors']} errors"
    )

    return RecipientCSVImportResponse(**result)


# ==================== MOBIADZ IMPORT ENDPOINT ====================


class MobiAdzRecipient(BaseModel):
    """Schema for a MobiAdz extraction result recipient."""

    email: EmailStr
    name: Optional[str] = None
    company: Optional[str] = None
    position: Optional[str] = None
    country: Optional[str] = None
    source: Optional[str] = "themobiadz"
    tags: Optional[str] = None
    custom_fields: Optional[Dict[str, Any]] = None


class MobiAdzBulkImportRequest(BaseModel):
    """Request for bulk importing MobiAdz extraction results."""

    recipients: List[MobiAdzRecipient]
    skip_duplicates: bool = True
    create_group: bool = False
    group_name: Optional[str] = None


class MobiAdzBulkImportResponse(BaseModel):
    """Response for MobiAdz bulk import."""

    created: int
    skipped: int
    errors: int
    group_id: Optional[int] = None


@router.post("/bulk-import-mobiadz", response_model=MobiAdzBulkImportResponse)
async def bulk_import_mobiadz(
    import_data: MobiAdzBulkImportRequest,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Bulk import recipients from TheMobiAdz extraction results.

    This endpoint accepts extraction results from the MobiAdz engine
    and creates recipients from them.

    Request Body:
        MobiAdzBulkImportRequest with list of recipients

    Returns:
        Import statistics (created, skipped, errors)
    """
    logger.info(
        f"📥 [RECIPIENTS] MobiAdz bulk import started for candidate {current_candidate.id} "
        f"with {len(import_data.recipients)} contacts"
    )

    repo = AsyncRecipientRepository(db)

    created = 0
    skipped = 0
    errors = 0
    created_ids = []

    for recipient_data in import_data.recipients:
        try:
            # Check for duplicate email (case-insensitive)
            normalized_email = recipient_data.email.strip().lower()
            existing = await repo.get_by_email(
                current_candidate.id, normalized_email
            )
            if existing:
                if import_data.skip_duplicates:
                    skipped += 1
                    continue
                else:
                    errors += 1
                    continue

            # Create recipient
            recipient_dict = {
                "email": normalized_email,
                "name": recipient_data.name,
                "company": recipient_data.company,
                "position": recipient_data.position,
                "country": recipient_data.country,
                "source": recipient_data.source or "themobiadz",
                "tags": recipient_data.tags,
                "custom_fields": recipient_data.custom_fields,
                "candidate_id": current_candidate.id,
                "is_active": True,
            }

            recipient = await repo.create(recipient_dict)
            created_ids.append(recipient.id)
            created += 1

        except Exception as e:
            logger.error(
                f"❌ [RECIPIENTS] Failed to import {recipient_data.email}: {e}"
            )
            errors += 1

    # Optionally create a group
    group_id = None
    if import_data.create_group and created_ids:
        try:
            from app.repositories.recipient_group import AsyncRecipientGroupRepository

            group_repo = AsyncRecipientGroupRepository(db)

            group_name = (
                import_data.group_name
                or f"MobiAdz Import {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
            group = await group_repo.create(
                {
                    "name": group_name,
                    "description": f"Auto-created from MobiAdz extraction ({created} contacts)",
                    "type": "static",
                    "candidate_id": current_candidate.id,
                }
            )

            # Add recipients to group
            await group_repo.add_recipients(group.id, created_ids)
            group_id = group.id

            logger.info(
                f"✅ [RECIPIENTS] Created group '{group_name}' with {created} recipients"
            )
        except Exception as e:
            logger.error(f"❌ [RECIPIENTS] Failed to create group: {e}")

    logger.info(
        f"✅ [RECIPIENTS] MobiAdz import complete: "
        f"{created} created, {skipped} skipped, {errors} errors"
    )

    return MobiAdzBulkImportResponse(
        created=created, skipped=skipped, errors=errors, group_id=group_id
    )


@router.post("/{recipient_id}/add-tag")
async def add_tag_to_recipient(
    recipient_id: int,
    tag: str = Query(..., min_length=1, max_length=50),
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Add a tag to a recipient.

    Path Parameters:
    - recipient_id: Recipient ID

    Query Parameters:
    - tag: Tag to add

    Returns:
        Updated recipient

    Raises:
        404: Recipient not found
    """
    logger.info(f"🏷️  [RECIPIENTS] Adding tag '{tag}' to recipient {recipient_id}")

    repo = AsyncRecipientRepository(db)

    # Get recipient
    recipient = await repo.get_by_id(recipient_id)

    if not recipient:
        raise HTTPException(status_code=404, detail="Recipient not found")

    # Multi-tenant isolation
    if recipient.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Recipient not found")

    # Add tag
    existing_tags = recipient.tags.split(",") if recipient.tags else []
    if tag not in existing_tags:
        existing_tags.append(tag)
        updated_recipient = await repo.update(
            recipient_id, {"tags": ",".join(existing_tags)}
        )
        logger.info(f"✅ [RECIPIENTS] Added tag '{tag}' to recipient {recipient_id}")
        return RecipientResponse.model_validate(updated_recipient)

    logger.info(
        f"⏭️  [RECIPIENTS] Tag '{tag}' already exists on recipient {recipient_id}"
    )
    return RecipientResponse.model_validate(recipient)


# ==================== STATISTICS ENDPOINTS ====================


@router.get("/statistics/overview", response_model=RecipientStatistics)
async def get_recipient_statistics(
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get recipient statistics for the current candidate.

    Returns:
        Statistics including:
        - Total recipients
        - Active recipients
        - Unsubscribed count
        - Never contacted count
        - Average engagement score
        - Top companies
    """
    logger.info(
        f"📊 [RECIPIENTS] Fetching statistics for candidate {current_candidate.id}"
    )

    repo = AsyncRecipientRepository(db)

    stats = await repo.get_statistics(current_candidate.id)

    logger.info(f"✅ [RECIPIENTS] Statistics computed (total: {stats['total']})")

    return RecipientStatistics(**stats)


@router.get("/high-engagement/", response_model=List[RecipientResponse])
async def get_high_engagement_recipients(
    min_score: float = Query(50.0, ge=0.0, le=100.0),
    limit: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get recipients with high engagement scores.

    Query Parameters:
    - min_score: Minimum engagement score (0-100)
    - limit: Max results

    Returns:
        List of high-engagement recipients
    """
    logger.info(
        f"⭐ [RECIPIENTS] Fetching high-engagement recipients "
        f"(min_score: {min_score}) for candidate {current_candidate.id}"
    )

    repo = AsyncRecipientRepository(db)

    recipients = await repo.get_high_engagement(
        candidate_id=current_candidate.id, min_score=min_score, limit=limit
    )

    logger.info(f"✅ [RECIPIENTS] Found {len(recipients)} high-engagement recipients")

    return [RecipientResponse.model_validate(r) for r in recipients]


@router.get("/never-contacted/", response_model=List[RecipientResponse])
async def get_never_contacted_recipients(
    limit: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get recipients who have never been contacted.

    Query Parameters:
    - limit: Max results

    Returns:
        List of never-contacted recipients
    """
    logger.info(
        f"📭 [RECIPIENTS] Fetching never-contacted recipients "
        f"for candidate {current_candidate.id}"
    )

    repo = AsyncRecipientRepository(db)

    recipients = await repo.get_never_contacted(
        candidate_id=current_candidate.id, limit=limit
    )

    logger.info(f"✅ [RECIPIENTS] Found {len(recipients)} never-contacted recipients")

    return [RecipientResponse.model_validate(r) for r in recipients]


# ==================== ULTRA AI EMAIL GENERATION ====================


@router.post("/{recipient_id}/research")
async def research_recipient_company(
    recipient_id: int,
    mode: str = Query(
        "job", description="Research mode: 'job' (career) or 'market' (business/sales)"
    ),
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Ultra-Deep Combined Research: Company + Person Intelligence

    Uses 100% FREE OSINT techniques to gather comprehensive intelligence on:
    1. Company (tech stack, culture, pain points, buying signals)
    2. Person (work history, education, online presence, achievements)

    Mode: "job" or "market"
    - Job Mode: Career opportunities, hiring signals, tech stack, team culture
    - Market Mode: Business opportunities, pain points, buying signals, decision making authority

    Searches 50+ sources including:
    - Company: Website (10+ pages), LinkedIn, news, tech blogs
    - Person: LinkedIn, GitHub, Twitter, Google Scholar, Patents, News mentions

    Returns:
    - Combined intelligence report with company and person data
    - Confidence scores, key insights, talking points
    - Complete research in 30-60 seconds
    """
    try:
        # Validate mode
        if mode not in ["job", "market"]:
            raise HTTPException(
                status_code=400, detail="Mode must be 'job' or 'market'"
            )

        logger.info(
            f"🔬 [COMBINED RESEARCH] Starting {mode} mode research for recipient {recipient_id}"
        )

        repo = AsyncRecipientRepository(db)
        recipient = await repo.get_by_id(recipient_id)

        if not recipient or recipient.candidate_id != current_candidate.id:
            logger.warning(
                f"⚠️  [COMBINED RESEARCH] Recipient {recipient_id} not found or access denied"
            )
            raise HTTPException(status_code=404, detail="Recipient not found")

        company_name = recipient.company or "Unknown Company"
        recipient_name = recipient.name or recipient.email.split("@")[0]

        logger.info(
            f"✅ [COMBINED RESEARCH] Found recipient: {recipient_name} at {company_name}"
        )

        # Get company website (use company name if no website)
        company_website = (
            recipient.custom_fields.get("website", "")
            if recipient.custom_fields
            else ""
        )
        if not company_website and recipient.company:
            # Construct from company name
            company_clean = (
                recipient.company.lower()
                .replace(" ", "")
                .replace(".", "")
                .replace(",", "")
            )
            company_website = f"https://{company_clean}.com"
            logger.info(
                f"🌐 [COMBINED RESEARCH] Constructed website URL: {company_website}"
            )
        else:
            logger.info(
                f"🌐 [COMBINED RESEARCH] Using stored website URL: {company_website}"
            )

        # Combined research (company + person)
        logger.info(
            f"🚀 [COMBINED RESEARCH] Starting ultra-deep research on "
            f"{recipient_name} AND {company_name}"
        )

        orchestrator = CombinedResearchOrchestrator()
        research_report = await orchestrator.research_recipient(
            recipient_name=recipient_name,
            recipient_email=recipient.email,
            recipient_title=recipient.position,
            recipient_location=recipient.country,
            company_name=company_name,
            company_website=company_website,
            mode=mode,
        )
        await orchestrator.close()

        company_intel_dict = research_report.get("company_intelligence", {})
        person_intel = research_report.get("person_intelligence", {})
        key_insights = research_report.get("key_insights", {})

        logger.info(
            f"✅ [COMBINED RESEARCH] Research complete! "
            f"Combined confidence: {research_report['combined_confidence_score']}%, "
            f"Duration: {research_report['research_duration_seconds']:.1f}s"
        )

        # Log company summary
        tech_stack = company_intel_dict.get("tech_stack", [])
        if tech_stack:
            logger.info(f"  🏢 Company: {len(tech_stack)} technologies found")

        # Log person summary
        person_sources = person_intel.get("sources_found", 0)
        logger.info(f"  👤 Person: Found in {person_sources}/14 sources")

        # Log key insights
        if key_insights.get("summary"):
            logger.info(f"  💡 Insights: {', '.join(key_insights['summary'][:3])}")

        # Return combined research report
        logger.info(f"✅ [COMBINED RESEARCH] Returning research report")

        return {
            "success": True,
            "recipient_id": recipient_id,
            "recipient_name": recipient_name,
            "recipient_company": company_name,
            "research_mode": mode,
            "combined_confidence_score": research_report["combined_confidence_score"],
            "research_duration_seconds": research_report["research_duration_seconds"],
            "company_intelligence": company_intel_dict,
            "person_intelligence": person_intel,
            "key_insights": key_insights,
            "timestamp": research_report["timestamp"],
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [COMBINED RESEARCH] Research error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to complete research",
        )


# ==================== BATCH ENRICHMENT ENDPOINT ====================


class BatchEnrichmentRequest(BaseModel):
    """Request model for batch enrichment"""

    recipient_ids: List[int]
    enable_email_validation: bool = True
    enable_phone_enrichment: bool = True
    enable_linkedin_enrichment: bool = True
    enable_job_title_enrichment: bool = True
    enable_company_enrichment: bool = True
    async_mode: bool = False  # When true, run in background and stream progress


class BatchEnrichmentProgress(BaseModel):
    """Progress update for batch enrichment"""

    done: int
    total: int
    failed: int
    current_recipient: Optional[str] = None
    status: str  # 'running', 'completed', 'failed'


async def _init_enrichment_job(job_id: str, total: int, candidate_id: int) -> None:
    """Initialize a new enrichment job in the database."""
    async with AsyncSessionLocal() as session:
        db_job = EnrichmentJob(
            job_id=job_id,
            candidate_id=candidate_id,
            status=EnrichmentJobStatus.RUNNING.value,
            total_recipients=total,
            completed_recipients=0,
            failed_recipients=0,
        )
        session.add(db_job)
        await session.commit()
        logger.info(f"📝 [DB] Initialized enrichment job {job_id}")


async def _update_enrichment_job(
    job_id: str, progress: Dict[str, Any], current: Optional[str] = None
) -> None:
    """Update enrichment job progress in the database."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            __import__("sqlalchemy")
            .select(EnrichmentJob)
            .filter(EnrichmentJob.job_id == job_id)
        )
        db_job = result.scalar()
        if not db_job:
            return

        # Update progress fields
        if "done" in progress:
            db_job.completed_recipients = progress["done"]
        if "failed" in progress:
            db_job.failed_recipients = progress["failed"]
        if current:
            db_job.current_step = current

        db_job.updated_at = datetime.now(timezone.utc)
        await session.merge(db_job)
        await session.commit()


async def _complete_enrichment_job(job_id: str, payload: Dict[str, Any]) -> None:
    """Mark enrichment job as completed and store results in database."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            __import__("sqlalchemy")
            .select(EnrichmentJob)
            .filter(EnrichmentJob.job_id == job_id)
        )
        db_job = result.scalar()
        if not db_job:
            return

        db_job.mark_completed(
            enrichment_results=payload.get("enrichment_results"),
            validation_results=payload.get("email_validation_results"),
            statistics=payload.get("statistics"),
        )
        db_job.completed_recipients = payload.get("done", db_job.completed_recipients)
        db_job.failed_recipients = payload.get("failed", db_job.failed_recipients)

        await session.merge(db_job)
        await session.commit()
        logger.info(f"✅ [DB] Completed enrichment job {job_id}")


async def _fail_enrichment_job(job_id: str, error_message: str) -> None:
    """Mark enrichment job as failed and store error in database."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            __import__("sqlalchemy")
            .select(EnrichmentJob)
            .filter(EnrichmentJob.job_id == job_id)
        )
        db_job = result.scalar()
        if not db_job:
            return

        db_job.mark_failed(error_message)
        await session.merge(db_job)
        await session.commit()
        logger.error(f"❌ [DB] Failed enrichment job {job_id}: {error_message}")


def _get_enrichment_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Get enrichment job from database (synchronous wrapper for async check)."""
    import asyncio
    import sys

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    async def _get_job_async():
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                __import__("sqlalchemy")
                .select(EnrichmentJob)
                .filter(EnrichmentJob.job_id == job_id)
            )
            db_job = result.scalar()
            if db_job:
                return {
                    "job_id": db_job.job_id,
                    "candidate_id": db_job.candidate_id,
                    "status": db_job.status,
                    "progress": {
                        "done": db_job.completed_recipients,
                        "total": db_job.total_recipients,
                        "failed": db_job.failed_recipients,
                    },
                    "current_recipient": db_job.current_step,
                    "started_at": db_job.started_at.isoformat()
                    if db_job.started_at
                    else None,
                    "finished_at": db_job.finished_at.isoformat()
                    if db_job.finished_at
                    else None,
                    "updated_at": db_job.updated_at.isoformat()
                    if db_job.updated_at
                    else None,
                    "attempt_number": db_job.attempt_number,
                    "next_retry_at": db_job.next_retry_at.isoformat()
                    if db_job.next_retry_at
                    else None,
                    "error": db_job.last_error,
                    "statistics": db_job.statistics or {},
                }
            return None

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    return loop.run_until_complete(_get_job_async())


async def _get_enrichment_job_async(job_id: str) -> Optional[Dict[str, Any]]:
    """Async version to get enrichment job from database."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            sqlalchemy.select(EnrichmentJob).filter(EnrichmentJob.job_id == job_id)
        )
        db_job = result.scalar()
        if db_job:
            return {
                "job_id": db_job.job_id,
                "candidate_id": db_job.candidate_id,
                "status": db_job.status,
                "progress": {
                    "done": db_job.completed_recipients,
                    "total": db_job.total_recipients,
                    "failed": db_job.failed_recipients,
                },
                "current_recipient": db_job.current_step,
                "started_at": db_job.started_at.isoformat()
                if db_job.started_at
                else None,
                "finished_at": db_job.finished_at.isoformat()
                if db_job.finished_at
                else None,
                "updated_at": db_job.updated_at.isoformat()
                if db_job.updated_at
                else None,
                "attempt_number": db_job.attempt_number,
                "next_retry_at": db_job.next_retry_at.isoformat()
                if db_job.next_retry_at
                else None,
                "error": db_job.last_error,
                "statistics": db_job.statistics or {},
            }
        return None


async def _execute_batch_enrichment(
    recipients: List[Any],
    request: BatchEnrichmentRequest,
    db: AsyncSession,
    candidate_id: int,
    job_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Shared enrichment logic for sync and async flows."""

    orchestrator = EnrichmentOrchestrator(
        apollo_api_key=None,  # TODO: wire from settings
        hunter_api_key=None,
    )

    strategies = ["cache"]
    if request.enable_email_validation:
        strategies.append("hunter_verify")
    if (
        request.enable_phone_enrichment
        or request.enable_linkedin_enrichment
        or request.enable_job_title_enrichment
    ):
        strategies.append("apollo_enrich")

    enrichment_results: List[Dict[str, Any]] = []
    email_validation_results: List[Dict[str, Any]] = []
    failed_count = 0

    total = len(recipients)

    for idx, recipient in enumerate(recipients):
        try:
            if job_id:
                await _update_enrichment_job(
                    job_id,
                    {
                        "done": idx,
                        "total": total,
                        "failed": failed_count,
                    },
                    current=recipient.email,
                )

            record = {
                "email": recipient.email,
                "name": recipient.name,
                "company": recipient.company,
                "title": recipient.position,
                "country": recipient.country,
            }

            enriched = await orchestrator.enrich_record(record, strategies=strategies)

            if enriched.get("phone") and not recipient.phone:
                recipient.phone = enriched["phone"]

            if enriched.get("linkedin_url"):
                if not recipient.custom_fields:
                    recipient.custom_fields = {}
                recipient.custom_fields["linkedin_url"] = enriched["linkedin_url"]

            if enriched.get("title") and not recipient.position:
                recipient.position = enriched["title"]

            if not recipient.custom_fields:
                recipient.custom_fields = {}
            recipient.custom_fields["enrichment_quality"] = enriched.get(
                "enrichment_quality", 0.0
            )
            recipient.custom_fields["enrichment_sources"] = enriched.get(
                "enrichment_sources", []
            )
            recipient.custom_fields["enriched_at"] = datetime.now(timezone.utc).isoformat()

            if (
                request.enable_email_validation
                and enriched.get("email_verified") is not None
            ):
                email_validation_results.append(
                    {
                        "recipient_id": recipient.id,
                        "email": recipient.email,
                        "recipient_name": recipient.name,
                        "is_valid": enriched.get("email_verified", False),
                        "deliverability": enriched.get("email_result", "unknown"),
                        "score": enriched.get("email_score", 0),
                        "mx_records_found": enriched.get("email_score", 0) > 50,
                        "is_disposable": enriched.get("is_disposable", False),
                        "is_role_based": enriched.get("is_role_email", False),
                        "is_free_email": enriched.get("is_free_email", False),
                        "reason": "Email verified"
                        if enriched.get("email_verified")
                        else "Email validation failed",
                    }
                )

            enrichment_results.append(
                {
                    "recipient_id": recipient.id,
                    "name": recipient.name,
                    "email": recipient.email,
                    "enrichment_quality": enriched.get("enrichment_quality", 0.0),
                    "enrichment_sources": enriched.get("enrichment_sources", []),
                    "phone": enriched.get("phone"),
                    "linkedin_url": enriched.get("linkedin_url"),
                    "title": enriched.get("title"),
                    "email_verified": enriched.get("email_verified"),
                    "email_score": enriched.get("email_score"),
                }
            )

            if job_id:
                await _update_enrichment_job(
                    job_id,
                    {
                        "done": idx + 1,
                        "total": total,
                        "failed": failed_count,
                    },
                    current=recipient.email,
                )

        except Exception as e:
            logger.error(
                f"❌ [{idx + 1}/{len(recipients)}] Failed to enrich {recipient.name}: {str(e)}"
            )
            failed_count += 1
            enrichment_results.append(
                {
                    "recipient_id": recipient.id,
                    "name": recipient.name,
                    "email": recipient.email,
                    "error": str(e),
                    "enrichment_quality": 0.0,
                }
            )
            if job_id:
                await _update_enrichment_job(
                    job_id,
                    {
                        "done": idx + 1,
                        "total": total,
                        "failed": failed_count,
                    },
                    current=recipient.email,
                )

    await db.commit()

    stats = orchestrator.get_stats()
    await orchestrator.close()

    result_payload = {
        "success": True,
        "total_recipients": len(recipients),
        "enriched_count": len(recipients) - failed_count,
        "failed_count": failed_count,
        "enrichment_results": enrichment_results,
        "email_validation_results": email_validation_results,
        "statistics": stats,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if job_id:
        await _complete_enrichment_job(
            job_id,
            {
                **result_payload,
                "progress": {
                    "done": len(recipients),
                    "total": len(recipients),
                    "failed": failed_count,
                },
            },
        )

    logger.info(
        f"✅ [BATCH ENRICHMENT] Completed for candidate {candidate_id}! "
        f"Success: {len(recipients) - failed_count}/{len(recipients)}, Failed: {failed_count}, "
        f"Cache hits: {stats.get('cache_hits', 0)}"
    )

    return result_payload


async def _fetch_valid_recipients(
    db: AsyncSession, recipient_ids: List[int], candidate_id: int
) -> List[Any]:
    repo = AsyncRecipientRepository(db)
    recipients: List[Any] = []

    for rid in recipient_ids:
        recipient = await repo.get_by_id(rid)
        if not recipient or recipient.candidate_id != candidate_id:
            logger.warning(
                f"⚠️  [BATCH ENRICHMENT] Recipient {rid} not found or access denied"
            )
            continue
        recipients.append(recipient)

    return recipients


async def _run_async_enrichment_job(
    job_id: str, request: BatchEnrichmentRequest, candidate_id: int
) -> None:
    try:
        async with AsyncSessionLocal() as session:
            # Create DB job record
            db_job = EnrichmentJob(
                job_id=job_id,
                candidate_id=candidate_id,
                status=EnrichmentJobStatus.RUNNING.value,
                started_at=datetime.now(timezone.utc),
                enrichment_config=request.dict(),
            )
            session.add(db_job)
            await session.flush()

            recipients = await _fetch_valid_recipients(
                session, request.recipient_ids, candidate_id
            )

            if not recipients:
                db_job.mark_failed("No valid recipients found")
                await session.merge(db_job)
                await session.commit()
                await _fail_enrichment_job(job_id, "No valid recipients found")
                return

            # Update total recipients in job record
            db_job.total_recipients = len(recipients)
            await session.merge(db_job)
            await session.flush()

            # Execute enrichment with retry logic
            result = await _execute_batch_enrichment(
                recipients, request, session, candidate_id, job_id
            )

            # Update DB job on success
            db_job.mark_completed(
                result.get("enrichment_results"),
                result.get("email_validation_results"),
                result.get("statistics"),
            )
            db_job.completed_recipients = len(recipients) - result.get(
                "failed_count", 0
            )
            db_job.failed_recipients = result.get("failed_count", 0)
            await session.merge(db_job)
            await session.commit()

    except Exception as exc:  # Capture any unexpected failures
        logger.error(
            f"❌ [BATCH ENRICHMENT] Async job {job_id} failed: {str(exc)}",
            exc_info=True,
        )

        # Attempt to schedule retry
        async with AsyncSessionLocal() as session:
            try:
                job_query = await session.execute(
                    __import__("sqlalchemy")
                    .select(EnrichmentJob)
                    .filter(EnrichmentJob.job_id == job_id)
                )
                db_job = job_query.scalar()
                if db_job and db_job.schedule_retry(
                    calculate_backoff_delay(db_job.attempt_number)
                ):
                    logger.info(
                        f"📅 [BATCH ENRICHMENT] Scheduled retry #{db_job.attempt_number} "
                        f"for job {job_id} at {db_job.next_retry_at}"
                    )
                    await session.merge(db_job)
                else:
                    db_job.mark_failed(str(exc))
                    await session.merge(db_job)
                await session.commit()
            except Exception as db_err:
                logger.error(f"Failed to update retry state: {db_err}")

        await _fail_enrichment_job(job_id, str(exc))


@router.post("/batch-enrich")
async def batch_enrich_recipients(
    request: BatchEnrichmentRequest,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Batch enrichment of multiple recipients using EnrichmentOrchestrator.

    This endpoint enriches multiple recipients with:
    - Email verification (Hunter.io)
    - Phone number discovery
    - LinkedIn profile finding
    - Job title validation
    - Company information

    Uses cache-first strategy to minimize API costs.
    Supports concurrent processing with progress tracking.

    Returns:
    - Enriched recipient data with confidence scores
    - Email validation results
    - Fraud detection results (if enabled)
    - Statistics on enrichment quality
    """
    try:
        logger.info(
            f"🔄 [BATCH ENRICHMENT] Starting batch enrichment for {len(request.recipient_ids)} recipients"
        )

        recipients = await _fetch_valid_recipients(
            db, request.recipient_ids, current_candidate.id
        )

        if not recipients:
            raise HTTPException(status_code=404, detail="No valid recipients found")

        logger.info(f"✅ [BATCH ENRICHMENT] Found {len(recipients)} valid recipients")

        # Async mode: start job and return immediately for progress streaming
        if request.async_mode:
            job_id = str(uuid4())
            await _init_enrichment_job(job_id, len(recipients), current_candidate.id)
            asyncio.create_task(
                _run_async_enrichment_job(job_id, request, current_candidate.id)
            )

            return {
                "success": True,
                "status": "running",
                "job_id": job_id,
                "progress_url": f"/recipients/batch-enrich/status/{job_id}",
                "stream_url": f"/recipients/batch-enrich/stream/{job_id}",
                "total_recipients": len(recipients),
            }

        # Sync mode: execute now and return results
        result = await _execute_batch_enrichment(
            recipients, request, db, current_candidate.id
        )
        result.update(
            {
                "status": "completed",
                "job_id": None,
                "progress_url": None,
                "stream_url": None,
            }
        )
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"❌ [BATCH ENRICHMENT] Batch enrichment error: {str(e)}", exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to complete batch enrichment",
        )


@router.get("/batch-enrich/status/{job_id}")
async def get_batch_enrichment_status(
    job_id: str, current_candidate: Candidate = Depends(get_current_candidate)
):
    job = await _get_enrichment_job_async(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Enrichment job not found")

    if job.get("candidate_id") != current_candidate.id:
        raise HTTPException(status_code=403, detail="Access denied for this job")

    return job


@router.get("/batch-enrich/stream/{job_id}")
async def stream_batch_enrichment(
    job_id: str, current_candidate: Candidate = Depends(get_current_candidate)
):
    job = await _get_enrichment_job_async(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Enrichment job not found")

    if job.get("candidate_id") != current_candidate.id:
        raise HTTPException(status_code=403, detail="Access denied for this job")

    async def event_generator():
        while True:
            current_job = await _get_enrichment_job_async(job_id)
            if not current_job:
                break

            payload = {
                "job_id": job_id,
                "status": current_job.get("status"),
                "progress": current_job.get("progress", {}),
                "current_recipient": current_job.get("current_recipient"),
                "statistics": current_job.get("statistics", {}),
                "error": current_job.get("error"),
                "updated_at": current_job.get("updated_at"),
                "attempt_number": current_job.get("attempt_number"),
                "next_retry_at": current_job.get("next_retry_at"),
            }

            yield f"data: {json.dumps(payload)}\n\n"

            if current_job.get("status") in {"completed", "failed"}:
                break

            await asyncio.sleep(1)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# ==================== ENTITY RESOLUTION / DEDUPLICATION ENDPOINT ====================


class DeduplicationRequest(BaseModel):
    """Request model for deduplication"""

    recipient_ids: List[int]
    match_threshold: float = 0.8  # Minimum similarity to consider a match
    high_confidence_threshold: float = 0.95  # High confidence match


class DuplicateGroup(BaseModel):
    """A group of duplicate recipients"""

    group_id: str
    confidence: float
    recipients: List[Dict[str, Any]]
    canonical_data: Dict[str, Any]
    field_provenance: Dict[str, str]
    suggested_merge_strategy: str  # "keep_first", "keep_most_complete", "manual"


@router.post("/deduplicate")
async def deduplicate_recipients(
    request: DeduplicationRequest,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Find and resolve duplicate recipients using EntityResolutionService.

    This endpoint uses fuzzy matching to identify duplicate recipients based on:
    - Email similarity (exact and fuzzy)
    - Name similarity (handles typos, variations)
    - Company matching
    - Phone number normalization

    Features:
    - Confidence scoring (0.0-1.0)
    - Field-level provenance tracking
    - Multiple merge strategies
    - High/low confidence grouping

    Returns:
    - Duplicate groups with confidence scores
    - Suggested merge strategies
    - Field provenance (which source for each field)
    - Statistics on duplicates found
    """
    try:
        logger.info(
            f"🔍 [DEDUPLICATION] Starting deduplication for {len(request.recipient_ids)} recipients"
        )

        # Validate recipient access and fetch recipients
        repo = AsyncRecipientRepository(db)
        recipients = []
        for rid in request.recipient_ids:
            recipient = await repo.get_by_id(rid)
            if not recipient or recipient.candidate_id != current_candidate.id:
                logger.warning(
                    f"⚠️  [DEDUPLICATION] Recipient {rid} not found or access denied"
                )
                continue
            recipients.append(recipient)

        if not recipients:
            raise HTTPException(status_code=404, detail="No valid recipients found")

        if len(recipients) < 2:
            logger.info(
                f"ℹ️  [DEDUPLICATION] Less than 2 recipients, no duplicates possible"
            )
            return {
                "success": True,
                "total_recipients": len(recipients),
                "duplicate_groups": [],
                "unique_recipients": len(recipients),
                "potential_savings": 0,
                "statistics": {"total_comparisons": 0, "duplicates_found": 0},
            }

        logger.info(f"✅ [DEDUPLICATION] Found {len(recipients)} valid recipients")

        # Convert recipients to Entity objects
        entities = []
        for recipient in recipients:
            entity = Entity(
                entity_id=str(recipient.id),
                entity_type="person",
                source="database",
                data={
                    "id": recipient.id,
                    "name": recipient.name,
                    "email": recipient.email,
                    "company": recipient.company,
                    "phone": recipient.phone,
                    "position": recipient.position,
                    "country": recipient.country,
                    "tags": recipient.tags or [],
                    "custom_fields": recipient.custom_fields or {},
                },
                confidence=1.0,
            )
            entities.append(entity)

        # Initialize entity resolution service
        resolution_service = EntityResolutionService(
            match_threshold=request.match_threshold,
            high_confidence_threshold=request.high_confidence_threshold,
        )

        # Resolve entities (find duplicates)
        logger.info(f"🔄 [DEDUPLICATION] Running entity resolution...")
        resolved_entities = resolution_service.resolve_entities(entities)

        # Group duplicates
        duplicate_groups = []
        group_map: Dict[str, List[Entity]] = {}

        for resolved in resolved_entities:
            if (
                len(resolved.source_entities) > 1
            ):  # Only groups with 2+ entities are duplicates
                group_id = resolved.merged_id
                group_map[group_id] = resolved.source_entities

                # Determine merge strategy
                if resolved.confidence >= request.high_confidence_threshold:
                    suggested_strategy = "keep_most_complete"
                elif resolved.confidence >= request.match_threshold:
                    suggested_strategy = "manual"
                else:
                    suggested_strategy = "keep_first"

                duplicate_groups.append(
                    {
                        "group_id": group_id,
                        "confidence": resolved.confidence,
                        "recipients": [
                            entity.data for entity in resolved.source_entities
                        ],
                        "canonical_data": resolved.canonical_data,
                        "field_provenance": resolved.field_provenance,
                        "suggested_merge_strategy": suggested_strategy,
                        "similarity_scores": {
                            f"{k[0]}_{k[1]}": v
                            for k, v in resolved.similarity_scores.items()
                        },
                    }
                )

        # Calculate statistics
        total_duplicates = sum(len(group_map[gid]) - 1 for gid in group_map)
        unique_recipients = len(recipients) - total_duplicates
        potential_savings = total_duplicates  # Number of recipients that can be removed

        stats = resolution_service.get_stats()

        logger.info(
            f"✅ [DEDUPLICATION] Completed! "
            f"Found {len(duplicate_groups)} duplicate groups, "
            f"Total duplicates: {total_duplicates}, "
            f"Unique: {unique_recipients}, "
            f"Potential savings: {potential_savings}"
        )

        return {
            "success": True,
            "total_recipients": len(recipients),
            "duplicate_groups": duplicate_groups,
            "unique_recipients": unique_recipients,
            "potential_savings": potential_savings,
            "statistics": stats,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [DEDUPLICATION] Deduplication error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to complete deduplication",
        )


class MergeRecipientsRequest(BaseModel):
    """Request model for merging duplicate recipients"""

    recipient_ids: List[int]
    keep_recipient_id: int
    merge_strategy: str = (
        "keep_most_complete"  # "keep_first", "keep_most_complete", "custom"
    )
    custom_data: Optional[Dict[str, Any]] = None  # For custom merge strategy


@router.post("/merge-duplicates")
async def merge_duplicate_recipients(
    request: MergeRecipientsRequest,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Merge duplicate recipients into a single recipient.

    Strategies:
    - keep_first: Keep data from the first recipient
    - keep_most_complete: Keep data from recipient with most fields filled
    - custom: Use custom_data provided in request

    This will:
    1. Merge all data into the kept recipient
    2. Update any references (campaigns, emails, etc.)
    3. Soft-delete the duplicate recipients
    4. Track provenance of merged fields

    Returns:
    - Merged recipient data
    - List of deleted recipient IDs
    - Merge summary
    """
    try:
        logger.info(
            f"🔀 [MERGE] Merging {len(request.recipient_ids)} recipients into {request.keep_recipient_id}"
        )

        # Validate access to all recipients
        repo = AsyncRecipientRepository(db)
        recipients_to_merge = []
        keep_recipient = None

        for rid in request.recipient_ids:
            recipient = await repo.get_by_id(rid)
            if not recipient or recipient.candidate_id != current_candidate.id:
                raise HTTPException(
                    status_code=404,
                    detail=f"Recipient {rid} not found or access denied",
                )

            if rid == request.keep_recipient_id:
                keep_recipient = recipient
            else:
                recipients_to_merge.append(recipient)

        if not keep_recipient:
            raise HTTPException(
                status_code=400,
                detail="keep_recipient_id must be in recipient_ids list",
            )

        if len(recipients_to_merge) == 0:
            raise HTTPException(
                status_code=400, detail="Must have at least 2 recipients to merge"
            )

        logger.info(
            f"✅ [MERGE] Merging {len(recipients_to_merge)} recipients into {keep_recipient.name} ({keep_recipient.id})"
        )

        # Apply merge strategy
        merged_fields = {}
        field_provenance = {}

        if request.merge_strategy == "custom" and request.custom_data:
            # Use custom data
            for key, value in request.custom_data.items():
                if value is not None:
                    merged_fields[key] = value
                    field_provenance[key] = "custom"

        elif request.merge_strategy == "keep_most_complete":
            # Keep data from recipient with most non-null fields
            all_recipients = [keep_recipient] + recipients_to_merge

            for field in [
                "name",
                "email",
                "phone",
                "company",
                "position",
                "country",
                "linkedin_url",
            ]:
                best_value = None
                best_source = None

                for recipient in all_recipients:
                    value = getattr(recipient, field, None)
                    if field == "linkedin_url" and recipient.custom_fields:
                        value = recipient.custom_fields.get("linkedin_url")

                    if value and (
                        not best_value or len(str(value)) > len(str(best_value))
                    ):
                        best_value = value
                        best_source = str(recipient.id)

                if best_value:
                    merged_fields[field] = best_value
                    field_provenance[field] = f"recipient_{best_source}"

        else:  # keep_first
            # Keep data from keep_recipient, only fill missing fields
            for field in ["name", "email", "phone", "company", "position", "country"]:
                value = getattr(keep_recipient, field, None)
                if value:
                    merged_fields[field] = value
                    field_provenance[field] = f"recipient_{keep_recipient.id}"
                else:
                    # Find first non-null value from other recipients
                    for recipient in recipients_to_merge:
                        value = getattr(recipient, field, None)
                        if value:
                            merged_fields[field] = value
                            field_provenance[field] = f"recipient_{recipient.id}"
                            break

        # Update keep_recipient with merged data
        for field, value in merged_fields.items():
            if field == "linkedin_url":
                if not keep_recipient.custom_fields:
                    keep_recipient.custom_fields = {}
                keep_recipient.custom_fields["linkedin_url"] = value
            else:
                setattr(keep_recipient, field, value)

        # Store merge metadata
        if not keep_recipient.custom_fields:
            keep_recipient.custom_fields = {}
        keep_recipient.custom_fields["merged_from"] = [
            r.id for r in recipients_to_merge
        ]
        keep_recipient.custom_fields["merge_strategy"] = request.merge_strategy
        keep_recipient.custom_fields["field_provenance"] = field_provenance
        keep_recipient.custom_fields["merged_at"] = datetime.now(timezone.utc).isoformat()

        # Merge tags
        all_tags = set(keep_recipient.tags or [])
        for recipient in recipients_to_merge:
            if recipient.tags:
                all_tags.update(recipient.tags)
        keep_recipient.tags = list(all_tags)

        # Soft-delete the duplicate recipients
        deleted_ids = []
        for recipient in recipients_to_merge:
            recipient.is_active = False
            recipient.tags = (recipient.tags or []) + ["merged_duplicate"]
            if not recipient.custom_fields:
                recipient.custom_fields = {}
            recipient.custom_fields["merged_into"] = keep_recipient.id
            recipient.custom_fields["merged_at"] = datetime.now(timezone.utc).isoformat()
            deleted_ids.append(recipient.id)
            logger.info(
                f"  🗑️  Soft-deleted recipient {recipient.id} ({recipient.name})"
            )

        # Commit changes
        await db.commit()
        await db.refresh(keep_recipient)

        logger.info(
            f"✅ [MERGE] Successfully merged {len(deleted_ids)} recipients into {keep_recipient.id}"
        )

        return {
            "success": True,
            "merged_recipient": RecipientResponse.model_validate(
                keep_recipient
            ).model_dump(),
            "deleted_recipient_ids": deleted_ids,
            "field_provenance": field_provenance,
            "merge_strategy": request.merge_strategy,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [MERGE] Merge error: {str(e)}", exc_info=True)
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to merge recipients",
        )


@router.post("/merge-execute")
async def execute_merge(
    request: MergeRecipientsRequest,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Execute merge with full tracking and rollback capability.

    Stores merge history in database for audit trail and rollback.
    Calculates confidence scores based on field matches.
    """
    merge_id = str(uuid4())
    try:
        logger.info(
            f"🔀 [MERGE] Starting merge {merge_id}: {len(request.recipient_ids)} recipients into {request.keep_recipient_id}"
        )

        # Validate access to all recipients
        repo = AsyncRecipientRepository(db)
        recipients_to_merge = []
        keep_recipient = None

        for rid in request.recipient_ids:
            recipient = await repo.get_by_id(rid)
            if not recipient or recipient.candidate_id != current_candidate.id:
                raise HTTPException(
                    status_code=404,
                    detail=f"Recipient {rid} not found or access denied",
                )

            if rid == request.keep_recipient_id:
                keep_recipient = recipient
            else:
                recipients_to_merge.append(recipient)

        if not keep_recipient:
            raise HTTPException(
                status_code=400,
                detail="keep_recipient_id must be in recipient_ids list",
            )

        if len(recipients_to_merge) == 0:
            raise HTTPException(
                status_code=400, detail="Must have at least 2 recipients to merge"
            )

        # Save data snapshot for rollback
        data_snapshot = {
            "primary_id": keep_recipient.id,
            "primary_data": {
                "name": keep_recipient.name,
                "email": keep_recipient.email,
                "phone": keep_recipient.phone,
                "company": keep_recipient.company,
                "position": keep_recipient.position,
                "country": keep_recipient.country,
                "tags": keep_recipient.tags or [],
                "custom_fields": keep_recipient.custom_fields or {},
            },
            "secondary_recipients": [
                {
                    "id": r.id,
                    "name": r.name,
                    "email": r.email,
                    "phone": r.phone,
                    "company": r.company,
                    "position": r.position,
                    "country": r.country,
                    "tags": r.tags or [],
                    "is_active": r.is_active,
                }
                for r in recipients_to_merge
            ],
        }

        logger.info(
            f"✅ [MERGE] Merging {len(recipients_to_merge)} recipients into {keep_recipient.name} ({keep_recipient.id})"
        )

        # Apply merge strategy
        merged_fields = {}
        field_provenance = {}
        confidence_score = 0

        if request.merge_strategy == "custom" and request.custom_data:
            # Use custom data
            for key, value in request.custom_data.items():
                if value is not None:
                    merged_fields[key] = value
                    field_provenance[key] = "custom"
            confidence_score = 50  # Custom merge is lower confidence

        elif request.merge_strategy == "keep_most_complete":
            # Keep data from recipient with most non-null fields
            all_recipients = [keep_recipient] + recipients_to_merge

            for field in [
                "name",
                "email",
                "phone",
                "company",
                "position",
                "country",
                "linkedin_url",
            ]:
                best_value = None
                best_source = None
                matching_count = 0

                for recipient in all_recipients:
                    value = getattr(recipient, field, None)
                    if field == "linkedin_url" and recipient.custom_fields:
                        value = recipient.custom_fields.get("linkedin_url")

                    if value and (
                        not best_value or len(str(value)) > len(str(best_value))
                    ):
                        best_value = value
                        best_source = str(recipient.id)
                        matching_count += 1

                if best_value:
                    merged_fields[field] = best_value
                    field_provenance[field] = f"recipient_{best_source}"
                    confidence_score += (
                        matching_count * 15
                    )  # Boost confidence per match

        else:  # keep_first
            # Keep data from keep_recipient, only fill missing fields
            for field in ["name", "email", "phone", "company", "position", "country"]:
                value = getattr(keep_recipient, field, None)
                if value:
                    merged_fields[field] = value
                    field_provenance[field] = f"recipient_{keep_recipient.id}"
                    confidence_score += 20
                else:
                    # Find first non-null value from other recipients
                    for recipient in recipients_to_merge:
                        value = getattr(recipient, field, None)
                        if value:
                            merged_fields[field] = value
                            field_provenance[field] = f"recipient_{recipient.id}"
                            confidence_score += 10
                            break

        # Normalize confidence score to 0-100
        confidence_score = min(100, max(0, confidence_score))

        # Update keep_recipient with merged data
        for field, value in merged_fields.items():
            if field == "linkedin_url":
                if not keep_recipient.custom_fields:
                    keep_recipient.custom_fields = {}
                keep_recipient.custom_fields["linkedin_url"] = value
            else:
                setattr(keep_recipient, field, value)

        # Store merge metadata
        if not keep_recipient.custom_fields:
            keep_recipient.custom_fields = {}
        keep_recipient.custom_fields["merged_from"] = [
            r.id for r in recipients_to_merge
        ]
        keep_recipient.custom_fields["merge_strategy"] = request.merge_strategy
        keep_recipient.custom_fields["field_provenance"] = field_provenance
        keep_recipient.custom_fields["merge_id"] = merge_id
        keep_recipient.custom_fields["merged_at"] = datetime.now(timezone.utc).isoformat()

        # Merge tags
        all_tags = set(keep_recipient.tags or [])
        for recipient in recipients_to_merge:
            if recipient.tags:
                all_tags.update(recipient.tags)
        keep_recipient.tags = list(all_tags)

        # Soft-delete the duplicate recipients
        deleted_ids = []
        for recipient in recipients_to_merge:
            recipient.is_active = False
            recipient.tags = (recipient.tags or []) + ["merged_duplicate"]
            if not recipient.custom_fields:
                recipient.custom_fields = {}
            recipient.custom_fields["merged_into"] = keep_recipient.id
            recipient.custom_fields["merged_at"] = datetime.now(timezone.utc).isoformat()
            deleted_ids.append(recipient.id)
            logger.info(
                f"  🗑️  Soft-deleted recipient {recipient.id} ({recipient.name})"
            )

        # Track merge in MergeHistory
        merge_record = MergeHistory(
            merge_id=merge_id,
            candidate_id=current_candidate.id,
            primary_recipient_id=keep_recipient.id,
            secondary_recipient_ids=deleted_ids,
            strategy=request.merge_strategy or "keep_most_complete",
            confidence_score=int(confidence_score),
            merged_fields=merged_fields,
            conflicts_resolved={},  # Could be enhanced with conflict details
            data_snapshot=data_snapshot,
            status="completed",
            created_by=str(current_candidate.id),
            notes=f"Merged {len(deleted_ids)} recipients into {keep_recipient.id}",
        )
        db.add(merge_record)

        # Commit changes
        await db.commit()
        await db.refresh(keep_recipient)

        logger.info(
            f"✅ [MERGE] Successfully completed merge {merge_id}: {len(deleted_ids)} recipients merged"
        )

        return {
            "success": True,
            "merge_id": merge_id,
            "merged_recipient": RecipientResponse.model_validate(
                keep_recipient
            ).model_dump(),
            "deleted_recipient_ids": deleted_ids,
            "field_provenance": field_provenance,
            "confidence_score": int(confidence_score),
            "merge_strategy": request.merge_strategy,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [MERGE] Merge {merge_id} error: {str(e)}", exc_info=True)
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to merge recipients",
        )


@router.post("/merge/{merge_id}/rollback")
async def rollback_merge(
    merge_id: str,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Rollback a merge operation by restoring original recipient states.

    This will:
    1. Restore soft-deleted recipients (set is_active=True)
    2. Restore primary recipient to original state (if modified)
    3. Mark merge record as rolled_back in audit trail

    Returns:
    - Rollback confirmation
    - Restored recipients
    """
    try:
        logger.info(f"↩️  [ROLLBACK] Starting rollback for merge {merge_id}")

        # Find merge record
        result = await db.execute(
            sqlalchemy.select(MergeHistory).filter(
                MergeHistory.merge_id == merge_id,
                MergeHistory.candidate_id == current_candidate.id,
            )
        )
        merge_record = result.scalar()

        if not merge_record:
            raise HTTPException(
                status_code=404, detail=f"Merge record {merge_id} not found"
            )

        if merge_record.status == "rolled_back":
            raise HTTPException(
                status_code=400, detail="This merge has already been rolled back"
            )

        # Restore primary recipient from snapshot if available
        repo = AsyncRecipientRepository(db)
        snapshot = merge_record.data_snapshot or {}

        # Restore secondary recipients (re-activate)
        restored_count = 0
        for secondary_id in merge_record.secondary_recipient_ids:
            secondary = await repo.get_by_id(secondary_id)
            if secondary:
                secondary.is_active = True
                tags = secondary.tags or []
                if "merged_duplicate" in tags:
                    tags.remove("merged_duplicate")
                secondary.tags = tags
                if secondary.custom_fields and "merged_into" in secondary.custom_fields:
                    del secondary.custom_fields["merged_into"]
                restored_count += 1
                logger.info(f"  ✅ Restored recipient {secondary_id}")

        # Restore primary recipient to original data
        primary = await repo.get_by_id(merge_record.primary_recipient_id)
        if primary and snapshot.get("primary_data"):
            original_data = snapshot["primary_data"]
            primary.name = original_data.get("name", primary.name)
            primary.email = original_data.get("email", primary.email)
            primary.phone = original_data.get("phone", primary.phone)
            primary.company = original_data.get("company", primary.company)
            primary.position = original_data.get("position", primary.position)
            primary.country = original_data.get("country", primary.country)
            primary.tags = original_data.get("tags", primary.tags or [])

            if primary.custom_fields:
                for key in [
                    "merged_from",
                    "merge_strategy",
                    "field_provenance",
                    "merge_id",
                    "merged_at",
                ]:
                    if key in primary.custom_fields:
                        del primary.custom_fields[key]

            logger.info(f"  ✅ Restored primary recipient {primary.id}")

        # Mark merge as rolled back
        merge_record.mark_rolled_back(reason="User initiated rollback")

        # Commit changes
        await db.commit()

        logger.info(
            f"✅ [ROLLBACK] Successfully rolled back merge {merge_id}: {restored_count} recipients restored"
        )

        return {
            "success": True,
            "merge_id": merge_id,
            "restored_recipient_ids": merge_record.secondary_recipient_ids,
            "restored_primary_id": merge_record.primary_recipient_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"❌ [ROLLBACK] Rollback {merge_id} error: {str(e)}", exc_info=True
        )
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to rollback merge",
        )


@router.get("/merge/{merge_id}/history")
async def get_merge_history(
    merge_id: str,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get merge history record with full details.
    """
    result = await db.execute(
        sqlalchemy.select(MergeHistory).filter(
            MergeHistory.merge_id == merge_id,
            MergeHistory.candidate_id == current_candidate.id,
        )
    )
    merge_record = result.scalar()

    if not merge_record:
        raise HTTPException(status_code=404, detail="Merge record not found")

    return {
        "merge_id": merge_record.merge_id,
        "primary_recipient_id": merge_record.primary_recipient_id,
        "secondary_recipient_ids": merge_record.secondary_recipient_ids,
        "strategy": merge_record.strategy,
        "confidence_score": merge_record.confidence_score,
        "merged_fields": merge_record.merged_fields,
        "status": merge_record.status,
        "created_at": merge_record.created_at.isoformat(),
        "rolled_back_at": merge_record.rolled_back_at.isoformat()
        if merge_record.rolled_back_at
        else None,
        "rollback_reason": merge_record.rollback_reason,
    }


@router.post("/{recipient_id}/generate-emails")
async def generate_email_variations(
    recipient_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Generate 5 email variations using ULTRA AI.

    Requires:
    - Candidate's parsed resume (from /resumes/parse)
    - Company intelligence (from /recipients/{id}/research)

    Generates 5 tones:
    - Professional
    - Enthusiastic
    - Story-driven
    - Value-first
    - Consultant

    Returns:
    - 5 email drafts with personalization scores, matched skills, talking points
    """
    try:
        logger.info(
            f"📧 [Ultra AI] Starting email generation for recipient {recipient_id}"
        )

        repo = AsyncRecipientRepository(db)
        recipient = await repo.get_by_id(recipient_id)

        if not recipient or recipient.candidate_id != current_candidate.id:
            logger.warning(
                f"⚠️  [Ultra AI] Recipient {recipient_id} not found or access denied"
            )
            raise HTTPException(status_code=404, detail="Recipient not found")

        logger.info(
            f"✅ [Ultra AI] Found recipient: {recipient.name or recipient.email} "
            f"at {recipient.company or 'Unknown Company'}"
        )

        # Get candidate's parsed resume from parsed_resumes table
        logger.info(
            f"📄 [Ultra AI] Retrieving parsed resume for candidate {current_candidate.id}"
        )

        logger.warning(f"🔍 [DEBUG] Querying parsed_resumes for candidate_id={current_candidate.id}")
        pr_result = await db.execute(
            sqlalchemy.select(ParsedResumeModel)
            .where(ParsedResumeModel.candidate_id == current_candidate.id)
            .order_by(ParsedResumeModel.id.desc())
            .limit(1)
        )
        parsed_resume_row = pr_result.scalar_one_or_none()
        logger.warning(f"🔍 [DEBUG] parsed_resume_row={parsed_resume_row}, skills={current_candidate.skills is not None}")

        if not parsed_resume_row and not current_candidate.skills:
            logger.error(
                f"❌ [Ultra AI] No parsed resume found for candidate {current_candidate.id}"
            )
            raise HTTPException(
                status_code=400,
                detail="No parsed resume found. Please upload and parse your resume first at /settings.",
            )

        if parsed_resume_row:
            logger.info(
                f"✅ [Ultra AI] Retrieved parsed resume: "
                f"{len(parsed_resume_row.technical_skills or [])} skills, "
                f"{len(parsed_resume_row.work_experience or [])} experiences, "
                f"{len(parsed_resume_row.projects or [])} projects"
            )
            candidate_data = {
                "name": parsed_resume_row.name or current_candidate.full_name,
                "title": current_candidate.title or "Software Engineer",
                "contact": {"email": parsed_resume_row.email or current_candidate.email},
                "skills_raw": (parsed_resume_row.technical_skills or []) + (parsed_resume_row.soft_skills or []),
                "skills_categorized": {"technical": parsed_resume_row.technical_skills or [], "soft": parsed_resume_row.soft_skills or []},
                "experience": parsed_resume_row.work_experience or [],
                "education": parsed_resume_row.education or [],
                "projects": parsed_resume_row.projects or [],
                "publications": parsed_resume_row.publications or [],
            }
        else:
            parsed_resume = current_candidate.skills
            logger.info(
                f"✅ [Ultra AI] Retrieved parsed resume from candidate.skills: "
                f"{len(parsed_resume.get('skills_raw', []))} skills"
            )
            candidate_data = {
                "name": parsed_resume.get("name", current_candidate.full_name),
                "title": parsed_resume.get("title", current_candidate.title or "Software Engineer"),
                "contact": parsed_resume.get("contact", {"email": current_candidate.email}),
                "skills_raw": parsed_resume.get("skills_raw", []),
                "skills_categorized": parsed_resume.get("skills_categorized", {}),
                "experience": parsed_resume.get("experience", []),
                "education": parsed_resume.get("education", []),
                "projects": parsed_resume.get("projects", []),
                "publications": parsed_resume.get("publications", []),
            }

        logger.info(
            f"📊 [Ultra AI] Candidate data prepared: {candidate_data.get('name')}, {len(candidate_data.get('skills_raw', []))} skills"
        )

        # Get company intelligence (from previous research or re-research)
        logger.info(f"🔍 [Ultra AI] Starting company research for {recipient.company}")

        company_website = (
            recipient.custom_fields.get("website", "")
            if recipient.custom_fields
            else ""
        )
        if not company_website and recipient.company:
            company_clean = (
                recipient.company.lower()
                .replace(" ", "")
                .replace(".", "")
                .replace(",", "")
            )
            company_website = f"https://{company_clean}.com"
            logger.info(f"🌐 [Ultra AI] Constructed website URL: {company_website}")

        intelligence_service = UltraCompanyIntelligence()
        company_intel = await intelligence_service.research_company(
            company_name=recipient.company or "Company", website=company_website
        )

        logger.info(
            f"✅ [Ultra AI] Company research complete: "
            f"{len(company_intel.tech_stack)} technologies, "
            f"{len(company_intel.job_openings)} job openings, "
            f"confidence: {company_intel.confidence_score:.1f}%"
        )

        # Get country guidance
        csv_parser = IntelligentCSVParser()
        country_guidance = (
            csv_parser.get_country_guidance(recipient.country)
            if recipient.country
            else None
        )

        if country_guidance:
            logger.info(
                f"🌍 [Ultra AI] Country guidance for {recipient.country}: {country_guidance.get('formality', 'Unknown')}"
            )

        # Generate emails
        logger.info(f"✍️  [Ultra AI] Generating 5 email variations")

        email_generator = UltraEmailGenerator()
        email_drafts = email_generator.generate_emails(
            candidate_data=candidate_data,
            company_intelligence=company_intel.to_dict(),
            recipient_name=recipient.name or "Hiring Manager",
            recipient_position=recipient.position or "Recruiter",
            country_guidance=country_guidance,
        )

        logger.info(
            f"✅ [Ultra AI] Generated {len(email_drafts)} email variations successfully"
        )

        # Log each draft summary
        for i, draft in enumerate(email_drafts, 1):
            logger.info(
                f"  📝 Draft {i} ({draft.tone.value}): "
                f"Score {draft.personalization_score:.0f}%, "
                f"{len(draft.matched_skills)} matched skills, "
                f"Est. response: {draft.estimated_response_rate}"
            )

        # ========== SAVE ALL 5 EMAILS TO DATABASE ==========
        logger.info(
            f"💾 [Ultra AI] Saving {len(email_drafts)} email drafts to database..."
        )
        from app.models.company_intelligence import PersonalizedEmailDraft
        from app.repositories.company_async import AsyncCompanyRepository
        import json

        # Get company_id (look up or create company if needed)
        company_repo = AsyncCompanyRepository(db)
        company = None
        if recipient.company:
            # Try to find company by name
            from sqlalchemy import select
            from app.models.company import Company

            stmt = select(Company).where(Company.name == recipient.company).limit(1)
            result = await db.execute(stmt)
            company = result.scalar_one_or_none()

        # If no company found, create a basic one
        if not company:
            from app.models.company import Company

            company = Company(
                name=recipient.company or "Unknown Company",
                website_url=f"https://{recipient.company.lower().replace(' ', '')}.com"
                if recipient.company
                else None,
            )
            db.add(company)
            await db.commit()
            await db.refresh(company)

        logger.info(f"  Using company ID: {company.id} for {company.name}")

        saved_draft_ids = []
        for draft in email_drafts:
            email_draft = PersonalizedEmailDraft(
                candidate_id=current_candidate.id,
                company_id=company.id,
                subject_line=draft.subject,
                subject_alternatives=[],
                email_body=draft.body,
                email_html=None,  # Can add HTML version later
                opening=None,  # Could parse from body
                skill_highlights=", ".join(draft.matched_skills)
                if draft.matched_skills
                else None,
                company_specific=None,
                call_to_action=None,
                closing=None,
                tone=draft.tone.value,
                personalization_level=draft.personalization_score
                / 100.0,  # Convert to 0-1
                confidence_score=draft.personalization_score / 100.0,
                relevance_score=draft.personalization_score / 100.0,
                is_favorite=False,
                is_used=False,
                generation_params={
                    "recipient_name": recipient.name,
                    "recipient_company": recipient.company,
                    "recipient_position": recipient.position,
                    "recipient_country": recipient.country,
                    "matched_skills": draft.matched_skills,
                    "estimated_response_rate": draft.estimated_response_rate,
                },
            )
            db.add(email_draft)
            saved_draft_ids.append(draft.tone.value)

        await db.commit()
        logger.info(
            f"✅ [Ultra AI] Saved {len(saved_draft_ids)} drafts: {', '.join(saved_draft_ids)}"
        )
        # ========================================

        return {
            "success": True,
            "recipient_id": recipient_id,
            "recipient_name": recipient.name,
            "recipient_company": recipient.company,
            "drafts_saved": len(saved_draft_ids),
            "templates_saved": len(
                saved_draft_ids
            ),  # Keep for backwards compat with test script
            "email_variations": [draft.to_dict() for draft in email_drafts],
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [Ultra AI] Email generation error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate emails",
        )


# ==================== FOLLOW-UP QUEUE ENDPOINTS ====================


@router.get("/follow-up/queue")
def get_follow_up_queue(
    days_since_contact: int = Query(
        5, ge=1, le=30, description="Days since last contact with no response"
    ),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get recipients who need follow-up (contacted but no response).

    Uses sync DB session (same as auth) to avoid SQLite dual-session locks.
    """
    from sqlalchemy import and_
    from app.models.recipient import Recipient
    from app.core.database import SessionLocal

    logger.info(
        f"📋 [FOLLOW-UP] Getting follow-up queue for candidate {current_candidate.id} "
        f"(days_since_contact: {days_since_contact})"
    )

    db = SessionLocal()
    try:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_since_contact)

        recipients = (
            db.query(Recipient)
            .filter(
                and_(
                    Recipient.candidate_id == current_candidate.id,
                    Recipient.is_active == True,
                    Recipient.last_contacted_at.isnot(None),
                    Recipient.last_contacted_at <= cutoff_date,
                )
            )
            .order_by(Recipient.last_contacted_at.asc())
            .all()
        )

        follow_up_queue = []
        for recipient in recipients:
            application = (
                db.query(Application)
                .filter(
                    and_(
                        Application.candidate_id == current_candidate.id,
                        Application.recipient_id == recipient.id,
                    )
                )
                .order_by(Application.created_at.desc())
                .first()
            )

            days_waiting = (
                (datetime.now(timezone.utc) - recipient.last_contacted_at).days
                if recipient.last_contacted_at
                else 0
            )

            follow_up_queue.append(
                {
                    "recipient_id": recipient.id,
                    "name": recipient.name,
                    "email": recipient.email,
                    "company": recipient.company,
                    "position": recipient.position,
                    "country": recipient.country,
                    "last_contacted_at": recipient.last_contacted_at.isoformat()
                    if recipient.last_contacted_at
                    else None,
                    "days_waiting": days_waiting,
                    "total_emails_sent": recipient.total_emails_sent or 0,
                    "application": {
                        "id": application.id,
                        "position_title": application.position_title,
                        "status": application.status.value if application else None,
                        "sent_at": application.sent_at.isoformat()
                        if application and application.sent_at
                        else None,
                    }
                    if application
                    else None,
                    "follow_up_count": recipient.total_emails_sent - 1
                    if recipient.total_emails_sent and recipient.total_emails_sent > 0
                    else 0,
                }
            )

        logger.info(
            f"✅ [FOLLOW-UP] Found {len(follow_up_queue)} recipients needing follow-up"
        )

        return {
            "success": True,
            "count": len(follow_up_queue),
            "days_threshold": days_since_contact,
            "queue": follow_up_queue,
        }
    finally:
        db.close()


@router.get("/follow-up/settings")
def get_follow_up_settings(
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get auto follow-up settings for the candidate.
    """
    # For now, return default settings (can be stored in candidate profile later)
    return {
        "auto_follow_up_enabled": False,
        "days_before_follow_up": 5,
        "max_follow_ups": 3,
        "follow_up_interval_days": 5,
        "stop_on_reply": True,
        "excluded_statuses": ["replied", "rejected", "accepted"],
    }


@router.put("/follow-up/settings")
def update_follow_up_settings(
    settings: dict,
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Update auto follow-up settings.

    Settings:
    - auto_follow_up_enabled: bool
    - days_before_follow_up: int (1-30)
    - max_follow_ups: int (1-10)
    - follow_up_interval_days: int (1-14)
    - stop_on_reply: bool
    """
    logger.info(f"⚙️ [FOLLOW-UP] Updating settings for candidate {current_candidate.id}")

    # TODO: Store in database (candidate profile or separate settings table)
    # For now, just return the settings as confirmation

    return {
        "success": True,
        "message": "Follow-up settings updated",
        "settings": settings,
    }


@router.post("/{recipient_id}/send-follow-up")
async def send_follow_up_email(
    recipient_id: int,
    follow_up_data: dict,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Send a follow-up email to a recipient.

    Request Body:
    - subject: Email subject
    - body: Email body
    - follow_up_number: Which follow-up this is (1, 2, 3...)
    - reference_original: Whether to reference original email

    Returns:
    - Success status
    - Updated recipient info
    """
    try:
        repo = AsyncRecipientRepository(db)
        recipient = await repo.get_by_id(recipient_id)

        if not recipient or recipient.candidate_id != current_candidate.id:
            raise HTTPException(status_code=404, detail="Recipient not found")

        logger.info(
            f"📧 [FOLLOW-UP] Sending follow-up #{follow_up_data.get('follow_up_number', 1)} "
            f"to {recipient.email}"
        )

        # Update recipient tracking
        recipient.last_contacted_at = datetime.now(timezone.utc)
        recipient.total_emails_sent = (recipient.total_emails_sent or 0) + 1

        await db.commit()

        # TODO: Integrate with actual email sending service

        logger.info(f"✅ [FOLLOW-UP] Follow-up sent to {recipient.email}")

        return {
            "success": True,
            "recipient_id": recipient_id,
            "recipient_email": recipient.email,
            "follow_up_number": follow_up_data.get("follow_up_number", 1),
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "total_emails_sent": recipient.total_emails_sent,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [FOLLOW-UP] Send error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Failed to send follow-up"
        )


@router.post("/follow-up/bulk-send")
async def bulk_send_follow_ups(
    recipient_ids: list[int],
    follow_up_template: dict,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Send follow-up emails to multiple recipients at once.

    Request Body:
    - recipient_ids: List of recipient IDs
    - follow_up_template: { subject_template, body_template }

    Templates can use placeholders: {name}, {company}, {position}, {days_waiting}
    """
    logger.info(
        f"📧 [FOLLOW-UP] Bulk sending follow-ups to {len(recipient_ids)} recipients"
    )

    repo = AsyncRecipientRepository(db)
    results = {"sent": [], "failed": [], "skipped": []}

    for recipient_id in recipient_ids:
        try:
            recipient = await repo.get_by_id(recipient_id)

            if not recipient or recipient.candidate_id != current_candidate.id:
                results["skipped"].append(
                    {"recipient_id": recipient_id, "reason": "Not found"}
                )
                continue

            # Personalize template
            subject = follow_up_template.get("subject_template", "Following up").format(
                name=recipient.name or "there",
                company=recipient.company or "your company",
                position=recipient.position or "the position",
            )

            days_waiting = (
                (datetime.now(timezone.utc) - recipient.last_contacted_at).days
                if recipient.last_contacted_at
                else 0
            )

            body = follow_up_template.get("body_template", "").format(
                name=recipient.name or "there",
                company=recipient.company or "your company",
                position=recipient.position or "the position",
                days_waiting=days_waiting,
            )

            # Update tracking
            recipient.last_contacted_at = datetime.now(timezone.utc)
            recipient.total_emails_sent = (recipient.total_emails_sent or 0) + 1

            results["sent"].append(
                {
                    "recipient_id": recipient_id,
                    "email": recipient.email,
                    "subject": subject,
                }
            )

        except Exception as e:
            results["failed"].append({"recipient_id": recipient_id, "error": str(e)})

    await db.commit()

    logger.info(
        f"✅ [FOLLOW-UP] Bulk send complete: "
        f"{len(results['sent'])} sent, {len(results['failed'])} failed, "
        f"{len(results['skipped'])} skipped"
    )

    return {"success": True, "total_processed": len(recipient_ids), "results": results}


class SendUltraEmailRequest(BaseModel):
    """Validated request body for send-ultra-email."""
    tone: str = "professional"
    subject: str = Field(..., min_length=1, max_length=500)
    body: str = Field(..., min_length=1)
    position_title: Optional[str] = None
    resume_id: Optional[int] = Field(None, gt=0)
    schedule_at: Optional[str] = None


def _html_to_plain_text(html_body: str) -> str:
    """Strip HTML tags and unescape entities for plain-text fallback."""
    text = re_mod.sub(r"<[^>]+>", "", html_body)
    return html_mod.unescape(text)


def _sanitize_filename(name: str) -> str:
    """Remove characters that could cause header injection."""
    return re_mod.sub(r'[^\w\-_. ]', '_', name)


@router.post("/{recipient_id}/send-ultra-email")
async def send_ultra_generated_email(
    recipient_id: int,
    email_data: SendUltraEmailRequest,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Send an AI-generated email with resume attachment.

    Automatically creates an Application record tracked in Pipeline.
    Application starts as DRAFT and is upgraded to SENT only after
    successful SMTP delivery. An EmailLog audit record is created
    for both success and failure cases.
    """
    try:
        recipient_repo = AsyncRecipientRepository(db)

        # ── Step 1: Validate recipient ──
        recipient = await recipient_repo.get_by_id(recipient_id)
        if not recipient or recipient.candidate_id != current_candidate.id:
            raise HTTPException(status_code=404, detail="Recipient not found")

        # ── Step 2: Validate SMTP credentials BEFORE any DB writes ──
        if not current_candidate.email_account or not current_candidate.email_password:
            raise HTTPException(
                status_code=400,
                detail="Email credentials not configured. Set email_account and email_password in your profile.",
            )

        logger.info(
            f"[Ultra AI] Sending email to recipient {recipient_id}: "
            f"{recipient.name} ({recipient.email})"
        )

        # ── Step 3: Extract validated fields ──
        tone = email_data.tone
        subject = email_data.subject
        body = email_data.body
        position_title = email_data.position_title or recipient.position or "Opportunity"
        resume_id = email_data.resume_id

        # ── Step 4: Find or create Company record ──
        company = None
        company_name = recipient.company
        if company_name:
            result = await db.execute(
                sqlalchemy.select(Company)
                .where(Company.name == company_name)
                .limit(1)
            )
            company = result.scalar_one_or_none()

        if not company:
            fallback_name = (
                recipient.email.split("@")[1] if "@" in recipient.email else "Unknown Company"
            )
            company = Company(
                name=company_name or fallback_name,
                website_url="",
                industry="",
            )
            db.add(company)
            await db.flush()
            logger.info(f"[Ultra AI] Created company: {company.name} (ID: {company.id})")

        # ── Step 5: Create Application as DRAFT ──
        application = Application(
            candidate_id=current_candidate.id,
            company_id=company.id,
            recipient_id=recipient.id,
            recruiter_name=recipient.name,
            recruiter_email=recipient.email,
            recruiter_country=recipient.country,
            position_title=position_title,
            position_country=recipient.country,
            email_subject=subject,
            email_body_html=body,
            status=ApplicationStatusEnum.DRAFT,
            resume_version_id=resume_id,
            notes=(
                f"Auto-created from ULTRA AI email panel\n"
                f"Tone: {tone}\n"
                f"Recipient: {recipient.name} ({recipient.company})"
            ),
            priority=1,
        )
        db.add(application)
        await db.flush()
        logger.info(f"[Ultra AI] Created DRAFT Application: ID={application.id}")

        # ── Step 6: Build MIME message ──
        # Read resume file data before building MIME (needs async DB + file I/O)
        resume_file_data: Optional[bytes] = None
        resume_filename: Optional[str] = None
        if resume_id:
            rv_result = await db.execute(
                sqlalchemy.select(ResumeVersion).where(
                    ResumeVersion.id == resume_id,
                    ResumeVersion.candidate_id == current_candidate.id,
                )
            )
            resume_obj = rv_result.scalar_one_or_none()
            if resume_obj and resume_obj.file_path and os.path.isfile(resume_obj.file_path):
                def _read_file():
                    with open(resume_obj.file_path, "rb") as f:
                        return f.read()
                resume_file_data = await asyncio.to_thread(_read_file)
                resume_filename = _sanitize_filename(resume_obj.filename)
                logger.info(f"[Ultra AI] Will attach resume: {resume_filename}")

        # Build the MIME structure:
        #   With attachment: mixed { alternative { plain, html }, attachment }
        #   Without attachment: alternative { plain, html }
        plain_text = _html_to_plain_text(body)
        alternative_part = MIMEMultipart("alternative")
        alternative_part.attach(MIMEText(plain_text, "plain"))
        alternative_part.attach(MIMEText(body, "html"))

        if resume_file_data and resume_filename:
            msg = MIMEMultipart("mixed")
            msg.attach(alternative_part)
            attachment_part = MIMEBase("application", "octet-stream")
            attachment_part.set_payload(resume_file_data)
            encoders.encode_base64(attachment_part)
            attachment_part.add_header(
                "Content-Disposition", "attachment", filename=resume_filename
            )
            msg.attach(attachment_part)
        else:
            msg = alternative_part

        msg["From"] = current_candidate.email_account
        msg["To"] = recipient.email
        msg["Subject"] = subject

        # ── Step 7: Send via SMTP in a background thread ──
        smtp_host = current_candidate.smtp_host or "smtp.gmail.com"
        smtp_port = current_candidate.smtp_port or 587
        email_account = current_candidate.email_account
        decrypted_password = decrypt_value(current_candidate.email_password)

        def _do_smtp_send():
            ssl_context = ssl.create_default_context()
            with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
                server.starttls(context=ssl_context)
                server.login(email_account, decrypted_password)
                server.send_message(msg)

        smtp_error: Optional[str] = None
        try:
            await asyncio.to_thread(_do_smtp_send)
        except smtplib.SMTPAuthenticationError as e:
            smtp_error = f"SMTP authentication failed: {e}"
            logger.error(f"[Ultra AI] {smtp_error}", exc_info=True)
        except Exception as e:
            smtp_error = f"SMTP send failed: {e}"
            logger.error(f"[Ultra AI] {smtp_error}", exc_info=True)

        now_utc = datetime.now(timezone.utc)

        if smtp_error:
            # ── SMTP failed: rollback DRAFT Application + Company, then log failure ──
            # Capture values from ORM objects before rollback detaches them.
            cand_id = current_candidate.id
            from_addr = current_candidate.email_account
            to_addr = recipient.email
            await db.rollback()

            # Write a failure EmailLog so the attempt is auditable
            fail_log = EmailLog(
                candidate_id=cand_id,
                application_id=None,  # app was rolled back
                from_email=from_addr,
                to_email=to_addr,
                subject=subject,
                body_html=body,
                status=EmailStatusEnum.FAILED,
                error_message=smtp_error,
            )
            db.add(fail_log)
            await db.commit()

            is_auth_error = "authentication" in smtp_error.lower()
            raise HTTPException(
                status_code=401 if is_auth_error else 500,
                detail=(
                    "SMTP authentication failed. Check your email credentials."
                    if is_auth_error
                    else "Failed to send email. Please try again later."
                ),
            )

        # ── Step 8: SMTP succeeded — upgrade to SENT ──
        application.status = ApplicationStatusEnum.SENT
        application.sent_at = now_utc

        # Create success EmailLog audit record
        email_log = EmailLog(
            candidate_id=current_candidate.id,
            application_id=application.id,
            from_email=current_candidate.email_account,
            to_email=recipient.email,
            subject=subject,
            body_html=body,
            status=EmailStatusEnum.SENT,
            sent_at=now_utc,
        )
        db.add(email_log)

        # Update recipient's email tracking
        recipient.last_contacted_at = now_utc
        recipient.total_emails_sent = (recipient.total_emails_sent or 0) + 1
        await db.commit()

        logger.info(
            f"[Ultra AI] Email sent successfully to {recipient.email}, "
            f"Application #{application.id} -> SENT"
        )

        return {
            "success": True,
            "recipient_id": recipient_id,
            "recipient_email": recipient.email,
            "application_id": application.id,
            "company_id": company.id,
            "subject": subject,
            "position_title": position_title,
            "status": "sent",
            "sent_at": application.sent_at.isoformat() if application.sent_at else None,
            "message": f"Email sent and tracked as application #{application.id} in Pipeline!",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[Ultra AI] Send email error: {str(e)}", exc_info=True)
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while sending the email.",
        )
