"""
Group Campaigns Endpoints

API endpoints for managing group email campaigns.

Features:
- CRUD operations for campaigns
- Campaign sending (background task)
- Progress monitoring
- Pause/Resume functionality
- Per-recipient status tracking
- Campaign statistics
- Real-time notifications
"""

from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, status, BackgroundTasks
from app.core.rate_limiter import limiter
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from typing import List, Optional
import logging

from app.core.database_async import get_async_db
from app.core.database import get_db
from app.core.auth import get_current_candidate
from app.models.candidate import Candidate
from app.models.group_campaign import CampaignStatusEnum
from app.models.group_campaign_recipient import GroupCampaignRecipient, RecipientStatusEnum
from app.models.recipient import Recipient
from app.repositories.group_campaign import AsyncGroupCampaignRepository
from app.repositories.recipient_group import AsyncRecipientGroupRepository
from app.schemas.recipient import (
    GroupCampaignCreate,
    GroupCampaignUpdate,
    GroupCampaignResponse,
    GroupCampaignListResponse,
    CampaignRecipientResponse,
    CampaignRecipientsListResponse,
    SendCampaignRequest,
    PauseCampaignResponse,
    ResumeCampaignResponse,
)
from app.tasks.campaign_send_task import process_campaign_send
from app.services.notification_service import NotificationService

logger = logging.getLogger(__name__)

router = APIRouter()


# ==================== LIST ENDPOINTS ====================


@router.get("/", response_model=GroupCampaignListResponse)
async def list_campaigns(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: Optional[CampaignStatusEnum] = None,
    group_id: Optional[int] = None,
    order_by: str = "created_at",
    order_desc: bool = True,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    List campaigns with pagination.

    Query Parameters:
    - page: Page number (1-indexed)
    - page_size: Results per page (1-100)
    - status: Filter by status
    - group_id: Filter by group
    - order_by: Sort field (created_at, campaign_name, sent_count)
    - order_desc: Sort descending

    Returns:
        Paginated list of campaigns
    """
    logger.info(
        f"📋 [CAMPAIGNS] Listing campaigns for candidate {current_candidate.id} "
        f"(page {page}, status: {status})"
    )

    repo = AsyncGroupCampaignRepository(db)

    # Build filters
    filters = {"candidate_id": current_candidate.id}
    if status:
        filters["status"] = status
    if group_id:
        filters["group_id"] = group_id

    # Calculate skip
    skip = (page - 1) * page_size

    # Get campaigns
    campaigns = await repo.get_all(
        filters=filters,
        skip=skip,
        limit=page_size,
        order_by=order_by,
        order_desc=order_desc,
    )

    # Get total count
    total = await repo.count(filters=filters)

    # Calculate total pages
    total_pages = (total + page_size - 1) // page_size

    logger.info(f"✅ [CAMPAIGNS] Found {len(campaigns)} campaigns (total: {total})")

    # Convert to response with computed properties
    campaign_responses = []
    for c in campaigns:
        response = GroupCampaignResponse.model_validate(c)
        # Compute rates
        if c.total_recipients > 0:
            response.success_rate = round((c.sent_count / c.total_recipients) * 100, 2)
        if c.sent_count > 0:
            response.open_rate = round((c.opened_count / c.sent_count) * 100, 2)
            response.reply_rate = round((c.replied_count / c.sent_count) * 100, 2)
        campaign_responses.append(response)

    return GroupCampaignListResponse(
        campaigns=campaign_responses,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


# ==================== STATISTICS ENDPOINTS (Must come before /{campaign_id}) ====================


@router.get("/statistics/overview")
async def get_campaign_statistics(
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get campaign statistics for the current candidate.

    Returns:
        Statistics including:
        - Total campaigns
        - Campaigns by status
        - Total emails sent
        - Average success rate
    """
    logger.info(
        f"📊 [CAMPAIGNS] Fetching statistics for candidate {current_candidate.id}"
    )

    repo = AsyncGroupCampaignRepository(db)

    stats = await repo.get_campaign_statistics(current_candidate.id)

    logger.info(
        f"✅ [CAMPAIGNS] Statistics computed (total: {stats['total_campaigns']})"
    )

    return stats


@router.get("/recent/", response_model=List[GroupCampaignResponse])
async def get_recent_campaigns(
    limit: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get recent campaigns.

    Query Parameters:
    - limit: Max campaigns to return

    Returns:
        List of recent campaigns
    """
    logger.info(
        f"🕒 [CAMPAIGNS] Fetching recent campaigns for candidate {current_candidate.id}"
    )

    repo = AsyncGroupCampaignRepository(db)

    campaigns = await repo.get_recent_campaigns(
        candidate_id=current_candidate.id, limit=limit
    )

    logger.info(f"✅ [CAMPAIGNS] Found {len(campaigns)} recent campaigns")

    return [GroupCampaignResponse.model_validate(c) for c in campaigns]


@router.get("/group/{group_id}/campaigns")
async def get_group_campaigns(
    group_id: int,
    status: Optional[CampaignStatusEnum] = None,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get all campaigns for a specific recipient group.

    Parameters:
    - group_id: Target group ID
    - status: Optional status filter

    Returns:
    - List of campaigns sent to this group
    - Aggregated metrics across campaigns
    """
    try:
        logger.info(f"📋 [GROUP CAMPAIGNS] Fetching campaigns for group {group_id}")

        # Verify group access
        group_repo = AsyncRecipientGroupRepository(db)
        group = await group_repo.get_by_id(group_id)

        if not group or group.candidate_id != current_candidate.id:
            raise HTTPException(
                status_code=404, detail="Group not found or access denied"
            )

        # Get campaigns for group
        repo = AsyncGroupCampaignRepository(db)
        filters = {"group_id": group_id, "candidate_id": current_candidate.id}
        if status:
            filters["status"] = status

        campaigns = await repo.get_all(filters=filters, order_desc=True)

        # Aggregate metrics
        total_sent = sum(c.sent_count for c in campaigns)
        total_opened = sum(c.opened_count for c in campaigns)
        total_replied = sum(c.replied_count for c in campaigns)
        total_failed = sum(c.failed_count for c in campaigns)

        logger.info(
            f"✅ [GROUP CAMPAIGNS] Found {len(campaigns)} campaigns for group {group_id}"
        )

        return {
            "group_id": group_id,
            "group_name": group.group_name,
            "total_campaigns": len(campaigns),
            "campaigns": [GroupCampaignResponse.model_validate(c) for c in campaigns],
            "aggregated_metrics": {
                "total_sent": total_sent,
                "total_opened": total_opened,
                "total_replied": total_replied,
                "total_failed": total_failed,
                "combined_open_rate": round(
                    (total_opened / total_sent * 100) if total_sent > 0 else 0, 2
                ),
                "combined_reply_rate": round(
                    (total_replied / total_sent * 100) if total_sent > 0 else 0, 2
                ),
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [GROUP CAMPAIGNS] Error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch group campaigns",
        )


# ==================== SINGLE CAMPAIGN ENDPOINTS ====================


@router.get("/{campaign_id}", response_model=GroupCampaignResponse)
async def get_campaign(
    campaign_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get a single campaign by ID.

    Path Parameters:
    - campaign_id: Campaign ID

    Returns:
        Campaign details

    Raises:
        404: Campaign not found
    """
    logger.info(f"🔍 [CAMPAIGNS] Fetching campaign {campaign_id}")

    repo = AsyncGroupCampaignRepository(db)

    campaign = await repo.get_by_id(campaign_id)

    if not campaign:
        logger.warning(f"⚠️  [CAMPAIGNS] Campaign {campaign_id} not found")
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Multi-tenant isolation
    if campaign.candidate_id != current_candidate.id:
        logger.warning(
            f"⚠️  [CAMPAIGNS] Access denied: campaign {campaign_id} "
            f"belongs to candidate {campaign.candidate_id}, not {current_candidate.id}"
        )
        raise HTTPException(status_code=404, detail="Campaign not found")

    logger.info(f"✅ [CAMPAIGNS] Fetched campaign {campaign_id}")

    response = GroupCampaignResponse.model_validate(campaign)
    # Compute rates
    if campaign.total_recipients > 0:
        response.success_rate = round(
            (campaign.sent_count / campaign.total_recipients) * 100, 2
        )
    if campaign.sent_count > 0:
        response.open_rate = round(
            (campaign.opened_count / campaign.sent_count) * 100, 2
        )
        response.reply_rate = round(
            (campaign.replied_count / campaign.sent_count) * 100, 2
        )

    return response


# ==================== CREATE ENDPOINTS ====================


@router.post(
    "/", response_model=GroupCampaignResponse, status_code=status.HTTP_201_CREATED
)
async def create_campaign(
    campaign_data: GroupCampaignCreate,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Create a new campaign (in DRAFT status).

    Request Body:
        GroupCampaignCreate schema

    Returns:
        Created campaign

    Raises:
        400: Invalid data or group not found
        404: Group not found
    """
    # Check campaign usage limit
    from app.services.usage_service import check_and_increment_campaign
    from app.core.database import SessionLocal
    sync_db = SessionLocal()
    try:
        from app.models.candidate import Candidate as CandModel
        cand = sync_db.query(CandModel).get(current_candidate.id)
        if cand:
            allowed, msg = check_and_increment_campaign(sync_db, cand)
            if not allowed:
                raise HTTPException(status_code=403, detail={"error": "limit_reached", "message": msg})
            sync_db.commit()
    except HTTPException:
        raise
    except Exception:
        sync_db.rollback()
    finally:
        sync_db.close()

    logger.info(
        f"[CAMPAIGNS] Creating campaign '{campaign_data.campaign_name}' "
        f"for group {campaign_data.group_id}"
    )

    campaign_repo = AsyncGroupCampaignRepository(db)
    group_repo = AsyncRecipientGroupRepository(db)

    # Verify group exists and belongs to candidate
    group = await group_repo.get_by_id(campaign_data.group_id)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    if group.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Group not found")

    # Check for duplicate campaign name
    existing = await campaign_repo.get_by_name(
        current_candidate.id, campaign_data.campaign_name
    )
    if existing:
        raise HTTPException(
            status_code=400,
            detail=f"Campaign with name '{campaign_data.campaign_name}' already exists",
        )

    # Get recipient count from group
    recipient_ids = await group_repo.get_recipient_ids(
        campaign_data.group_id, active_only=True
    )

    if not recipient_ids:
        raise HTTPException(
            status_code=400, detail=f"Group '{group.name}' has no active recipients"
        )

    # Create campaign
    campaign_dict = campaign_data.model_dump()
    campaign_dict["candidate_id"] = current_candidate.id
    campaign_dict["status"] = CampaignStatusEnum.DRAFT
    campaign_dict["total_recipients"] = len(recipient_ids)

    campaign = await campaign_repo.create(campaign_dict)

    # Create campaign_recipients records (one per recipient)
    from app.models.group_campaign_recipient import GroupCampaignRecipient

    campaign_recipients = [
        GroupCampaignRecipient(
            campaign_id=campaign.id,
            recipient_id=recipient_id,
            status=RecipientStatusEnum.PENDING,
        )
        for recipient_id in recipient_ids
    ]

    db.add_all(campaign_recipients)
    await db.commit()

    logger.info(
        f"✅ [CAMPAIGNS] Created campaign {campaign.id} ('{campaign.campaign_name}') "
        f"with {len(recipient_ids)} recipients"
    )

    response = GroupCampaignResponse.model_validate(campaign)
    return response


# ==================== UPDATE ENDPOINTS ====================


@router.patch("/{campaign_id}", response_model=GroupCampaignResponse)
async def update_campaign(
    campaign_id: int,
    campaign_data: GroupCampaignUpdate,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Update a campaign (DRAFT status only).

    Path Parameters:
    - campaign_id: Campaign ID

    Request Body:
        GroupCampaignUpdate schema (partial update)

    Returns:
        Updated campaign

    Raises:
        404: Campaign not found
        400: Campaign not in DRAFT status
    """
    logger.info(f"✏️  [CAMPAIGNS] Updating campaign {campaign_id}")

    repo = AsyncGroupCampaignRepository(db)

    # Get existing campaign
    campaign = await repo.get_by_id(campaign_id)

    if not campaign:
        logger.warning(f"⚠️  [CAMPAIGNS] Campaign {campaign_id} not found")
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Multi-tenant isolation
    if campaign.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Can only update DRAFT campaigns
    if campaign.status != CampaignStatusEnum.DRAFT:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot update campaign in {campaign.status} status. Only DRAFT campaigns can be updated.",
        )

    # Update campaign
    update_dict = campaign_data.model_dump(exclude_unset=True)

    if not update_dict:
        # No fields to update
        return GroupCampaignResponse.model_validate(campaign)

    updated_campaign = await repo.update(campaign_id, update_dict)

    logger.info(f"✅ [CAMPAIGNS] Updated campaign {campaign_id}")

    return GroupCampaignResponse.model_validate(updated_campaign)


# ==================== DELETE ENDPOINTS ====================


@router.delete("/{campaign_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_campaign(
    campaign_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Soft delete a campaign (DRAFT, COMPLETED, FAILED, or CANCELLED only).

    Path Parameters:
    - campaign_id: Campaign ID

    Returns:
        204 No Content on success

    Raises:
        404: Campaign not found
        400: Campaign is actively sending
    """
    logger.info(f"🗑️  [CAMPAIGNS] Deleting campaign {campaign_id}")

    repo = AsyncGroupCampaignRepository(db)

    # Get existing campaign
    campaign = await repo.get_by_id(campaign_id)

    if not campaign:
        logger.warning(f"⚠️  [CAMPAIGNS] Campaign {campaign_id} not found")
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Multi-tenant isolation
    if campaign.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Cannot delete active campaigns
    if campaign.status in [CampaignStatusEnum.SENDING, CampaignStatusEnum.SCHEDULED]:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot delete campaign in {campaign.status} status. Pause or cancel it first.",
        )

    # Soft delete
    success = await repo.soft_delete(campaign_id)

    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete campaign")

    logger.info(
        f"✅ [CAMPAIGNS] Deleted campaign {campaign_id} ('{campaign.campaign_name}')"
    )

    return None


# ==================== SEND CAMPAIGN ENDPOINTS ====================


@router.post("/{campaign_id}/send")
@limiter.limit("10/minute")
async def send_campaign(
    request: Request,
    campaign_id: int,
    background_tasks: BackgroundTasks,
    send_request: SendCampaignRequest = Body(default=SendCampaignRequest()),
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Send or schedule a campaign.

    This endpoint:
    1. Validates campaign is in DRAFT status
    2. Updates status to SENDING or SCHEDULED
    3. Triggers background task to send emails (if immediate)

    Path Parameters:
    - campaign_id: Campaign ID

    Request Body:
        SendCampaignRequest

    Returns:
        Campaign details

    Raises:
        404: Campaign not found
        400: Campaign not in DRAFT status
    """
    logger.info(f"📤 [CAMPAIGNS] Send request for campaign {campaign_id}")

    repo = AsyncGroupCampaignRepository(db)

    # Get campaign
    campaign = await repo.get_by_id(campaign_id)

    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Multi-tenant isolation
    if campaign.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Must be in DRAFT status
    if campaign.status != CampaignStatusEnum.DRAFT:
        raise HTTPException(
            status_code=400,
            detail=f"Campaign is in {campaign.status} status. Only DRAFT campaigns can be sent.",
        )

    # Validate scheduled_at is in the future (if scheduling)
    if not send_request.send_immediately and send_request.scheduled_at:
        # Handle both datetime objects and ISO strings
        scheduled_time = send_request.scheduled_at
        if isinstance(scheduled_time, str):
            try:
                scheduled_time = datetime.fromisoformat(scheduled_time.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid scheduled_at format. Use ISO 8601 format (e.g., 2024-01-15T09:00:00Z)",
                )

        # Ensure scheduled time is at least 5 minutes in the future
        min_schedule_time = datetime.now(timezone.utc) + timedelta(minutes=5)
        if scheduled_time.replace(tzinfo=None) < min_schedule_time:
            raise HTTPException(
                status_code=400,
                detail="Scheduled time must be at least 5 minutes in the future",
            )

    # Update campaign status
    if send_request.send_immediately:
        # Start sending immediately
        await repo.update_campaign_status(campaign_id, CampaignStatusEnum.SENDING)

        # Trigger background task to process campaign
        background_tasks.add_task(process_campaign_send, campaign_id, None)

        logger.info(
            f"📨 [CAMPAIGNS] Campaign {campaign_id} sending started (background task queued)"
        )

        message = f"Campaign '{campaign.campaign_name}' is now sending. Emails will be sent with {campaign.send_delay_seconds}s delay between each."
    else:
        # Schedule for later
        await repo.update(
            campaign_id,
            {
                "status": CampaignStatusEnum.SCHEDULED,
                "scheduled_at": send_request.scheduled_at,
            },
        )

        logger.info(
            f"📅 [CAMPAIGNS] Campaign {campaign_id} scheduled for {send_request.scheduled_at}"
        )

        message = f"Campaign '{campaign.campaign_name}' scheduled for {send_request.scheduled_at}"

    # Reload campaign
    campaign = await repo.get_by_id(campaign_id, use_cache=False)

    return {
        "campaign_id": campaign.id,
        "status": campaign.status,
        "message": message,
        "total_recipients": campaign.total_recipients,
    }


@router.post("/{campaign_id}/pause", response_model=PauseCampaignResponse)
async def pause_campaign(
    campaign_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Pause a sending campaign.

    Path Parameters:
    - campaign_id: Campaign ID

    Returns:
        Pause confirmation

    Raises:
        404: Campaign not found
        400: Campaign not in SENDING status
    """
    logger.info(f"⏸️  [CAMPAIGNS] Pausing campaign {campaign_id}")

    repo = AsyncGroupCampaignRepository(db)

    # Get campaign
    campaign = await repo.get_by_id(campaign_id)

    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Multi-tenant isolation
    if campaign.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Must be SENDING
    if campaign.status != CampaignStatusEnum.SENDING:
        raise HTTPException(
            status_code=400,
            detail=f"Campaign is in {campaign.status} status. Only SENDING campaigns can be paused.",
        )

    # Pause campaign
    campaign = await repo.pause_campaign(campaign_id)

    logger.info(f"✅ [CAMPAIGNS] Campaign {campaign_id} paused")

    return PauseCampaignResponse(
        campaign_id=campaign.id,
        status=campaign.status,
        paused_at=campaign.paused_at,
        message=f"Campaign '{campaign.campaign_name}' has been paused. Use /resume to continue sending.",
    )


@router.post("/{campaign_id}/resume", response_model=ResumeCampaignResponse)
async def resume_campaign(
    campaign_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Resume a paused campaign.

    Path Parameters:
    - campaign_id: Campaign ID

    Returns:
        Resume confirmation

    Raises:
        404: Campaign not found
        400: Campaign not in PAUSED status
    """
    logger.info(f"▶️  [CAMPAIGNS] Resuming campaign {campaign_id}")

    repo = AsyncGroupCampaignRepository(db)

    # Get campaign
    campaign = await repo.get_by_id(campaign_id)

    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Multi-tenant isolation
    if campaign.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Must be PAUSED
    if campaign.status != CampaignStatusEnum.PAUSED:
        raise HTTPException(
            status_code=400,
            detail=f"Campaign is in {campaign.status} status. Only PAUSED campaigns can be resumed.",
        )

    # Resume campaign - update status to SENDING
    campaign = await repo.resume_campaign(campaign_id)

    # Count remaining recipients to send
    from sqlalchemy import select, func
    remaining_query = select(func.count()).select_from(GroupCampaignRecipient).where(
        GroupCampaignRecipient.campaign_id == campaign_id,
        GroupCampaignRecipient.status == RecipientStatusEnum.PENDING
    )
    remaining_result = await db.execute(remaining_query)
    remaining_count = remaining_result.scalar() or 0

    if remaining_count > 0:
        # Trigger background task to continue processing
        # Pass None for db_session - task will create its own session
        background_tasks.add_task(process_campaign_send, campaign_id, None)

        logger.info(
            f"✅ [CAMPAIGNS] Campaign {campaign_id} resumed - "
            f"{remaining_count} remaining recipients, background task queued"
        )

        message = (
            f"Campaign '{campaign.campaign_name}' has been resumed. "
            f"Sending will continue for {remaining_count} remaining recipients."
        )
    else:
        # No pending recipients - mark as completed
        await repo.update_campaign_status(campaign_id, CampaignStatusEnum.COMPLETED)
        campaign = await repo.get_by_id(campaign_id, use_cache=False)

        logger.info(f"✅ [CAMPAIGNS] Campaign {campaign_id} resumed but no pending recipients - marked COMPLETED")

        message = f"Campaign '{campaign.campaign_name}' has no pending recipients. Marked as completed."

    return ResumeCampaignResponse(
        campaign_id=campaign.id,
        status=campaign.status,
        resumed_at=datetime.now(timezone.utc),
        message=message,
    )


@router.post("/{campaign_id}/cancel")
async def cancel_campaign(
    campaign_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Cancel a campaign.

    Cancels a campaign in DRAFT, SCHEDULED, SENDING, or PAUSED status.
    Stopped campaigns cannot be resumed or resumed.

    Path Parameters:
    - campaign_id: Campaign ID

    Returns:
        Cancel confirmation

    Raises:
        404: Campaign not found
        400: Campaign cannot be cancelled (already completed/failed/cancelled)
    """
    logger.info(f"🛑 [CAMPAIGNS] Cancel request for campaign {campaign_id}")

    repo = AsyncGroupCampaignRepository(db)

    # Get campaign
    campaign = await repo.get_by_id(campaign_id)

    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Multi-tenant isolation
    if campaign.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Check if campaign can be cancelled
    cancellable_statuses = [
        CampaignStatusEnum.DRAFT,
        CampaignStatusEnum.SCHEDULED,
        CampaignStatusEnum.SENDING,
        CampaignStatusEnum.PAUSED,
    ]

    if campaign.status not in cancellable_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel campaign in {campaign.status} status. Only DRAFT, SCHEDULED, SENDING, or PAUSED campaigns can be cancelled.",
        )

    # Update campaign status
    await repo.update_campaign_status(campaign_id, CampaignStatusEnum.CANCELLED)

    logger.info(f"✅ [CAMPAIGNS] Campaign {campaign_id} cancelled successfully")

    # Reload campaign
    campaign = await repo.get_by_id(campaign_id, use_cache=False)

    return {
        "campaign_id": campaign.id,
        "status": campaign.status,
        "message": f"Campaign '{campaign.campaign_name}' has been cancelled.",
        "sent_count": campaign.sent_count,
        "failed_count": campaign.failed_count,
        "skipped_count": campaign.skipped_count,
    }


@router.get("/{campaign_id}/status")
async def get_campaign_status(
    campaign_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get campaign status and progress metrics.

    Returns current status, progress counts, timing, and error details.

    Path Parameters:
    - campaign_id: Campaign ID

    Returns:
        Campaign status object with metrics

    Raises:
        404: Campaign not found
    """
    logger.info(f"📊 [CAMPAIGNS] Fetching status for campaign {campaign_id}")

    repo = AsyncGroupCampaignRepository(db)

    # Get campaign
    campaign = await repo.get_by_id(campaign_id)

    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Multi-tenant isolation
    if campaign.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Calculate rates
    success_rate = 0.0
    open_rate = 0.0
    reply_rate = 0.0
    bounce_rate = 0.0

    if campaign.total_recipients > 0:
        success_rate = round((campaign.sent_count / campaign.total_recipients) * 100, 2)

    if campaign.sent_count > 0:
        open_rate = round((campaign.opened_count / campaign.sent_count) * 100, 2)
        reply_rate = round((campaign.replied_count / campaign.sent_count) * 100, 2)
        bounce_rate = round((campaign.bounced_count / campaign.sent_count) * 100, 2)

    # Calculate duration
    duration_seconds = None
    if campaign.started_at and campaign.completed_at:
        duration_seconds = int(
            (campaign.completed_at - campaign.started_at).total_seconds()
        )

    logger.info(
        f"✅ [CAMPAIGNS] Status retrieved: {campaign.status} "
        f"({campaign.sent_count}/{campaign.total_recipients} sent)"
    )

    return {
        "campaign_id": campaign.id,
        "campaign_name": campaign.campaign_name,
        "status": campaign.status,
        "progress": {
            "total_recipients": campaign.total_recipients,
            "sent_count": campaign.sent_count,
            "failed_count": campaign.failed_count,
            "skipped_count": campaign.skipped_count,
            "opened_count": campaign.opened_count,
            "replied_count": campaign.replied_count,
            "bounced_count": campaign.bounced_count,
        },
        "rates": {
            "success_rate": success_rate,
            "open_rate": open_rate,
            "reply_rate": reply_rate,
            "bounce_rate": bounce_rate,
        },
        "timing": {
            "created_at": campaign.created_at,
            "started_at": campaign.started_at,
            "completed_at": campaign.completed_at,
            "duration_seconds": duration_seconds,
        },
        "error_message": campaign.error_message,
    }


@router.get("/{campaign_id}/events")
async def get_campaign_events(
    campaign_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Stream campaign events via Server-Sent Events (SSE).

    Provides real-time updates on campaign progress:
    - Email sending progress
    - Status changes
    - Error events
    - Completion events

    Path Parameters:
    - campaign_id: Campaign ID

    Returns:
        EventSourceResponse with SSE stream

    Raises:
        404: Campaign not found
    """
    from fastapi.responses import StreamingResponse
    import asyncio
    import json

    logger.info(f"📡 [CAMPAIGNS] SSE stream started for campaign {campaign_id}")

    # Get campaign
    repo = AsyncGroupCampaignRepository(db)
    campaign = await repo.get_by_id(campaign_id)

    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Multi-tenant isolation
    if campaign.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Campaign not found")

    async def event_generator():
        """Generate SSE events for campaign progress"""
        last_sent_count = campaign.sent_count
        last_status = campaign.status
        poll_count = 0
        max_polls = 600  # 10 minutes with 1s polling

        try:
            # Send initial status
            yield f"data: {json.dumps({'type': 'connected', 'campaign_id': campaign_id, 'status': campaign.status})}\n\n"

            while poll_count < max_polls:
                await asyncio.sleep(1)  # Poll every 1 second
                poll_count += 1

                # Refresh campaign data
                updated_campaign = await repo.get_by_id(campaign_id, use_cache=False)

                if not updated_campaign:
                    yield f"data: {json.dumps({'type': 'error', 'message': 'Campaign not found'})}\n\n"
                    break

                # Check for status change
                if updated_campaign.status != last_status:
                    yield f"data: {json.dumps({'type': 'status_changed', 'status': updated_campaign.status})}\n\n"
                    last_status = updated_campaign.status

                # Check for progress update
                if updated_campaign.sent_count != last_sent_count:
                    yield f"data: {json.dumps({'type': 'progress', 'sent_count': updated_campaign.sent_count, 'total': updated_campaign.total_recipients})}\n\n"
                    last_sent_count = updated_campaign.sent_count

                # If campaign completed/failed, send final event and close
                if updated_campaign.status in [
                    CampaignStatusEnum.COMPLETED,
                    CampaignStatusEnum.FAILED,
                    CampaignStatusEnum.CANCELLED,
                ]:
                    yield f"data: {json.dumps({'type': 'completed', 'status': updated_campaign.status, 'sent_count': updated_campaign.sent_count, 'failed_count': updated_campaign.failed_count, 'error_message': updated_campaign.error_message})}\n\n"
                    break

        except asyncio.CancelledError:
            logger.info(
                f"📡 [CAMPAIGNS] SSE stream cancelled for campaign {campaign_id}"
            )
        except Exception as e:
            logger.error(f"❌ [CAMPAIGNS] Error in SSE stream: {str(e)}")
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

        logger.info(f"📡 [CAMPAIGNS] SSE stream ended for campaign {campaign_id}")

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ==================== RECIPIENT STATUS ENDPOINTS ====================


@router.get("/{campaign_id}/recipients", response_model=CampaignRecipientsListResponse)
async def get_campaign_recipients(
    campaign_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: Optional[RecipientStatusEnum] = None,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get per-recipient status for a campaign.

    Path Parameters:
    - campaign_id: Campaign ID

    Query Parameters:
    - page: Page number
    - page_size: Results per page
    - status: Filter by recipient status

    Returns:
        Paginated list of campaign recipients

    Raises:
        404: Campaign not found
    """
    logger.info(
        f"📋 [CAMPAIGNS] Fetching recipients for campaign {campaign_id} (page {page})"
    )

    campaign_repo = AsyncGroupCampaignRepository(db)

    # Get campaign
    campaign = await campaign_repo.get_by_id(campaign_id)

    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Multi-tenant isolation
    if campaign.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Calculate skip
    skip = (page - 1) * page_size

    # Get campaign recipients
    campaign_recipients, total = await campaign_repo.get_campaign_recipients(
        campaign_id=campaign_id, status=status, skip=skip, limit=page_size
    )

    logger.info(
        f"✅ [CAMPAIGNS] Found {len(campaign_recipients)} recipients (total: {total})"
    )

    # Convert to response
    recipients = []
    for cr in campaign_recipients:
        recipients.append(
            CampaignRecipientResponse(
                id=cr.id,
                campaign_id=cr.campaign_id,
                recipient_id=cr.recipient_id,
                recipient_email=cr.recipient.email if cr.recipient else None,
                recipient_name=cr.recipient.name if cr.recipient else None,
                rendered_subject=cr.rendered_subject,
                rendered_body_html=cr.rendered_body_html,
                status=cr.status,
                tracking_id=cr.tracking_id,
                sent_at=cr.sent_at,
                delivered_at=cr.delivered_at,
                opened_at=cr.opened_at,
                replied_at=cr.replied_at,
                bounced_at=cr.bounced_at,
                error_message=cr.error_message,
                retry_count=cr.retry_count,
                email_log_id=cr.email_log_id,
                created_at=cr.created_at,
                updated_at=cr.updated_at,
            )
        )

    return CampaignRecipientsListResponse(
        recipients=recipients, total=total, page=page, page_size=page_size
    )


# ==================== GROUP CAMPAIGN CREATION ====================


@router.post("/from-group")
async def create_campaign_from_group(
    group_id: int,
    subject_template: str,
    body_template_html: str,
    send_delay_seconds: int = 60,
    scheduled_at: Optional[str] = None,
    email_template_id: Optional[int] = None,
    # Follow-up configuration
    enable_follow_up: bool = False,
    follow_up_sequence_id: Optional[int] = None,
    follow_up_stop_on_reply: bool = True,
    follow_up_stop_on_bounce: bool = False,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Create a campaign directly from a recipient group.

    This endpoint:
    1. Creates a campaign record linked to the group
    2. Fetches all active recipients in the group
    3. Creates campaign_recipients records
    4. Sets campaign as ready to send
    5. Configures follow-up sequence (if enabled)

    Parameters:
    - group_id: Target recipient group ID
    - subject_template: Email subject with {variables}
    - body_template_html: Email body HTML with {variables}
    - send_delay_seconds: Delay between sending each email
    - scheduled_at: ISO timestamp for scheduled send (optional)
    - email_template_id: Reference to saved template (optional)
    - enable_follow_up: Enable follow-up emails for this campaign
    - follow_up_sequence_id: ID of the follow-up sequence to use
    - follow_up_stop_on_reply: Stop follow-ups when recipient replies
    - follow_up_stop_on_bounce: Stop follow-ups when email bounces
    """
    try:
        logger.info(
            f"🚀 [GROUP CAMPAIGN] Creating campaign from group {group_id} "
            f"for candidate {current_candidate.id}"
        )

        # Verify group access
        group_repo = AsyncRecipientGroupRepository(db)
        group = await group_repo.get_by_id(group_id)

        if not group or group.candidate_id != current_candidate.id:
            raise HTTPException(
                status_code=404, detail=f"Group {group_id} not found or access denied"
            )

        # Get all active recipients in the group
        from app.repositories.group_recipient import AsyncGroupRecipientRepository

        gr_repo = AsyncGroupRecipientRepository(db)
        group_recipients = await gr_repo.get_by_group(group_id)

        if not group_recipients:
            raise HTTPException(
                status_code=400, detail=f"Group {group_id} has no active recipients"
            )

        # Validate follow-up sequence if enabled
        if enable_follow_up:
            if not follow_up_sequence_id:
                raise HTTPException(
                    status_code=400,
                    detail="follow_up_sequence_id is required when enable_follow_up is True"
                )
            # Verify sequence exists and belongs to candidate
            from app.models.follow_up import FollowUpSequence
            sequence = await db.get(FollowUpSequence, follow_up_sequence_id)
            if not sequence or sequence.candidate_id != current_candidate.id:
                raise HTTPException(
                    status_code=404,
                    detail=f"Follow-up sequence {follow_up_sequence_id} not found or access denied"
                )
            logger.info(f"📋 [GROUP CAMPAIGN] Follow-up enabled with sequence '{sequence.name}' ({len(sequence.steps)} steps)")

        # Create campaign
        from app.models.group_campaign import GroupCampaign

        campaign = GroupCampaign(
            candidate_id=current_candidate.id,
            group_id=group_id,
            campaign_name=f"Campaign - {group.group_name} ({len(group_recipients)} recipients)",
            email_template_id=email_template_id,
            subject_template=subject_template,
            body_template_html=body_template_html,
            send_delay_seconds=send_delay_seconds,
            scheduled_at=scheduled_at,
            status=CampaignStatusEnum.DRAFT,
            total_recipients=len(group_recipients),
            # Follow-up configuration
            enable_follow_up=enable_follow_up,
            follow_up_sequence_id=follow_up_sequence_id if enable_follow_up else None,
            follow_up_stop_on_reply=follow_up_stop_on_reply,
            follow_up_stop_on_bounce=follow_up_stop_on_bounce,
        )
        db.add(campaign)
        await db.flush()

        # Create campaign_recipients records
        from app.models.group_campaign_recipient import (
            GroupCampaignRecipient,
            RecipientStatusEnum,
        )

        for group_recipient in group_recipients:
            campaign_recipient = GroupCampaignRecipient(
                campaign_id=campaign.id,
                recipient_id=group_recipient.recipient_id,
                status=RecipientStatusEnum.PENDING,
            )
            db.add(campaign_recipient)

        await db.commit()
        await db.refresh(campaign)

        logger.info(
            f"✅ [GROUP CAMPAIGN] Created campaign {campaign.id} "
            f"with {len(group_recipients)} recipients"
        )

        # Send notification for campaign creation
        # Need to get sync session for notification service
        from app.core.database import SessionLocal

        sync_db = SessionLocal()
        try:
            NotificationService.notify_campaign_created(
                db=sync_db,
                campaign_id=campaign.id,
                campaign_name=campaign.campaign_name,
                recipient_count=len(group_recipients),
                candidate_id=current_candidate.id,
            )
        except Exception as e:
            logger.warning(f"[GROUP CAMPAIGN] Failed to send notification: {e}")
        finally:
            sync_db.close()

        follow_up_info = ""
        if enable_follow_up:
            follow_up_info = f" with follow-up sequence enabled"

        return {
            "success": True,
            "campaign_id": campaign.id,
            "campaign_name": campaign.campaign_name,
            "group_id": group_id,
            "total_recipients": len(group_recipients),
            "status": campaign.status,
            "enable_follow_up": enable_follow_up,
            "follow_up_sequence_id": follow_up_sequence_id if enable_follow_up else None,
            "message": f"Campaign created with {len(group_recipients)} recipients from group '{group.group_name}'{follow_up_info}",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"❌ [GROUP CAMPAIGN] Error creating campaign: {str(e)}", exc_info=True
        )
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create campaign",
        )


# ==================== GROUP ANALYTICS ====================


@router.get("/{campaign_id}/group-analytics")
async def get_campaign_group_analytics(
    campaign_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get detailed group-level analytics for a campaign.

    Returns:
    - Overall stats (sent, open, reply, bounce rates)
    - Group-level breakdown if campaign is group-based
    - Recipient segmentation metrics
    - Performance timeline
    """
    try:
        logger.info(
            f"📊 [GROUP ANALYTICS] Fetching analytics for campaign {campaign_id}"
        )

        # Fetch campaign
        repo = AsyncGroupCampaignRepository(db)
        campaign = await repo.get_by_id(campaign_id)

        if not campaign or campaign.candidate_id != current_candidate.id:
            raise HTTPException(
                status_code=404, detail="Campaign not found or access denied"
            )

        # Get recipient status breakdown
        from app.repositories.group_campaign_recipient import (
            AsyncGroupCampaignRecipientRepository,
        )

        recipient_repo = AsyncGroupCampaignRecipientRepository(db)
        recipients = await recipient_repo.get_by_campaign(campaign_id)

        # Calculate metrics
        metrics = {
            "overall": {
                "total_recipients": campaign.total_recipients,
                "sent": campaign.sent_count,
                "failed": campaign.failed_count,
                "skipped": campaign.skipped_count,
                "opened": campaign.opened_count,
                "replied": campaign.replied_count,
                "bounced": campaign.bounced_count,
                "success_rate": campaign.success_rate,
                "open_rate": campaign.open_rate,
                "reply_rate": campaign.reply_rate,
            },
            "status_breakdown": {
                "pending": sum(1 for r in recipients if r.status == "pending"),
                "sent": sum(1 for r in recipients if r.status == "sent"),
                "failed": sum(1 for r in recipients if r.status == "failed"),
                "opened": sum(1 for r in recipients if r.status == "opened"),
                "replied": sum(1 for r in recipients if r.status == "replied"),
                "bounced": sum(1 for r in recipients if r.status == "bounced"),
            },
            "timing": {
                "created_at": campaign.created_at.isoformat()
                if campaign.created_at
                else None,
                "started_at": campaign.started_at.isoformat()
                if campaign.started_at
                else None,
                "completed_at": campaign.completed_at.isoformat()
                if campaign.completed_at
                else None,
                "scheduled_at": campaign.scheduled_at.isoformat()
                if campaign.scheduled_at
                else None,
            },
            "campaign_info": {
                "campaign_id": campaign.id,
                "campaign_name": campaign.campaign_name,
                "group_id": campaign.group_id,
                "status": campaign.status,
                "send_delay_seconds": campaign.send_delay_seconds,
            },
        }

        logger.info(
            f"✅ [GROUP ANALYTICS] Generated analytics for campaign {campaign_id}"
        )

        return metrics

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [GROUP ANALYTICS] Error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch analytics",
        )


# ==================== FOLLOW-UP MANAGEMENT ENDPOINTS ====================


@router.get("/{campaign_id}/followup-status")
async def get_campaign_followup_status(
    campaign_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    filter_status: Optional[str] = Query(None, description="Filter by: sent, pending, has_followup, no_followup"),
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get all recipients of a campaign with their follow-up status.

    Returns detailed information for Step 5 Follow-Up management:
    - Recipient details (email, name, company)
    - Original email status (sent_at, opened, replied)
    - Follow-up campaign status (if exists)
    - Smart scheduling recommendations

    Filter options:
    - sent: Only recipients with sent emails
    - pending: Recipients pending send
    - has_followup: Recipients with follow-up campaigns
    - no_followup: Recipients without follow-up campaigns
    """
    try:
        logger.info(f"📋 [FOLLOWUP STATUS] Getting follow-up status for campaign {campaign_id}")

        # Get campaign
        repo = AsyncGroupCampaignRepository(db)
        campaign = await repo.get_by_id(campaign_id)

        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")

        if campaign.candidate_id != current_candidate.id:
            raise HTTPException(status_code=404, detail="Campaign not found")

        # Import models
        from app.models.follow_up import FollowUpCampaign, CampaignStatus as FollowUpStatus
        from sqlalchemy import select, func, case, and_, or_
        from sqlalchemy.orm import selectinload

        # Build query for recipients with follow-up info
        skip = (page - 1) * page_size

        # Get campaign recipients
        query = (
            select(GroupCampaignRecipient)
            .where(GroupCampaignRecipient.campaign_id == campaign_id)
            .options(selectinload(GroupCampaignRecipient.recipient))
        )

        # Apply filters
        if filter_status == "sent":
            query = query.where(GroupCampaignRecipient.status == RecipientStatusEnum.SENT)
        elif filter_status == "pending":
            query = query.where(GroupCampaignRecipient.status == RecipientStatusEnum.PENDING)

        # Get total count
        count_query = select(func.count()).select_from(
            query.subquery()
        )
        total_result = await db.execute(count_query)
        total = total_result.scalar() or 0

        # Get paginated results
        query = query.offset(skip).limit(page_size).order_by(GroupCampaignRecipient.sent_at.desc())
        result = await db.execute(query)
        campaign_recipients = result.scalars().all()

        # Get follow-up campaigns for these recipients
        recipient_ids = [cr.id for cr in campaign_recipients]
        followup_query = (
            select(FollowUpCampaign)
            .where(FollowUpCampaign.group_campaign_recipient_id.in_(recipient_ids))
        )
        followup_result = await db.execute(followup_query)
        followup_campaigns = {fc.group_campaign_recipient_id: fc for fc in followup_result.scalars().all()}

        # Build response
        recipients_data = []
        for cr in campaign_recipients:
            recipient = cr.recipient
            followup = followup_campaigns.get(cr.id)

            # Skip if filter requires follow-up status
            if filter_status == "has_followup" and not followup:
                continue
            if filter_status == "no_followup" and followup:
                continue

            # Calculate days since sent
            days_since_sent = None
            if cr.sent_at:
                days_since_sent = (datetime.now(timezone.utc) - cr.sent_at).days

            # Smart recommendation based on status
            recommendation = None
            if cr.status == RecipientStatusEnum.SENT and not followup:
                if cr.replied_at:
                    recommendation = "replied_no_followup"
                elif cr.opened_at:
                    recommendation = "opened_high_priority"
                elif days_since_sent and days_since_sent >= 3:
                    recommendation = "no_response_followup_recommended"
                else:
                    recommendation = "waiting"

            recipients_data.append({
                "id": cr.id,
                "recipient_id": cr.recipient_id,
                "email": recipient.email if recipient else None,
                "name": recipient.name if recipient else None,
                "company": recipient.company if recipient else None,
                "position": recipient.position if recipient else None,

                # Original email status
                "email_status": cr.status.value,
                "sent_at": cr.sent_at.isoformat() if cr.sent_at else None,
                "opened_at": cr.opened_at.isoformat() if cr.opened_at else None,
                "replied_at": cr.replied_at.isoformat() if cr.replied_at else None,
                "bounced_at": cr.bounced_at.isoformat() if cr.bounced_at else None,
                "days_since_sent": days_since_sent,

                # Follow-up status
                "has_followup": followup is not None,
                "followup_id": followup.id if followup else None,
                "followup_status": followup.status.value if followup else None,
                "followup_current_step": followup.current_step if followup else None,
                "followup_total_steps": followup.total_steps if followup else None,
                "followup_next_send": followup.next_send_date.isoformat() if followup and followup.next_send_date else None,
                "followup_emails_sent": followup.emails_sent if followup else 0,

                # Smart recommendation
                "recommendation": recommendation,
            })

        # Calculate summary stats
        total_sent = sum(1 for r in recipients_data if r["email_status"] == "sent")
        total_opened = sum(1 for r in recipients_data if r["opened_at"])
        total_replied = sum(1 for r in recipients_data if r["replied_at"])
        total_with_followup = sum(1 for r in recipients_data if r["has_followup"])
        total_needing_followup = sum(1 for r in recipients_data if r["recommendation"] in ["opened_high_priority", "no_response_followup_recommended"])

        logger.info(f"✅ [FOLLOWUP STATUS] Retrieved {len(recipients_data)} recipients")

        return {
            "campaign_id": campaign_id,
            "campaign_name": campaign.campaign_name,
            "campaign_status": campaign.status.value,
            "enable_follow_up": campaign.enable_follow_up,
            "follow_up_sequence_id": campaign.follow_up_sequence_id,

            "summary": {
                "total_recipients": total,
                "total_sent": total_sent,
                "total_opened": total_opened,
                "total_replied": total_replied,
                "total_with_followup": total_with_followup,
                "total_needing_followup": total_needing_followup,
                "open_rate": round((total_opened / total_sent * 100) if total_sent > 0 else 0, 1),
                "reply_rate": round((total_replied / total_sent * 100) if total_sent > 0 else 0, 1),
                "followup_coverage": round((total_with_followup / total_sent * 100) if total_sent > 0 else 0, 1),
            },

            "recipients": recipients_data,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": (total + page_size - 1) // page_size,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [FOLLOWUP STATUS] Error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get follow-up status",
        )


@router.post("/{campaign_id}/schedule-followups")
async def schedule_bulk_followups(
    campaign_id: int,
    recipient_ids: List[int] = Query(None, description="Specific recipient IDs, or omit for all eligible"),
    schedule_mode: str = Query("smart", description="smart, immediate, or custom"),
    delay_days: int = Query(3, ge=1, le=30, description="Days delay for custom mode"),
    sequence_id: Optional[int] = Query(None, description="Override sequence ID"),
    stop_on_reply: bool = Query(True),
    stop_on_bounce: bool = Query(True),
    priority: str = Query("normal", description="high, normal, low - affects ML scheduling"),
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Schedule follow-ups for campaign recipients in bulk.

    Schedule Modes:
    - smart: AI-based optimal timing (uses ML send-time optimizer)
    - immediate: Schedule for next available slot
    - custom: Use specified delay_days

    Priority affects ML scheduling:
    - high: Prioritize earlier send times
    - normal: Standard ML optimization
    - low: Can be delayed for better deliverability
    """
    try:
        logger.info(f"📅 [SCHEDULE FOLLOWUPS] Scheduling for campaign {campaign_id}, mode: {schedule_mode}")

        # Get campaign
        repo = AsyncGroupCampaignRepository(db)
        campaign = await repo.get_by_id(campaign_id)

        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")

        if campaign.candidate_id != current_candidate.id:
            raise HTTPException(status_code=404, detail="Campaign not found")

        # Get sequence
        from app.models.follow_up import FollowUpSequence, FollowUpCampaign, CampaignStatus as FollowUpStatus

        seq_id = sequence_id or campaign.follow_up_sequence_id
        if not seq_id:
            raise HTTPException(
                status_code=400,
                detail="No follow-up sequence specified. Provide sequence_id or enable follow-up on campaign."
            )

        sequence = await db.get(FollowUpSequence, seq_id)
        if not sequence or sequence.candidate_id != current_candidate.id:
            raise HTTPException(status_code=404, detail="Follow-up sequence not found")

        if not sequence.steps:
            raise HTTPException(status_code=400, detail="Sequence has no steps defined")

        # Get eligible recipients
        from sqlalchemy import select, and_

        query = (
            select(GroupCampaignRecipient)
            .where(
                GroupCampaignRecipient.campaign_id == campaign_id,
                GroupCampaignRecipient.status == RecipientStatusEnum.SENT,
            )
        )

        if recipient_ids:
            query = query.where(GroupCampaignRecipient.id.in_(recipient_ids))

        result = await db.execute(query)
        recipients = result.scalars().all()

        if not recipients:
            raise HTTPException(status_code=400, detail="No eligible recipients found")

        # Check which already have follow-ups
        existing_query = (
            select(FollowUpCampaign.group_campaign_recipient_id)
            .where(FollowUpCampaign.group_campaign_recipient_id.in_([r.id for r in recipients]))
        )
        existing_result = await db.execute(existing_query)
        existing_followups = set(existing_result.scalars().all())

        # Filter out recipients that already have follow-ups
        recipients_to_schedule = [r for r in recipients if r.id not in existing_followups]

        if not recipients_to_schedule:
            return {
                "success": True,
                "scheduled_count": 0,
                "skipped_count": len(recipients),
                "message": "All selected recipients already have follow-ups scheduled",
            }

        # Get candidate
        candidate = await db.get(Candidate, current_candidate.id)

        # Calculate base delay based on mode
        scheduled_count = 0
        errors = []

        for cr in recipients_to_schedule:
            try:
                # Calculate next_send_date based on mode
                base_time = cr.sent_at or datetime.now(timezone.utc)

                if schedule_mode == "smart":
                    # Use ML optimizer if available
                    try:
                        from app.services.ml.send_time_ml import SendTimeMLOptimizer
                        ml_optimizer = SendTimeMLOptimizer(db)

                        # Get recipient for domain info
                        recipient = await db.get(Recipient, cr.recipient_id)
                        domain = recipient.email.split("@")[1] if recipient and recipient.email else None

                        ml_result = ml_optimizer.get_optimal_send_time(
                            candidate_id=current_candidate.id,
                            recipient_domain=domain,
                        )

                        # Apply ML recommendation with first step delay
                        first_step = sequence.steps[0]
                        base_delay = timedelta(days=first_step.delay_days, hours=first_step.delay_hours or 0)

                        # Adjust based on ML recommended hour
                        next_send = base_time + base_delay
                        if ml_result and ml_result.recommended_hour:
                            next_send = next_send.replace(hour=ml_result.recommended_hour, minute=0)

                        # Priority adjustment
                        if priority == "high":
                            next_send = next_send - timedelta(hours=12)
                        elif priority == "low":
                            next_send = next_send + timedelta(hours=12)

                    except Exception as ml_error:
                        logger.warning(f"ML optimizer failed, using default: {ml_error}")
                        first_step = sequence.steps[0]
                        next_send = base_time + timedelta(days=first_step.delay_days, hours=first_step.delay_hours or 0)

                elif schedule_mode == "immediate":
                    # Schedule for 1 hour from now
                    next_send = datetime.now(timezone.utc) + timedelta(hours=1)

                else:  # custom
                    next_send = base_time + timedelta(days=delay_days)

                # Ensure not in the past
                if next_send < datetime.now(timezone.utc):
                    next_send = datetime.now(timezone.utc) + timedelta(hours=1)

                # Get recipient for context
                recipient = await db.get(Recipient, cr.recipient_id)

                # Create follow-up campaign
                followup = FollowUpCampaign(
                    sequence_id=sequence.id,
                    group_campaign_recipient_id=cr.id,
                    group_campaign_id=campaign.id,
                    candidate_id=current_candidate.id,
                    status=FollowUpStatus.ACTIVE,
                    is_auto_mode=True,
                    auto_mode_approved=True,
                    auto_mode_approved_at=datetime.now(timezone.utc),
                    current_step=0,
                    total_steps=len(sequence.steps),
                    next_send_date=next_send,
                    last_sent_date=cr.sent_at,
                    original_email_context={
                        "subject": cr.rendered_subject,
                        "body_preview": (cr.rendered_body_html or "")[:500],
                        "sent_at": cr.sent_at.isoformat() if cr.sent_at else None,
                        "campaign_name": campaign.campaign_name,
                    },
                    company_context={
                        "name": recipient.name if recipient else None,
                        "email": recipient.email if recipient else None,
                        "company": recipient.company if recipient else None,
                        "position": recipient.position if recipient else None,
                    },
                    candidate_context={
                        "name": candidate.full_name if candidate else None,
                        "email": candidate.email if candidate else None,
                    },
                )

                db.add(followup)
                scheduled_count += 1

            except Exception as e:
                errors.append(f"Recipient {cr.id}: {str(e)}")
                logger.error(f"Failed to schedule followup for {cr.id}: {e}")

        await db.commit()

        logger.info(f"✅ [SCHEDULE FOLLOWUPS] Scheduled {scheduled_count} follow-ups for campaign {campaign_id}")

        return {
            "success": True,
            "scheduled_count": scheduled_count,
            "skipped_count": len(existing_followups),
            "error_count": len(errors),
            "errors": errors[:5] if errors else None,
            "sequence_name": sequence.name,
            "schedule_mode": schedule_mode,
            "message": f"Successfully scheduled {scheduled_count} follow-ups using '{sequence.name}' sequence",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [SCHEDULE FOLLOWUPS] Error: {str(e)}", exc_info=True)
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to schedule follow-ups",
        )


@router.get("/{campaign_id}/followup-sequences")
async def get_available_followup_sequences(
    campaign_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Get available follow-up sequences for scheduling.

    Returns all sequences owned by the candidate with their stats.
    """
    try:
        # Verify campaign access
        repo = AsyncGroupCampaignRepository(db)
        campaign = await repo.get_by_id(campaign_id)

        if not campaign or campaign.candidate_id != current_candidate.id:
            raise HTTPException(status_code=404, detail="Campaign not found")

        # Get sequences
        from app.models.follow_up import FollowUpSequence, SequenceStatus
        from sqlalchemy import select

        query = (
            select(FollowUpSequence)
            .where(
                FollowUpSequence.candidate_id == current_candidate.id,
                FollowUpSequence.status == SequenceStatus.ACTIVE,
            )
            .order_by(FollowUpSequence.times_used.desc())
        )

        result = await db.execute(query)
        sequences = result.scalars().all()

        return {
            "campaign_id": campaign_id,
            "current_sequence_id": campaign.follow_up_sequence_id,
            "sequences": [
                {
                    "id": seq.id,
                    "name": seq.name,
                    "description": seq.description,
                    "is_system_preset": seq.is_system_preset,
                    "num_steps": len(seq.steps),
                    "total_delay_days": sum(s.delay_days for s in seq.steps),
                    "times_used": seq.times_used,
                    "reply_rate": seq.reply_rate,
                    "is_current": seq.id == campaign.follow_up_sequence_id,
                    "steps_preview": [
                        {
                            "step": s.step_number,
                            "delay_days": s.delay_days,
                            "strategy": s.strategy.value if hasattr(s.strategy, 'value') else s.strategy,
                            "tone": s.tone.value if hasattr(s.tone, 'value') else s.tone,
                        }
                        for s in seq.steps[:3]  # Preview first 3 steps
                    ],
                }
                for seq in sequences
            ],
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [FOLLOWUP SEQUENCES] Error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get sequences",
        )


@router.post("/{campaign_id}/cancel-followups")
async def cancel_bulk_followups(
    campaign_id: int,
    recipient_ids: List[int] = Query(None, description="Specific recipient IDs, or omit for all"),
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate),
):
    """
    Cancel follow-ups for selected recipients.
    """
    try:
        logger.info(f"🛑 [CANCEL FOLLOWUPS] Cancelling for campaign {campaign_id}")

        # Verify campaign access
        repo = AsyncGroupCampaignRepository(db)
        campaign = await repo.get_by_id(campaign_id)

        if not campaign or campaign.candidate_id != current_candidate.id:
            raise HTTPException(status_code=404, detail="Campaign not found")

        from app.models.follow_up import FollowUpCampaign, CampaignStatus as FollowUpStatus
        from sqlalchemy import select, update

        # Build query
        query = (
            select(FollowUpCampaign)
            .where(
                FollowUpCampaign.group_campaign_id == campaign_id,
                FollowUpCampaign.status.in_([FollowUpStatus.ACTIVE, FollowUpStatus.PENDING_APPROVAL, FollowUpStatus.PAUSED]),
            )
        )

        if recipient_ids:
            query = query.where(FollowUpCampaign.group_campaign_recipient_id.in_(recipient_ids))

        result = await db.execute(query)
        followups = result.scalars().all()

        cancelled_count = 0
        for followup in followups:
            followup.status = FollowUpStatus.CANCELLED
            followup.cancellation_reason = "Bulk cancelled from campaign management"
            cancelled_count += 1

        await db.commit()

        logger.info(f"✅ [CANCEL FOLLOWUPS] Cancelled {cancelled_count} follow-ups")

        return {
            "success": True,
            "cancelled_count": cancelled_count,
            "message": f"Successfully cancelled {cancelled_count} follow-ups",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [CANCEL FOLLOWUPS] Error: {str(e)}", exc_info=True)
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to cancel follow-ups",
        )
