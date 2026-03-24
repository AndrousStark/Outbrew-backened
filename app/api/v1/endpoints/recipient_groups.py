"""
Recipient Groups Endpoints

API endpoints for managing recipient groups (static and dynamic).

Features:
- CRUD operations for groups
- Static group membership management
- Dynamic group filter evaluation and refresh
- Group statistics
"""
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import logging

from app.core.database_async import get_async_db
from app.core.auth import get_current_candidate
from app.models.candidate import Candidate
from app.models.recipient_group import GroupTypeEnum
from app.repositories.recipient_group import AsyncRecipientGroupRepository
from app.repositories.recipient import AsyncRecipientRepository
from app.schemas.recipient import (
    RecipientGroupCreate,
    RecipientGroupUpdate,
    RecipientGroupResponse,
    RecipientGroupListResponse,
    RecipientGroupWithRecipientsResponse,
    AddRecipientsToGroupRequest,
    RemoveRecipientsFromGroupRequest,
    RefreshDynamicGroupResponse,
    GroupStatistics,
    RecipientResponse,
    RecipientListResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ==================== LIST ENDPOINTS ====================

@router.get("/", response_model=RecipientGroupListResponse)
async def list_groups(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    group_type: Optional[GroupTypeEnum] = None,
    order_by: str = "created_at",
    order_desc: bool = True,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    List recipient groups with pagination.

    Query Parameters:
    - page: Page number (1-indexed)
    - page_size: Results per page (1-100)
    - group_type: Filter by type (static/dynamic)
    - order_by: Sort field (created_at, name, total_recipients)
    - order_desc: Sort descending

    Returns:
        Paginated list of groups
    """
    logger.info(
        f"📋 [GROUPS] Listing groups for candidate {current_candidate.id} "
        f"(page {page}, type: {group_type})"
    )

    repo = AsyncRecipientGroupRepository(db)

    # Build filters
    filters = {"candidate_id": current_candidate.id}
    if group_type:
        filters["group_type"] = group_type

    # Calculate skip
    skip = (page - 1) * page_size

    # Get groups
    groups = await repo.get_all(
        filters=filters,
        skip=skip,
        limit=page_size,
        order_by=order_by,
        order_desc=order_desc
    )

    # Get total count
    total = await repo.count(filters=filters)

    # Calculate total pages
    total_pages = (total + page_size - 1) // page_size

    logger.info(f"✅ [GROUPS] Found {len(groups)} groups (total: {total})")

    return RecipientGroupListResponse(
        groups=[RecipientGroupResponse.model_validate(g) for g in groups],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages
    )


# ==================== SINGLE GROUP ENDPOINTS ====================

@router.get("/{group_id}", response_model=RecipientGroupResponse)
async def get_group(
    group_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Get a single group by ID.

    Path Parameters:
    - group_id: Group ID

    Returns:
        Group details

    Raises:
        404: Group not found
    """
    logger.info(f"🔍 [GROUPS] Fetching group {group_id}")

    repo = AsyncRecipientGroupRepository(db)

    group = await repo.get_by_id(group_id)

    if not group:
        logger.warning(f"⚠️  [GROUPS] Group {group_id} not found")
        raise HTTPException(status_code=404, detail="Group not found")

    # Multi-tenant isolation
    if group.candidate_id != current_candidate.id:
        logger.warning(
            f"⚠️  [GROUPS] Access denied: group {group_id} "
            f"belongs to candidate {group.candidate_id}, not {current_candidate.id}"
        )
        raise HTTPException(status_code=404, detail="Group not found")

    logger.info(f"✅ [GROUPS] Fetched group {group_id}")

    return RecipientGroupResponse.model_validate(group)


@router.get("/{group_id}/with-recipients", response_model=RecipientGroupWithRecipientsResponse)
async def get_group_with_recipients(
    group_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Get group with all recipient details loaded.

    Path Parameters:
    - group_id: Group ID

    Returns:
        Group with recipient list

    Raises:
        404: Group not found
    """
    logger.info(f"🔍 [GROUPS] Fetching group {group_id} with recipients")

    repo = AsyncRecipientGroupRepository(db)

    group = await repo.get_with_recipients(group_id)

    if not group:
        logger.warning(f"⚠️  [GROUPS] Group {group_id} not found")
        raise HTTPException(status_code=404, detail="Group not found")

    # Multi-tenant isolation
    if group.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Group not found")

    logger.info(f"✅ [GROUPS] Fetched group {group_id} with recipients")

    # Extract recipients from group_recipients relationship
    recipients = [gr.recipient for gr in group.group_recipients]

    return RecipientGroupWithRecipientsResponse(
        **RecipientGroupResponse.model_validate(group).model_dump(),
        recipients=[RecipientResponse.model_validate(r) for r in recipients]
    )


# ==================== CREATE ENDPOINTS ====================

@router.post("/", response_model=RecipientGroupResponse, status_code=status.HTTP_201_CREATED)
async def create_group(
    group_data: RecipientGroupCreate,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Create a new recipient group (static or dynamic).

    Request Body:
        RecipientGroupCreate schema

    Returns:
        Created group

    Raises:
        400: Duplicate name or validation error
    """
    logger.info(
        f"➕ [GROUPS] Creating {group_data.group_type} group '{group_data.name}' "
        f"for candidate {current_candidate.id}"
    )

    repo = AsyncRecipientGroupRepository(db)

    # Check for duplicate name
    existing = await repo.get_by_name(current_candidate.id, group_data.name)
    if existing:
        logger.warning(
            f"⚠️  [GROUPS] Duplicate group name: {group_data.name} "
            f"(existing ID: {existing.id})"
        )
        raise HTTPException(
            status_code=400,
            detail=f"Group with name '{group_data.name}' already exists"
        )

    # Create group
    group_dict = group_data.model_dump()
    group_dict["candidate_id"] = current_candidate.id

    # filter_criteria is already a dict after model_dump(), no further conversion needed
    # Remove None values from filter_criteria if present
    if group_dict.get("filter_criteria") and isinstance(group_dict["filter_criteria"], dict):
        group_dict["filter_criteria"] = {k: v for k, v in group_dict["filter_criteria"].items() if v is not None}

    group = await repo.create(group_dict)

    # If dynamic group, trigger initial refresh
    if group.group_type == GroupTypeEnum.DYNAMIC and group.filter_criteria:
        logger.info(f"♻️  [GROUPS] Triggering initial refresh for dynamic group {group.id}")
        try:
            await repo.refresh_dynamic_group(group.id, force=True)
            # Reload group to get updated stats
            group = await repo.get_by_id(group.id, use_cache=False)
        except Exception as e:
            logger.error(f"❌ [GROUPS] Initial refresh failed: {e}")

    logger.info(f"✅ [GROUPS] Created group {group.id} ('{group.name}')")

    return RecipientGroupResponse.model_validate(group)


# ==================== UPDATE ENDPOINTS ====================

@router.patch("/{group_id}", response_model=RecipientGroupResponse)
async def update_group(
    group_id: int,
    group_data: RecipientGroupUpdate,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Update an existing group.

    Path Parameters:
    - group_id: Group ID

    Request Body:
        RecipientGroupUpdate schema (partial update)

    Returns:
        Updated group

    Raises:
        404: Group not found
        400: Duplicate name or validation error
    """
    logger.info(f"✏️  [GROUPS] Updating group {group_id}")

    repo = AsyncRecipientGroupRepository(db)

    # Get existing group
    group = await repo.get_by_id(group_id)

    if not group:
        logger.warning(f"⚠️  [GROUPS] Group {group_id} not found")
        raise HTTPException(status_code=404, detail="Group not found")

    # Multi-tenant isolation
    if group.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Group not found")

    # Check for duplicate name if changing name
    if group_data.name and group_data.name != group.name:
        existing = await repo.get_by_name(current_candidate.id, group_data.name)
        if existing:
            raise HTTPException(
                status_code=400,
                detail=f"Group with name '{group_data.name}' already exists"
            )

    # Update group
    update_dict = group_data.model_dump(exclude_unset=True)

    # filter_criteria is already a dict after model_dump(), no further conversion needed
    if "filter_criteria" in update_dict and update_dict["filter_criteria"] and isinstance(update_dict["filter_criteria"], dict):
        update_dict["filter_criteria"] = {k: v for k, v in update_dict["filter_criteria"].items() if v is not None}

    if not update_dict:
        # No fields to update
        return RecipientGroupResponse.model_validate(group)

    updated_group = await repo.update(group_id, update_dict)

    # If dynamic group and filter_criteria changed, trigger refresh
    if (
        group.group_type == GroupTypeEnum.DYNAMIC
        and "filter_criteria" in update_dict
    ):
        logger.info(f"♻️  [GROUPS] Triggering refresh after filter update for group {group_id}")
        try:
            await repo.refresh_dynamic_group(group_id, force=True)
            # Reload group to get updated stats
            updated_group = await repo.get_by_id(group_id, use_cache=False)
        except Exception as e:
            logger.error(f"❌ [GROUPS] Refresh after update failed: {e}")

    logger.info(f"✅ [GROUPS] Updated group {group_id}")

    return RecipientGroupResponse.model_validate(updated_group)


# ==================== DELETE ENDPOINTS ====================

@router.delete("/{group_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_group(
    group_id: int,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Soft delete a group.

    Note: Deleting a group does NOT delete the recipients, only the group itself
    and its membership records.

    Path Parameters:
    - group_id: Group ID

    Returns:
        204 No Content on success

    Raises:
        404: Group not found
    """
    logger.info(f"🗑️  [GROUPS] Deleting group {group_id}")

    repo = AsyncRecipientGroupRepository(db)

    # Get existing group
    group = await repo.get_by_id(group_id)

    if not group:
        logger.warning(f"⚠️  [GROUPS] Group {group_id} not found")
        raise HTTPException(status_code=404, detail="Group not found")

    # Multi-tenant isolation
    if group.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Group not found")

    # Soft delete
    success = await repo.soft_delete(group_id)

    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete group")

    logger.info(f"✅ [GROUPS] Deleted group {group_id} ('{group.name}')")

    return None


# ==================== GROUP MEMBERSHIP ENDPOINTS ====================

@router.get("/{group_id}/recipients", response_model=RecipientListResponse)
async def get_group_recipients(
    group_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    active_only: bool = False,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Get recipients in a group with pagination.

    Path Parameters:
    - group_id: Group ID

    Query Parameters:
    - page: Page number
    - page_size: Results per page
    - active_only: Only return active, non-unsubscribed recipients

    Returns:
        Paginated list of recipients in the group

    Raises:
        404: Group not found
    """
    logger.info(f"📋 [GROUPS] Fetching recipients for group {group_id} (page {page})")

    group_repo = AsyncRecipientGroupRepository(db)

    # Get group
    group = await group_repo.get_by_id(group_id)

    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    # Multi-tenant isolation
    if group.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Group not found")

    # Calculate skip
    skip = (page - 1) * page_size

    # Get recipients
    recipients, total = await group_repo.get_recipients(
        group_id=group_id,
        active_only=active_only,
        skip=skip,
        limit=page_size
    )

    # Calculate total pages
    total_pages = (total + page_size - 1) // page_size

    logger.info(f"✅ [GROUPS] Found {len(recipients)} recipients (total: {total})")

    return RecipientListResponse(
        recipients=[RecipientResponse.model_validate(r) for r in recipients],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages
    )


@router.post("/{group_id}/recipients/add")
async def add_recipients_to_group(
    group_id: int,
    request_data: AddRecipientsToGroupRequest,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Add recipients to a STATIC group.

    Note: This endpoint only works for static groups. Dynamic groups are
    managed automatically via filter criteria.

    Path Parameters:
    - group_id: Group ID

    Request Body:
        AddRecipientsToGroupRequest with recipient IDs

    Returns:
        Number of recipients added

    Raises:
        404: Group not found
        400: Group is dynamic (not allowed)
    """
    logger.info(
        f"➕ [GROUPS] Adding {len(request_data.recipient_ids)} recipients "
        f"to group {group_id}"
    )

    repo = AsyncRecipientGroupRepository(db)

    # Get group
    group = await repo.get_by_id(group_id)

    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    # Multi-tenant isolation
    if group.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Group not found")

    # Validate it's a static group
    if group.group_type == GroupTypeEnum.DYNAMIC:
        raise HTTPException(
            status_code=400,
            detail="Cannot manually add recipients to dynamic groups. Use refresh instead."
        )

    # Add recipients
    try:
        added_count = await repo.add_recipients(
            group_id=group_id,
            recipient_ids=request_data.recipient_ids,
            is_dynamic=False
        )

        logger.info(f"✅ [GROUPS] Added {added_count} recipients to group {group_id}")

        return {
            "group_id": group_id,
            "added_count": added_count,
            "total_recipients": added_count + (group.total_recipients or 0)
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{group_id}/recipients/remove")
async def remove_recipients_from_group(
    group_id: int,
    request_data: RemoveRecipientsFromGroupRequest,
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Remove recipients from a STATIC group.

    Note: This endpoint only works for static groups.

    Path Parameters:
    - group_id: Group ID

    Request Body:
        RemoveRecipientsFromGroupRequest with recipient IDs

    Returns:
        Number of recipients removed

    Raises:
        404: Group not found
        400: Group is dynamic (not allowed)
    """
    logger.info(
        f"🗑️  [GROUPS] Removing {len(request_data.recipient_ids)} recipients "
        f"from group {group_id}"
    )

    repo = AsyncRecipientGroupRepository(db)

    # Get group
    group = await repo.get_by_id(group_id)

    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    # Multi-tenant isolation
    if group.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Group not found")

    # Validate it's a static group
    if group.group_type == GroupTypeEnum.DYNAMIC:
        raise HTTPException(
            status_code=400,
            detail="Cannot manually remove recipients from dynamic groups. Use refresh instead."
        )

    # Remove recipients
    try:
        removed_count = await repo.remove_recipients(
            group_id=group_id,
            recipient_ids=request_data.recipient_ids
        )

        logger.info(f"✅ [GROUPS] Removed {removed_count} recipients from group {group_id}")

        # Refresh group to get accurate total after removal
        updated_group = await repo.get_by_id(group_id, use_cache=False)
        actual_total = updated_group.total_recipients if updated_group else max(0, (group.total_recipients or 0) - removed_count)

        return {
            "group_id": group_id,
            "removed_count": removed_count,
            "total_recipients": actual_total
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ==================== DYNAMIC GROUP ENDPOINTS ====================

@router.post("/{group_id}/refresh", response_model=RefreshDynamicGroupResponse)
async def refresh_dynamic_group(
    group_id: int,
    force: bool = Query(False, description="Force refresh even if recently refreshed"),
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Refresh a DYNAMIC group by re-evaluating filters.

    This endpoint:
    1. Evaluates the group's filter criteria
    2. Finds all matching recipients
    3. Updates group membership (adds new matches, removes non-matches)
    4. Updates group statistics

    Path Parameters:
    - group_id: Group ID

    Query Parameters:
    - force: Force refresh even if recently refreshed

    Returns:
        Refresh statistics

    Raises:
        404: Group not found
        400: Group is not dynamic
    """
    logger.info(f"♻️  [GROUPS] Refreshing dynamic group {group_id} (force: {force})")

    repo = AsyncRecipientGroupRepository(db)

    # Get group
    group = await repo.get_by_id(group_id)

    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    # Multi-tenant isolation
    if group.candidate_id != current_candidate.id:
        raise HTTPException(status_code=404, detail="Group not found")

    # Validate it's a dynamic group
    if group.group_type != GroupTypeEnum.DYNAMIC:
        raise HTTPException(
            status_code=400,
            detail="Can only refresh dynamic groups. This is a static group."
        )

    # Refresh group
    try:
        result = await repo.refresh_dynamic_group(group_id, force=force)

        logger.info(
            f"✅ [GROUPS] Refreshed group {group_id}: "
            f"added {result.get('added', 0)}, removed {result.get('removed', 0)}"
        )

        return RefreshDynamicGroupResponse(**result)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ==================== STATISTICS ENDPOINTS ====================

@router.get("/statistics/overview", response_model=GroupStatistics)
async def get_group_statistics(
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Get group statistics for the current candidate.

    Returns:
        Statistics including:
        - Total groups
        - Static vs dynamic counts
        - Unique recipients across all groups
        - Average group size
    """
    logger.info(f"📊 [GROUPS] Fetching statistics for candidate {current_candidate.id}")

    repo = AsyncRecipientGroupRepository(db)

    stats = await repo.get_group_statistics(current_candidate.id)

    logger.info(f"✅ [GROUPS] Statistics computed (total: {stats['total_groups']})")

    return GroupStatistics(**stats)


@router.get("/needing-refresh/", response_model=List[RecipientGroupResponse])
async def get_groups_needing_refresh(
    stale_threshold_hours: int = Query(24, ge=1, le=168),
    db: AsyncSession = Depends(get_async_db),
    current_candidate: Candidate = Depends(get_current_candidate)
):
    """
    Get dynamic groups that need refreshing.

    Query Parameters:
    - stale_threshold_hours: Consider groups stale after this many hours (1-168)

    Returns:
        List of groups needing refresh
    """
    logger.info(
        f"🔍 [GROUPS] Finding groups needing refresh "
        f"(threshold: {stale_threshold_hours}h) for candidate {current_candidate.id}"
    )

    repo = AsyncRecipientGroupRepository(db)

    groups = await repo.get_groups_needing_refresh(
        candidate_id=current_candidate.id,
        stale_threshold_hours=stale_threshold_hours
    )

    logger.info(f"✅ [GROUPS] Found {len(groups)} groups needing refresh")

    return [RecipientGroupResponse.model_validate(g) for g in groups]
