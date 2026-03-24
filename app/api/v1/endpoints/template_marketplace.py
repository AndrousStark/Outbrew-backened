"""
Template Marketplace API Endpoints

Provides template marketplace functionality including:
- Publishing templates to marketplace
- Browsing and searching templates
- Rating and reviewing templates
- Cloning templates to personal library
- Managing favorites and collections
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime

logger = logging.getLogger(__name__)

from app.api.dependencies import get_db, get_current_candidate
from app.models.candidate import Candidate
from app.models.template_marketplace import (
    TemplateCategory,
    TemplateLanguage,
    TemplateVisibility,
)
from app.services.template_marketplace_service import TemplateMarketplaceService

router = APIRouter()


# ==================== SCHEMAS ====================


class TemplatePublishRequest(BaseModel):
    """Schema for publishing a template"""

    personal_template_id: Optional[int] = None
    title: str = Field(..., min_length=3, max_length=255)
    description: str = Field(..., min_length=10, max_length=2000)
    category: TemplateCategory
    language: TemplateLanguage = TemplateLanguage.ENGLISH
    subject_template: Optional[str] = None
    body_template_text: Optional[str] = None
    body_template_html: Optional[str] = None
    tags: List[str] = []
    target_industry: Optional[str] = None
    target_position_level: Optional[str] = None
    target_role: Optional[str] = None
    visibility: TemplateVisibility = TemplateVisibility.PUBLIC


class PublicTemplateResponse(BaseModel):
    """Schema for public template response"""

    id: int
    creator_id: Optional[int]
    creator_name: str
    title: str
    description: Optional[str]
    category: TemplateCategory
    language: TemplateLanguage
    subject_template: str
    preview_text: Optional[str]
    tags: List[str]
    variables: List[str]
    target_industry: Optional[str]
    target_position_level: Optional[str]
    target_role: Optional[str]
    visibility: TemplateVisibility
    is_featured: bool
    is_verified: bool
    total_clones: int
    total_uses: int
    total_views: int
    avg_response_rate: float
    avg_rating: float
    total_ratings: int
    created_at: datetime
    published_at: Optional[datetime]

    class Config:
        from_attributes = True


class PublicTemplateDetailResponse(PublicTemplateResponse):
    """Schema for detailed template view"""

    body_template_text: str
    body_template_html: Optional[str]
    successful_uses: int
    total_opens: int
    total_clicks: int
    total_replies: int


class TemplateRatingRequest(BaseModel):
    """Schema for rating a template"""

    rating: int = Field(..., ge=1, le=5)
    was_successful: Optional[bool] = None
    response_time_hours: Optional[int] = None
    used_for_industry: Optional[str] = None
    used_for_role: Optional[str] = None


class TemplateReviewRequest(BaseModel):
    """Schema for reviewing a template"""

    review_text: str = Field(..., min_length=20, max_length=2000)
    pros: Optional[str] = None
    cons: Optional[str] = None
    emails_sent: Optional[int] = None
    responses_received: Optional[int] = None


class TemplateReviewResponse(BaseModel):
    """Schema for template review response"""

    id: int
    template_id: int
    candidate_id: int
    review_text: str
    pros: Optional[str]
    cons: Optional[str]
    emails_sent: Optional[int]
    responses_received: Optional[int]
    helpful_count: int
    not_helpful_count: int
    is_verified_use: bool
    created_at: datetime

    class Config:
        from_attributes = True


class CollectionCreateRequest(BaseModel):
    """Schema for creating a collection"""

    name: str = Field(..., min_length=3, max_length=255)
    description: str = Field(..., max_length=1000)
    template_ids: List[int]
    is_public: bool = False


class CollectionResponse(BaseModel):
    """Schema for collection response"""

    id: int
    creator_id: Optional[int]
    creator_name: str
    name: str
    description: Optional[str]
    total_templates: int
    total_views: int
    total_followers: int
    is_public: bool
    is_featured: bool
    created_at: datetime

    class Config:
        from_attributes = True


class MarketplaceStatsResponse(BaseModel):
    """Schema for marketplace statistics"""

    total_templates: int
    total_creators: int
    total_clones: int
    total_ratings: int
    total_reviews: int
    avg_rating: float


class EmailTemplateResponse(BaseModel):
    """Schema for cloned personal template"""

    id: int
    name: str
    description: Optional[str]
    subject_template: str
    category: str
    language: str
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


# ==================== PUBLISHING ENDPOINTS ====================


@router.post(
    "/publish",
    response_model=PublicTemplateResponse,
    status_code=status.HTTP_201_CREATED,
)
def publish_template(
    template_data: TemplatePublishRequest,
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Publish a template to the marketplace

    Can publish from:
    - Personal template (provide personal_template_id)
    - New content (provide subject_template, body_template_text)

    Templates require moderation approval before appearing publicly.
    """
    service = TemplateMarketplaceService(db)

    try:
        if template_data.personal_template_id:
            # Publish from personal template
            template = service.publish_from_personal_template(
                creator_id=current_user.id,
                personal_template_id=template_data.personal_template_id,
                title=template_data.title,
                description=template_data.description,
                category=template_data.category,
                language=template_data.language,
                tags=template_data.tags,
                target_industry=template_data.target_industry,
                target_position_level=template_data.target_position_level,
                target_role=template_data.target_role,
                visibility=template_data.visibility,
            )
        else:
            # Publish new template
            if (
                not template_data.subject_template
                or not template_data.body_template_text
            ):
                raise ValueError("subject_template and body_template_text are required")

            template = service.publish_template(
                creator_id=current_user.id,
                template_id=None,
                title=template_data.title,
                description=template_data.description,
                category=template_data.category,
                language=template_data.language,
                subject_template=template_data.subject_template,
                body_template_text=template_data.body_template_text,
                body_template_html=template_data.body_template_html,
                tags=template_data.tags,
                target_industry=template_data.target_industry,
                target_position_level=template_data.target_position_level,
                target_role=template_data.target_role,
                visibility=template_data.visibility,
            )

        return template
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to publish template: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to publish template"
        )


@router.delete("/templates/{template_id}", status_code=status.HTTP_204_NO_CONTENT)
def unpublish_template(
    template_id: int,
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Unpublish a template (make it private)

    Only the creator can unpublish their templates.
    """
    service = TemplateMarketplaceService(db)
    success = service.unpublish_template(template_id, current_user.id)

    if not success:
        raise HTTPException(
            status_code=404, detail="Template not found or not authorized"
        )

    return None


# ==================== BROWSING ENDPOINTS ====================


@router.post("/search", response_model=List[PublicTemplateResponse])
def search_templates(
    search_query: Optional[str] = None,
    category: Optional[TemplateCategory] = None,
    language: Optional[TemplateLanguage] = None,
    tags: List[str] = [],
    target_industry: Optional[str] = None,
    target_role: Optional[str] = None,
    min_rating: Optional[float] = Query(None, ge=0, le=5),
    sort_by: str = Query("popular", pattern="^(popular|newest|top_rated|most_used)$"),
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    current_user: Optional[Candidate] = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Search templates with advanced filtering (POST version for complex queries)

    **Search criteria:**
    - search_query: Keyword search in title, description, subject
    - category: Filter by template category
    - language: Filter by language
    - tags: Filter by multiple tags
    - target_industry: Filter by industry
    - target_role: Filter by role
    - min_rating: Minimum average rating

    **Sort options:**
    - popular: Most cloned templates
    - newest: Recently published
    - top_rated: Highest rated
    - most_used: Most frequently used

    **Returns:**
    - Paginated list of matching templates
    - Sorted by specified criteria
    """
    service = TemplateMarketplaceService(db)

    # Apply filters
    templates = service.browse_templates(
        skip=skip,
        limit=limit,
        category=category,
        language=language,
        search_query=search_query,
        sort_by=sort_by,
        tags=tags,
        target_industry=target_industry,
    )

    # Additional filtering (min_rating, target_role)
    if min_rating is not None:
        templates = [t for t in templates if t.avg_rating >= min_rating]

    if target_role:
        templates = [
            t
            for t in templates
            if t.target_role and target_role.lower() in t.target_role.lower()
        ]

    return templates


@router.get("/browse", response_model=List[PublicTemplateResponse])
def browse_templates(
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    category: Optional[TemplateCategory] = None,
    language: Optional[TemplateLanguage] = None,
    search: Optional[str] = Query(None, max_length=200),
    sort_by: str = Query("popular", pattern="^(popular|newest|top_rated)$"),
    tags: Optional[str] = Query(None, description="Comma-separated tags"),
    target_industry: Optional[str] = None,
    current_user: Optional[Candidate] = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Browse public templates with filtering and sorting

    **Filters:**
    - category: Template category
    - language: Template language
    - search: Search in title, description, subject
    - tags: Comma-separated tags
    - target_industry: Filter by industry

    **Sort options:**
    - popular: Most cloned templates
    - newest: Recently published
    - top_rated: Highest rated
    """
    service = TemplateMarketplaceService(db)

    # Parse tags
    tag_list = [tag.strip() for tag in tags.split(",")] if tags else []

    templates = service.browse_templates(
        skip=skip,
        limit=limit,
        category=category,
        language=language,
        search_query=search,
        sort_by=sort_by,
        tags=tag_list,
        target_industry=target_industry,
    )

    return templates


@router.get("/featured", response_model=List[PublicTemplateResponse])
def get_featured_templates(
    limit: int = Query(10, ge=1, le=50),
    current_user: Optional[Candidate] = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Get featured templates (admin-curated)

    These are high-quality templates selected by moderators.
    """
    service = TemplateMarketplaceService(db)
    templates = service.get_featured_templates(limit=limit)
    return templates


@router.get("/templates/{template_id}", response_model=PublicTemplateDetailResponse)
def get_template_detail(
    template_id: int,
    current_user: Optional[Candidate] = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Get detailed template information

    Includes full content and statistics.
    Increments view count.
    """
    service = TemplateMarketplaceService(db)
    viewer_id = current_user.id if current_user else None
    template = service.get_template_details(template_id, viewer_id)

    if not template:
        raise HTTPException(status_code=404, detail="Template not found")

    return template


# ==================== CLONING ENDPOINTS ====================


@router.post("/templates/{template_id}/clone", response_model=EmailTemplateResponse)
def clone_template(
    template_id: int,
    custom_name: Optional[str] = Query(None, max_length=255),
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Clone a template to your personal library

    Creates a copy of the template that you can edit and use.
    """
    service = TemplateMarketplaceService(db)

    try:
        personal_template = service.clone_template(
            template_id=template_id,
            candidate_id=current_user.id,
            custom_name=custom_name,
        )
        return personal_template
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to clone template: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to clone template"
        )


# ==================== RATING & REVIEW ENDPOINTS ====================


@router.post("/templates/{template_id}/rate", status_code=status.HTTP_204_NO_CONTENT)
def rate_template(
    template_id: int,
    rating_data: TemplateRatingRequest,
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Rate a template (1-5 stars)

    Can include additional context about usage and success.
    """
    service = TemplateMarketplaceService(db)

    try:
        service.rate_template(
            template_id=template_id,
            candidate_id=current_user.id,
            rating=rating_data.rating,
            was_successful=rating_data.was_successful,
            response_time_hours=rating_data.response_time_hours,
            used_for_industry=rating_data.used_for_industry,
            used_for_role=rating_data.used_for_role,
        )
        return None
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post(
    "/templates/{template_id}/review",
    response_model=TemplateReviewResponse,
    status_code=status.HTTP_201_CREATED,
)
def add_review(
    template_id: int,
    review_data: TemplateReviewRequest,
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Write a detailed review for a template

    Share your experience, pros, cons, and results.
    """
    service = TemplateMarketplaceService(db)

    try:
        review = service.add_review(
            template_id=template_id,
            candidate_id=current_user.id,
            review_text=review_data.review_text,
            pros=review_data.pros,
            cons=review_data.cons,
            emails_sent=review_data.emails_sent,
            responses_received=review_data.responses_received,
        )
        return review
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get(
    "/templates/{template_id}/reviews", response_model=List[TemplateReviewResponse]
)
def get_template_reviews(
    template_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=50),
    current_user: Optional[Candidate] = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Get reviews for a template

    Sorted by helpfulness.
    """
    service = TemplateMarketplaceService(db)
    reviews = service.get_template_reviews(template_id, skip=skip, limit=limit)
    return reviews


@router.post("/reviews/{review_id}/helpful", status_code=status.HTTP_204_NO_CONTENT)
def mark_review_helpful(
    review_id: int,
    is_helpful: bool = Query(True),
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Mark a review as helpful or not helpful
    """
    service = TemplateMarketplaceService(db)
    service.mark_review_helpful(review_id, is_helpful)
    return None


# ==================== FAVORITES ENDPOINTS ====================


@router.post("/templates/{template_id}/favorite", response_model=dict)
def toggle_favorite(
    template_id: int,
    notes: Optional[str] = Query(None, max_length=500),
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Toggle favorite status for a template

    Add or remove template from your favorites.
    """
    service = TemplateMarketplaceService(db)
    is_favorited = service.toggle_favorite(template_id, current_user.id, notes)
    return {"is_favorited": is_favorited}


@router.get("/favorites", response_model=List[PublicTemplateResponse])
def get_favorites(
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Get your favorite templates
    """
    service = TemplateMarketplaceService(db)
    templates = service.get_user_favorites(current_user.id)
    return templates


# ==================== COLLECTIONS ENDPOINTS ====================


@router.post(
    "/collections",
    response_model=CollectionResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_collection(
    collection_data: CollectionCreateRequest,
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Create a curated collection of templates

    Collections can be public or private.
    """
    service = TemplateMarketplaceService(db)

    try:
        collection = service.create_collection(
            creator_id=current_user.id,
            name=collection_data.name,
            description=collection_data.description,
            template_ids=collection_data.template_ids,
            is_public=collection_data.is_public,
        )
        return collection
    except Exception as e:
        logger.error(f"Failed to create collection: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to create collection"
        )


@router.get("/collections", response_model=List[CollectionResponse])
def browse_collections(
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    current_user: Optional[Candidate] = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Browse public template collections

    Curated sets of templates for specific use cases.
    """
    service = TemplateMarketplaceService(db)
    collections = service.get_public_collections(skip=skip, limit=limit)
    return collections


# ==================== STATS ENDPOINTS ====================


@router.get("/stats", response_model=MarketplaceStatsResponse)
def get_marketplace_stats(
    current_user: Optional[Candidate] = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Get overall marketplace statistics

    Shows total templates, ratings, reviews, etc.
    """
    service = TemplateMarketplaceService(db)
    stats = service.get_marketplace_stats()
    return stats


@router.get("/my-templates", response_model=List[PublicTemplateResponse])
def get_my_published_templates(
    current_user: Candidate = Depends(get_current_candidate),
    db: Session = Depends(get_db),
):
    """
    Get templates you've published to the marketplace
    """
    service = TemplateMarketplaceService(db)
    templates = service.get_user_published_templates(current_user.id)
    return templates
