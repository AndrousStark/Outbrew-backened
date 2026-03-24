"""
Template Marketplace Service - Share and discover email templates

Features:
- Publish templates to marketplace
- Browse and search public templates
- Rate and review templates
- Clone templates to personal library
- Track usage statistics
- Curated collections
"""

import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import desc, func, and_, or_

from app.models.template_marketplace import (
    PublicTemplate, TemplateRating, TemplateReview,
    TemplateUsageReport, TemplateFavorite, TemplateCollection,
    TemplateVisibility, TemplateCategory, TemplateLanguage
)
from app.models.email_template import EmailTemplate
from app.models.candidate import Candidate

logger = logging.getLogger(__name__)


class TemplateMarketplaceService:
    """Service for template marketplace operations"""

    def __init__(self, db: Session):
        self.db = db
        logger.debug("[TemplateMarketplace] Initialized")

    # ==================== TEMPLATE PUBLISHING ====================

    def publish_template(
        self,
        creator_id: int,
        template_id: Optional[int],
        title: str,
        description: str,
        category: TemplateCategory,
        language: TemplateLanguage,
        subject_template: str,
        body_template_text: str,
        body_template_html: Optional[str] = None,
        tags: List[str] = [],
        target_industry: Optional[str] = None,
        target_position_level: Optional[str] = None,
        target_role: Optional[str] = None,
        visibility: TemplateVisibility = TemplateVisibility.PUBLIC
    ) -> PublicTemplate:
        """
        Publish a template to the marketplace
        """
        logger.info(f"[TemplateMarketplace] Publishing template by user {creator_id}")

        # Get creator name
        creator = self.db.query(Candidate).filter(Candidate.id == creator_id).first()
        creator_name = creator.full_name if creator else "Anonymous"

        # Extract variables from template
        import re
        variables = list(set(re.findall(r'\{([^}]+)\}', subject_template + body_template_text)))

        # Generate preview text
        preview_text = body_template_text[:500] if body_template_text else ""

        template = PublicTemplate(
            creator_id=creator_id,
            creator_name=creator_name,
            title=title,
            description=description,
            category=category,
            language=language,
            subject_template=subject_template,
            body_template_text=body_template_text,
            body_template_html=body_template_html,
            preview_text=preview_text,
            tags=tags,
            variables=variables,
            target_industry=target_industry,
            target_position_level=target_position_level,
            target_role=target_role,
            visibility=visibility,
            is_approved=False  # Requires moderation
        )

        self.db.add(template)
        self.db.commit()
        self.db.refresh(template)

        logger.info(f"[TemplateMarketplace] Published template {template.id}")
        return template

    def publish_from_personal_template(
        self,
        creator_id: int,
        personal_template_id: int,
        **kwargs
    ) -> PublicTemplate:
        """
        Publish an existing personal template to marketplace
        """
        logger.info(f"[TemplateMarketplace] Publishing personal template {personal_template_id}")

        # Get personal template
        personal = self.db.query(EmailTemplate).filter(
            EmailTemplate.id == personal_template_id,
            EmailTemplate.candidate_id == creator_id
        ).first()

        if not personal:
            raise ValueError(f"Personal template {personal_template_id} not found")

        # Use personal template content if not provided
        return self.publish_template(
            creator_id=creator_id,
            template_id=personal_template_id,
            title=kwargs.get('title', personal.name),
            description=kwargs.get('description', personal.description or ""),
            category=kwargs.get('category', TemplateCategory.COLD_OUTREACH),
            language=kwargs.get('language', TemplateLanguage.ENGLISH),
            subject_template=personal.subject_template,
            body_template_text=personal.body_template_text,
            body_template_html=personal.body_template_html,
            tags=kwargs.get('tags', []),
            target_industry=kwargs.get('target_industry'),
            target_position_level=kwargs.get('target_position_level'),
            target_role=kwargs.get('target_role'),
            visibility=kwargs.get('visibility', TemplateVisibility.PUBLIC)
        )

    def unpublish_template(self, template_id: int, creator_id: int) -> bool:
        """Unpublish a template (soft delete)"""
        template = self.db.query(PublicTemplate).filter(
            PublicTemplate.id == template_id,
            PublicTemplate.creator_id == creator_id
        ).first()

        if template:
            template.visibility = TemplateVisibility.PRIVATE
            self.db.commit()
            logger.info(f"[TemplateMarketplace] Unpublished template {template_id}")
            return True
        return False

    # ==================== BROWSING & SEARCH ====================

    def browse_templates(
        self,
        skip: int = 0,
        limit: int = 20,
        category: Optional[TemplateCategory] = None,
        language: Optional[TemplateLanguage] = None,
        search_query: Optional[str] = None,
        sort_by: str = "popular",  # popular, newest, top_rated
        tags: List[str] = [],
        target_industry: Optional[str] = None
    ) -> List[PublicTemplate]:
        """
        Browse public templates with filtering and sorting
        """
        logger.info(f"[TemplateMarketplace] Browsing templates (sort: {sort_by})")

        # Base query - only public and approved
        query = self.db.query(PublicTemplate).filter(
            PublicTemplate.visibility == TemplateVisibility.PUBLIC,
            PublicTemplate.is_approved == True,
            PublicTemplate.deleted_at.is_(None)
        )

        # Filters
        if category:
            query = query.filter(PublicTemplate.category == category)

        if language:
            query = query.filter(PublicTemplate.language == language)

        if target_industry:
            query = query.filter(PublicTemplate.target_industry == target_industry)

        if tags:
            # Filter by tags (at least one tag matches)
            tag_filters = [PublicTemplate.tags.contains([tag]) for tag in tags]
            query = query.filter(or_(*tag_filters))

        if search_query:
            search_term = f"%{search_query}%"
            query = query.filter(
                or_(
                    PublicTemplate.title.ilike(search_term),
                    PublicTemplate.description.ilike(search_term),
                    PublicTemplate.subject_template.ilike(search_term)
                )
            )

        # Sorting
        if sort_by == "popular":
            query = query.order_by(desc(PublicTemplate.total_clones))
        elif sort_by == "newest":
            query = query.order_by(desc(PublicTemplate.published_at))
        elif sort_by == "top_rated":
            query = query.order_by(desc(PublicTemplate.avg_rating))
        else:
            query = query.order_by(desc(PublicTemplate.total_views))

        return query.offset(skip).limit(limit).all()

    def get_featured_templates(self, limit: int = 10) -> List[PublicTemplate]:
        """Get featured templates (admin-curated)"""
        return self.db.query(PublicTemplate).filter(
            PublicTemplate.is_featured == True,
            PublicTemplate.visibility == TemplateVisibility.PUBLIC,
            PublicTemplate.is_approved == True,
            PublicTemplate.deleted_at.is_(None)
        ).order_by(desc(PublicTemplate.total_views)).limit(limit).all()

    def get_template_details(self, template_id: int, viewer_id: Optional[int] = None) -> Optional[PublicTemplate]:
        """
        Get template details and increment view count
        """
        template = self.db.query(PublicTemplate).filter(
            PublicTemplate.id == template_id,
            PublicTemplate.deleted_at.is_(None)
        ).first()

        if template:
            # Increment view count (only if not the creator)
            if not viewer_id or viewer_id != template.creator_id:
                template.total_views += 1
                self.db.commit()

        return template

    # ==================== CLONING ====================

    def clone_template(
        self,
        template_id: int,
        candidate_id: int,
        custom_name: Optional[str] = None
    ) -> EmailTemplate:
        """
        Clone a public template to user's personal library
        """
        logger.info(f"[TemplateMarketplace] User {candidate_id} cloning template {template_id}")

        # Get public template
        public_template = self.db.query(PublicTemplate).filter(
            PublicTemplate.id == template_id
        ).first()

        if not public_template:
            raise ValueError(f"Template {template_id} not found")

        # Create personal template
        personal = EmailTemplate(
            candidate_id=candidate_id,
            name=custom_name or f"{public_template.title} (Cloned)",
            description=f"Cloned from marketplace: {public_template.description}",
            subject_template=public_template.subject_template,
            body_template_text=public_template.body_template_text,
            body_template_html=public_template.body_template_html,
            category=public_template.category.value,
            language=public_template.language.value,
            is_active=True
        )

        self.db.add(personal)

        # Update public template stats
        public_template.total_clones += 1

        # Create or update usage report
        usage_report = self.db.query(TemplateUsageReport).filter(
            TemplateUsageReport.template_id == template_id,
            TemplateUsageReport.candidate_id == candidate_id
        ).first()

        if usage_report:
            usage_report.times_used += 1
        else:
            usage_report = TemplateUsageReport(
                template_id=template_id,
                candidate_id=candidate_id,
                times_used=1
            )
            self.db.add(usage_report)

        self.db.commit()
        self.db.refresh(personal)

        logger.info(f"[TemplateMarketplace] Cloned template to personal library: {personal.id}")
        return personal

    # ==================== RATINGS & REVIEWS ====================

    def rate_template(
        self,
        template_id: int,
        candidate_id: int,
        rating: int,
        was_successful: Optional[bool] = None,
        response_time_hours: Optional[int] = None,
        used_for_industry: Optional[str] = None,
        used_for_role: Optional[str] = None
    ) -> TemplateRating:
        """
        Rate a template (1-5 stars)
        """
        if not 1 <= rating <= 5:
            raise ValueError("Rating must be between 1 and 5")

        logger.info(f"[TemplateMarketplace] User {candidate_id} rating template {template_id}: {rating} stars")

        # Check if already rated
        existing = self.db.query(TemplateRating).filter(
            TemplateRating.template_id == template_id,
            TemplateRating.candidate_id == candidate_id
        ).first()

        if existing:
            # Update existing rating
            old_rating = existing.rating
            existing.rating = rating
            existing.was_successful = was_successful
            existing.response_time_hours = response_time_hours
            existing.used_for_industry = used_for_industry
            existing.used_for_role = used_for_role
            template_rating = existing
        else:
            # Create new rating
            template_rating = TemplateRating(
                template_id=template_id,
                candidate_id=candidate_id,
                rating=rating,
                was_successful=was_successful,
                response_time_hours=response_time_hours,
                used_for_industry=used_for_industry,
                used_for_role=used_for_role
            )
            self.db.add(template_rating)

        # Recalculate average rating
        self._update_template_avg_rating(template_id)

        self.db.commit()
        logger.info(f"[TemplateMarketplace] Rating saved")
        return template_rating

    def _update_template_avg_rating(self, template_id: int):
        """Recalculate and update average rating"""
        result = self.db.query(
            func.avg(TemplateRating.rating),
            func.count(TemplateRating.id)
        ).filter(
            TemplateRating.template_id == template_id
        ).first()

        avg_rating, total_ratings = result

        template = self.db.query(PublicTemplate).filter(
            PublicTemplate.id == template_id
        ).first()

        if template:
            template.avg_rating = float(avg_rating) if avg_rating else 0.0
            template.total_ratings = total_ratings or 0

    def add_review(
        self,
        template_id: int,
        candidate_id: int,
        review_text: str,
        pros: Optional[str] = None,
        cons: Optional[str] = None,
        emails_sent: Optional[int] = None,
        responses_received: Optional[int] = None
    ) -> TemplateReview:
        """
        Add a review for a template
        """
        logger.info(f"[TemplateMarketplace] User {candidate_id} reviewing template {template_id}")

        # Check if already reviewed
        existing = self.db.query(TemplateReview).filter(
            TemplateReview.template_id == template_id,
            TemplateReview.candidate_id == candidate_id
        ).first()

        if existing:
            raise ValueError("You have already reviewed this template")

        review = TemplateReview(
            template_id=template_id,
            candidate_id=candidate_id,
            review_text=review_text,
            pros=pros,
            cons=cons,
            emails_sent=emails_sent,
            responses_received=responses_received
        )

        self.db.add(review)
        self.db.commit()
        self.db.refresh(review)

        logger.info(f"[TemplateMarketplace] Review added: {review.id}")
        return review

    def get_template_reviews(
        self,
        template_id: int,
        skip: int = 0,
        limit: int = 10
    ) -> List[TemplateReview]:
        """Get reviews for a template"""
        return self.db.query(TemplateReview).filter(
            TemplateReview.template_id == template_id,
            TemplateReview.is_flagged == False
        ).order_by(desc(TemplateReview.helpful_count)).offset(skip).limit(limit).all()

    def mark_review_helpful(
        self,
        review_id: int,
        is_helpful: bool = True
    ) -> bool:
        """Mark a review as helpful or not helpful"""
        review = self.db.query(TemplateReview).filter(
            TemplateReview.id == review_id
        ).first()

        if review:
            if is_helpful:
                review.helpful_count += 1
            else:
                review.not_helpful_count += 1
            self.db.commit()
            return True
        return False

    # ==================== FAVORITES ====================

    def toggle_favorite(self, template_id: int, candidate_id: int, notes: Optional[str] = None) -> bool:
        """Toggle favorite status for a template"""
        existing = self.db.query(TemplateFavorite).filter(
            TemplateFavorite.template_id == template_id,
            TemplateFavorite.candidate_id == candidate_id
        ).first()

        if existing:
            # Remove favorite
            self.db.delete(existing)
            self.db.commit()
            logger.info(f"[TemplateMarketplace] Removed favorite {template_id} for user {candidate_id}")
            return False
        else:
            # Add favorite
            favorite = TemplateFavorite(
                template_id=template_id,
                candidate_id=candidate_id,
                notes=notes
            )
            self.db.add(favorite)
            self.db.commit()
            logger.info(f"[TemplateMarketplace] Added favorite {template_id} for user {candidate_id}")
            return True

    def get_user_favorites(self, candidate_id: int) -> List[PublicTemplate]:
        """Get user's favorite templates"""
        favorites = self.db.query(TemplateFavorite).filter(
            TemplateFavorite.candidate_id == candidate_id
        ).all()

        template_ids = [f.template_id for f in favorites]

        return self.db.query(PublicTemplate).filter(
            PublicTemplate.id.in_(template_ids),
            PublicTemplate.deleted_at.is_(None)
        ).all()

    # ==================== COLLECTIONS ====================

    def create_collection(
        self,
        creator_id: int,
        name: str,
        description: str,
        template_ids: List[int],
        is_public: bool = False
    ) -> TemplateCollection:
        """Create a curated collection of templates"""
        logger.info(f"[TemplateMarketplace] Creating collection '{name}' by user {creator_id}")

        creator = self.db.query(Candidate).filter(Candidate.id == creator_id).first()
        creator_name = creator.full_name if creator else "Anonymous"

        collection = TemplateCollection(
            creator_id=creator_id,
            creator_name=creator_name,
            name=name,
            description=description,
            template_ids=template_ids,
            total_templates=len(template_ids),
            is_public=is_public
        )

        self.db.add(collection)
        self.db.commit()
        self.db.refresh(collection)

        logger.info(f"[TemplateMarketplace] Created collection {collection.id}")
        return collection

    def get_public_collections(self, skip: int = 0, limit: int = 20) -> List[TemplateCollection]:
        """Get public template collections"""
        return self.db.query(TemplateCollection).filter(
            TemplateCollection.is_public == True
        ).order_by(desc(TemplateCollection.total_views)).offset(skip).limit(limit).all()

    # ==================== STATS & ANALYTICS ====================

    def get_marketplace_stats(self) -> Dict[str, Any]:
        """Get overall marketplace statistics"""
        return {
            "total_templates": self.db.query(PublicTemplate).filter(
                PublicTemplate.visibility == TemplateVisibility.PUBLIC,
                PublicTemplate.is_approved == True
            ).count(),
            "total_creators": self.db.query(PublicTemplate.creator_id).distinct().count(),
            "total_clones": self.db.query(func.sum(PublicTemplate.total_clones)).scalar() or 0,
            "total_ratings": self.db.query(TemplateRating).count(),
            "total_reviews": self.db.query(TemplateReview).count(),
            "avg_rating": self.db.query(func.avg(PublicTemplate.avg_rating)).scalar() or 0.0
        }

    def get_user_published_templates(self, creator_id: int) -> List[PublicTemplate]:
        """Get templates published by a user"""
        return self.db.query(PublicTemplate).filter(
            PublicTemplate.creator_id == creator_id,
            PublicTemplate.deleted_at.is_(None)
        ).order_by(desc(PublicTemplate.created_at)).all()
