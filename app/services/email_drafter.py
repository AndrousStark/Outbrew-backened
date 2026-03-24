"""Personalized Email Drafter Service - AI-powered email generation"""
import re
import random
import html
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_

# Setup logger
logger = logging.getLogger(__name__)

from app.models.company import Company
from app.models.candidate import Candidate
from app.models.company_intelligence import (
    SkillMatch, PersonalizedEmailDraft, CandidateSkillProfile,
    CompanyResearchCache, EmailToneEnum, MatchStrengthEnum,
    EMAIL_TEMPLATES, SKILL_CATEGORIES
)


class PersonalizedEmailDrafter:
    """
    Generates personalized, context-aware email drafts based on:
    - Candidate skills and experience
    - Company research data
    - Skill match analysis
    - Best practices for cold outreach
    """

    def __init__(self, db: Session):
        self.db = db

    async def generate_email_draft(
        self,
        candidate_id: int,
        company_id: int,
        skill_match_id: Optional[int] = None,
        tone: EmailToneEnum = EmailToneEnum.PROFESSIONAL,
        include_projects: bool = True,
        include_achievements: bool = True,
        custom_opening: Optional[str] = None,
        job_title: Optional[str] = None
    ) -> PersonalizedEmailDraft:
        """
        Generate a personalized email draft for a candidate-company pair
        """
        # Get candidate and company
        candidate = self.db.query(Candidate).filter(Candidate.id == candidate_id).first()
        company = self.db.query(Company).filter(Company.id == company_id).first()

        if not candidate or not company:
            raise ValueError("Candidate or Company not found")

        # Get skill match with error handling
        skill_match = None
        try:
            if skill_match_id:
                skill_match = self.db.query(SkillMatch).filter(SkillMatch.id == skill_match_id).first()
            else:
                skill_match = self.db.query(SkillMatch).filter(
                    and_(
                        SkillMatch.candidate_id == candidate_id,
                        SkillMatch.company_id == company_id
                    )
                ).first()
        except Exception as e:
            logger.warning(f"[EmailDrafter] Failed to fetch skill match: {e}")
            skill_match = None

        # Get candidate profile with error handling
        profile = None
        try:
            profile = self.db.query(CandidateSkillProfile).filter(
                CandidateSkillProfile.candidate_id == candidate_id
            ).first()
        except Exception as e:
            logger.warning(f"[EmailDrafter] Failed to fetch candidate profile: {e}")
            profile = None

        # Get company research cache with error handling
        cache = None
        try:
            cache = self.db.query(CompanyResearchCache).filter(
                CompanyResearchCache.company_id == company_id
            ).first()
        except Exception as e:
            logger.warning(f"[EmailDrafter] Failed to fetch company research cache: {e}")
            cache = None

        logger.debug(f"[EmailDrafter] Data fetched - skill_match: {skill_match is not None}, profile: {profile is not None}, cache: {cache is not None}")

        # Generate email components
        subject_line, alternatives = self._generate_subject_lines(
            candidate, company, skill_match, profile, job_title, tone
        )

        opening = custom_opening or self._generate_opening(company, cache, tone)
        skill_highlights = self._generate_skill_highlights(
            candidate, company, skill_match, profile, cache, include_projects
        )
        company_specific = self._generate_company_specific(company, cache, skill_match)
        call_to_action = self._generate_call_to_action(tone)
        closing = self._generate_closing(candidate, tone)

        # Combine into full email
        email_body = self._combine_email_parts(
            opening, skill_highlights, company_specific, call_to_action, closing, candidate
        )

        # Generate HTML version
        email_html = self._generate_html_version(email_body)

        # Calculate scores
        confidence_score = self._calculate_confidence(skill_match, profile, cache)
        relevance_score = self._calculate_relevance(skill_match, cache)
        personalization_level = self._calculate_personalization(
            cache, skill_match, include_projects, include_achievements
        )

        # Create draft
        draft = PersonalizedEmailDraft(
            candidate_id=candidate_id,
            company_id=company_id,
            skill_match_id=skill_match.id if skill_match else None,
            subject_line=subject_line,
            subject_alternatives=alternatives,
            email_body=email_body,
            email_html=email_html,
            opening=opening,
            skill_highlights=skill_highlights,
            company_specific=company_specific,
            call_to_action=call_to_action,
            closing=closing,
            tone=tone,
            personalization_level=personalization_level,
            confidence_score=confidence_score,
            relevance_score=relevance_score,
            generation_params={
                "include_projects": include_projects,
                "include_achievements": include_achievements,
                "job_title": job_title,
                "custom_opening": custom_opening is not None
            }
        )

        try:
            self.db.add(draft)
            self.db.commit()
            self.db.refresh(draft)
            logger.info(f"[EmailDrafter] Created draft {draft.id} for candidate {candidate_id}, company {company_id}")
        except Exception as e:
            self.db.rollback()
            logger.error(f"[EmailDrafter] Failed to save draft: {e}")
            raise ValueError(f"Failed to save email draft: {e}")

        return draft

    def _generate_subject_lines(
        self,
        candidate: Candidate,
        company: Company,
        skill_match: Optional[SkillMatch],
        profile: Optional[CandidateSkillProfile],
        job_title: Optional[str],
        tone: EmailToneEnum
    ) -> tuple[str, List[str]]:
        """Generate compelling subject lines"""
        templates = {
            EmailToneEnum.PROFESSIONAL: [
                "{title} Interested in Opportunities at {company}",
                "Experienced {primary_skill} Professional - {company}",
                "{years}+ Years {primary_skill} Experience - Interested in {company}",
                "Application: {title} at {company}",
            ],
            EmailToneEnum.FRIENDLY: [
                "Excited About {company}'s Work!",
                "Love What You're Building at {company}",
                "{primary_skill} Dev Eager to Join {company}",
                "Big Fan of {company} - Would Love to Connect!",
            ],
            EmailToneEnum.ENTHUSIASTIC: [
                "Thrilled to Apply to {company}!",
                "{company} + My {primary_skill} Skills = Perfect Match!",
                "Can't Wait to Contribute to {company}!",
                "Your {project} Project Inspired Me to Reach Out!",
            ],
            EmailToneEnum.FORMAL: [
                "Application for {title} Position at {company}",
                "Expression of Interest: {company}",
                "Professional Inquiry: {title} Opportunities",
                "Regarding Employment Opportunities at {company}",
            ],
            EmailToneEnum.CASUAL: [
                "Quick Hello from a Fellow {primary_skill} Dev",
                "Saw {company}'s Work - Had to Reach Out",
                "Hey from a {primary_skill} Enthusiast!",
                "{company} Caught My Eye - Let's Chat?",
            ]
        }

        # Get relevant data
        title = job_title or candidate.title or "Software Engineer"
        primary_skill = ""
        if profile and profile.primary_expertise:
            primary_skill = profile.primary_expertise[0]
        elif candidate.skills:
            skills = candidate.skills if isinstance(candidate.skills, list) else [candidate.skills]
            primary_skill = skills[0] if skills else "Tech"

        years = ""
        if profile and profile.years_experience:
            years = str(int(profile.years_experience))
        else:
            years = "3"  # Default assumption

        project = ""
        if skill_match and skill_match.matched_skills:
            project = skill_match.matched_skills[0]

        # Format templates
        subject_templates = templates.get(tone, templates[EmailToneEnum.PROFESSIONAL])
        formatted = []

        for template in subject_templates:
            try:
                formatted.append(template.format(
                    title=title,
                    company=company.name,
                    primary_skill=primary_skill,
                    years=years,
                    project=project
                ))
            except (KeyError, IndexError, ValueError) as e:
                logger.debug(f"[EmailDrafter] Subject template formatting failed: {e}")
                formatted.append(f"Application to {company.name}")

        # Primary subject and alternatives
        primary = formatted[0] if formatted else f"Application to {company.name}"
        alternatives = formatted[1:4] if len(formatted) > 1 else []

        return primary, alternatives

    def _generate_opening(
        self,
        company: Company,
        cache: Optional[CompanyResearchCache],
        tone: EmailToneEnum
    ) -> str:
        """Generate personalized opening paragraph"""
        tone_key = tone.value if tone.value in ["professional", "friendly", "enthusiastic"] else "professional"
        templates = EMAIL_TEMPLATES["opening_lines"].get(tone_key, EMAIL_TEMPLATES["opening_lines"]["professional"])

        # Pick random template
        template = random.choice(templates)

        # Determine domain/focus
        domain = "technology"
        if company.industry:
            domain = company.industry.lower()
        elif cache and cache.company_culture:
            keywords = cache.company_culture.get("keywords", [])
            if keywords:
                domain = keywords[0]

        opening = template.format(
            company_name=company.name,
            domain=domain
        )

        # Add specific hook if we have research
        if cache:
            if cache.github_repos and len(cache.github_repos) > 0:
                repo = cache.github_repos[0]
                opening += f" I was particularly impressed by your {repo.get('name', 'open source')} project."
            elif cache.about_summary:
                # Extract key phrase
                summary = cache.about_summary[:100]
                opening += f" Your mission to {summary.lower()}... resonates with me."
            elif cache.growth_signals:
                opening += f" Your company's growth trajectory is impressive."

        return opening

    def _generate_skill_highlights(
        self,
        candidate: Candidate,
        company: Company,
        skill_match: Optional[SkillMatch],
        profile: Optional[CandidateSkillProfile],
        cache: Optional[CompanyResearchCache],
        include_projects: bool
    ) -> str:
        """Generate skill highlight section"""
        paragraphs = []

        # Main skill statement
        if skill_match and skill_match.matched_skills:
            matched = skill_match.matched_skills[:5]
            paragraphs.append(
                f"My expertise in {self._format_skill_list(matched)} aligns well with "
                f"your technical requirements."
            )
        elif profile and profile.primary_expertise:
            paragraphs.append(
                f"I bring strong skills in {self._format_skill_list(profile.primary_expertise)} "
                f"that I believe would be valuable to {company.name}."
            )
        elif candidate.skills:
            skills = candidate.skills if isinstance(candidate.skills, list) else [candidate.skills]
            paragraphs.append(
                f"My technical background includes {self._format_skill_list(skills[:5])}."
            )

        # Project mention
        if include_projects and profile and profile.projects:
            project = profile.projects[0] if profile.projects else None
            if project:
                project_name = project.get("name", "a recent project")
                project_tech = project.get("technologies", [])
                if project_tech:
                    paragraphs.append(
                        f"In my recent work on {project_name}, I utilized {self._format_skill_list(project_tech[:3])} "
                        f"to deliver impactful results."
                    )

        # Experience mention
        if profile and profile.work_experience:
            exp = profile.work_experience[0] if profile.work_experience else None
            if exp:
                company_name = exp.get("company", "my previous company")
                paragraphs.append(
                    f"At {company_name}, I developed solutions that "
                    f"demonstrate my ability to contribute immediately to your team."
                )

        # Talking points from skill match
        if skill_match and skill_match.talking_points:
            for point in skill_match.talking_points[:2]:
                paragraphs.append(f"Additionally, {point.lower()}.")

        return " ".join(paragraphs)

    def _generate_company_specific(
        self,
        company: Company,
        cache: Optional[CompanyResearchCache],
        skill_match: Optional[SkillMatch]
    ) -> str:
        """Generate company-specific paragraph"""
        parts = []

        if cache:
            # Culture alignment
            if cache.company_culture:
                if cache.company_culture.get("remote_friendly"):
                    parts.append(
                        f"I appreciate {company.name}'s embrace of flexible work arrangements"
                    )
                if cache.company_culture.get("diversity_focus"):
                    parts.append(
                        f"and your commitment to building a diverse, inclusive team"
                    )

            # Tech alignment
            if cache.tech_stack_detailed:
                detected = cache.tech_stack_detailed.get("detected", [])
                if detected:
                    parts.append(
                        f"Your tech stack ({self._format_skill_list(detected[:3])}) "
                        f"is well-suited to my experience"
                    )

            # Growth/opportunity
            if cache.growth_signals:
                parts.append(
                    f"The company's growth phase presents exciting opportunities "
                    f"for meaningful contribution and professional development"
                    )

        if not parts:
            # Generic but relevant
            parts.append(
                f"I'm drawn to {company.name}'s approach to innovation "
                f"and would be excited to contribute to your continued success"
            )

        # Add match context if available
        if skill_match and skill_match.match_context:
            parts.append(f". {skill_match.match_context}")

        return ". ".join(parts) + "."

    def _generate_call_to_action(self, tone: EmailToneEnum) -> str:
        """Generate call to action"""
        tone_key = tone.value if tone.value in ["professional", "friendly", "call_to_action"] else "call_to_action"
        templates = EMAIL_TEMPLATES["closing_lines"].get(tone_key, EMAIL_TEMPLATES["closing_lines"]["call_to_action"])

        return random.choice(templates)

    def _generate_closing(self, candidate: Candidate, tone: EmailToneEnum) -> str:
        """Generate email closing"""
        closings = {
            EmailToneEnum.PROFESSIONAL: [
                "Best regards,",
                "Sincerely,",
                "Thank you for your consideration,",
            ],
            EmailToneEnum.FRIENDLY: [
                "Looking forward to hearing from you,",
                "Thanks so much,",
                "Cheers,",
            ],
            EmailToneEnum.ENTHUSIASTIC: [
                "Can't wait to hear from you!",
                "Excitedly awaiting your response,",
                "With enthusiasm,",
            ],
            EmailToneEnum.FORMAL: [
                "Respectfully,",
                "Yours sincerely,",
                "With regards,",
            ],
            EmailToneEnum.CASUAL: [
                "Talk soon,",
                "Thanks!",
                "Best,",
            ]
        }

        closing = random.choice(closings.get(tone, closings[EmailToneEnum.PROFESSIONAL]))
        name = candidate.full_name or candidate.username

        return f"{closing}\n{name}"

    def _combine_email_parts(
        self,
        opening: str,
        skill_highlights: str,
        company_specific: str,
        call_to_action: str,
        closing: str,
        candidate: Candidate
    ) -> str:
        """Combine all parts into cohesive email"""
        # Determine greeting
        greeting = "Dear Hiring Manager,"

        parts = [
            greeting,
            "",
            opening,
            "",
            skill_highlights,
            "",
            company_specific,
            "",
            call_to_action,
            "",
            closing
        ]

        return "\n".join(parts)

    def _generate_html_version(self, plain_text: str) -> str:
        """Generate HTML version of email with proper escaping for security"""
        logger.debug("[EmailDrafter] Generating HTML version of email")

        # Convert newlines to paragraphs
        paragraphs = plain_text.split("\n\n")
        html_parts = []

        for p in paragraphs:
            if p.strip():
                # Escape HTML special characters to prevent XSS
                escaped_p = html.escape(p.strip())
                # Handle line breaks within paragraph (after escaping)
                escaped_p = escaped_p.replace("\n", "<br>")
                html_parts.append(f"<p style='margin: 0 0 15px 0; line-height: 1.6;'>{escaped_p}</p>")

        html_body = "\n".join(html_parts)

        return f"""
        <div style="font-family: 'Segoe UI', Arial, sans-serif; font-size: 14px; color: #333; max-width: 600px;">
            {html_body}
        </div>
        """

    def _format_skill_list(self, skills: List[str]) -> str:
        """Format list of skills naturally"""
        if not skills:
            return ""
        if len(skills) == 1:
            return skills[0]
        if len(skills) == 2:
            return f"{skills[0]} and {skills[1]}"
        return f"{', '.join(skills[:-1])}, and {skills[-1]}"

    def _calculate_confidence(
        self,
        skill_match: Optional[SkillMatch],
        profile: Optional[CandidateSkillProfile],
        cache: Optional[CompanyResearchCache]
    ) -> float:
        """Calculate confidence score for the draft"""
        score = 0.5  # Base score

        if skill_match:
            score += 0.2 * (skill_match.overall_score / 100)

        if profile:
            score += 0.15 * profile.completeness_score

        if cache:
            score += 0.15 * cache.completeness_score

        return min(score, 1.0)

    def _calculate_relevance(
        self,
        skill_match: Optional[SkillMatch],
        cache: Optional[CompanyResearchCache]
    ) -> float:
        """Calculate relevance score"""
        if skill_match:
            return skill_match.overall_score / 100
        return 0.5

    def _calculate_personalization(
        self,
        cache: Optional[CompanyResearchCache],
        skill_match: Optional[SkillMatch],
        include_projects: bool,
        include_achievements: bool
    ) -> float:
        """Calculate personalization level"""
        score = 0.3  # Base personalization

        if cache:
            if cache.about_summary:
                score += 0.1
            if cache.github_repos:
                score += 0.15
            if cache.company_culture:
                score += 0.1
            if cache.job_openings:
                score += 0.1

        if skill_match:
            if skill_match.talking_points:
                score += 0.15
            if skill_match.matched_skills:
                score += 0.1

        return min(score, 1.0)

    # ============= DRAFT MANAGEMENT =============

    async def get_drafts_for_company(
        self,
        candidate_id: int,
        company_id: int
    ) -> List[PersonalizedEmailDraft]:
        """Get all drafts for a candidate-company pair"""
        return self.db.query(PersonalizedEmailDraft).filter(
            and_(
                PersonalizedEmailDraft.candidate_id == candidate_id,
                PersonalizedEmailDraft.company_id == company_id
            )
        ).order_by(PersonalizedEmailDraft.created_at.desc()).all()

    async def get_all_drafts(
        self,
        candidate_id: int,
        limit: int = 50
    ) -> List[PersonalizedEmailDraft]:
        """Get all drafts for a candidate"""
        return self.db.query(PersonalizedEmailDraft).filter(
            PersonalizedEmailDraft.candidate_id == candidate_id
        ).order_by(PersonalizedEmailDraft.created_at.desc()).limit(limit).all()

    async def mark_draft_as_used(self, draft_id: int, candidate_id: int = None) -> Optional[PersonalizedEmailDraft]:
        """Mark a draft as used"""
        logger.debug(f"[EmailDrafter] Marking draft {draft_id} as used")

        filters = [PersonalizedEmailDraft.id == draft_id]
        if candidate_id is not None:
            filters.append(PersonalizedEmailDraft.candidate_id == candidate_id)

        draft = self.db.query(PersonalizedEmailDraft).filter(
            *filters
        ).first()

        if not draft:
            logger.warning(f"[EmailDrafter] Draft {draft_id} not found for marking as used")
            return None

        try:
            draft.is_used = True
            draft.used_at = datetime.now(timezone.utc)
            self.db.commit()
            logger.info(f"[EmailDrafter] Draft {draft_id} marked as used")
        except Exception as e:
            self.db.rollback()
            logger.error(f"[EmailDrafter] Failed to mark draft {draft_id} as used: {e}")
            raise ValueError(f"Failed to update draft: {e}")

        return draft

    async def toggle_favorite(self, draft_id: int, candidate_id: int = None) -> Optional[PersonalizedEmailDraft]:
        """Toggle favorite status"""
        logger.debug(f"[EmailDrafter] Toggling favorite for draft {draft_id}")

        filters = [PersonalizedEmailDraft.id == draft_id]
        if candidate_id is not None:
            filters.append(PersonalizedEmailDraft.candidate_id == candidate_id)

        draft = self.db.query(PersonalizedEmailDraft).filter(
            *filters
        ).first()

        if not draft:
            logger.warning(f"[EmailDrafter] Draft {draft_id} not found for toggling favorite")
            return None

        try:
            draft.is_favorite = not draft.is_favorite
            self.db.commit()
            logger.info(f"[EmailDrafter] Draft {draft_id} favorite toggled to {draft.is_favorite}")
        except Exception as e:
            self.db.rollback()
            logger.error(f"[EmailDrafter] Failed to toggle favorite for draft {draft_id}: {e}")
            raise ValueError(f"Failed to update draft: {e}")

        return draft

    async def regenerate_draft(
        self,
        draft_id: int,
        **kwargs
    ) -> PersonalizedEmailDraft:
        """Regenerate a draft with new parameters"""
        old_draft = self.db.query(PersonalizedEmailDraft).filter(
            PersonalizedEmailDraft.id == draft_id
        ).first()

        if not old_draft:
            raise ValueError("Draft not found")

        # Merge old params with new
        params = old_draft.generation_params or {}
        params.update(kwargs)

        return await self.generate_email_draft(
            candidate_id=old_draft.candidate_id,
            company_id=old_draft.company_id,
            skill_match_id=old_draft.skill_match_id,
            tone=kwargs.get("tone", old_draft.tone),
            include_projects=params.get("include_projects", True),
            include_achievements=params.get("include_achievements", True),
            job_title=params.get("job_title")
        )

    async def generate_variations(
        self,
        candidate_id: int,
        company_id: int,
        count: int = 3
    ) -> List[PersonalizedEmailDraft]:
        """Generate multiple email variations"""
        variations = []
        tones = [EmailToneEnum.PROFESSIONAL, EmailToneEnum.FRIENDLY, EmailToneEnum.ENTHUSIASTIC]

        for i, tone in enumerate(tones[:count]):
            draft = await self.generate_email_draft(
                candidate_id=candidate_id,
                company_id=company_id,
                tone=tone
            )
            variations.append(draft)

        return variations

    # ============= QUICK ACTIONS =============

    async def quick_draft(
        self,
        candidate_id: int,
        company_id: int
    ) -> Dict[str, Any]:
        """Quick draft generation with defaults"""
        draft = await self.generate_email_draft(
            candidate_id=candidate_id,
            company_id=company_id,
            tone=EmailToneEnum.PROFESSIONAL
        )

        return {
            "draft_id": draft.id,
            "subject": draft.subject_line,
            "body": draft.email_body,
            "html": draft.email_html,
            "alternatives": draft.subject_alternatives,
            "confidence": draft.confidence_score,
            "relevance": draft.relevance_score,
            "personalization": draft.personalization_level
        }
