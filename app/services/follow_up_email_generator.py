"""
Intelligent Follow-Up Email Generator

Generates highly personalized follow-up emails based on:
- Original email content and context
- Company information and research
- Candidate's full profile (skills, projects, social links)
- Follow-up strategy and tone
- Previous emails in the sequence
"""

import re
import random
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from sqlalchemy.orm import Session, joinedload

logger = logging.getLogger(__name__)

from app.models.follow_up import (
    FollowUpCampaign, FollowUpStep, FollowUpEmail, CandidateProfile,
    FollowUpTone, FollowUpStrategy, FollowUpEmailStatus,
    FOLLOW_UP_TEMPLATES
)
from app.models.application import Application
from app.models.company import Company
from app.models.candidate import Candidate
from app.models.company_intelligence import CompanyResearchCache


class FollowUpEmailGenerator:
    """
    Generates intelligent, personalized follow-up emails.

    Uses all available context to create compelling follow-ups that:
    - Reference the original email naturally
    - Include relevant candidate info (links, skills, projects)
    - Align with the company's focus and tech stack
    - Match the specified tone and strategy
    """

    def __init__(self, db: Session):
        self.db = db

    # ============= MAIN GENERATION METHOD =============

    async def generate_follow_up_email(
        self,
        campaign: FollowUpCampaign,
        step: FollowUpStep,
        step_number: int
    ) -> FollowUpEmail:
        """
        Generate a complete follow-up email for a campaign step.

        Returns a FollowUpEmail in DRAFT status for user review.
        """
        logger.info(f"[EmailGenerator] Generating email for campaign {campaign.id}, step {step_number}")

        # Gather all context
        context = await self._gather_context(campaign)
        logger.debug(f"[EmailGenerator] Context gathered: {len(context)} keys")

        # Generate subject line
        subject = self._generate_subject(
            step=step,
            context=context,
            step_number=step_number
        )

        # Generate email body
        body_text, body_html = self._generate_body(
            step=step,
            context=context,
            step_number=step_number
        )

        # Track personalization
        personalization_data = self._get_personalization_data(context, step)

        # Create the email record
        email = FollowUpEmail(
            campaign_id=campaign.id,
            step_id=step.id,
            step_number=step_number,
            status=FollowUpEmailStatus.DRAFT,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            is_auto_generated=True,
            original_subject=subject,
            original_body=body_text,
            strategy_used=step.strategy,
            tone_used=step.tone,
            personalization_data=personalization_data
        )

        try:
            self.db.add(email)
            self.db.commit()
            self.db.refresh(email)
            logger.info(f"[EmailGenerator] Created email {email.id} for campaign {campaign.id}, step {step_number}")
        except Exception as e:
            self.db.rollback()
            logger.error(f"[EmailGenerator] Failed to save email for campaign {campaign.id}: {e}")
            raise ValueError(f"Failed to save follow-up email: {e}")

        return email

    async def regenerate_email(
        self,
        email: FollowUpEmail,
        new_tone: Optional[FollowUpTone] = None,
        new_strategy: Optional[FollowUpStrategy] = None,
        custom_hints: Optional[Dict] = None
    ) -> FollowUpEmail:
        """
        Regenerate an email with optional new tone/strategy.
        """
        campaign = email.campaign
        step = email.step

        # Apply overrides
        if new_tone:
            step.tone = new_tone
        if new_strategy:
            step.strategy = new_strategy

        context = await self._gather_context(campaign)

        if custom_hints:
            context["custom_hints"] = custom_hints

        subject = self._generate_subject(step, context, email.step_number)
        body_text, body_html = self._generate_body(step, context, email.step_number)

        # Update email
        email.subject = subject
        email.body_text = body_text
        email.body_html = body_html
        email.strategy_used = step.strategy
        email.tone_used = step.tone
        email.personalization_data = self._get_personalization_data(context, step)
        email.updated_at = datetime.now(timezone.utc)

        try:
            self.db.commit()
            logger.info(f"[EmailGenerator] Regenerated email {email.id} for campaign {email.campaign_id}")
        except Exception as e:
            self.db.rollback()
            logger.error(f"[EmailGenerator] Failed to regenerate email {email.id}: {e}")
            raise ValueError(f"Failed to regenerate email: {e}")

        return email

    # ============= CONTEXT GATHERING =============

    async def _gather_context(self, campaign: FollowUpCampaign) -> Dict[str, Any]:
        """Gather all relevant context for email generation."""
        context = {
            "campaign": campaign,
            "original_email": campaign.original_email_context or {},
            "company": {},
            "candidate": {},
            "previous_emails": [],
            "research": {}
        }

        # Get application and company
        application = self.db.query(Application).filter(
            Application.id == campaign.application_id
        ).first()

        if application:
            context["application"] = {
                "id": application.id,
                "position": application.position_title,
                "status": application.status
            }

            if application.company:
                company = application.company
                context["company"] = {
                    "id": company.id,
                    "name": company.name,
                    "industry": company.industry,
                    "domain": company.domain,
                    "tech_stack": company.tech_stack or [],
                    "description": company.description,
                    "website": company.website_url,
                    "linkedin": company.linkedin_url
                }

                # Get research data
                research = self.db.query(CompanyResearchCache).filter(
                    CompanyResearchCache.company_id == company.id
                ).first()

                if research:
                    context["research"] = {
                        "about": research.about_summary,
                        "culture": research.company_culture,
                        "tech_detailed": research.tech_stack_detailed,
                        "job_openings": research.job_openings,
                        "github_repos": research.github_repos,
                        "blog_posts": research.blog_posts,
                        "growth_signals": research.growth_signals
                    }

        # Get candidate info
        candidate = self.db.query(Candidate).filter(
            Candidate.id == campaign.candidate_id
        ).first()

        if candidate:
            context["candidate"] = {
                "id": candidate.id,
                "name": candidate.full_name,
                "email": candidate.email,
                "title": candidate.title,
                "skills": candidate.skills or []
            }

            # Get extended profile
            profile = self.db.query(CandidateProfile).filter(
                CandidateProfile.candidate_id == candidate.id
            ).first()

            if profile:
                context["candidate"]["profile"] = {
                    "phone": profile.phone_number,
                    "personal_email": profile.personal_email,
                    "linkedin": profile.linkedin_url,
                    "github": profile.github_url,
                    "twitter": profile.twitter_url,
                    "website": profile.website_url,
                    "portfolio": profile.portfolio_url,
                    "headline": profile.headline,
                    "bio": profile.bio,
                    "years_experience": profile.years_experience,
                    "portfolio_projects": profile.portfolio_projects or [],
                    "achievements": profile.achievements or [],
                    "value_propositions": profile.value_propositions or [],
                    "signature": profile.email_signature
                }

        # Get previous emails in this campaign
        previous = self.db.query(FollowUpEmail).filter(
            FollowUpEmail.campaign_id == campaign.id,
            FollowUpEmail.status == FollowUpEmailStatus.SENT
        ).order_by(FollowUpEmail.step_number).all()

        context["previous_emails"] = [
            {
                "step": e.step_number,
                "subject": e.subject,
                "body_preview": e.body_text[:200] if e.body_text else "",
                "sent_at": e.sent_at.isoformat() if e.sent_at else None
            }
            for e in previous
        ]

        return context

    # ============= SUBJECT GENERATION =============

    def _generate_subject(
        self,
        step: FollowUpStep,
        context: Dict,
        step_number: int
    ) -> str:
        """Generate email subject line."""
        company_name = context["company"].get("name", "the team")
        position = context.get("application", {}).get("position", "opportunity")
        original_subject = context["original_email"].get("subject", "")
        name_parts = context["candidate"].get("name", "").split()
        candidate_name = name_parts[0] if name_parts else ""

        # Use template subject if provided
        if step.subject_template:
            subject = self._replace_placeholders(step.subject_template, context)
            return subject

        # Strategy-based subject generation
        strategy = step.strategy
        tone = step.tone

        subjects = {
            FollowUpStrategy.SOFT_BUMP: [
                f"Re: {original_subject}" if original_subject else f"Following up - {position}",
                f"Quick follow-up on {position}",
                f"Checking in - {company_name}",
                f"Still interested in {position}",
            ],
            FollowUpStrategy.ADD_VALUE: [
                f"Thought you might find this interesting",
                f"Quick insight about {context['company'].get('industry', 'your industry')}",
                f"Something relevant for {company_name}",
                f"Additional context for our conversation",
            ],
            FollowUpStrategy.SOCIAL_PROOF: [
                f"Recent project I thought you'd appreciate",
                f"A quick update from my side",
                f"Wanted to share this with you",
                f"Recent achievement relevant to {position}",
            ],
            FollowUpStrategy.QUESTION: [
                f"Quick question about {company_name}",
                f"Curious about something",
                f"Would love your perspective",
                f"Thoughts on this?",
            ],
            FollowUpStrategy.REFERENCE_ORIGINAL: [
                f"Re: {original_subject}" if original_subject else f"Following up",
                f"Circling back on {position}",
                f"Per my previous email",
            ],
            FollowUpStrategy.BREAKUP: [
                f"Closing the loop",
                f"Should I close the file?",
                f"One last note",
                f"Final follow-up",
            ]
        }

        options = subjects.get(strategy, subjects[FollowUpStrategy.SOFT_BUMP])
        subject = random.choice(options)

        return subject

    # ============= BODY GENERATION =============

    def _generate_body(
        self,
        step: FollowUpStep,
        context: Dict,
        step_number: int
    ) -> Tuple[str, str]:
        """Generate email body (text and HTML)."""
        strategy = step.strategy
        tone = step.tone

        # Get components
        greeting = self._generate_greeting(context, tone)
        opening = self._generate_opening(context, strategy, step_number)
        main_content = self._generate_main_content(context, strategy, step)
        call_to_action = self._generate_call_to_action(context, strategy, tone)
        signature = self._generate_signature(context, step)

        # Combine
        body_parts = [greeting, "", opening]

        if main_content:
            body_parts.extend(["", main_content])

        if call_to_action and step.include_call_to_action:
            body_parts.extend(["", call_to_action])

        body_parts.extend(["", signature])

        body_text = "\n".join(body_parts)
        body_html = self._convert_to_html(body_text, context)

        return body_text, body_html

    def _generate_greeting(self, context: Dict, tone: FollowUpTone) -> str:
        """Generate appropriate greeting."""
        # Try to get hiring manager name from original email context
        recipient_name = context["original_email"].get("recipient_name", "")

        if recipient_name:
            if tone == FollowUpTone.PROFESSIONAL:
                return f"Dear {recipient_name},"
            elif tone == FollowUpTone.FRIENDLY:
                return f"Hi {recipient_name},"
            elif tone == FollowUpTone.CASUAL:
                return f"Hey {recipient_name},"
            else:
                return f"Hello {recipient_name},"
        else:
            if tone == FollowUpTone.PROFESSIONAL:
                return "Dear Hiring Team,"
            elif tone == FollowUpTone.FRIENDLY:
                return "Hi there,"
            else:
                return "Hello,"

    def _generate_opening(
        self,
        context: Dict,
        strategy: FollowUpStrategy,
        step_number: int
    ) -> str:
        """Generate opening line based on strategy."""
        company_name = context["company"].get("name", "your company")
        position = context.get("application", {}).get("position", "the role")
        days_since = self._calculate_days_since_original(context)

        openings = {
            FollowUpStrategy.SOFT_BUMP: [
                f"I hope this email finds you well. I wanted to follow up on my application for the {position} position.",
                f"I'm reaching out to follow up on my previous email regarding the {position} role at {company_name}.",
                f"I hope you've had a chance to review my application. I remain very interested in the {position} position.",
            ],
            FollowUpStrategy.ADD_VALUE: [
                f"I came across something that made me think of {company_name} and wanted to share.",
                f"I wanted to add some additional context to my application that I think you'll find relevant.",
                f"While researching further, I discovered something I thought would be valuable to share.",
            ],
            FollowUpStrategy.SOCIAL_PROOF: [
                f"I wanted to share a recent accomplishment that I believe aligns well with what you're looking for.",
                f"I thought you might be interested in some recent work I've completed.",
                f"Since my last email, I've had some developments I'd like to share.",
            ],
            FollowUpStrategy.QUESTION: [
                f"I was doing some research on {company_name} and had a quick question.",
                f"I'd love to get your perspective on something if you have a moment.",
                f"I was thinking about {company_name}'s approach to {context['company'].get('industry', 'your industry')} and was curious...",
            ],
            FollowUpStrategy.REFERENCE_ORIGINAL: [
                f"I'm following up on my email from {days_since} days ago regarding the {position} position.",
                f"I wanted to circle back on my application for the {position} role.",
                f"Reaching out again about the opportunity at {company_name}.",
            ],
            FollowUpStrategy.BREAKUP: [
                f"I know you're busy, so I wanted to send one final note regarding my application.",
                f"I understand timing may not be right, but I wanted to close the loop professionally.",
                f"I'll keep this brief as I know your inbox is full.",
            ]
        }

        options = openings.get(strategy, openings[FollowUpStrategy.SOFT_BUMP])
        return random.choice(options)

    def _generate_main_content(
        self,
        context: Dict,
        strategy: FollowUpStrategy,
        step: FollowUpStep
    ) -> str:
        """Generate main body content based on strategy."""
        candidate = context["candidate"]
        profile = candidate.get("profile", {})
        company = context["company"]
        research = context.get("research", {})

        content_parts = []

        if strategy == FollowUpStrategy.SOFT_BUMP:
            # Brief reminder of qualifications
            skills = candidate.get("skills", [])
            if skills:
                skill_text = ", ".join(skills[:4])
                content_parts.append(
                    f"As a reminder, I bring strong experience in {skill_text}, "
                    f"which I believe would be valuable for your team."
                )

        elif strategy == FollowUpStrategy.ADD_VALUE:
            # Add value proposition or insight
            value_props = profile.get("value_propositions", [])
            if value_props:
                content_parts.append(value_props[0])
            else:
                # Generate based on company tech stack
                tech_stack = company.get("tech_stack", [])
                skills = candidate.get("skills", [])
                matching = set(t.lower() for t in tech_stack) & set(s.lower() for s in skills)
                if matching:
                    content_parts.append(
                        f"I noticed your team uses {', '.join(list(matching)[:3])}. "
                        f"In my recent work, I've {self._generate_skill_highlight(matching)}."
                    )

        elif strategy == FollowUpStrategy.SOCIAL_PROOF:
            # Share achievements or projects
            projects = profile.get("portfolio_projects", [])
            achievements = profile.get("achievements", [])

            if projects:
                project = projects[0]
                content_parts.append(
                    f"I recently completed {project.get('name', 'a project')} "
                    f"which {project.get('highlights', ['achieved great results'])[0] if project.get('highlights') else 'demonstrated my skills'}."
                )
            elif achievements:
                achievement = achievements[0]
                content_parts.append(
                    f"I recently {achievement.get('title', 'achieved something significant')} "
                    f"which I believe showcases my fit for this role."
                )

        elif strategy == FollowUpStrategy.QUESTION:
            # Ask engaging question
            if research.get("github_repos"):
                repo = research["github_repos"][0]
                content_parts.append(
                    f"I noticed your team's work on {repo.get('name', 'open source projects')}. "
                    f"I'm curious about your approach to {repo.get('language', 'development')} in that context."
                )
            elif research.get("blog_posts"):
                post = research["blog_posts"][0]
                content_parts.append(
                    f"I read your team's article about {post.get('title', 'your work')} "
                    f"and would love to hear more about your approach."
                )
            else:
                content_parts.append(
                    f"I'm particularly interested in how {company.get('name', 'your team')} "
                    f"approaches {company.get('industry', 'innovation')} challenges."
                )

        elif strategy == FollowUpStrategy.BREAKUP:
            # Final professional close
            content_parts.append(
                "I completely understand if the timing isn't right or if you've moved forward "
                "with other candidates. I just wanted to express my continued interest and "
                "leave the door open for future opportunities."
            )

        # Add links if appropriate
        if step.include_portfolio_link and profile.get("portfolio"):
            content_parts.append(f"\nYou can see my work at: {profile['portfolio']}")

        return "\n\n".join(content_parts)

    def _generate_call_to_action(
        self,
        context: Dict,
        strategy: FollowUpStrategy,
        tone: FollowUpTone
    ) -> str:
        """Generate appropriate call to action."""
        ctas = {
            FollowUpStrategy.SOFT_BUMP: [
                "Would you have 15 minutes this week for a quick call?",
                "I'd love the opportunity to discuss how I can contribute to your team.",
                "Please let me know if you need any additional information from my side.",
            ],
            FollowUpStrategy.ADD_VALUE: [
                "Happy to discuss this further if you're interested.",
                "Let me know if you'd like to explore this more.",
                "I'd be glad to share more details.",
            ],
            FollowUpStrategy.SOCIAL_PROOF: [
                "I'd welcome the chance to discuss how this experience could benefit your team.",
                "Would love to walk you through this in more detail.",
            ],
            FollowUpStrategy.QUESTION: [
                "Would you be open to a brief chat about this?",
                "I'd really value your perspective.",
            ],
            FollowUpStrategy.BREAKUP: [
                "If you'd like to reconnect in the future, please don't hesitate to reach out.",
                "I wish you and the team all the best.",
                "No need to respond - just wanted to close the loop.",
            ]
        }

        options = ctas.get(strategy, ctas[FollowUpStrategy.SOFT_BUMP])
        return random.choice(options)

    def _generate_signature(self, context: Dict, step: FollowUpStep) -> str:
        """Generate email signature."""
        candidate = context["candidate"]
        profile = candidate.get("profile", {})

        # Use custom signature if available
        if profile.get("signature"):
            return profile["signature"]

        # Build signature
        name = candidate.get("name", "")
        title = candidate.get("title", "")

        sig_parts = ["Best regards,", name]

        if title:
            sig_parts.append(title)

        # Add links based on sequence settings
        if step.sequence and step.sequence.include_candidate_links:
            links = []
            if profile.get("linkedin"):
                links.append(f"LinkedIn: {profile['linkedin']}")
            if profile.get("github"):
                links.append(f"GitHub: {profile['github']}")
            if profile.get("website"):
                links.append(f"Website: {profile['website']}")
            if profile.get("phone"):
                links.append(f"Phone: {profile['phone']}")

            if links:
                sig_parts.extend(["", "---"] + links)

        return "\n".join(sig_parts)

    # ============= HELPER METHODS =============

    def _replace_placeholders(self, template: str, context: Dict) -> str:
        """Replace placeholders in template with actual values."""
        replacements = {
            "{company_name}": context["company"].get("name", "the company"),
            "{position}": context.get("application", {}).get("position", "the position"),
            "{candidate_name}": context["candidate"].get("name", ""),
            "{candidate_first_name}": context["candidate"].get("name", "").split()[0] if context["candidate"].get("name") else "",
            "{original_subject}": context["original_email"].get("subject", "my previous email"),
            "{industry}": context["company"].get("industry", "your industry"),
        }

        result = template
        for placeholder, value in replacements.items():
            result = result.replace(placeholder, str(value))

        # Validate no unreplaced placeholders remain (prevents template injection)
        remaining_placeholders = re.findall(r'\{[a-zA-Z_]+\}', result)
        if remaining_placeholders:
            # Log warning and replace remaining placeholders with empty string
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"[FollowUpEmailGenerator] Unreplaced placeholders found: {remaining_placeholders}")
            for placeholder in remaining_placeholders:
                result = result.replace(placeholder, "")

        return result

    def _convert_to_html(self, text: str, context: Dict) -> str:
        """Convert plain text to HTML email."""
        # Split into paragraphs
        paragraphs = text.split("\n\n")
        html_parts = []

        for p in paragraphs:
            if p.strip():
                # Convert line breaks
                p = p.replace("\n", "<br>")
                # Convert URLs to links
                p = re.sub(
                    r'(https?://[^\s]+)',
                    r'<a href="\1" style="color: #3b82f6;">\1</a>',
                    p
                )
                html_parts.append(f"<p style='margin: 0 0 15px 0; line-height: 1.6;'>{p}</p>")

        body_html = "\n".join(html_parts)

        return f"""
        <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; font-size: 14px; color: #333; max-width: 600px;">
            {body_html}
        </div>
        """

    def _calculate_days_since_original(self, context: Dict) -> int:
        """Calculate days since original email was sent."""
        sent_at = context["original_email"].get("sent_at")
        if sent_at:
            try:
                sent_date = datetime.fromisoformat(sent_at.replace("Z", "+00:00"))
                return (datetime.now(timezone.utc) - sent_date.replace(tzinfo=None)).days
            except (ValueError, AttributeError, TypeError) as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.debug(f"[FollowUpEmailGenerator] Could not parse sent_at '{sent_at}': {e}")
        return 3  # Default

    def _generate_skill_highlight(self, skills: set) -> str:
        """Generate a skill highlight statement."""
        highlights = [
            "developed high-performance solutions",
            "built scalable systems",
            "delivered impactful projects",
            "solved complex technical challenges"
        ]
        return random.choice(highlights)

    def _get_personalization_data(self, context: Dict, step: FollowUpStep) -> Dict:
        """Get personalization tracking data."""
        profile = context["candidate"].get("profile", {})

        return {
            "placeholders_replaced": [
                k for k in ["company_name", "position", "candidate_name"]
                if context.get("company", {}).get("name") or context.get("application", {}).get("position")
            ],
            "links_included": {
                "linkedin": bool(profile.get("linkedin")),
                "github": bool(profile.get("github")),
                "website": bool(profile.get("website")),
                "portfolio": bool(profile.get("portfolio"))
            },
            "portfolio_mentioned": step.include_portfolio_link and bool(profile.get("portfolio_projects")),
            "skills_highlighted": context["candidate"].get("skills", [])[:5],
            "company_reference": bool(context.get("research", {}).get("about")),
            "strategy": step.strategy.value if step.strategy else None,
            "tone": step.tone.value if step.tone else None
        }

    # ============= PREVIEW GENERATION =============

    async def generate_all_previews(
        self,
        campaign: FollowUpCampaign
    ) -> List[Dict[str, Any]]:
        """
        Generate previews for all steps in a sequence.
        Used for auto-mode approval screen.
        """
        logger.info(f"[EmailGenerator] Generating all previews for campaign {campaign.id}")

        if not campaign.sequence:
            logger.warning(f"[EmailGenerator] Campaign {campaign.id} has no sequence")
            return []

        # Access steps - should be eager loaded by caller to avoid N+1
        steps = campaign.sequence.steps if campaign.sequence.steps else []
        logger.debug(f"[EmailGenerator] Processing {len(steps)} steps")

        context = await self._gather_context(campaign)
        previews = []

        for step in steps:
            if not step.is_active:
                continue

            subject = self._generate_subject(step, context, step.step_number)
            body_text, body_html = self._generate_body(step, context, step.step_number)

            previews.append({
                "step_number": step.step_number,
                "delay_days": step.delay_days,
                "strategy": step.strategy.value,
                "tone": step.tone.value,
                "subject": subject,
                "body_text": body_text,
                "body_html": body_html,
                "scheduled_date": self._calculate_send_date(campaign, step.step_number, step.delay_days),
                "personalization": self._get_personalization_data(context, step)
            })

        logger.info(f"[EmailGenerator] Generated {len(previews)} previews for campaign {campaign.id}")
        return previews

    def _calculate_send_date(
        self,
        campaign: FollowUpCampaign,
        step_number: int,
        delay_days: int
    ) -> str:
        """Calculate when this step would be sent."""
        from datetime import timedelta

        base_date = campaign.last_sent_date or campaign.created_at or datetime.now(timezone.utc)

        # Sum delays for all previous steps
        if campaign.sequence:
            total_delay = sum(
                s.delay_days for s in campaign.sequence.steps
                if s.step_number < step_number
            ) + delay_days
        else:
            total_delay = delay_days

        send_date = base_date + timedelta(days=total_delay)
        return send_date.isoformat()
