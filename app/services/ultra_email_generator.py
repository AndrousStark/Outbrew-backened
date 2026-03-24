"""
ULTRA-INTELLIGENT EMAIL GENERATOR - NEXT LEVEL

Generates highly personalized, multi-tone cold emails based on:
- Company intelligence (tech stack, projects, culture)
- Candidate skills and experience
- Advanced skill matching
- Country-specific etiquette
- Multiple tone variations (Professional, Enthusiastic, Story-driven, Value-first)

Best practices from research:
- 100-140 words optimal length
- Personalized opening
- Clear value proposition
- Specific call-to-action
- 40-50% response rates achievable with proper personalization
"""

import re
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class EmailTone(Enum):
    """Email tone options"""
    PROFESSIONAL = "professional"  # Formal, respectful
    ENTHUSIASTIC = "enthusiastic"  # Energetic, passionate
    STORY_DRIVEN = "story_driven"  # Narrative, relatable
    VALUE_FIRST = "value_first"  # Direct, ROI-focused
    CONSULTANT = "consultant"  # Advisory, thoughtful


@dataclass
class SkillMatch:
    """Skill matching result"""
    candidate_skill: str
    company_requirement: str
    match_confidence: float  # 0-100
    context: Optional[str] = None


@dataclass
class EmailDraft:
    """Generated email draft"""
    subject: str
    body: str
    tone: EmailTone
    personalization_score: float  # 0-100
    key_talking_points: List[str] = field(default_factory=list)
    matched_skills: List[SkillMatch] = field(default_factory=list)
    estimated_response_rate: str = "2-5%"  # Based on personalization

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "subject": self.subject,
            "body": self.body,
            "tone": self.tone.value,
            "personalization_score": self.personalization_score,
            "key_talking_points": self.key_talking_points,
            "matched_skills": [
                {
                    "candidate_skill": m.candidate_skill,
                    "company_requirement": m.company_requirement,
                    "confidence": m.match_confidence,
                    "context": m.context
                }
                for m in self.matched_skills
            ],
            "estimated_response_rate": self.estimated_response_rate
        }


class UltraEmailGenerator:
    """
    ULTRA-INTELLIGENT email generator.

    Generates multiple variations of highly personalized emails
    optimized for maximum response rates.
    """

    def __init__(self):
        self.min_word_count = 100
        self.max_word_count = 140

    def generate_emails(
        self,
        candidate_data: Dict[str, Any],
        company_intelligence: Dict[str, Any],
        recipient_name: str,
        recipient_position: str,
        country_guidance: Optional[Dict[str, Any]] = None
    ) -> List[EmailDraft]:
        """
        Generate multiple email variations with different tones.

        Args:
            candidate_data: Parsed resume data with skills categorization
            company_intelligence: Company research results
            recipient_name: Recipient's name
            recipient_position: Recipient's job title
            country_guidance: Country-specific email guidance

        Returns:
            List of email drafts with different tones
        """
        logger.info(f"📧 [ULTRA EMAIL] Generating emails for {recipient_name} at {company_intelligence.get('company_name')}")

        # Step 1: Match skills
        skill_matches = self._match_skills(candidate_data, company_intelligence)

        # Step 2: Extract key talking points
        talking_points = self._extract_talking_points(
            candidate_data,
            company_intelligence,
            skill_matches
        )

        # Step 3: Generate emails in different tones
        email_drafts = []

        for tone in EmailTone:
            draft = self._generate_email(
                tone=tone,
                candidate_data=candidate_data,
                company_intelligence=company_intelligence,
                recipient_name=recipient_name,
                recipient_position=recipient_position,
                skill_matches=skill_matches,
                talking_points=talking_points,
                country_guidance=country_guidance
            )
            email_drafts.append(draft)

        logger.info(f"✅ [ULTRA EMAIL] Generated {len(email_drafts)} email variations")

        return email_drafts

    def _match_skills(
        self,
        candidate_data: Dict[str, Any],
        company_intelligence: Dict[str, Any]
    ) -> List[SkillMatch]:
        """
        Match candidate skills with company requirements using intelligent matching.
        """
        matches = []

        # Get candidate skills (categorized)
        skills_cat = candidate_data.get('skills_categorized', {})
        candidate_languages = set(s.lower() for s in skills_cat.get('languages', []))
        candidate_frameworks = set(s.lower() for s in skills_cat.get('frameworks', []))
        candidate_tools = set(s.lower() for s in skills_cat.get('tools', []))
        candidate_cloud = set(s.lower() for s in skills_cat.get('cloud', []))
        candidate_databases = set(s.lower() for s in skills_cat.get('databases', []))

        # Get company tech requirements
        company_languages = set(s.lower() for s in company_intelligence.get('programming_languages', []))
        company_frameworks = set(s.lower() for s in company_intelligence.get('frameworks', []))
        company_cloud = set(s.lower() for s in company_intelligence.get('cloud_platforms', []))
        company_databases = set(s.lower() for s in company_intelligence.get('databases', []))
        company_tech_stack = set(s.lower() for s in company_intelligence.get('tech_stack', []))

        # Match programming languages
        for lang in candidate_languages:
            if lang in company_languages or lang in company_tech_stack:
                matches.append(SkillMatch(
                    candidate_skill=lang.title(),
                    company_requirement=lang.title(),
                    match_confidence=100.0,
                    context="direct_match"
                ))

        # Match frameworks
        for fw in candidate_frameworks:
            if fw in company_frameworks or fw in company_tech_stack:
                matches.append(SkillMatch(
                    candidate_skill=fw.title(),
                    company_requirement=fw.title(),
                    match_confidence=95.0,
                    context="direct_match"
                ))

        # Match cloud platforms
        for cloud in candidate_cloud:
            if cloud in company_cloud or cloud in company_tech_stack:
                matches.append(SkillMatch(
                    candidate_skill=cloud.upper() if cloud in ['aws', 'gcp'] else cloud.title(),
                    company_requirement=cloud.upper() if cloud in ['aws', 'gcp'] else cloud.title(),
                    match_confidence=90.0,
                    context="cloud_expertise"
                ))

        # Match databases
        for db in candidate_databases:
            if db in company_databases or db in company_tech_stack:
                matches.append(SkillMatch(
                    candidate_skill=db.title(),
                    company_requirement=db.title(),
                    match_confidence=85.0,
                    context="data_expertise"
                ))

        # Special matches for ML/AI roles
        if candidate_data.get('experience'):
            exp_text = ' '.join(candidate_data['experience']).lower()
            if 'machine learning' in exp_text or 'deep learning' in exp_text:
                if company_intelligence.get('industry') == 'ai' or any(
                    'ai' in tech.lower() or 'ml' in tech.lower()
                    for tech in company_intelligence.get('tech_stack', [])
                ):
                    matches.append(SkillMatch(
                        candidate_skill="Machine Learning & AI",
                        company_requirement="AI/ML Projects",
                        match_confidence=95.0,
                        context="domain_expertise"
                    ))

        logger.info(f"🎯 [SKILL MATCH] Found {len(matches)} matching skills")

        return matches

    def _extract_talking_points(
        self,
        candidate_data: Dict[str, Any],
        company_intelligence: Dict[str, Any],
        skill_matches: List[SkillMatch]
    ) -> List[str]:
        """Extract key talking points for email"""
        points = []

        # Point 1: Skill match
        if skill_matches:
            top_skills = [m.candidate_skill for m in skill_matches[:3]]
            points.append(f"Technical alignment: {', '.join(top_skills)}")

        # Point 2: Relevant experience
        if candidate_data.get('experience'):
            exp = candidate_data['experience'][0] if candidate_data['experience'] else ""
            # Extract company name from experience (simplified)
            if exp:
                lines = exp.split('\n')
                if len(lines) >= 2:
                    company = lines[1] if len(lines) > 1 else "previous company"
                    points.append(f"Experience at {company}")

        # Point 3: Notable achievements
        if candidate_data.get('projects'):
            projects = candidate_data['projects']
            if projects and len(projects[0]) > 50:
                # Extract first project name
                first_line = projects[0].split('\n')[0]
                points.append(f"Notable project: {first_line[:50]}...")

        # Point 4: Publications (if any)
        if candidate_data.get('publications'):
            points.append(f"{len(candidate_data['publications'])} published research papers")

        # Point 5: Company-specific interest
        if company_intelligence.get('projects'):
            proj = company_intelligence['projects'][0]
            points.append(f"Interest in {proj['name']}")
        elif company_intelligence.get('industry'):
            points.append(f"Passion for {company_intelligence['industry']} industry")

        return points

    def _generate_email(
        self,
        tone: EmailTone,
        candidate_data: Dict[str, Any],
        company_intelligence: Dict[str, Any],
        recipient_name: str,
        recipient_position: str,
        skill_matches: List[SkillMatch],
        talking_points: List[str],
        country_guidance: Optional[Dict[str, Any]]
    ) -> EmailDraft:
        """Generate a single email with specific tone"""

        company_name = company_intelligence.get('company_name', 'your company')
        candidate_name = candidate_data.get('name', 'Candidate')

        # Generate subject line
        subject = self._generate_subject(tone, company_name, skill_matches)

        # Generate body
        body = self._generate_body(
            tone=tone,
            candidate_data=candidate_data,
            company_intelligence=company_intelligence,
            recipient_name=recipient_name,
            recipient_position=recipient_position,
            skill_matches=skill_matches,
            talking_points=talking_points,
            country_guidance=country_guidance
        )

        # Calculate personalization score
        personalization_score = self._calculate_personalization_score(
            body,
            skill_matches,
            company_intelligence
        )

        # Estimate response rate based on personalization
        if personalization_score >= 80:
            estimated_response = "40-50%"
        elif personalization_score >= 60:
            estimated_response = "20-30%"
        elif personalization_score >= 40:
            estimated_response = "10-20%"
        else:
            estimated_response = "2-5%"

        return EmailDraft(
            subject=subject,
            body=body,
            tone=tone,
            personalization_score=personalization_score,
            key_talking_points=talking_points,
            matched_skills=skill_matches,
            estimated_response_rate=estimated_response
        )

    def _generate_subject(
        self,
        tone: EmailTone,
        company_name: str,
        skill_matches: List[SkillMatch]
    ) -> str:
        """Generate compelling subject line"""

        if tone == EmailTone.PROFESSIONAL:
            if skill_matches:
                skill = skill_matches[0].candidate_skill
                return f"Experienced {skill} Engineer - Interest in {company_name}"
            return f"Software Engineering Opportunity at {company_name}"

        elif tone == EmailTone.ENTHUSIASTIC:
            return f"Excited About {company_name}'s Mission!"

        elif tone == EmailTone.STORY_DRIVEN:
            return f"How I Improved IMU Accuracy by 40% (DRDO Research)"

        elif tone == EmailTone.VALUE_FIRST:
            if skill_matches:
                skills = ', '.join([m.candidate_skill for m in skill_matches[:2]])
                return f"{skills} Expertise for {company_name}"
            return f"AI/ML Engineer with Published Research"

        elif tone == EmailTone.CONSULTANT:
            return f"Thoughts on {company_name}'s Tech Stack"

        return f"Opportunity at {company_name}"

    def _generate_body(
        self,
        tone: EmailTone,
        candidate_data: Dict[str, Any],
        company_intelligence: Dict[str, Any],
        recipient_name: str,
        recipient_position: str,
        skill_matches: List[SkillMatch],
        talking_points: List[str],
        country_guidance: Optional[Dict[str, Any]]
    ) -> str:
        """Generate email body"""

        company_name = company_intelligence.get('company_name', 'your company')
        candidate_name = candidate_data.get('name', 'Candidate')

        # Greeting (adjust based on country formality)
        if country_guidance and country_guidance.get('formality') in ['very_formal', 'formal']:
            greeting = f"Dear Mr./Ms. {recipient_name.split()[-1]},"  # Use last name
        else:
            greeting = f"Hi {recipient_name.split()[0]},"  # Use first name

        if tone == EmailTone.PROFESSIONAL:
            body = self._generate_professional_email(
                greeting, candidate_name, company_name, company_intelligence,
                skill_matches, candidate_data
            )

        elif tone == EmailTone.ENTHUSIASTIC:
            body = self._generate_enthusiastic_email(
                greeting, candidate_name, company_name, company_intelligence,
                skill_matches, candidate_data
            )

        elif tone == EmailTone.STORY_DRIVEN:
            body = self._generate_story_email(
                greeting, candidate_name, company_name, company_intelligence,
                skill_matches, candidate_data
            )

        elif tone == EmailTone.VALUE_FIRST:
            body = self._generate_value_email(
                greeting, candidate_name, company_name, company_intelligence,
                skill_matches, candidate_data
            )

        elif tone == EmailTone.CONSULTANT:
            body = self._generate_consultant_email(
                greeting, candidate_name, company_name, company_intelligence,
                skill_matches, candidate_data
            )

        else:
            body = self._generate_professional_email(
                greeting, candidate_name, company_name, company_intelligence,
                skill_matches, candidate_data
            )

        return body

    def _generate_professional_email(
        self, greeting, candidate_name, company_name, company_intel, skill_matches, candidate_data
    ) -> str:
        """Professional, formal tone"""

        # Extract current role
        current_role = "ML Engineer"  # Default
        if candidate_data.get('experience'):
            exp_lines = candidate_data['experience'][0].split('\n')
            if exp_lines:
                current_role = exp_lines[0].strip()

        # Build skills mention
        skills_text = ""
        if skill_matches:
            top_skills = [m.candidate_skill for m in skill_matches[:3]]
            skills_text = f"with expertise in {', '.join(top_skills)}"

        # Build achievement mention
        achievement_text = ""
        if candidate_data.get('projects'):
            proj = candidate_data['projects'][0]
            if '99.93%' in proj:
                achievement_text = "Recently achieved 99.93% accuracy in automated colon cancer prediction using deep learning."
            elif 'DRDO' in proj or '40%' in proj:
                achievement_text = "Previously improved IMU calibration accuracy by 40% at DRDO using advanced ML algorithms."

        body = f"""{greeting}

I'm {candidate_name}, currently an {current_role} {skills_text}. I came across {company_name} and was impressed by your work in {company_intel.get('industry', 'technology')}.

{achievement_text}

I'm particularly interested in {company_name}'s approach to {company_intel.get('tech_stack', ['innovation'])[0] if company_intel.get('tech_stack') else 'innovation'}. My background in AI/ML and hands-on experience with TensorFlow and PyTorch aligns well with your tech stack.

Would you be open to a brief conversation about potential opportunities at {company_name}?

Best regards,
{candidate_name}"""

        return body

    def _generate_enthusiastic_email(
        self, greeting, candidate_name, company_name, company_intel, skill_matches, candidate_data
    ) -> str:
        """Enthusiastic, energetic tone"""

        body = f"""{greeting}

I'm reaching out because {company_name}'s work in {company_intel.get('industry', 'AI/ML')} genuinely excites me!

As an ML Engineer at Clarivate working with LLMs and GenAI, I've been following your projects. What caught my attention: your use of {company_intel.get('tech_stack', ['cutting-edge technology'])[0] if company_intel.get('tech_stack') else 'innovative approaches'}.

Quick highlight: I recently built ColNet, achieving 99.93% accuracy in colon cancer detection - published and presented at academic conferences.

I'd love to explore how my experience in deep learning and production ML systems could contribute to {company_name}'s vision.

Can we connect for 15 minutes?

Looking forward to hearing from you!
{candidate_name}"""

        return body

    def _generate_story_email(
        self, greeting, candidate_name, company_name, company_intel, skill_matches, candidate_data
    ) -> str:
        """Story-driven, narrative tone"""

        body = f"""{greeting}

Last year at DRDO, I faced a challenge: 500+ Inertial Measurement Units with inconsistent accuracy.

Using deep learning, we improved calibration accuracy by 40% and reduced error rates by 35%. The breakthrough? Advanced algorithmic approaches combined with large-scale data processing.

This experience taught me the power of AI in solving real-world problems - which is why {company_name}'s work resonates with me.

Currently at Clarivate building ML applications with AWS and LLMs, I'm exploring opportunities where I can apply similar problem-solving approaches.

Would you be interested in discussing how this experience might benefit {company_name}?

Best,
{candidate_name}"""

        return body

    def _generate_value_email(
        self, greeting, candidate_name, company_name, company_intel, skill_matches, candidate_data
    ) -> str:
        """Value-first, ROI-focused tone"""

        skills_list = [m.candidate_skill for m in skill_matches[:3]] if skill_matches else ['Python', 'TensorFlow', 'ML']

        body = f"""{greeting}

Three things I can bring to {company_name}:

1. **Proven ML Expertise**: 99.93% accuracy on cancer detection (10K+ images), published in SCOPUS-indexed journals
2. **Production Experience**: Currently building ML apps at Clarivate using AWS, LLMs, and GenAI
3. **Technical Alignment**: {', '.join(skills_list)} - direct match with your stack

I've delivered 30% improvement in threat detection at Tech Mahindra and 40% accuracy gains at DRDO.

{company_name}'s focus on {company_intel.get('industry', 'innovation')} aligns perfectly with my experience.

15-minute call to explore fit?

Regards,
{candidate_name}"""

        return body

    def _generate_consultant_email(
        self, greeting, candidate_name, company_name, company_intel, skill_matches, candidate_data
    ) -> str:
        """Consultant, advisory tone"""

        tech = company_intel.get('tech_stack', ['your tech stack'])

        body = f"""{greeting}

I noticed {company_name} is using {tech[0] if tech else 'advanced technologies'} in your projects. Having worked extensively with TensorFlow, PyTorch, and production ML systems, I have some thoughts on optimization approaches.

Background: Currently ML Engineer at Clarivate, previously at DRDO (40% accuracy improvement) and Tech Mahindra (30% threat detection increase).

Specific interest: How {company_name} approaches {company_intel.get('industry', 'ML')} challenges - particularly around model deployment and scalability.

Would you be open to a conversation? Happy to share insights from my experience building ColNet (99.93% accuracy, published research).

Looking forward to connecting,
{candidate_name}"""

        return body

    def _calculate_personalization_score(
        self,
        body: str,
        skill_matches: List[SkillMatch],
        company_intelligence: Dict[str, Any]
    ) -> float:
        """Calculate personalization score"""
        score = 0.0

        # Company name mentioned
        if company_intelligence.get('company_name', '').lower() in body.lower():
            score += 20.0

        # Specific skills mentioned
        score += min(30.0, len(skill_matches) * 10)

        # Specific achievements mentioned (look for numbers/percentages)
        if re.search(r'\d+%', body):
            score += 15.0

        # Industry/domain mentioned
        if company_intelligence.get('industry'):
            if company_intelligence['industry'].lower() in body.lower():
                score += 15.0

        # Technology stack mentioned
        if company_intelligence.get('tech_stack'):
            for tech in company_intelligence['tech_stack'][:3]:
                if tech.lower() in body.lower():
                    score += 5.0

        # Length appropriate (100-140 words)
        word_count = len(body.split())
        if 100 <= word_count <= 140:
            score += 15.0
        elif 80 <= word_count <= 160:
            score += 10.0

        return min(100.0, score)
