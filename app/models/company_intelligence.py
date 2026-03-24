"""Company Intelligence Models - Smart Company Research & Skill Matching"""
from sqlalchemy import Column, Integer, String, Text, Float, Boolean, DateTime, JSON, ForeignKey, Enum as SQLEnum
from sqlalchemy.orm import relationship
from datetime import datetime
from enum import Enum
from app.core.database import Base


class MatchStrengthEnum(str, Enum):
    """Skill match strength levels"""
    PERFECT = "perfect"      # 90-100% match
    STRONG = "strong"        # 70-89% match
    MODERATE = "moderate"    # 50-69% match
    WEAK = "weak"            # 30-49% match
    MINIMAL = "minimal"      # <30% match


class ResearchDepthEnum(str, Enum):
    """Research depth levels"""
    QUICK = "quick"          # Basic info only
    STANDARD = "standard"    # Normal research
    DEEP = "deep"            # Comprehensive research
    EXHAUSTIVE = "exhaustive" # Maximum depth


class EmailToneEnum(str, Enum):
    """Email tone options"""
    PROFESSIONAL = "professional"
    FRIENDLY = "friendly"
    ENTHUSIASTIC = "enthusiastic"
    FORMAL = "formal"
    CASUAL = "casual"
    STORY_DRIVEN = "story_driven"
    VALUE_FIRST = "value_first"
    CONSULTANT = "consultant"


class ProjectTypeEnum(str, Enum):
    """Company project types"""
    PRODUCT = "product"
    SERVICE = "service"
    OPEN_SOURCE = "open_source"
    INTERNAL = "internal"
    CLIENT_WORK = "client_work"
    RESEARCH = "research"


# Skill Categories for matching
SKILL_CATEGORIES = {
    "programming_languages": {
        "name": "Programming Languages",
        "weight": 0.25,
        "keywords": ["python", "javascript", "typescript", "java", "c++", "c#", "go", "rust",
                     "ruby", "php", "swift", "kotlin", "scala", "r", "matlab", "perl"]
    },
    "frameworks": {
        "name": "Frameworks & Libraries",
        "weight": 0.20,
        "keywords": ["react", "angular", "vue", "django", "flask", "fastapi", "spring",
                     "express", "nextjs", "rails", "laravel", "tensorflow", "pytorch"]
    },
    "databases": {
        "name": "Databases",
        "weight": 0.15,
        "keywords": ["postgresql", "mysql", "mongodb", "redis", "elasticsearch", "cassandra",
                     "dynamodb", "sqlite", "oracle", "sql server", "neo4j", "firebase"]
    },
    "cloud_devops": {
        "name": "Cloud & DevOps",
        "weight": 0.15,
        "keywords": ["aws", "azure", "gcp", "docker", "kubernetes", "terraform", "jenkins",
                     "github actions", "gitlab ci", "ansible", "prometheus", "grafana"]
    },
    "tools": {
        "name": "Tools & Technologies",
        "weight": 0.10,
        "keywords": ["git", "jira", "confluence", "figma", "postman", "swagger", "nginx",
                     "apache", "linux", "windows server", "agile", "scrum"]
    },
    "soft_skills": {
        "name": "Soft Skills",
        "weight": 0.10,
        "keywords": ["leadership", "communication", "teamwork", "problem solving",
                     "project management", "mentoring", "presentation", "analytical"]
    },
    "domain_knowledge": {
        "name": "Domain Knowledge",
        "weight": 0.05,
        "keywords": ["fintech", "healthcare", "ecommerce", "saas", "ai/ml", "blockchain",
                     "iot", "cybersecurity", "data science", "mobile", "web"]
    }
}

# Email template components
EMAIL_TEMPLATES = {
    "opening_lines": {
        "professional": [
            "I hope this email finds you well.",
            "I recently came across {company_name}'s impressive work and felt compelled to reach out.",
            "After researching {company_name}'s innovative projects, I wanted to connect.",
        ],
        "friendly": [
            "I've been following {company_name}'s work and I'm genuinely impressed!",
            "Your company's approach to {domain} caught my attention.",
            "I'm excited to reach out after discovering {company_name}'s amazing work.",
        ],
        "enthusiastic": [
            "I'm thrilled to connect with {company_name}!",
            "Your groundbreaking work in {domain} has truly inspired me!",
            "I couldn't wait to reach out after seeing {company_name}'s incredible projects!",
        ]
    },
    "skill_highlight_formats": {
        "direct": "My expertise in {skills} directly aligns with your {project} project.",
        "experience": "With {years} years of experience in {skills}, I can contribute to {project}.",
        "achievement": "I've successfully delivered {achievement} using {skills}, which relates to your {project}.",
        "specific": "My work on {candidate_project} using {skills} demonstrates my ability to tackle challenges like {project}."
    },
    "closing_lines": {
        "professional": [
            "I would welcome the opportunity to discuss how I can contribute to {company_name}'s success.",
            "I look forward to the possibility of contributing to your team.",
            "Thank you for considering my application. I'm eager to discuss this opportunity further.",
        ],
        "friendly": [
            "I'd love to chat about how we might work together!",
            "Looking forward to potentially joining the {company_name} team!",
            "Can't wait to hear from you!",
        ],
        "call_to_action": [
            "Would you be available for a brief call this week?",
            "I'd appreciate 15 minutes of your time to discuss this further.",
            "When might be a good time for a quick conversation?",
        ]
    }
}


class CompanyProject(Base):
    """Discovered company projects from research"""
    __tablename__ = "company_projects"

    id = Column(Integer, primary_key=True, index=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True)

    # Project details
    name = Column(String(255), nullable=False)
    description = Column(Text)
    project_type = Column(SQLEnum(ProjectTypeEnum), default=ProjectTypeEnum.PRODUCT)
    url = Column(String(500))

    # Technical details
    technologies = Column(JSON, default=list)  # List of technologies used
    skills_required = Column(JSON, default=list)  # Skills identified as needed

    # Status and dates
    is_active = Column(Boolean, default=True)
    discovered_at = Column(DateTime, default=datetime.utcnow)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Source tracking
    source_url = Column(String(500))
    confidence_score = Column(Float, default=0.5)  # How confident we are about this info

    # Relationship
    company = relationship("Company", back_populates="projects")
    skill_matches = relationship("SkillMatch", back_populates="project", cascade="all, delete-orphan")


class CompanyResearchCache(Base):
    """Cache for company research results"""
    __tablename__ = "company_research_cache"

    id = Column(Integer, primary_key=True, index=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True)

    # Research results
    research_depth = Column(SQLEnum(ResearchDepthEnum), default=ResearchDepthEnum.STANDARD)

    # Extracted data
    about_summary = Column(Text)  # Company summary
    mission_statement = Column(Text)
    company_culture = Column(JSON, default=dict)  # Culture keywords and values
    recent_news = Column(JSON, default=list)  # Recent news/updates
    job_openings = Column(JSON, default=list)  # Active job positions
    key_people = Column(JSON, default=list)  # Key team members
    funding_info = Column(JSON, default=dict)  # Funding rounds, investors
    competitors = Column(JSON, default=list)  # Known competitors

    # Technical intelligence
    tech_stack_detailed = Column(JSON, default=dict)  # Detailed tech breakdown
    github_repos = Column(JSON, default=list)  # Public repos
    blog_posts = Column(JSON, default=list)  # Technical blog posts
    patents = Column(JSON, default=list)  # Patents/IP

    # Social presence
    social_links = Column(JSON, default=dict)  # Twitter, LinkedIn, etc.
    employee_count_estimate = Column(Integer)
    growth_signals = Column(JSON, default=list)  # Hiring trends, expansion

    # Cache management
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)
    last_refreshed = Column(DateTime, default=datetime.utcnow)

    # Quality metrics
    completeness_score = Column(Float, default=0.0)  # How complete is the data
    data_sources = Column(JSON, default=list)  # Where data came from

    # Relationship
    company = relationship("Company", back_populates="research_cache")


class SkillMatch(Base):
    """Skill matches between candidate and company projects"""
    __tablename__ = "skill_matches"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True)
    project_id = Column(Integer, ForeignKey("company_projects.id", ondelete="SET NULL"), nullable=True)

    # Match details
    match_strength = Column(SQLEnum(MatchStrengthEnum), default=MatchStrengthEnum.MODERATE)
    overall_score = Column(Float, default=0.0)  # 0-100 score

    # Detailed breakdown
    matched_skills = Column(JSON, default=list)  # Skills that matched
    candidate_skills_used = Column(JSON, default=list)  # Candidate skills used
    company_needs = Column(JSON, default=list)  # Company skills needed

    # Category scores
    category_scores = Column(JSON, default=dict)  # Score per skill category

    # Context
    match_context = Column(Text)  # Why this is a good match
    talking_points = Column(JSON, default=list)  # Key points for email

    # Timestamps
    calculated_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)

    # Relationships
    candidate = relationship("Candidate", back_populates="skill_matches")
    company = relationship("Company", back_populates="skill_matches")
    project = relationship("CompanyProject", back_populates="skill_matches")
    email_drafts = relationship("PersonalizedEmailDraft", back_populates="skill_match", cascade="all, delete-orphan")


class PersonalizedEmailDraft(Base):
    """AI-generated personalized email drafts"""
    __tablename__ = "personalized_email_drafts"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True)
    skill_match_id = Column(Integer, ForeignKey("skill_matches.id", ondelete="SET NULL"), nullable=True)

    # Email content
    subject_line = Column(String(255), nullable=False)
    subject_alternatives = Column(JSON, default=list)  # Alternative subject lines

    email_body = Column(Text, nullable=False)
    email_html = Column(Text)  # HTML formatted version

    # Email structure
    opening = Column(Text)
    skill_highlights = Column(Text)
    company_specific = Column(Text)
    call_to_action = Column(Text)
    closing = Column(Text)

    # Customization
    tone = Column(SQLEnum(EmailToneEnum), default=EmailToneEnum.PROFESSIONAL)
    personalization_level = Column(Float, default=0.7)  # 0-1 how personalized

    # Quality metrics
    confidence_score = Column(Float, default=0.0)  # AI confidence
    relevance_score = Column(Float, default=0.0)  # How relevant to company

    # Tracking
    is_favorite = Column(Boolean, default=False)
    is_used = Column(Boolean, default=False)
    used_at = Column(DateTime)

    # Metadata
    generation_params = Column(JSON, default=dict)  # Parameters used to generate
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    candidate = relationship("Candidate", back_populates="email_drafts")
    company = relationship("Company", back_populates="email_drafts")
    skill_match = relationship("SkillMatch", back_populates="email_drafts")


class CandidateSkillProfile(Base):
    """Parsed and categorized candidate skills"""
    __tablename__ = "candidate_skill_profiles"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, unique=True)

    # Categorized skills
    programming_languages = Column(JSON, default=list)
    frameworks = Column(JSON, default=list)
    databases = Column(JSON, default=list)
    cloud_devops = Column(JSON, default=list)
    tools = Column(JSON, default=list)
    soft_skills = Column(JSON, default=list)
    domain_knowledge = Column(JSON, default=list)

    # Experience details
    projects = Column(JSON, default=list)  # {name, description, technologies, achievements}
    work_experience = Column(JSON, default=list)  # {company, role, duration, highlights, technologies}
    education = Column(JSON, default=list)  # {institution, degree, field, achievements}
    achievements = Column(JSON, default=list)  # Notable achievements
    certifications = Column(JSON, default=list)

    # Skill levels
    skill_levels = Column(JSON, default=dict)  # {skill: level (1-5)}

    # Summary
    primary_expertise = Column(JSON, default=list)  # Top 5 skills
    secondary_skills = Column(JSON, default=list)  # Supporting skills
    years_experience = Column(Float, default=0.0)

    # Profile quality
    completeness_score = Column(Float, default=0.0)
    last_analyzed = Column(DateTime, default=datetime.utcnow)

    # Source tracking
    source_resume_id = Column(Integer)  # Resume version used
    extraction_method = Column(String(50), default="auto")

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationship
    candidate = relationship("Candidate", back_populates="skill_profile")
