"""
Document Models for Resumes and Company Info Docs

Stores parsed data from:
- Resumes (for job applications)
- Company/Service Info Docs (for marketing/sales)
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum

from app.core.database import Base


class ParsedResume(Base):
    """Parsed resume data with skills, experience, education, etc."""
    __tablename__ = "parsed_resumes"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)
    resume_version_id = Column(Integer, ForeignKey("resume_versions.id", ondelete="CASCADE"), nullable=True, index=True)

    # Basic Info
    name = Column(String(255))
    email = Column(String(255))
    phone = Column(String(50))
    location = Column(String(255))
    linkedin_url = Column(String(500))
    github_url = Column(String(500))
    portfolio_url = Column(String(500))

    # Summary
    professional_summary = Column(Text)
    years_of_experience = Column(Float)

    # Skills (JSON arrays)
    technical_skills = Column(JSON)  # ["Python", "React", "PostgreSQL"]
    soft_skills = Column(JSON)  # ["Leadership", "Communication"]
    languages_spoken = Column(JSON)  # [{"language": "English", "proficiency": "Native"}]
    certifications = Column(JSON)  # [{"name": "AWS Certified", "year": 2023}]

    # Experience (JSON array of objects)
    work_experience = Column(JSON)
    # [
    #   {
    #     "company": "Google",
    #     "position": "Software Engineer",
    #     "start_date": "2020-01",
    #     "end_date": "2023-05",
    #     "description": "Built scalable systems",
    #     "achievements": ["Improved performance by 40%"]
    #   }
    # ]

    # Education (JSON array)
    education = Column(JSON)
    # [
    #   {
    #     "institution": "MIT",
    #     "degree": "BS Computer Science",
    #     "graduation_year": 2020,
    #     "gpa": 3.9
    #   }
    # ]

    # Projects (JSON array)
    projects = Column(JSON)
    # [
    #   {
    #     "name": "AI Chatbot",
    #     "description": "Built using GPT-3",
    #     "technologies": ["Python", "FastAPI", "React"],
    #     "url": "https://github.com/..."
    #   }
    # ]

    # Publications & Patents
    publications = Column(JSON)
    patents = Column(JSON)

    # Achievements & Awards
    achievements = Column(JSON)
    awards = Column(JSON)

    # Metadata
    parsing_confidence_score = Column(Float, default=0.0)  # 0-100
    total_pages = Column(Integer)
    word_count = Column(Integer)
    parsed_at = Column(DateTime(timezone=True), server_default=func.now())
    last_updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    candidate = relationship("Candidate", backref="parsed_resumes")
    resume_version = relationship("ResumeVersion", backref="parsed_data")


class CompanyInfoDoc(Base):
    """Company/Service information documents for marketing/sales"""
    __tablename__ = "company_info_docs"

    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False, index=True)

    # Document Info
    name = Column(String(255), nullable=False)  # e.g., "SaaS Marketing Deck", "Consulting Services"
    description = Column(Text)
    doc_type = Column(String(50))  # "product", "service", "company", "portfolio"

    # File Info
    filename = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=False)
    file_size = Column(Integer)  # bytes

    # Company/Service Details (extracted)
    company_name = Column(String(255))
    tagline = Column(String(500))
    industry = Column(String(255))
    target_market = Column(String(255))

    # Products/Services (JSON array)
    products_services = Column(JSON)
    # [
    #   {
    #     "name": "AI Platform",
    #     "description": "Enterprise AI solutions",
    #     "pricing": "Starting at $99/mo",
    #     "features": ["ML models", "API access", "24/7 support"]
    #   }
    # ]

    # Value Propositions
    key_benefits = Column(JSON)  # ["Reduce costs by 40%", "Increase productivity"]
    unique_selling_points = Column(JSON)  # ["Only platform with X", "10x faster"]
    problem_solved = Column(Text)  # What problem does it solve

    # Target Customers
    ideal_customer_profile = Column(JSON)
    # {
    #   "company_size": "50-500 employees",
    #   "industries": ["Tech", "Finance"],
    #   "pain_points": ["Manual processes", "High costs"]
    # }

    # Case Studies & Testimonials
    case_studies = Column(JSON)
    testimonials = Column(JSON)
    client_logos = Column(JSON)  # List of client companies

    # Pricing & Plans
    pricing_tiers = Column(JSON)
    # [
    #   {"name": "Starter", "price": "$99/mo", "features": [...]},
    #   {"name": "Pro", "price": "$299/mo", "features": [...]}
    # ]

    # Contact & Team
    contact_info = Column(JSON)
    # {
    #   "email": "sales@company.com",
    #   "phone": "+1-555-1234",
    #   "website": "https://company.com"
    # }
    team_members = Column(JSON)  # Key team members to mention

    # Competitive Advantages
    competitors = Column(JSON)  # List of competitors
    differentiators = Column(JSON)  # How we're different/better

    # Metadata
    parsing_confidence_score = Column(Float, default=0.0)  # 0-100
    total_pages = Column(Integer)
    word_count = Column(Integer)
    is_active = Column(Boolean, default=True)
    is_default = Column(Boolean, default=False)

    # Usage tracking
    times_used = Column(Integer, default=0)
    last_used_at = Column(DateTime(timezone=True))

    # Timestamps
    parsed_at = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    candidate = relationship("Candidate", backref="company_info_docs")
