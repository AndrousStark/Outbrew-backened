"""Company Model"""
from sqlalchemy import Column, Integer, String, Text, JSON, DateTime, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class Company(Base):
    __tablename__ = "companies"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, index=True, nullable=False)
    domain = Column(String(255))
    industry = Column(String(255))
    description = Column(Text)

    # Location
    headquarters_country = Column(String(100), index=True)
    headquarters_city = Column(String(255))
    primary_language = Column(String(50))  # Primary business language

    # Technical
    tech_stack = Column(JSON)
    company_size = Column(String(50))

    # Links
    website_url = Column(String(500))
    linkedin_url = Column(String(500))
    careers_url = Column(String(500))

    # Alignment (cached)
    alignment_pragya_text = Column(Text)
    alignment_pragya_score = Column(Float, default=0.0)
    alignment_aniruddh_text = Column(Text)
    alignment_aniruddh_score = Column(Float, default=0.0)

    # Job postings (cached)
    job_postings_pragya = Column(JSON)
    job_postings_aniruddh = Column(JSON)

    # Research metadata
    last_researched_at = Column(DateTime(timezone=True))
    research_source = Column(String(100))

    # Stats
    total_applications = Column(Integer, default=0)
    total_responses = Column(Integer, default=0)

    # Soft delete
    deleted_at = Column(DateTime(timezone=True), nullable=True, index=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    applications = relationship("Application", back_populates="company", cascade="all, delete-orphan")

    # Company Intelligence relationships
    projects = relationship("CompanyProject", back_populates="company", cascade="all, delete-orphan")
    research_cache = relationship("CompanyResearchCache", back_populates="company", uselist=False, cascade="all, delete-orphan")
    skill_matches = relationship("SkillMatch", back_populates="company", cascade="all, delete-orphan")
    email_drafts = relationship("PersonalizedEmailDraft", back_populates="company", cascade="all, delete-orphan")
