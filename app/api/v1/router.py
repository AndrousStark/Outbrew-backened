"""API v1 Router"""

from fastapi import APIRouter

from app.api.v1.endpoints import (
    auth,
    applications,
    # applications_async,  # Phase 2: Async optimized endpoints (disabled - file not found)
    health,  # Health check endpoints
    companies,
    analytics,
    dashboards,
    search,
    email_preview,
    resumes,
    email_templates,
    templates_unified,  # Unified templates + AI drafts endpoint
    intelligence,  # Company intelligence/research data
    users,
    email_warming,  # Email warmup features
    rate_limiting,  # Rate limiting controls
    notifications,
    send_time,  # Send time optimization
    warmup_health,  # Warmup health monitoring
    company_intelligence,  # Company intelligence features
    follow_up,  # Follow-up sequences
    email_inbox,  # Email inbox integration
    template_marketplace,  # Template marketplace
    attachments,  # Application attachments
    template_analytics,  # Template analytics
    scheduler_admin,  # Scheduler admin features
    recipients,  # Recipient Groups feature
    recipient_groups,  # Recipient Groups feature
    group_campaigns,  # Recipient Groups feature
    extraction,  # ULTRA PRO MAX EXTRACTION ENGINE
    documents,  # Resume & Info Doc Management
    mobiadz,  # TheMobiAdz Extraction Engine - App/Game/E-commerce
    enrichment,  # Enrichment execution and management
    warmup_pool,  # Advanced Email Warmup Pool System (Smartlead/Instantly competitor)
    warmup_advanced,  # Phase 4: ML/DL Engine + Adaptive Control + Optimization
    warmup_orchestration,  # Phase 5: Campaign Orchestration + A/B Testing + Analytics
    ml_analytics,  # ML Intelligence for Follow-Up (Reply Prediction, Send Time ML)
    email_tracking,  # Email open/click/bounce/unsubscribe tracking
    app_config,  # Public application configuration
)

api_router = APIRouter()

# Authentication
api_router.include_router(auth.router, prefix="/auth", tags=["authentication"])

# Core endpoints
api_router.include_router(
    applications.router, prefix="/applications", tags=["applications"]
)

# Phase 2: Async optimized endpoints (use these for better performance)
# api_router.include_router(applications_async.router, prefix="/applications-async", tags=["applications-async"])  # Disabled - file not found

# Health and monitoring endpoints
api_router.include_router(health.router, prefix="/health", tags=["health"])
api_router.include_router(companies.router, prefix="/companies", tags=["companies"])
api_router.include_router(analytics.router, prefix="/analytics", tags=["analytics"])

# Enhanced features
api_router.include_router(dashboards.router, prefix="/dashboards", tags=["dashboards"])
api_router.include_router(search.router, prefix="/search", tags=["search"])
api_router.include_router(email_preview.router, prefix="/email", tags=["email"])

# i18n and Multi-version features
api_router.include_router(resumes.router, prefix="/resumes", tags=["resumes"])
api_router.include_router(
    email_templates.router, prefix="/email-templates", tags=["email-templates"]
)
api_router.include_router(
    templates_unified.router, prefix="/templates", tags=["templates"]
)  # Unified templates + AI drafts

# Company Intelligence (Smart Research, Skill Matching, Email Drafting)
api_router.include_router(
    intelligence.router, prefix="/intelligence", tags=["intelligence"]
)  # Company research data
api_router.include_router(
    company_intelligence.router,
    prefix="/company-intelligence",
    tags=["company-intelligence"],
)

# User Management (Super Admin only)
api_router.include_router(users.router, prefix="/users", tags=["user-management"])

# Email Controls (Warming & Rate Limiting)
api_router.include_router(
    email_warming.router, prefix="/email-warming", tags=["email-warming"]
)
api_router.include_router(
    rate_limiting.router, prefix="/rate-limiting", tags=["rate-limiting"]
)

# Notifications
api_router.include_router(
    notifications.router, prefix="/notifications", tags=["notifications"]
)

# Send Time Optimization
api_router.include_router(send_time.router, prefix="/send-time", tags=["send-time"])

# Warmup Health Monitoring
api_router.include_router(
    warmup_health.router, prefix="/warmup-health", tags=["warmup-health"]
)

# Follow-Up Sequences (Auto-mode, Email Campaigns, Pipeline Integration)
api_router.include_router(follow_up.router, prefix="/follow-up", tags=["follow-up"])

# Email Inbox Integration (IMAP Sync, Thread View, Storage Management)
api_router.include_router(email_inbox.router, prefix="/inbox", tags=["email-inbox"])

# Template Marketplace (Share, Discover, Rate Templates)
api_router.include_router(
    template_marketplace.router, prefix="/marketplace", tags=["template-marketplace"]
)

# Application Attachments (Document Management, File Upload/Download)
api_router.include_router(
    attachments.router, prefix="/attachments", tags=["attachments"]
)

# Template Analytics (Performance Tracking, Trending, Rankings)
# router already has prefix "/template-analytics" defined in endpoint module
api_router.include_router(template_analytics.router, tags=["template-analytics"])

# Scheduler Admin (Background Jobs Management)
api_router.include_router(
    scheduler_admin.router, prefix="/scheduler-admin", tags=["admin"]
)

# Recipient Groups Feature (Recipients Directory, Groups, Campaigns)
api_router.include_router(
    recipients.router, prefix="/recipients", tags=["recipient-groups"]
)
api_router.include_router(
    recipient_groups.router, prefix="/recipient-groups", tags=["recipient-groups"]
)
api_router.include_router(
    group_campaigns.router, prefix="/group-campaigns", tags=["recipient-groups"]
)

# Enrichment (Data Enrichment Execution and Management)
api_router.include_router(enrichment.router, prefix="/enrichment", tags=["enrichment"])

# ULTRA PRO MAX EXTRACTION ENGINE (Multi-Layer Web Scraping, Data Extraction, Recipients Integration)
api_router.include_router(extraction.router, prefix="/extraction", tags=["extraction"])

# Documents (Resume & Info Doc Management)
api_router.include_router(documents.router, prefix="/documents", tags=["documents"])

# TheMobiAdz Extraction Engine (App/Game/E-commerce Company Discovery)
api_router.include_router(
    mobiadz.router, prefix="/mobiadz", tags=["mobiadz-extraction"]
)

# Advanced Email Warmup Pool System (Smartlead/Instantly competitor features)
# - Warmup pool network with intelligent partner matching
# - AI-generated human-like conversations
# - Inbox placement testing across providers
# - Spam rescue and blacklist monitoring
api_router.include_router(
    warmup_pool.router, prefix="/warmup-pool", tags=["warmup-pool"]
)

# Phase 4: Advanced ML/DL Warmup Engine (God-Level Intelligence)
# - Deep Q-Network (DQN) for optimal action selection with epsilon-greedy exploration
# - LSTM Neural Networks for sequence prediction and engagement patterns
# - Multi-Armed Bandits (Thompson Sampling + UCB1) for A/B testing optimization
# - Isolation Forest for spam pattern anomaly detection
# - Gradient Boosting for deliverability score prediction
# - Holt-Winters exponential smoothing for time series forecasting
# - Adaptive control with throttling, reputation protection, and fallback strategies
# - Provider-specific optimization (Gmail, Outlook, Yahoo timing)
# - Cross-account load balancing and health monitoring
api_router.include_router(
    warmup_advanced.router, prefix="/warmup-advanced", tags=["warmup-advanced"]
)

# Phase 5: Warmup Orchestration & Intelligence (Campaign Management + A/B Testing + Analytics)
# - Campaign orchestration with goal-based automation
# - Multi-stage warmup sequences with branching logic
# - Scientific A/B testing with statistical significance (Z-test, Bayesian)
# - Multi-armed bandits for adaptive allocation (Thompson, UCB1, Epsilon-greedy)
# - Sequential testing with early stopping
# - Comprehensive analytics with KPI tracking
# - Cohort analysis and funnel optimization
# - Industry benchmarking and comparison
# - Automated report generation
# - Real-time anomaly detection and alerting
api_router.include_router(
    warmup_orchestration.router, prefix="/warmup-orchestration", tags=["warmup-orchestration"]
)

# ML Intelligence for Follow-Up System (ULTRA V2.0 Sprint 2)
# - Reply probability prediction (Gradient Boosting + Heuristic fallback)
# - Send time optimization with day/hour heatmap
# - Prediction accuracy tracking and reporting
# - ML insights dashboard
# - Manual model training triggers
api_router.include_router(
    ml_analytics.router, prefix="/ml", tags=["ml-analytics"]
)

# Email Tracking (Open/Click/Bounce/Unsubscribe)
api_router.include_router(
    email_tracking.router, prefix="/tracking", tags=["email-tracking"]
)

# Public Application Configuration (no auth required)
api_router.include_router(
    app_config.router, prefix="/config", tags=["app-config"]
)
