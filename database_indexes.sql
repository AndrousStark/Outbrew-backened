-- ============================================================================
-- Phase 3: Database Indexes for Maximum Performance
-- ============================================================================
--
-- IMPACT: 10-100x faster query execution on large datasets
--
-- This file contains strategic indexes based on:
-- - Common query patterns from repositories
-- - Foreign key relationships
-- - Frequently filtered columns
-- - Sort operations
-- - Full-text search requirements
--
-- EXECUTION: Run this file after Phase 2 async migration
-- ============================================================================

-- ============================================================================
-- APPLICATIONS TABLE - Most Critical (High Traffic)
-- ============================================================================

-- Existing indexes (already in model):
-- - PRIMARY KEY on id
-- - INDEX on candidate_id
-- - INDEX on company_id
-- - INDEX on resume_version_id
-- - INDEX on email_template_id
-- - INDEX on parent_application_id
-- - INDEX on application_type
-- - INDEX on recruiter_email
-- - INDEX on position_country
-- - INDEX on status
-- - UNIQUE INDEX on tracking_id
-- - INDEX on deleted_at
-- - COMPOSITE INDEX on (candidate_id, status)
-- - COMPOSITE INDEX on (candidate_id, created_at)
-- - COMPOSITE INDEX on (status, created_at)

-- NEW STRATEGIC INDEXES:

-- For queries filtering by candidate + status + created_at (common pattern)
CREATE INDEX IF NOT EXISTS ix_app_candidate_status_created
ON applications(candidate_id, status, created_at DESC)
WHERE deleted_at IS NULL;

-- For queries filtering by candidate + deleted status (soft delete queries)
CREATE INDEX IF NOT EXISTS ix_app_candidate_not_deleted
ON applications(candidate_id, created_at DESC)
WHERE deleted_at IS NULL;

-- For status-based dashboard queries with date sorting
CREATE INDEX IF NOT EXISTS ix_app_status_updated
ON applications(status, updated_at DESC)
WHERE deleted_at IS NULL;

-- For recruiter email tracking (unique recruiter queries)
CREATE INDEX IF NOT EXISTS ix_app_recruiter_status
ON applications(recruiter_email, status, created_at DESC)
WHERE deleted_at IS NULL;

-- For company-based queries
CREATE INDEX IF NOT EXISTS ix_app_company_created
ON applications(company_id, created_at DESC)
WHERE deleted_at IS NULL;

-- For position title search (case-insensitive)
CREATE INDEX IF NOT EXISTS ix_app_position_title_lower
ON applications(LOWER(position_title));

-- For tracking sent/opened applications (performance metrics)
CREATE INDEX IF NOT EXISTS ix_app_sent_not_null
ON applications(candidate_id, sent_at DESC)
WHERE sent_at IS NOT NULL AND deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_app_opened_not_null
ON applications(candidate_id, opened_at DESC)
WHERE opened_at IS NOT NULL AND deleted_at IS NULL;

-- For response tracking
CREATE INDEX IF NOT EXISTS ix_app_response_received
ON applications(candidate_id, response_received, replied_at DESC)
WHERE response_received = TRUE AND deleted_at IS NULL;

-- For starred applications (quick access)
CREATE INDEX IF NOT EXISTS ix_app_starred
ON applications(candidate_id, is_starred, created_at DESC)
WHERE is_starred = TRUE AND deleted_at IS NULL;

-- For follow-up queries
CREATE INDEX IF NOT EXISTS ix_app_parent_application
ON applications(parent_application_id, application_type, created_at DESC)
WHERE parent_application_id IS NOT NULL AND deleted_at IS NULL;

-- ============================================================================
-- COMPANIES TABLE - Frequent Lookups
-- ============================================================================

-- Existing indexes:
-- - PRIMARY KEY on id
-- - UNIQUE INDEX on name
-- - INDEX on headquarters_country
-- - INDEX on deleted_at

-- NEW STRATEGIC INDEXES:

-- For domain-based lookups (very common in repositories)
CREATE INDEX IF NOT EXISTS ix_company_domain
ON companies(domain)
WHERE deleted_at IS NULL AND domain IS NOT NULL;

-- For case-insensitive name search
CREATE INDEX IF NOT EXISTS ix_company_name_lower
ON companies(LOWER(name))
WHERE deleted_at IS NULL;

-- For industry filtering
CREATE INDEX IF NOT EXISTS ix_company_industry
ON companies(industry, name)
WHERE deleted_at IS NULL AND industry IS NOT NULL;

-- For company size filtering
CREATE INDEX IF NOT EXISTS ix_company_size
ON companies(company_size, name)
WHERE deleted_at IS NULL AND company_size IS NOT NULL;

-- For country-based search
CREATE INDEX IF NOT EXISTS ix_company_country_city
ON companies(headquarters_country, headquarters_city)
WHERE deleted_at IS NULL;

-- Full-text search on company name (PostgreSQL specific)
-- Enable pg_trgm extension first
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Trigram index for fuzzy name search
CREATE INDEX IF NOT EXISTS ix_company_name_trgm
ON companies USING gin(name gin_trgm_ops);

-- For recently researched companies
CREATE INDEX IF NOT EXISTS ix_company_last_researched
ON companies(last_researched_at DESC)
WHERE last_researched_at IS NOT NULL AND deleted_at IS NULL;

-- ============================================================================
-- CANDIDATES TABLE - Authentication & Profiles
-- ============================================================================

-- Existing indexes:
-- - PRIMARY KEY on id
-- - UNIQUE INDEX on username
-- - UNIQUE INDEX on email
-- - INDEX on role
-- - INDEX on deleted_at

-- NEW STRATEGIC INDEXES:

-- For email authentication (most critical)
CREATE INDEX IF NOT EXISTS ix_candidate_email_active
ON candidates(email, hashed_password)
WHERE is_active = TRUE AND deleted_at IS NULL;

-- For username authentication
CREATE INDEX IF NOT EXISTS ix_candidate_username_active
ON candidates(username, hashed_password)
WHERE is_active = TRUE AND deleted_at IS NULL;

-- For role-based queries
CREATE INDEX IF NOT EXISTS ix_candidate_role_active
ON candidates(role, is_active)
WHERE deleted_at IS NULL;

-- For case-insensitive email search (admin features)
CREATE INDEX IF NOT EXISTS ix_candidate_email_lower
ON candidates(LOWER(email))
WHERE deleted_at IS NULL;

-- ============================================================================
-- EMAIL_LOGS TABLE - Email Tracking
-- ============================================================================

-- Existing indexes:
-- - PRIMARY KEY on id
-- - INDEX on candidate_id
-- - INDEX on application_id
-- - INDEX on to_email
-- - INDEX on status
-- - INDEX on deleted_at
-- - COMPOSITE INDEX on (candidate_id, created_at)
-- - COMPOSITE INDEX on (status, created_at)

-- NEW STRATEGIC INDEXES:

-- For application-specific email logs
CREATE INDEX IF NOT EXISTS ix_email_log_application_created
ON email_logs(application_id, created_at DESC)
WHERE deleted_at IS NULL;

-- For tracking opened emails
CREATE INDEX IF NOT EXISTS ix_email_log_opened
ON email_logs(candidate_id, opened, opened_at DESC)
WHERE opened = TRUE AND deleted_at IS NULL;

-- For tracking clicked emails
CREATE INDEX IF NOT EXISTS ix_email_log_clicked
ON email_logs(candidate_id, clicked, created_at DESC)
WHERE clicked = TRUE AND deleted_at IS NULL;

-- For failed email tracking
CREATE INDEX IF NOT EXISTS ix_email_log_failed
ON email_logs(candidate_id, status, created_at DESC)
WHERE status = 'failed' AND deleted_at IS NULL;

-- For sent email tracking with recipient
CREATE INDEX IF NOT EXISTS ix_email_log_to_sent
ON email_logs(to_email, status, sent_at DESC)
WHERE status = 'sent' AND deleted_at IS NULL;

-- ============================================================================
-- RESUME_VERSIONS TABLE
-- ============================================================================

-- For candidate resume queries
CREATE INDEX IF NOT EXISTS ix_resume_candidate_created
ON resume_versions(candidate_id, created_at DESC)
WHERE deleted_at IS NULL;

-- For active resumes
CREATE INDEX IF NOT EXISTS ix_resume_candidate_active
ON resume_versions(candidate_id, is_active, created_at DESC)
WHERE deleted_at IS NULL;

-- ============================================================================
-- EMAIL_TEMPLATES TABLE
-- ============================================================================

-- For candidate template queries
CREATE INDEX IF NOT EXISTS ix_template_candidate_created
ON email_templates(candidate_id, created_at DESC)
WHERE deleted_at IS NULL;

-- For template name search
CREATE INDEX IF NOT EXISTS ix_template_candidate_name
ON email_templates(candidate_id, name)
WHERE deleted_at IS NULL;

-- For default templates
CREATE INDEX IF NOT EXISTS ix_template_default
ON email_templates(candidate_id, is_default)
WHERE is_default = TRUE AND deleted_at IS NULL;

-- ============================================================================
-- NOTIFICATIONS TABLE
-- ============================================================================

-- For unread notifications
CREATE INDEX IF NOT EXISTS ix_notification_unread
ON notifications(candidate_id, is_read, created_at DESC)
WHERE is_read = FALSE;

-- For notification type filtering
CREATE INDEX IF NOT EXISTS ix_notification_type
ON notifications(candidate_id, type, created_at DESC);

-- ============================================================================
-- APPLICATION_HISTORY TABLE
-- ============================================================================

-- For application timeline queries
CREATE INDEX IF NOT EXISTS ix_app_history_application
ON application_history(application_id, created_at DESC);

-- ============================================================================
-- COMPANY INTELLIGENCE TABLES
-- ============================================================================

-- Company Projects
CREATE INDEX IF NOT EXISTS ix_company_project_company
ON company_projects(company_id, created_at DESC)
WHERE deleted_at IS NULL;

-- Company Research Cache
CREATE INDEX IF NOT EXISTS ix_research_cache_company
ON company_research_cache(company_id)
WHERE deleted_at IS NULL;

-- Skill Matches
CREATE INDEX IF NOT EXISTS ix_skill_match_candidate
ON skill_matches(candidate_id, match_score DESC, created_at DESC)
WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_skill_match_company
ON skill_matches(company_id, match_score DESC, created_at DESC)
WHERE deleted_at IS NULL;

-- ============================================================================
-- EMAIL WARMING TABLES
-- ============================================================================

-- Email Warming Config
CREATE INDEX IF NOT EXISTS ix_warming_config_candidate
ON email_warming_config(candidate_id, is_active);

-- Warmup Health
CREATE INDEX IF NOT EXISTS ix_warmup_health_candidate
ON warmup_health(candidate_id, health_date DESC);

-- ============================================================================
-- SCHEDULED EMAILS TABLE
-- ============================================================================

-- For scheduled email processing
CREATE INDEX IF NOT EXISTS ix_scheduled_email_status_scheduled
ON scheduled_emails(status, scheduled_for ASC)
WHERE status = 'scheduled';

CREATE INDEX IF NOT EXISTS ix_scheduled_email_candidate
ON scheduled_emails(candidate_id, status, scheduled_for DESC);

-- ============================================================================
-- FOLLOW_UPS TABLE
-- ============================================================================

-- For application follow-ups
CREATE INDEX IF NOT EXISTS ix_follow_up_application
ON follow_ups(application_id, status, scheduled_date)
WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_follow_up_candidate
ON follow_ups(candidate_id, status, scheduled_date)
WHERE deleted_at IS NULL;

-- ============================================================================
-- EMAIL INBOX TABLE
-- ============================================================================

-- For inbox queries
CREATE INDEX IF NOT EXISTS ix_email_inbox_candidate_received
ON email_inbox(candidate_id, received_at DESC)
WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_email_inbox_unread
ON email_inbox(candidate_id, is_read, received_at DESC)
WHERE is_read = FALSE AND deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_email_inbox_from
ON email_inbox(from_email, received_at DESC)
WHERE deleted_at IS NULL;

-- ============================================================================
-- RATE LIMITING TABLE
-- ============================================================================

CREATE INDEX IF NOT EXISTS ix_rate_limit_candidate
ON rate_limit_config(candidate_id, is_active);

-- ============================================================================
-- TEMPLATE MARKETPLACE TABLE
-- ============================================================================

CREATE INDEX IF NOT EXISTS ix_template_marketplace_category
ON template_marketplace(category, rating DESC, created_at DESC)
WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS ix_template_marketplace_author
ON template_marketplace(author_id, created_at DESC)
WHERE is_active = TRUE;

-- ============================================================================
-- INDEX STATISTICS & MONITORING
-- ============================================================================

-- After creating indexes, analyze tables for optimal query planning
ANALYZE applications;
ANALYZE companies;
ANALYZE candidates;
ANALYZE email_logs;
ANALYZE resume_versions;
ANALYZE email_templates;
ANALYZE notifications;
ANALYZE application_history;
ANALYZE skill_matches;
ANALYZE scheduled_emails;
ANALYZE follow_ups;
ANALYZE email_inbox;

-- ============================================================================
-- VACUUM for performance (run during maintenance window)
-- ============================================================================

-- VACUUM ANALYZE applications;
-- VACUUM ANALYZE companies;
-- VACUUM ANALYZE candidates;
-- VACUUM ANALYZE email_logs;

-- ============================================================================
-- INDEX USAGE MONITORING QUERIES
-- ============================================================================

-- Check index usage (run after application has been running for a while)
-- SELECT
--     schemaname,
--     tablename,
--     indexname,
--     idx_scan as index_scans,
--     idx_tup_read as tuples_read,
--     idx_tup_fetch as tuples_fetched
-- FROM pg_stat_user_indexes
-- WHERE schemaname = 'public'
-- ORDER BY idx_scan DESC;

-- Find unused indexes (candidates for removal)
-- SELECT
--     schemaname,
--     tablename,
--     indexname,
--     idx_scan
-- FROM pg_stat_user_indexes
-- WHERE schemaname = 'public'
--   AND idx_scan = 0
--   AND indexname NOT LIKE '%_pkey'
-- ORDER BY tablename, indexname;

-- Check index size
-- SELECT
--     schemaname,
--     tablename,
--     indexname,
--     pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
-- FROM pg_stat_user_indexes
-- WHERE schemaname = 'public'
-- ORDER BY pg_relation_size(indexrelid) DESC;

-- ============================================================================
-- NOTES & BEST PRACTICES
-- ============================================================================

/*
1. PARTIAL INDEXES:
   - Used extensively with "WHERE deleted_at IS NULL"
   - Dramatically reduces index size
   - Only indexes active records (not soft-deleted)
   - Perfect for our soft-delete pattern

2. COMPOSITE INDEXES:
   - Order matters! Most selective column first
   - Supports queries on leading columns
   - Example: (candidate_id, status, created_at) supports:
     - WHERE candidate_id = X
     - WHERE candidate_id = X AND status = Y
     - WHERE candidate_id = X AND status = Y AND created_at > Z

3. FUNCTIONAL INDEXES:
   - LOWER() for case-insensitive search
   - Only effective if queries use same function

4. GIN/TRIGRAM INDEXES:
   - For full-text fuzzy search
   - Enables LIKE '%term%' queries efficiently
   - Great for company name search

5. INDEX MAINTENANCE:
   - Run ANALYZE regularly (weekly)
   - Run VACUUM during low-traffic periods (monthly)
   - Monitor index usage with pg_stat_user_indexes
   - Remove unused indexes

6. QUERY OPTIMIZATION:
   - Use EXPLAIN ANALYZE to verify index usage
   - Check if index scans are being used
   - Watch for sequential scans on large tables

7. PERFORMANCE IMPACT:
   - Reads: 10-100x faster with proper indexes
   - Writes: Slight overhead (worth it!)
   - Storage: Additional disk space (minimal)
*/
