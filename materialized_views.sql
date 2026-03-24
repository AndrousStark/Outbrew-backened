-- ============================================================================
-- Phase 3: Materialized Views for Expensive Aggregate Queries
-- ============================================================================
--
-- IMPACT: 100-1000x faster for dashboard/statistics queries
--
-- Materialized views are pre-computed query results stored as tables.
-- Perfect for expensive aggregations that are frequently accessed.
--
-- REFRESH STRATEGY:
-- - Manual: REFRESH MATERIALIZED VIEW view_name;
-- - Scheduled: Use pg_cron or application-level scheduler
-- - Recommended: Refresh every 5-15 minutes for near-real-time data
--
-- ============================================================================

-- ============================================================================
-- 1. APPLICATION STATISTICS BY CANDIDATE
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_application_stats_by_candidate AS
SELECT
    candidate_id,

    -- Total counts
    COUNT(*) as total_applications,

    -- Status breakdown
    COUNT(*) FILTER (WHERE status = 'draft') as draft_count,
    COUNT(*) FILTER (WHERE status = 'sent') as sent_count,
    COUNT(*) FILTER (WHERE status = 'opened') as opened_count,
    COUNT(*) FILTER (WHERE status = 'responded') as responded_count,
    COUNT(*) FILTER (WHERE status = 'interview') as interview_count,
    COUNT(*) FILTER (WHERE status = 'offer') as offer_count,
    COUNT(*) FILTER (WHERE status = 'rejected') as rejected_count,
    COUNT(*) FILTER (WHERE status = 'accepted') as accepted_count,

    -- Response tracking
    COUNT(*) FILTER (WHERE response_received = TRUE) as total_responses,
    COUNT(*) FILTER (WHERE sent_at IS NOT NULL) as total_sent,
    COUNT(*) FILTER (WHERE opened_at IS NOT NULL) as total_opened,
    COUNT(*) FILTER (WHERE replied_at IS NOT NULL) as total_replied,

    -- Rates (calculated as percentages)
    CASE
        WHEN COUNT(*) FILTER (WHERE sent_at IS NOT NULL) > 0
        THEN ROUND(
            (COUNT(*) FILTER (WHERE opened_at IS NOT NULL)::DECIMAL /
             COUNT(*) FILTER (WHERE sent_at IS NOT NULL)) * 100,
            2
        )
        ELSE 0.0
    END as open_rate_percent,

    CASE
        WHEN COUNT(*) FILTER (WHERE sent_at IS NOT NULL) > 0
        THEN ROUND(
            (COUNT(*) FILTER (WHERE status IN ('responded', 'interview', 'offer', 'accepted'))::DECIMAL /
             COUNT(*) FILTER (WHERE sent_at IS NOT NULL)) * 100,
            2
        )
        ELSE 0.0
    END as response_rate_percent,

    -- Time-based stats
    MIN(created_at) as first_application_date,
    MAX(created_at) as last_application_date,
    MAX(sent_at) as last_sent_date,
    MAX(opened_at) as last_opened_date,
    MAX(replied_at) as last_reply_date,

    -- Metadata
    NOW() as last_refreshed_at

FROM applications
WHERE deleted_at IS NULL
GROUP BY candidate_id;

-- Index for fast lookups
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_app_stats_candidate
ON mv_application_stats_by_candidate(candidate_id);

COMMENT ON MATERIALIZED VIEW mv_application_stats_by_candidate IS
'Pre-computed application statistics per candidate. Refresh every 5-15 minutes.';

-- ============================================================================
-- 2. COMPANY STATISTICS (Applications per Company)
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_company_stats AS
SELECT
    c.id as company_id,
    c.name as company_name,
    c.domain,
    c.industry,
    c.headquarters_country,

    -- Application counts
    COUNT(a.id) as total_applications,
    COUNT(DISTINCT a.candidate_id) as unique_candidates,

    -- Status breakdown
    COUNT(*) FILTER (WHERE a.status = 'sent') as sent_count,
    COUNT(*) FILTER (WHERE a.status = 'opened') as opened_count,
    COUNT(*) FILTER (WHERE a.status = 'responded') as responded_count,
    COUNT(*) FILTER (WHERE a.status = 'interview') as interview_count,
    COUNT(*) FILTER (WHERE a.status = 'offer') as offer_count,
    COUNT(*) FILTER (WHERE a.status = 'rejected') as rejected_count,

    -- Response rates
    CASE
        WHEN COUNT(*) FILTER (WHERE a.sent_at IS NOT NULL) > 0
        THEN ROUND(
            (COUNT(*) FILTER (WHERE a.opened_at IS NOT NULL)::DECIMAL /
             COUNT(*) FILTER (WHERE a.sent_at IS NOT NULL)) * 100,
            2
        )
        ELSE 0.0
    END as open_rate_percent,

    CASE
        WHEN COUNT(*) FILTER (WHERE a.sent_at IS NOT NULL) > 0
        THEN ROUND(
            (COUNT(*) FILTER (WHERE a.status IN ('responded', 'interview', 'offer'))::DECIMAL /
             COUNT(*) FILTER (WHERE a.sent_at IS NOT NULL)) * 100,
            2
        )
        ELSE 0.0
    END as response_rate_percent,

    -- Time stats
    MAX(a.created_at) as last_application_date,
    MAX(a.sent_at) as last_sent_date,

    -- Metadata
    NOW() as last_refreshed_at

FROM companies c
LEFT JOIN applications a ON c.id = a.company_id AND a.deleted_at IS NULL
WHERE c.deleted_at IS NULL
GROUP BY c.id, c.name, c.domain, c.industry, c.headquarters_country;

-- Indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_company_stats_id
ON mv_company_stats(company_id);

CREATE INDEX IF NOT EXISTS idx_mv_company_stats_apps
ON mv_company_stats(total_applications DESC);

CREATE INDEX IF NOT EXISTS idx_mv_company_stats_response
ON mv_company_stats(response_rate_percent DESC);

COMMENT ON MATERIALIZED VIEW mv_company_stats IS
'Company performance statistics. Refresh every 15 minutes.';

-- ============================================================================
-- 3. EMAIL PERFORMANCE DASHBOARD
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_email_performance AS
SELECT
    candidate_id,

    -- Email counts
    COUNT(*) as total_emails,
    COUNT(*) FILTER (WHERE status = 'sent') as sent_count,
    COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
    COUNT(*) FILTER (WHERE status = 'bounced') as bounced_count,
    COUNT(*) FILTER (WHERE opened = TRUE) as opened_count,
    COUNT(*) FILTER (WHERE clicked = TRUE) as clicked_count,

    -- Performance metrics
    ROUND(
        (COUNT(*) FILTER (WHERE status = 'sent')::DECIMAL /
         GREATEST(COUNT(*), 1)) * 100,
        2
    ) as send_success_rate,

    ROUND(
        (COUNT(*) FILTER (WHERE opened = TRUE)::DECIMAL /
         GREATEST(COUNT(*) FILTER (WHERE status = 'sent'), 1)) * 100,
        2
    ) as open_rate,

    ROUND(
        (COUNT(*) FILTER (WHERE clicked = TRUE)::DECIMAL /
         GREATEST(COUNT(*) FILTER (WHERE opened = TRUE), 1)) * 100,
        2
    ) as click_through_rate,

    -- Time-based
    MAX(created_at) as last_email_date,
    MAX(sent_at) as last_sent_date,
    MAX(opened_at) as last_opened_date,

    -- Metadata
    NOW() as last_refreshed_at

FROM email_logs
WHERE deleted_at IS NULL
GROUP BY candidate_id;

-- Index
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_email_perf_candidate
ON mv_email_performance(candidate_id);

COMMENT ON MATERIALIZED VIEW mv_email_performance IS
'Email performance metrics per candidate. Refresh every 10 minutes.';

-- ============================================================================
-- 4. DAILY APPLICATION TRENDS
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_application_trends_daily AS
SELECT
    DATE(created_at) as application_date,
    candidate_id,

    -- Daily counts
    COUNT(*) as applications_created,
    COUNT(*) FILTER (WHERE status = 'sent') as applications_sent,
    COUNT(*) FILTER (WHERE status = 'opened') as applications_opened,
    COUNT(*) FILTER (WHERE status = 'responded') as applications_responded,

    -- Unique companies
    COUNT(DISTINCT company_id) as unique_companies,

    -- Metadata
    NOW() as last_refreshed_at

FROM applications
WHERE deleted_at IS NULL
  AND created_at >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY DATE(created_at), candidate_id;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_mv_app_trends_date_candidate
ON mv_application_trends_daily(application_date DESC, candidate_id);

COMMENT ON MATERIALIZED VIEW mv_application_trends_daily IS
'Daily application trends for last 90 days. Refresh daily.';

-- ============================================================================
-- 5. TOP PERFORMING COMPANIES (by Response Rate)
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_companies_by_response AS
SELECT
    c.id as company_id,
    c.name as company_name,
    c.domain,
    c.industry,

    COUNT(a.id) as total_applications,
    COUNT(*) FILTER (WHERE a.status = 'sent') as sent_count,
    COUNT(*) FILTER (WHERE a.status IN ('responded', 'interview', 'offer')) as response_count,

    -- Response rate (only for companies with 3+ sent applications)
    CASE
        WHEN COUNT(*) FILTER (WHERE a.sent_at IS NOT NULL) >= 3
        THEN ROUND(
            (COUNT(*) FILTER (WHERE a.status IN ('responded', 'interview', 'offer'))::DECIMAL /
             COUNT(*) FILTER (WHERE a.sent_at IS NOT NULL)) * 100,
            2
        )
        ELSE NULL
    END as response_rate_percent,

    -- Metadata
    NOW() as last_refreshed_at

FROM companies c
INNER JOIN applications a ON c.id = a.company_id AND a.deleted_at IS NULL
WHERE c.deleted_at IS NULL
GROUP BY c.id, c.name, c.domain, c.industry
HAVING COUNT(*) FILTER (WHERE a.sent_at IS NOT NULL) >= 3
ORDER BY response_rate_percent DESC NULLS LAST
LIMIT 100;

-- Index
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_top_companies_id
ON mv_top_companies_by_response(company_id);

COMMENT ON MATERIALIZED VIEW mv_top_companies_by_response IS
'Top 100 companies by response rate (min 3 applications). Refresh daily.';

-- ============================================================================
-- 6. RECRUITER RESPONSIVENESS
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_recruiter_stats AS
SELECT
    recruiter_email,
    recruiter_name,

    COUNT(*) as total_applications,
    COUNT(*) FILTER (WHERE status = 'sent') as sent_count,
    COUNT(*) FILTER (WHERE status IN ('responded', 'interview', 'offer')) as response_count,

    ROUND(
        (COUNT(*) FILTER (WHERE status IN ('responded', 'interview', 'offer'))::DECIMAL /
         GREATEST(COUNT(*) FILTER (WHERE sent_at IS NOT NULL), 1)) * 100,
        2
    ) as response_rate_percent,

    -- Average response time (days)
    AVG(
        EXTRACT(EPOCH FROM (replied_at - sent_at)) / 86400
    ) FILTER (WHERE replied_at IS NOT NULL AND sent_at IS NOT NULL) as avg_response_time_days,

    MAX(created_at) as last_application_date,

    -- Metadata
    NOW() as last_refreshed_at

FROM applications
WHERE deleted_at IS NULL
  AND recruiter_email IS NOT NULL
GROUP BY recruiter_email, recruiter_name
HAVING COUNT(*) >= 2;

-- Index
CREATE INDEX IF NOT EXISTS idx_mv_recruiter_stats_email
ON mv_recruiter_stats(recruiter_email);

CREATE INDEX IF NOT EXISTS idx_mv_recruiter_stats_response
ON mv_recruiter_stats(response_rate_percent DESC);

COMMENT ON MATERIALIZED VIEW mv_recruiter_stats IS
'Recruiter responsiveness statistics. Refresh daily.';

-- ============================================================================
-- REFRESH FUNCTIONS (Call these on schedule)
-- ============================================================================

-- Function to refresh all materialized views
CREATE OR REPLACE FUNCTION refresh_all_materialized_views()
RETURNS void AS $$
BEGIN
    RAISE NOTICE 'Refreshing mv_application_stats_by_candidate...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_application_stats_by_candidate;

    RAISE NOTICE 'Refreshing mv_company_stats...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_company_stats;

    RAISE NOTICE 'Refreshing mv_email_performance...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_email_performance;

    RAISE NOTICE 'Refreshing mv_application_trends_daily...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_application_trends_daily;

    RAISE NOTICE 'Refreshing mv_top_companies_by_response...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_top_companies_by_response;

    RAISE NOTICE 'Refreshing mv_recruiter_stats...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_recruiter_stats;

    RAISE NOTICE 'All materialized views refreshed successfully!';
END;
$$ LANGUAGE plpgsql;

-- Function to refresh stats views (most frequently updated)
CREATE OR REPLACE FUNCTION refresh_stats_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_application_stats_by_candidate;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_email_performance;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- SCHEDULED REFRESH (using pg_cron extension)
-- ============================================================================

-- Install pg_cron extension first
-- CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Schedule stats refresh every 5 minutes
-- SELECT cron.schedule(
--     'refresh-stats-views',
--     '*/5 * * * *',
--     'SELECT refresh_stats_views();'
-- );

-- Schedule full refresh every hour
-- SELECT cron.schedule(
--     'refresh-all-views',
--     '0 * * * *',
--     'SELECT refresh_all_materialized_views();'
-- );

-- ============================================================================
-- ALTERNATIVE: Application-Level Refresh (FastAPI Background Task)
-- ============================================================================

/*
In your FastAPI application, create a background task scheduler:

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import text

scheduler = AsyncIOScheduler()

@scheduler.scheduled_job('interval', minutes=5)
async def refresh_stats_views():
    async with async_engine.begin() as conn:
        await conn.execute(text("SELECT refresh_stats_views();"))
        logger.info("Stats views refreshed")

@scheduler.scheduled_job('interval', hours=1)
async def refresh_all_views():
    async with async_engine.begin() as conn:
        await conn.execute(text("SELECT refresh_all_materialized_views();"))
        logger.info("All materialized views refreshed")

scheduler.start()
*/

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

/*
-- Get candidate statistics (instant, no computation)
SELECT * FROM mv_application_stats_by_candidate WHERE candidate_id = 1;

-- Get top companies by applications
SELECT * FROM mv_company_stats ORDER BY total_applications DESC LIMIT 10;

-- Get email performance
SELECT * FROM mv_email_performance WHERE candidate_id = 1;

-- Get daily trends
SELECT *
FROM mv_application_trends_daily
WHERE candidate_id = 1
  AND application_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY application_date DESC;

-- Get top responsive companies
SELECT * FROM mv_top_companies_by_response LIMIT 10;

-- Get best recruiters to target
SELECT *
FROM mv_recruiter_stats
WHERE response_rate_percent >= 50
ORDER BY response_count DESC
LIMIT 20;
*/

-- ============================================================================
-- MAINTENANCE NOTES
-- ============================================================================

/*
1. CONCURRENT REFRESH:
   - Uses CONCURRENTLY to avoid locking
   - Allows queries during refresh
   - Requires UNIQUE indexes

2. REFRESH FREQUENCY:
   - Stats views: Every 5 minutes (near real-time)
   - Analytics views: Every hour or daily
   - Balance freshness vs. load

3. MONITORING:
   - Check last_refreshed_at column
   - Monitor refresh duration
   - Watch for failed refreshes

4. STORAGE:
   - Materialized views use disk space
   - Monitor with pg_relation_size()
   - Benefits far outweigh costs

5. PERFORMANCE GAINS:
   - Statistics: 100-1000x faster
   - No real-time aggregation needed
   - Instant dashboard queries

6. FALLBACK:
   - If view is stale, query base tables
   - Application decides freshness tolerance
   - Can add age threshold checks
*/
