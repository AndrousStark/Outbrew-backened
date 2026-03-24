"""
Database Optimization Utilities for Phase 3

Provides tools for:
- Query performance monitoring
- Index usage analysis
- Slow query detection
- Connection pool monitoring
- Database health checks
"""
import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


# ============================================================================
# QUERY PERFORMANCE MONITORING
# ============================================================================

async def get_slow_queries(
    db: AsyncSession,
    min_duration_ms: float = 100.0,
    limit: int = 20
) -> List[Dict[str, Any]]:
    """
    Get slow queries from pg_stat_statements.

    Requires: CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

    Args:
        db: Async database session
        min_duration_ms: Minimum query duration in milliseconds
        limit: Max results

    Returns:
        List of slow queries with stats
    """
    query = text("""
        SELECT
            query,
            calls,
            total_exec_time as total_time_ms,
            mean_exec_time as mean_time_ms,
            max_exec_time as max_time_ms,
            stddev_exec_time as stddev_time_ms,
            rows
        FROM pg_stat_statements
        WHERE mean_exec_time > :min_duration
        ORDER BY mean_exec_time DESC
        LIMIT :limit
    """)

    result = await db.execute(
        query,
        {"min_duration": min_duration_ms, "limit": limit}
    )

    rows = result.fetchall()

    return [
        {
            "query": row.query,
            "calls": row.calls,
            "total_time_ms": round(row.total_time_ms, 2),
            "mean_time_ms": round(row.mean_time_ms, 2),
            "max_time_ms": round(row.max_time_ms, 2),
            "rows": row.rows
        }
        for row in rows
    ]


async def get_query_stats_by_table(
    db: AsyncSession,
    table_name: str
) -> Dict[str, Any]:
    """
    Get query statistics for a specific table.

    Args:
        db: Async database session
        table_name: Table name

    Returns:
        Dict with query statistics
    """
    query = text("""
        SELECT
            schemaname,
            tablename,
            seq_scan,
            seq_tup_read,
            idx_scan,
            idx_tup_fetch,
            n_tup_ins,
            n_tup_upd,
            n_tup_del
        FROM pg_stat_user_tables
        WHERE tablename = :table_name
    """)

    result = await db.execute(query, {"table_name": table_name})
    row = result.fetchone()

    if not row:
        return {}

    return {
        "table_name": row.tablename,
        "sequential_scans": row.seq_scan,
        "sequential_rows_read": row.seq_tup_read,
        "index_scans": row.idx_scan,
        "index_rows_fetched": row.idx_tup_fetch,
        "inserts": row.n_tup_ins,
        "updates": row.n_tup_upd,
        "deletes": row.n_tup_del,
        "index_usage_ratio": (
            round(row.idx_scan / (row.seq_scan + row.idx_scan) * 100, 2)
            if (row.seq_scan + row.idx_scan) > 0
            else 0.0
        )
    }


# ============================================================================
# INDEX ANALYSIS
# ============================================================================

async def get_index_usage(
    db: AsyncSession,
    min_size_mb: float = 1.0
) -> List[Dict[str, Any]]:
    """
    Get index usage statistics.

    Helps identify:
    - Unused indexes (candidates for removal)
    - Heavily used indexes (critical)
    - Large unused indexes (wasting space)

    Args:
        db: Async database session
        min_size_mb: Minimum index size in MB

    Returns:
        List of indexes with usage stats
    """
    query = text("""
        SELECT
            schemaname,
            tablename,
            indexname,
            idx_scan as index_scans,
            idx_tup_read as tuples_read,
            idx_tup_fetch as tuples_fetched,
            pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
            pg_relation_size(indexrelid) as index_size_bytes
        FROM pg_stat_user_indexes
        WHERE schemaname = 'public'
          AND pg_relation_size(indexrelid) > :min_size_bytes
        ORDER BY idx_scan ASC, pg_relation_size(indexrelid) DESC
    """)

    min_size_bytes = min_size_mb * 1024 * 1024

    result = await db.execute(query, {"min_size_bytes": min_size_bytes})
    rows = result.fetchall()

    return [
        {
            "table_name": row.tablename,
            "index_name": row.indexname,
            "index_scans": row.index_scans,
            "tuples_read": row.tuples_read,
            "index_size": row.index_size,
            "status": "unused" if row.index_scans == 0 else "active"
        }
        for row in rows
    ]


async def get_missing_indexes(
    db: AsyncSession,
    min_seq_scans: int = 1000
) -> List[Dict[str, Any]]:
    """
    Identify tables that might benefit from additional indexes.

    Tables with high sequential scans and low index usage are candidates.

    Args:
        db: Async database session
        min_seq_scans: Minimum sequential scans to report

    Returns:
        List of tables needing indexes
    """
    query = text("""
        SELECT
            schemaname,
            tablename,
            seq_scan,
            seq_tup_read,
            idx_scan,
            n_live_tup as live_tuples,
            pg_size_pretty(pg_relation_size(relid)) as table_size
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
          AND seq_scan > :min_seq_scans
          AND (idx_scan = 0 OR seq_scan > idx_scan * 10)
        ORDER BY seq_scan DESC
    """)

    result = await db.execute(query, {"min_seq_scans": min_seq_scans})
    rows = result.fetchall()

    return [
        {
            "table_name": row.tablename,
            "sequential_scans": row.seq_scan,
            "index_scans": row.idx_scan,
            "live_tuples": row.live_tuples,
            "table_size": row.table_size,
            "recommendation": "Consider adding indexes on frequently filtered columns"
        }
        for row in rows
    ]


# ============================================================================
# TABLE STATISTICS
# ============================================================================

async def get_table_sizes(
    db: AsyncSession,
    limit: int = 20
) -> List[Dict[str, Any]]:
    """
    Get table sizes sorted by largest first.

    Args:
        db: Async database session
        limit: Max results

    Returns:
        List of tables with sizes
    """
    query = text("""
        SELECT
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
            pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) -
                          pg_relation_size(schemaname||'.'||tablename)) as indexes_size,
            pg_total_relation_size(schemaname||'.'||tablename) as total_size_bytes
        FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        LIMIT :limit
    """)

    result = await db.execute(query, {"limit": limit})
    rows = result.fetchall()

    return [
        {
            "table_name": row.tablename,
            "total_size": row.total_size,
            "table_size": row.table_size,
            "indexes_size": row.indexes_size
        }
        for row in rows
    ]


async def get_table_bloat(db: AsyncSession) -> List[Dict[str, Any]]:
    """
    Estimate table bloat (wasted space).

    High bloat indicates need for VACUUM FULL.

    Args:
        db: Async database session

    Returns:
        List of tables with bloat estimates
    """
    query = text("""
        SELECT
            schemaname,
            tablename,
            n_dead_tup as dead_tuples,
            n_live_tup as live_tuples,
            CASE
                WHEN n_live_tup > 0
                THEN ROUND((n_dead_tup::float / n_live_tup) * 100, 2)
                ELSE 0
            END as bloat_ratio
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
          AND n_dead_tup > 1000
        ORDER BY n_dead_tup DESC
    """)

    result = await db.execute(query)
    rows = result.fetchall()

    return [
        {
            "table_name": row.tablename,
            "dead_tuples": row.dead_tuples,
            "live_tuples": row.live_tuples,
            "bloat_ratio": row.bloat_ratio,
            "recommendation": (
                "Run VACUUM" if row.bloat_ratio < 20
                else "Run VACUUM FULL" if row.bloat_ratio < 50
                else "Urgent: Run VACUUM FULL"
            )
        }
        for row in rows
    ]


# ============================================================================
# CONNECTION MONITORING
# ============================================================================

async def get_active_connections(db: AsyncSession) -> List[Dict[str, Any]]:
    """
    Get currently active database connections.

    Args:
        db: Async database session

    Returns:
        List of active connections
    """
    query = text("""
        SELECT
            pid,
            usename as username,
            application_name,
            client_addr as client_address,
            state,
            query,
            query_start,
            NOW() - query_start as duration
        FROM pg_stat_activity
        WHERE state = 'active'
          AND pid != pg_backend_pid()
        ORDER BY query_start
    """)

    result = await db.execute(query)
    rows = result.fetchall()

    return [
        {
            "pid": row.pid,
            "username": row.username,
            "application": row.application_name,
            "client_address": str(row.client_address) if row.client_address else None,
            "state": row.state,
            "query": row.query[:100] + "..." if len(row.query) > 100 else row.query,
            "duration_seconds": row.duration.total_seconds() if row.duration else 0
        }
        for row in rows
    ]


async def get_connection_stats(db: AsyncSession) -> Dict[str, Any]:
    """
    Get connection pool statistics.

    Args:
        db: Async database session

    Returns:
        Dict with connection stats
    """
    query = text("""
        SELECT
            COUNT(*) as total_connections,
            COUNT(*) FILTER (WHERE state = 'active') as active,
            COUNT(*) FILTER (WHERE state = 'idle') as idle,
            COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
            MAX(EXTRACT(EPOCH FROM (NOW() - query_start))) as longest_query_seconds
        FROM pg_stat_activity
        WHERE pid != pg_backend_pid()
    """)

    result = await db.execute(query)
    row = result.fetchone()

    return {
        "total_connections": row.total_connections,
        "active": row.active,
        "idle": row.idle,
        "idle_in_transaction": row.idle_in_transaction,
        "longest_query_seconds": round(row.longest_query_seconds, 2) if row.longest_query_seconds else 0
    }


# ============================================================================
# HEALTH CHECKS
# ============================================================================

async def run_database_health_check(db: AsyncSession) -> Dict[str, Any]:
    """
    Run comprehensive database health check.

    Args:
        db: Async database session

    Returns:
        Dict with health status and recommendations
    """
    health = {
        "status": "healthy",
        "issues": [],
        "warnings": [],
        "recommendations": []
    }

    try:
        # Check slow queries
        slow_queries = await get_slow_queries(db, min_duration_ms=500, limit=5)
        if slow_queries:
            health["warnings"].append(
                f"Found {len(slow_queries)} slow queries (>500ms)"
            )
            health["recommendations"].append("Review and optimize slow queries")

        # Check missing indexes
        missing_indexes = await get_missing_indexes(db, min_seq_scans=1000)
        if missing_indexes:
            health["warnings"].append(
                f"Found {len(missing_indexes)} tables with high sequential scans"
            )
            health["recommendations"].append("Consider adding indexes")

        # Check table bloat
        bloat = await get_table_bloat(db)
        high_bloat = [t for t in bloat if t["bloat_ratio"] > 20]
        if high_bloat:
            health["warnings"].append(
                f"Found {len(high_bloat)} tables with >20% bloat"
            )
            health["recommendations"].append("Run VACUUM on bloated tables")

        # Check connections
        conn_stats = await get_connection_stats(db)
        if conn_stats["total_connections"] > 80:  # Assuming max 100
            health["warnings"].append("High connection count detected")
            health["recommendations"].append("Consider connection pooling (PgBouncer)")

        if health["warnings"]:
            health["status"] = "degraded"

    except Exception as e:
        health["status"] = "error"
        health["issues"].append(str(e))

    return health


# ============================================================================
# OPTIMIZATION RECOMMENDATIONS
# ============================================================================

async def get_optimization_recommendations(db: AsyncSession) -> List[str]:
    """
    Get AI-powered optimization recommendations.

    Args:
        db: Async database session

    Returns:
        List of recommendations
    """
    recommendations = []

    # Check index usage
    indexes = await get_index_usage(db, min_size_mb=1.0)
    unused = [idx for idx in indexes if idx["status"] == "unused"]

    if unused:
        recommendations.append(
            f"Found {len(unused)} unused indexes consuming disk space. "
            "Consider removing them to improve write performance."
        )

    # Check missing indexes
    missing = await get_missing_indexes(db, min_seq_scans=1000)
    if missing:
        recommendations.append(
            f"Found {len(missing)} tables with high sequential scans. "
            "Adding strategic indexes could dramatically improve query performance."
        )

    # Check slow queries
    slow = await get_slow_queries(db, min_duration_ms=100, limit=10)
    if slow:
        recommendations.append(
            f"Found {len(slow)} slow queries. "
            "Review execution plans with EXPLAIN ANALYZE and optimize."
        )

    # Check bloat
    bloat = await get_table_bloat(db)
    if bloat:
        recommendations.append(
            f"Found {len(bloat)} tables with dead tuples. "
            "Run VACUUM to reclaim space and improve performance."
        )

    if not recommendations:
        recommendations.append("Database is well-optimized! No major issues detected.")

    return recommendations
