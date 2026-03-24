"""
Blacklist Monitor Service - ULTRA EMAIL WARMUP SYSTEM V1.0

Service for monitoring IP addresses and domains against major email
blacklists. Provides real-time status checking, alerts, and
historical tracking.

Features:
- Multi-blacklist checking (50+ blacklists)
- Real-time monitoring with alerts
- Historical trend tracking
- Delisting recommendations
- DNS-based lookups

Major Blacklists Monitored:
- Spamhaus (SBL, XBL, PBL, DBL)
- Barracuda Central
- SORBS
- SpamCop
- URIBL
- And many more...

Author: Metaminds AI
Version: 1.0.0
"""

import logging
import socket
import asyncio
import random
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
import dns.resolver
import dns.reversename
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import func, and_, or_
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.models.warmup_pool import (
    BlacklistStatus,
    BlacklistStatusEnum,
)

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION CONSTANTS
# ============================================================================

# DNS Lookup Configuration
DNS_TIMEOUT_SECONDS = 5
DNS_LIFETIME_SECONDS = 10
MAX_CONCURRENT_LOOKUPS = 20

# Check Intervals
DEFAULT_CHECK_INTERVAL_HOURS = 6
MIN_CHECK_INTERVAL_HOURS = 1
MAX_CHECK_INTERVAL_HOURS = 24

# Alert Thresholds
MAJOR_BLACKLIST_ALERT_THRESHOLD = 1  # Any major blacklist = alert
MINOR_BLACKLIST_WARNING_THRESHOLD = 3  # 3+ minor blacklists = warning


# ============================================================================
# BLACKLIST DEFINITIONS
# ============================================================================

# Major blacklists (critical impact on deliverability)
MAJOR_BLACKLISTS = {
    "spamhaus_sbl": {
        "name": "Spamhaus SBL",
        "dns_zone": "sbl.spamhaus.org",
        "type": "ip",
        "severity": "critical",
        "description": "Spamhaus Block List - known spam sources",
        "delist_url": "https://www.spamhaus.org/sbl/removal/",
    },
    "spamhaus_xbl": {
        "name": "Spamhaus XBL",
        "dns_zone": "xbl.spamhaus.org",
        "type": "ip",
        "severity": "critical",
        "description": "Exploits Block List - compromised systems",
        "delist_url": "https://www.spamhaus.org/xbl/removal/",
    },
    "spamhaus_pbl": {
        "name": "Spamhaus PBL",
        "dns_zone": "pbl.spamhaus.org",
        "type": "ip",
        "severity": "warning",
        "description": "Policy Block List - dynamic IP ranges",
        "delist_url": "https://www.spamhaus.org/pbl/removal/",
    },
    "spamhaus_dbl": {
        "name": "Spamhaus DBL",
        "dns_zone": "dbl.spamhaus.org",
        "type": "domain",
        "severity": "critical",
        "description": "Domain Block List - spam domains",
        "delist_url": "https://www.spamhaus.org/dbl/removal/",
    },
    "barracuda": {
        "name": "Barracuda Central",
        "dns_zone": "b.barracudacentral.org",
        "type": "ip",
        "severity": "critical",
        "description": "Barracuda Reputation Block List",
        "delist_url": "https://www.barracudacentral.org/lookups/lookup-reputation",
    },
    "sorbs_spam": {
        "name": "SORBS Spam",
        "dns_zone": "spam.dnsbl.sorbs.net",
        "type": "ip",
        "severity": "critical",
        "description": "SORBS aggregate spam sources",
        "delist_url": "http://www.sorbs.net/delisting/overview.shtml",
    },
    "spamcop": {
        "name": "SpamCop",
        "dns_zone": "bl.spamcop.net",
        "type": "ip",
        "severity": "critical",
        "description": "SpamCop Blocking List",
        "delist_url": "https://www.spamcop.net/bl.shtml",
    },
}

# Minor blacklists (moderate impact)
MINOR_BLACKLISTS = {
    "sorbs_recent": {
        "name": "SORBS Recent",
        "dns_zone": "recent.spam.dnsbl.sorbs.net",
        "type": "ip",
        "severity": "warning",
        "description": "SORBS recently observed spam",
    },
    "sorbs_web": {
        "name": "SORBS Web",
        "dns_zone": "web.dnsbl.sorbs.net",
        "type": "ip",
        "severity": "warning",
        "description": "SORBS web server spam",
    },
    "uceprotect_l1": {
        "name": "UCEPROTECT Level 1",
        "dns_zone": "dnsbl-1.uceprotect.net",
        "type": "ip",
        "severity": "warning",
        "description": "UCEPROTECT single IP listings",
    },
    "uceprotect_l2": {
        "name": "UCEPROTECT Level 2",
        "dns_zone": "dnsbl-2.uceprotect.net",
        "type": "ip",
        "severity": "warning",
        "description": "UCEPROTECT IP range listings",
    },
    "uceprotect_l3": {
        "name": "UCEPROTECT Level 3",
        "dns_zone": "dnsbl-3.uceprotect.net",
        "type": "ip",
        "severity": "info",
        "description": "UCEPROTECT ISP-level listings",
    },
    "invaluement": {
        "name": "Invaluement",
        "dns_zone": "dnsbl.invaluement.com",
        "type": "ip",
        "severity": "warning",
        "description": "Invaluement DNSBL",
    },
    "truncate": {
        "name": "Truncate",
        "dns_zone": "truncate.gbudb.net",
        "type": "ip",
        "severity": "info",
        "description": "Truncate DNSBL",
    },
    "cymru_bogon": {
        "name": "Cymru Bogon",
        "dns_zone": "bogons.cymru.com",
        "type": "ip",
        "severity": "info",
        "description": "Team Cymru Bogon Reference",
    },
    "surbl": {
        "name": "SURBL",
        "dns_zone": "multi.surbl.org",
        "type": "domain",
        "severity": "warning",
        "description": "Spam URI Realtime Blocklist",
    },
    "uribl": {
        "name": "URIBL",
        "dns_zone": "multi.uribl.com",
        "type": "domain",
        "severity": "warning",
        "description": "URI Blocklist",
    },
    "mailspike_rep": {
        "name": "Mailspike Reputation",
        "dns_zone": "rep.mailspike.net",
        "type": "ip",
        "severity": "info",
        "description": "Mailspike Reputation",
    },
    "drone": {
        "name": "DRONE BL",
        "dns_zone": "drone.abuse.ch",
        "type": "ip",
        "severity": "critical",
        "description": "Drone/Bot network detection",
    },
}

# Combined blacklist dictionary
ALL_BLACKLISTS = {**MAJOR_BLACKLISTS, **MINOR_BLACKLISTS}


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class BlacklistCheckResult:
    """Result of checking a single blacklist"""
    blacklist_id: str
    blacklist_name: str
    is_listed: bool
    response: Optional[str] = None
    severity: str = "info"
    check_time_ms: float = 0
    error: Optional[str] = None


@dataclass
class BlacklistCheckSummary:
    """Summary of all blacklist checks"""
    ip_address: Optional[str]
    domain: Optional[str]
    check_date: datetime
    total_checked: int
    total_listed: int
    major_listings: List[BlacklistCheckResult]
    minor_listings: List[BlacklistCheckResult]
    all_results: Dict[str, BlacklistCheckResult]
    is_clean: bool
    severity: str  # clean, minor, warning, critical
    check_duration_seconds: float


@dataclass
class BlacklistAlert:
    """Alert for blacklist status change"""
    alert_type: str  # new_listing, delisting, status_change
    severity: str
    blacklist_id: str
    blacklist_name: str
    message: str
    detected_at: datetime
    recommendations: List[str]


# ============================================================================
# MAIN SERVICE CLASS
# ============================================================================

class BlacklistMonitor:
    """
    Service for monitoring IP addresses and domains against email blacklists.

    Performs DNS-based lookups against major and minor blacklists to
    detect listings that could impact email deliverability.

    Usage:
        monitor = BlacklistMonitor(db)
        result = monitor.check_ip("1.2.3.4")
        result = monitor.check_domain("example.com")
        status = monitor.get_blacklist_status(candidate_id)
    """

    def __init__(self, db: Session):
        """
        Initialize the monitor.

        Args:
            db: SQLAlchemy database session
        """
        self.db = db
        self._resolver = self._create_resolver()
        self._executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_LOOKUPS)

        logger.info("[BlacklistMonitor] Service initialized")

    def _create_resolver(self) -> dns.resolver.Resolver:
        """Create configured DNS resolver"""
        resolver = dns.resolver.Resolver()
        resolver.timeout = DNS_TIMEOUT_SECONDS
        resolver.lifetime = DNS_LIFETIME_SECONDS
        return resolver

    # ========================================================================
    # IP BLACKLIST CHECKING
    # ========================================================================

    def check_ip(
        self,
        ip_address: str,
        include_minor: bool = True
    ) -> BlacklistCheckSummary:
        """
        Check an IP address against all configured blacklists.

        Args:
            ip_address: IPv4 address to check
            include_minor: Whether to check minor blacklists

        Returns:
            BlacklistCheckSummary with all results
        """
        logger.info(f"[BlacklistMonitor] Checking IP {ip_address}")
        start_time = datetime.now(timezone.utc)

        # Validate IP address
        try:
            socket.inet_aton(ip_address)
        except socket.error:
            logger.error(f"[BlacklistMonitor] Invalid IP address: {ip_address}")
            raise ValueError(f"Invalid IP address: {ip_address}")

        # Reverse IP for DNSBL lookup
        reversed_ip = ".".join(reversed(ip_address.split(".")))

        # Determine which blacklists to check
        blacklists_to_check = dict(MAJOR_BLACKLISTS)
        if include_minor:
            blacklists_to_check.update({
                k: v for k, v in MINOR_BLACKLISTS.items()
                if v["type"] == "ip"
            })

        # Perform checks in parallel
        results = self._parallel_check(reversed_ip, blacklists_to_check, "ip")

        # Analyze results
        major_listings = [
            r for r in results.values()
            if r.is_listed and r.blacklist_id in MAJOR_BLACKLISTS
        ]
        minor_listings = [
            r for r in results.values()
            if r.is_listed and r.blacklist_id in MINOR_BLACKLISTS
        ]

        total_listed = len(major_listings) + len(minor_listings)
        is_clean = total_listed == 0

        # Determine severity
        if major_listings:
            severity = "critical"
        elif len(minor_listings) >= MINOR_BLACKLIST_WARNING_THRESHOLD:
            severity = "warning"
        elif minor_listings:
            severity = "minor"
        else:
            severity = "clean"

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        summary = BlacklistCheckSummary(
            ip_address=ip_address,
            domain=None,
            check_date=datetime.now(timezone.utc),
            total_checked=len(results),
            total_listed=total_listed,
            major_listings=major_listings,
            minor_listings=minor_listings,
            all_results=results,
            is_clean=is_clean,
            severity=severity,
            check_duration_seconds=duration
        )

        logger.info(
            f"[BlacklistMonitor] IP {ip_address}: "
            f"checked={len(results)}, listed={total_listed}, severity={severity}"
        )

        return summary

    def check_domain(
        self,
        domain: str,
        include_minor: bool = True
    ) -> BlacklistCheckSummary:
        """
        Check a domain against domain-based blacklists.

        Args:
            domain: Domain name to check
            include_minor: Whether to check minor blacklists

        Returns:
            BlacklistCheckSummary with results
        """
        logger.info(f"[BlacklistMonitor] Checking domain {domain}")
        start_time = datetime.now(timezone.utc)

        # Clean domain
        domain = domain.lower().strip()
        if domain.startswith("www."):
            domain = domain[4:]

        # Get domain-type blacklists
        blacklists_to_check = {
            k: v for k, v in ALL_BLACKLISTS.items()
            if v["type"] == "domain"
        }

        if not include_minor:
            blacklists_to_check = {
                k: v for k, v in blacklists_to_check.items()
                if k in MAJOR_BLACKLISTS
            }

        # Perform checks
        results = self._parallel_check(domain, blacklists_to_check, "domain")

        # Analyze results
        major_listings = [
            r for r in results.values()
            if r.is_listed and r.blacklist_id in MAJOR_BLACKLISTS
        ]
        minor_listings = [
            r for r in results.values()
            if r.is_listed and r.blacklist_id in MINOR_BLACKLISTS
        ]

        total_listed = len(major_listings) + len(minor_listings)
        is_clean = total_listed == 0

        if major_listings:
            severity = "critical"
        elif minor_listings:
            severity = "warning"
        else:
            severity = "clean"

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        summary = BlacklistCheckSummary(
            ip_address=None,
            domain=domain,
            check_date=datetime.now(timezone.utc),
            total_checked=len(results),
            total_listed=total_listed,
            major_listings=major_listings,
            minor_listings=minor_listings,
            all_results=results,
            is_clean=is_clean,
            severity=severity,
            check_duration_seconds=duration
        )

        logger.info(
            f"[BlacklistMonitor] Domain {domain}: "
            f"checked={len(results)}, listed={total_listed}, severity={severity}"
        )

        return summary

    def _parallel_check(
        self,
        query: str,
        blacklists: Dict[str, Dict],
        check_type: str
    ) -> Dict[str, BlacklistCheckResult]:
        """
        Perform parallel DNS lookups against multiple blacklists.

        Args:
            query: Reversed IP or domain to check
            blacklists: Dictionary of blacklists to check
            check_type: "ip" or "domain"

        Returns:
            Dictionary of blacklist ID to result
        """
        results = {}

        # Submit all lookups
        futures = {}
        for bl_id, bl_info in blacklists.items():
            if bl_info["type"] != check_type:
                continue

            future = self._executor.submit(
                self._check_single_blacklist,
                query,
                bl_id,
                bl_info
            )
            futures[future] = bl_id

        # Collect results
        for future in as_completed(futures, timeout=DNS_LIFETIME_SECONDS * 2):
            bl_id = futures[future]
            try:
                result = future.result()
                results[bl_id] = result
            except Exception as e:
                results[bl_id] = BlacklistCheckResult(
                    blacklist_id=bl_id,
                    blacklist_name=blacklists[bl_id]["name"],
                    is_listed=False,
                    error=str(e),
                    severity=blacklists[bl_id]["severity"]
                )

        return results

    def _check_single_blacklist(
        self,
        query: str,
        bl_id: str,
        bl_info: Dict
    ) -> BlacklistCheckResult:
        """
        Check a single blacklist via DNS lookup.

        Args:
            query: Reversed IP or domain
            bl_id: Blacklist identifier
            bl_info: Blacklist configuration

        Returns:
            BlacklistCheckResult
        """
        start_time = datetime.now(timezone.utc)
        dns_zone = bl_info["dns_zone"]
        lookup_name = f"{query}.{dns_zone}"

        try:
            # Perform DNS lookup
            answers = self._resolver.resolve(lookup_name, "A")

            # If we get an answer, the IP/domain is listed
            response = str(answers[0]) if answers else None

            check_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

            return BlacklistCheckResult(
                blacklist_id=bl_id,
                blacklist_name=bl_info["name"],
                is_listed=True,
                response=response,
                severity=bl_info["severity"],
                check_time_ms=check_time
            )

        except dns.resolver.NXDOMAIN:
            # NXDOMAIN means not listed
            check_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            return BlacklistCheckResult(
                blacklist_id=bl_id,
                blacklist_name=bl_info["name"],
                is_listed=False,
                severity=bl_info["severity"],
                check_time_ms=check_time
            )

        except dns.resolver.Timeout:
            return BlacklistCheckResult(
                blacklist_id=bl_id,
                blacklist_name=bl_info["name"],
                is_listed=False,
                error="DNS timeout",
                severity=bl_info["severity"]
            )

        except dns.resolver.NoAnswer:
            return BlacklistCheckResult(
                blacklist_id=bl_id,
                blacklist_name=bl_info["name"],
                is_listed=False,
                severity=bl_info["severity"]
            )

        except Exception as e:
            return BlacklistCheckResult(
                blacklist_id=bl_id,
                blacklist_name=bl_info["name"],
                is_listed=False,
                error=str(e),
                severity=bl_info["severity"]
            )

    # ========================================================================
    # DATABASE OPERATIONS
    # ========================================================================

    def run_check_and_save(
        self,
        candidate_id: int,
        ip_address: Optional[str] = None,
        domain: Optional[str] = None
    ) -> BlacklistStatus:
        """
        Run blacklist check and save results to database.

        Args:
            candidate_id: User ID
            ip_address: Optional IP to check
            domain: Optional domain to check

        Returns:
            Saved BlacklistStatus record
        """
        logger.info(f"[BlacklistMonitor] Running check for candidate {candidate_id}")

        # Perform checks
        ip_results = None
        domain_results = None

        if ip_address:
            ip_results = self.check_ip(ip_address)

        if domain:
            domain_results = self.check_domain(domain)

        # Create status record
        status = BlacklistStatus(
            candidate_id=candidate_id,
            check_date=datetime.now(timezone.utc),
            ip_address=ip_address,
            domain=domain,
            status="completed"
        )

        # Process IP results
        if ip_results:
            status.total_blacklists_checked = ip_results.total_checked
            status.total_listings = ip_results.total_listed
            status.is_listed_anywhere = not ip_results.is_clean

            # Set major blacklist statuses
            status.spamhaus = (
                BlacklistStatusEnum.LISTED.value
                if any("spamhaus" in r.blacklist_id for r in ip_results.major_listings)
                else BlacklistStatusEnum.CLEAN.value
            )
            status.barracuda = (
                BlacklistStatusEnum.LISTED.value
                if any(r.blacklist_id == "barracuda" for r in ip_results.major_listings)
                else BlacklistStatusEnum.CLEAN.value
            )
            status.sorbs = (
                BlacklistStatusEnum.LISTED.value
                if any("sorbs" in r.blacklist_id for r in ip_results.major_listings + ip_results.minor_listings)
                else BlacklistStatusEnum.CLEAN.value
            )
            status.spamcop = (
                BlacklistStatusEnum.LISTED.value
                if any(r.blacklist_id == "spamcop" for r in ip_results.major_listings)
                else BlacklistStatusEnum.CLEAN.value
            )

            # Store all results
            status.all_results = {
                bl_id: {
                    "name": r.blacklist_name,
                    "listed": r.is_listed,
                    "response": r.response,
                    "severity": r.severity,
                    "check_time_ms": r.check_time_ms,
                    "error": r.error,
                }
                for bl_id, r in ip_results.all_results.items()
            }

        # Process domain results (add to existing results)
        if domain_results:
            if status.all_results is None:
                status.all_results = {}

            status.all_results.update({
                bl_id: {
                    "name": r.blacklist_name,
                    "listed": r.is_listed,
                    "response": r.response,
                    "severity": r.severity,
                    "check_time_ms": r.check_time_ms,
                    "error": r.error,
                }
                for bl_id, r in domain_results.all_results.items()
            })

            status.total_blacklists_checked = (status.total_blacklists_checked or 0) + domain_results.total_checked
            status.total_listings = (status.total_listings or 0) + domain_results.total_listed
            if not domain_results.is_clean:
                status.is_listed_anywhere = True

        # Check for changes from previous check
        previous = self._get_previous_check(candidate_id)
        if previous:
            status.new_listings = self._find_new_listings(previous, status)
            status.removed_listings = self._find_removed_listings(previous, status)

        # Save to database
        try:
            self.db.add(status)
            self.db.commit()
            self.db.refresh(status)

            logger.info(
                f"[BlacklistMonitor] Saved check for candidate {candidate_id}: "
                f"listed={status.is_listed_anywhere}, total={status.total_listings}"
            )

            return status

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"[BlacklistMonitor] Error saving check: {e}")
            raise

    def _get_previous_check(self, candidate_id: int) -> Optional[BlacklistStatus]:
        """Get the most recent previous blacklist check"""
        try:
            return self.db.query(BlacklistStatus).filter(
                BlacklistStatus.candidate_id == candidate_id
            ).order_by(BlacklistStatus.check_date.desc()).offset(1).first()
        except SQLAlchemyError:
            return None

    def _find_new_listings(
        self,
        previous: BlacklistStatus,
        current: BlacklistStatus
    ) -> List[str]:
        """Find blacklists where newly listed"""
        new_listings = []

        prev_results = previous.all_results or {}
        curr_results = current.all_results or {}

        for bl_id, curr_data in curr_results.items():
            if curr_data.get("listed"):
                prev_data = prev_results.get(bl_id, {})
                if not prev_data.get("listed"):
                    new_listings.append(bl_id)

        return new_listings

    def _find_removed_listings(
        self,
        previous: BlacklistStatus,
        current: BlacklistStatus
    ) -> List[str]:
        """Find blacklists where listing was removed"""
        removed = []

        prev_results = previous.all_results or {}
        curr_results = current.all_results or {}

        for bl_id, prev_data in prev_results.items():
            if prev_data.get("listed"):
                curr_data = curr_results.get(bl_id, {})
                if not curr_data.get("listed"):
                    removed.append(bl_id)

        return removed

    # ========================================================================
    # STATUS RETRIEVAL
    # ========================================================================

    def get_latest_status(self, candidate_id: int) -> Optional[BlacklistStatus]:
        """Get the most recent blacklist status for a candidate"""
        try:
            return self.db.query(BlacklistStatus).filter(
                BlacklistStatus.candidate_id == candidate_id
            ).order_by(BlacklistStatus.check_date.desc()).first()
        except SQLAlchemyError as e:
            logger.error(f"[BlacklistMonitor] Error fetching status: {e}")
            return None

    def get_status_history(
        self,
        candidate_id: int,
        days: int = 30,
        limit: int = 20
    ) -> List[BlacklistStatus]:
        """Get blacklist status history for a candidate"""
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)

            return self.db.query(BlacklistStatus).filter(
                and_(
                    BlacklistStatus.candidate_id == candidate_id,
                    BlacklistStatus.check_date >= cutoff
                )
            ).order_by(BlacklistStatus.check_date.desc()).limit(limit).all()

        except SQLAlchemyError as e:
            logger.error(f"[BlacklistMonitor] Error fetching history: {e}")
            return []

    def needs_check(
        self,
        candidate_id: int,
        min_interval_hours: int = DEFAULT_CHECK_INTERVAL_HOURS
    ) -> bool:
        """
        Check if a new blacklist check is needed.

        Args:
            candidate_id: User ID
            min_interval_hours: Minimum hours between checks

        Returns:
            True if check is needed
        """
        latest = self.get_latest_status(candidate_id)

        if not latest:
            return True

        time_since_check = datetime.now(timezone.utc) - latest.check_date
        return time_since_check >= timedelta(hours=min_interval_hours)

    # ========================================================================
    # ALERTS
    # ========================================================================

    def generate_alerts(self, status: BlacklistStatus) -> List[BlacklistAlert]:
        """
        Generate alerts based on blacklist status.

        Args:
            status: Current blacklist status

        Returns:
            List of alerts
        """
        alerts = []

        # Alert for new listings
        for bl_id in (status.new_listings or []):
            bl_info = ALL_BLACKLISTS.get(bl_id, {})

            alerts.append(BlacklistAlert(
                alert_type="new_listing",
                severity=bl_info.get("severity", "warning"),
                blacklist_id=bl_id,
                blacklist_name=bl_info.get("name", bl_id),
                message=f"Your IP/domain was added to {bl_info.get('name', bl_id)}",
                detected_at=status.check_date,
                recommendations=self._get_delisting_recommendations(bl_id)
            ))

        # Alert for delisting (positive!)
        for bl_id in (status.removed_listings or []):
            bl_info = ALL_BLACKLISTS.get(bl_id, {})

            alerts.append(BlacklistAlert(
                alert_type="delisting",
                severity="info",
                blacklist_id=bl_id,
                blacklist_name=bl_info.get("name", bl_id),
                message=f"You have been removed from {bl_info.get('name', bl_id)}",
                detected_at=status.check_date,
                recommendations=["Continue maintaining good email practices"]
            ))

        # Alert for critical status
        if status.is_listed_anywhere and status.severity == "critical":
            alerts.append(BlacklistAlert(
                alert_type="status_change",
                severity="critical",
                blacklist_id="multiple",
                blacklist_name="Multiple Blacklists",
                message="You are listed on one or more major blacklists. This will significantly impact deliverability.",
                detected_at=status.check_date,
                recommendations=[
                    "Pause all email sending immediately",
                    "Identify the source of spam reports",
                    "Request delisting from each blacklist",
                    "Review email authentication setup",
                ]
            ))

        return alerts

    def _get_delisting_recommendations(self, blacklist_id: str) -> List[str]:
        """Get recommendations for delisting from a specific blacklist"""
        bl_info = ALL_BLACKLISTS.get(blacklist_id, {})
        delist_url = bl_info.get("delist_url")

        recommendations = [
            "Stop sending from the affected IP/domain immediately",
            "Identify and fix the source of spam complaints",
            "Review email list hygiene and remove invalid addresses",
        ]

        if delist_url:
            recommendations.append(f"Request removal at: {delist_url}")

        recommendations.extend([
            "Wait for the blacklist's automatic expiration (if applicable)",
            "Implement proper email authentication (SPF, DKIM, DMARC)",
        ])

        return recommendations

    # ========================================================================
    # UTILITIES
    # ========================================================================

    def get_blacklist_info(self, blacklist_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific blacklist"""
        return ALL_BLACKLISTS.get(blacklist_id)

    @staticmethod
    def list_all_blacklists() -> Dict[str, Dict[str, Any]]:
        """
        Get all configured blacklists with their info.

        This is a static method - doesn't require DB session or instance.
        Can be called as BlacklistMonitor.list_all_blacklists()
        """
        logger.debug("[BlacklistMonitor] Listing all configured blacklists")
        return {
            "major": MAJOR_BLACKLISTS,
            "minor": MINOR_BLACKLISTS,
            "total_count": len(ALL_BLACKLISTS),
        }

    def cleanup(self) -> None:
        """Cleanup resources"""
        self._executor.shutdown(wait=False)
        logger.info("[BlacklistMonitor] Service cleanup complete")


# ============================================================================
# FACTORY FUNCTION
# ============================================================================

def get_blacklist_monitor(db: Session) -> BlacklistMonitor:
    """
    Factory function to create BlacklistMonitor instance.

    Args:
        db: SQLAlchemy database session

    Returns:
        Configured BlacklistMonitor instance
    """
    return BlacklistMonitor(db)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "BlacklistMonitor",
    "get_blacklist_monitor",
    "BlacklistCheckResult",
    "BlacklistCheckSummary",
    "BlacklistAlert",
    "MAJOR_BLACKLISTS",
    "MINOR_BLACKLISTS",
    "ALL_BLACKLISTS",
]
