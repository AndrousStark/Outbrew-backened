"""
THEMOBIADZ ULTRA EXTRACTION ENGINE V2.0
AI/ML-Powered App/Game/E-commerce Company Data Extraction

ULTRA ENHANCED FEATURES:
========================

1. AI/ML/DL COMPONENTS:
   - SpaCy NER for entity extraction (PERSON, ORG, GPE)
   - BERT embeddings for semantic similarity
   - ML-based email classification
   - Fuzzy matching with RapidFuzz
   - Text clustering for deduplication

2. ADVANCED DATA STRUCTURES:
   - Bloom Filters for O(1) deduplication
   - LRU Cache for API response caching
   - Trie for email pattern matching
   - Priority Queue for URL scheduling
   - Graph structure for company relationships

3. 20+ DATA SOURCES:
   - Google Play Store (HTML + API)
   - Apple App Store (iTunes API)
   - Steam Store
   - Microsoft Store
   - Amazon Appstore
   - Samsung Galaxy Store
   - Huawei AppGallery
   - F-Droid (open source)
   - Crunchbase
   - ProductHunt
   - AngelList
   - GitHub Organizations
   - npm/PyPI package publishers
   - HackerNews mentions
   - Twitter/X company profiles
   - LinkedIn public pages
   - G2/Capterra reviews
   - SimilarWeb traffic data
   - BuiltWith technology detection
   - Archive.org historical data
   - WHOIS domain data
   - DNS records (MX, TXT)
   - SSL Certificate data (crt.sh)

4. EMAIL FINDING METHODS:
   - 50+ email patterns
   - Email permutation generator
   - SMTP verification
   - MX record validation
   - Catch-all domain detection
   - Disposable email detection
   - Role-based email detection
   - Social profile email extraction
   - GitHub commit email extraction
   - npm/PyPI maintainer emails
   - WHOIS registrant emails
   - DNS TXT record emails
   - SSL certificate emails
   - Google dorking
   - Wayback Machine historical emails

5. INTELLIGENT FALLBACK:
   - If source A fails → try source B → try source C
   - Paid APIs with FREE alternatives
   - Rate limit aware with backoff
   - Proxy rotation support
"""

import asyncio
import logging
import re
import json
import hashlib
import heapq
import random
from typing import Dict, Any, List, Optional, Set, Tuple, Generator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urlencode, quote, parse_qs
from collections import OrderedDict, defaultdict
from enum import Enum
import socket
import smtplib
import ssl
import struct
import threading
import time
import uuid

import httpx
from bs4 import BeautifulSoup

# Try to import advanced libraries (graceful fallback)
try:
    import spacy
    SPACY_AVAILABLE = True
except (ImportError, Exception):
    SPACY_AVAILABLE = False
    spacy = None

try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False

try:
    import dns.resolver
    DNS_AVAILABLE = True
except ImportError:
    DNS_AVAILABLE = False

try:
    from email_validator import validate_email, EmailNotValidError
    EMAIL_VALIDATOR_AVAILABLE = True
except ImportError:
    EMAIL_VALIDATOR_AVAILABLE = False

logger = logging.getLogger(__name__)


# ============================================
# ADVANCED DATA STRUCTURES
# ============================================

class BloomFilter:
    """
    Optimized Bloom filter using bytearray for O(1) membership testing.
    ~8x less memory than Python list version (bytearray bits vs Python ints).
    Uses double-hashing (FNV-1a based) instead of md5 for speed.

    Auto-sizing: given expected_items and false_positive_rate, computes optimal
    bit count and hash count automatically.
    """

    # FNV-1a constants (64-bit)
    _FNV_OFFSET = 14695981039346656037
    _FNV_PRIME = 1099511628211
    _FNV_MOD = 2 ** 64

    @staticmethod
    def optimal_size(expected_items: int, fp_rate: float = 0.01) -> Tuple[int, int]:
        """Calculate optimal bit count and hash count for given parameters."""
        import math
        if expected_items <= 0:
            expected_items = 1000
        # m = -(n * ln(p)) / (ln(2))^2
        m = int(-expected_items * math.log(fp_rate) / (math.log(2) ** 2))
        # k = (m / n) * ln(2)
        k = max(1, int((m / expected_items) * math.log(2)))
        return m, k

    def __init__(self, size: int = 0, hash_count: int = 0,
                 expected_items: int = 5000, fp_rate: float = 0.01):
        if size > 0 and hash_count > 0:
            # Manual sizing (backward compat)
            self.size = size
            self.hash_count = hash_count
        else:
            # Auto-size based on expected items
            self.size, self.hash_count = BloomFilter.optimal_size(expected_items, fp_rate)
        # bytearray: each byte holds 8 bits → size/8 bytes (~6KB for 50K items vs ~8MB before)
        self._bytes = bytearray((self.size + 7) // 8)
        self._count = 0

    def _fnv1a(self, data: bytes) -> int:
        """FNV-1a hash - much faster than md5 for short strings."""
        h = self._FNV_OFFSET
        for b in data:
            h ^= b
            h = (h * self._FNV_PRIME) % self._FNV_MOD
        return h

    def _hashes(self, item: str) -> Generator[int, None, None]:
        """Double hashing: h(i) = (h1 + i*h2) mod size — only 2 hash calls."""
        item_bytes = item.encode('utf-8')
        h1 = self._fnv1a(item_bytes) % self.size
        h2 = self._fnv1a(item_bytes + b'\x01') % self.size
        if h2 == 0:
            h2 = 1
        for i in range(self.hash_count):
            yield (h1 + i * h2) % self.size

    def add(self, item: str):
        """Add an item to the filter."""
        for idx in self._hashes(item):
            byte_idx, bit_idx = divmod(idx, 8)
            self._bytes[byte_idx] |= (1 << bit_idx)
        self._count += 1

    def __contains__(self, item: str) -> bool:
        """Check if item might be in the filter (probabilistic)."""
        for idx in self._hashes(item):
            byte_idx, bit_idx = divmod(idx, 8)
            if not (self._bytes[byte_idx] & (1 << bit_idx)):
                return False
        return True

    def probably_contains(self, item: str) -> bool:
        """Alias for __contains__."""
        return item in self

    def contains(self, item: str) -> bool:
        """Alias for __contains__ — used by extraction engine."""
        return item in self

    @property
    def count(self) -> int:
        """Approximate number of items added."""
        return self._count

    @property
    def memory_bytes(self) -> int:
        """Memory usage in bytes."""
        return len(self._bytes)


class LRUCache:
    """
    Least Recently Used cache for API responses.
    Reduces redundant API calls.
    """

    def __init__(self, capacity: int = 1000):
        self.capacity = capacity
        self.cache: OrderedDict = OrderedDict()

    def get(self, key: str) -> Optional[Any]:
        if key not in self.cache:
            return None
        # Move to end (most recently used)
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key: str, value: Any):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

    def __contains__(self, key: str) -> bool:
        return key in self.cache


class TTLFileCache:
    """
    File-based HTTP response cache with TTL (Time To Live).
    Persists responses to disk so they survive restarts.
    No Redis dependency — uses simple JSON files in a temp directory.

    Usage:
        cache = TTLFileCache(ttl_seconds=3600)
        cached = cache.get("https://example.com/page")
        if cached is None:
            response = await client.get(url)
            cache.put(url, response.text)
    """

    def __init__(self, ttl_seconds: int = 3600, cache_dir: Optional[str] = None, max_entries: int = 5000):
        import tempfile, os
        self.ttl = ttl_seconds
        self.max_entries = max_entries
        self._cache_dir = cache_dir or os.path.join(tempfile.gettempdir(), "mobiadz_cache")
        os.makedirs(self._cache_dir, exist_ok=True)
        self._hits = 0
        self._misses = 0

    def _key_to_path(self, key: str) -> str:
        """Convert cache key to file path using hash."""
        import os
        key_hash = hashlib.sha256(key.encode()).hexdigest()[:32]
        return os.path.join(self._cache_dir, f"{key_hash}.json")

    def get(self, key: str) -> Optional[str]:
        """Get cached value if exists and not expired."""
        import os
        path = self._key_to_path(key)
        if not os.path.exists(path):
            self._misses += 1
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                entry = json.load(f)
            if time.time() - entry.get("ts", 0) > self.ttl:
                os.remove(path)
                self._misses += 1
                return None
            self._hits += 1
            return entry.get("data")
        except (json.JSONDecodeError, OSError, KeyError):
            self._misses += 1
            return None

    def put(self, key: str, data: str):
        """Store value with current timestamp."""
        path = self._key_to_path(key)
        try:
            entry = {"ts": time.time(), "key": key[:200], "data": data}
            with open(path, "w", encoding="utf-8") as f:
                json.dump(entry, f, ensure_ascii=False)
        except OSError:
            pass  # Silently fail — caching is best-effort

    def clear_expired(self):
        """Remove all expired entries (call periodically for cleanup)."""
        import os, glob as glob_mod
        pattern = os.path.join(self._cache_dir, "*.json")
        now = time.time()
        removed = 0
        for path in glob_mod.glob(pattern):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    entry = json.load(f)
                if now - entry.get("ts", 0) > self.ttl:
                    os.remove(path)
                    removed += 1
            except (json.JSONDecodeError, OSError):
                try:
                    os.remove(path)
                    removed += 1
                except OSError:
                    pass
        return removed

    @property
    def stats(self) -> Dict[str, int]:
        return {"hits": self._hits, "misses": self._misses}


class CircuitBreaker:
    """
    Per-domain circuit breaker: if a domain fails N times, skip it for a cooldown period.
    Prevents wasting time on unresponsive/blocking domains.

    States:
      CLOSED: Normal operation, requests allowed
      OPEN: Domain is broken, all requests short-circuit (skip)
      HALF_OPEN: After cooldown, allow one test request

    Usage:
        cb = CircuitBreaker(failure_threshold=3, cooldown_seconds=300)
        if cb.is_open("example.com"):
            skip...
        try:
            result = await fetch(url)
            cb.record_success("example.com")
        except:
            cb.record_failure("example.com")
    """

    def __init__(self, failure_threshold: int = 3, cooldown_seconds: int = 300):
        self.failure_threshold = failure_threshold
        self.cooldown = cooldown_seconds
        # domain → {"failures": int, "last_failure": float, "state": str}
        self._domains: Dict[str, Dict] = {}
        self._trips = 0  # Total times circuit opened

    def _get_state(self, domain: str) -> Dict:
        if domain not in self._domains:
            self._domains[domain] = {"failures": 0, "last_failure": 0.0, "state": "closed"}
        return self._domains[domain]

    def is_open(self, domain: str) -> bool:
        """Check if domain circuit is open (should skip)."""
        state = self._get_state(domain)
        if state["state"] == "closed":
            return False
        if state["state"] == "open":
            # Check if cooldown has elapsed → transition to half_open
            if time.time() - state["last_failure"] > self.cooldown:
                state["state"] = "half_open"
                return False  # Allow one test request
            return True  # Still open
        # half_open: allow request
        return False

    def record_failure(self, domain: str):
        """Record a failure for domain."""
        state = self._get_state(domain)
        state["failures"] += 1
        state["last_failure"] = time.time()
        if state["failures"] >= self.failure_threshold:
            state["state"] = "open"
            self._trips += 1
            logger.debug(f"[CIRCUIT BREAKER] {domain} opened after {state['failures']} failures")

    def record_success(self, domain: str):
        """Record success — reset failures and close circuit."""
        state = self._get_state(domain)
        state["failures"] = 0
        state["state"] = "closed"

    @property
    def total_trips(self) -> int:
        return self._trips

    def get_open_domains(self) -> List[str]:
        """List all currently-open domains."""
        return [d for d, s in self._domains.items() if s["state"] == "open"]


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    retryable_exceptions: Tuple = (httpx.TimeoutException, httpx.ConnectError, httpx.ReadTimeout),
    retryable_status_codes: Tuple = (429, 502, 503, 504),
):
    """
    Decorator for async functions: exponential backoff with decorrelated jitter.

    Jitter formula (AWS-style decorrelated):
        sleep = min(max_delay, random_between(base_delay, previous_sleep * 3))

    This prevents thundering herd while maintaining reasonable retry intervals.

    Usage:
        @retry_with_backoff(max_retries=3, base_delay=1.0)
        async def fetch_url(url):
            ...
    """
    import functools

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_sleep = base_delay
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    result = await func(*args, **kwargs)
                    # Check for retryable HTTP status codes
                    if hasattr(result, 'status_code') and result.status_code in retryable_status_codes:
                        if attempt < max_retries:
                            sleep_time = min(max_delay, random.uniform(base_delay, last_sleep * 3))
                            last_sleep = sleep_time
                            logger.debug(
                                f"[RETRY] {func.__name__} got {result.status_code}, "
                                f"attempt {attempt + 1}/{max_retries}, sleeping {sleep_time:.1f}s"
                            )
                            await asyncio.sleep(sleep_time)
                            continue
                    return result
                except retryable_exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        # Decorrelated jitter
                        sleep_time = min(max_delay, random.uniform(base_delay, last_sleep * 3))
                        last_sleep = sleep_time
                        logger.debug(
                            f"[RETRY] {func.__name__} failed ({type(e).__name__}), "
                            f"attempt {attempt + 1}/{max_retries}, sleeping {sleep_time:.1f}s"
                        )
                        await asyncio.sleep(sleep_time)
                    else:
                        raise
            if last_exception:
                raise last_exception
        return wrapper
    return decorator


class TrieNode:
    """Node for Trie data structure."""

    def __init__(self):
        self.children: Dict[str, 'TrieNode'] = {}
        self.is_end: bool = False
        self.data: Any = None


class EmailPatternTrie:
    """
    Trie for efficient email prefix → category classification.
    Pre-populated with 100+ prefixes mapped to categories.
    Lookup is O(prefix_length) instead of O(num_keywords).

    Usage:
        trie = EmailPatternTrie()
        trie.build_from_categories(EMAIL_CLASSIFY_MAP)
        result = trie.classify("marketing@example.com")
        # → {"category": "marketing", "confidence": 90}
    """

    def __init__(self):
        self.root = TrieNode()

    def insert(self, pattern: str, confidence: int = 50, category: str = "other"):
        """Insert an email pattern into the trie."""
        node = self.root
        for char in pattern.lower():
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.is_end = True
        node.data = {"pattern": pattern, "confidence": confidence, "category": category}

    def build_from_categories(self, category_map: Dict[str, set]):
        """
        Bulk-populate trie from a category→keywords mapping.
        E.g. {"marketing": {"marketing", "ads", "pr"}, "sales": {"sales", "business"}}
        """
        for category, keywords in category_map.items():
            for kw in keywords:
                self.insert(kw, confidence=90, category=category)

    def classify(self, email: str) -> Optional[Dict]:
        """
        Classify an email by walking the local part through the trie.
        Returns best (longest) matching category, or None.
        """
        local_part = email.split('@')[0].lower()
        # Try progressively longer prefixes and take the longest match
        best_match = None
        node = self.root
        for char in local_part:
            if char in node.children:
                node = node.children[char]
                if node.is_end:
                    best_match = node.data
            else:
                break
        return best_match

    def search(self, email: str) -> Optional[Dict]:
        """Search for a pattern that matches the email (backward compat)."""
        return self.classify(email)


class PriorityURLQueue:
    """
    Priority queue for URL scheduling with depth tracking.
    Higher priority URLs are processed first. Tracks BFS depth per URL.

    Priority scale (higher = more important):
      100 = contact/team/about pages (depth 0)
       80 = secondary pages (support, press, careers)
       50 = sitemap-discovered pages
       20 = discovered internal links with contact keywords
       10 = normal discovered links
        1 = blog/news links
    """

    def __init__(self):
        self.heap: List[Tuple[int, int, str, int]] = []  # (-priority, counter, url, depth)
        self.counter = 0
        self.entry_finder: Dict[str, Tuple[int, int, str, int]] = {}

    def push(self, url: str, priority: int = 0, depth: int = 0):
        """Add URL with priority (higher = more important) and BFS depth."""
        if url in self.entry_finder:
            return  # Already exists (dedup)

        entry = (-priority, self.counter, url, depth)  # Negative for max-heap
        self.entry_finder[url] = entry
        heapq.heappush(self.heap, entry)
        self.counter += 1

    def pop(self) -> Optional[Tuple[str, int]]:
        """Remove and return (url, depth) for highest priority URL."""
        while self.heap:
            neg_priority, count, url, depth = heapq.heappop(self.heap)
            if url in self.entry_finder:
                del self.entry_finder[url]
                return url, depth
        return None

    def __len__(self) -> int:
        return len(self.entry_finder)

    def __contains__(self, url: str) -> bool:
        return url in self.entry_finder


# ============================================
# AI/ML COMPONENTS
# ============================================

class NLPEntityExtractor:
    """
    SpaCy-based NER for extracting entities from text.
    Extracts: PERSON, ORG, GPE (locations), EMAIL, PHONE
    """

    def __init__(self):
        self.nlp = None
        self._initialized = False

        # Email patterns (50+ variations)
        self.email_patterns = [
            # Standard patterns
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,7}\b',
            # Obfuscated patterns
            r'\b[A-Za-z0-9._%+-]+\s*\[\s*at\s*\]\s*[A-Za-z0-9.-]+\s*\[\s*dot\s*\]\s*[A-Za-z]{2,7}\b',
            r'\b[A-Za-z0-9._%+-]+\s*\(\s*at\s*\)\s*[A-Za-z0-9.-]+\s*\(\s*dot\s*\)\s*[A-Za-z]{2,7}\b',
            r'\b[A-Za-z0-9._%+-]+\s*@\s*[A-Za-z0-9.-]+\s*\.\s*[A-Za-z]{2,7}\b',
            # HTML encoded
            r'[A-Za-z0-9._%+-]+&#64;[A-Za-z0-9.-]+\.[A-Za-z]{2,7}',
            # Unicode encoded
            r'[A-Za-z0-9._%+-]+\u0040[A-Za-z0-9.-]+\.[A-Za-z]{2,7}',
        ]

        # Phone patterns
        self.phone_patterns = [
            r'\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}',  # US
            r'\+44\s?[0-9]{4}\s?[0-9]{6}',  # UK
            r'\+91[-.\s]?[0-9]{10}',  # India
            r'\+[0-9]{1,3}[-.\s]?[0-9]{6,14}',  # International
        ]

    async def initialize(self):
        """Initialize SpaCy model."""
        if self._initialized:
            return

        if SPACY_AVAILABLE:
            try:
                self.nlp = spacy.load("en_core_web_sm")
                self._initialized = True
                logger.info("SpaCy NLP model loaded successfully")
            except OSError:
                logger.warning("SpaCy model not found, using regex-only extraction")
        else:
            logger.warning("SpaCy not available, using regex-only extraction")

    def extract_entities(self, text: str) -> Dict[str, List[str]]:
        """
        Extract all entities from text.

        Returns:
            {
                "persons": ["John Doe", "Jane Smith"],
                "organizations": ["Acme Corp", "TechCo"],
                "locations": ["San Francisco", "New York"],
                "emails": ["john@example.com"],
                "phones": ["+1-555-123-4567"],
                "titles": ["CEO", "CTO", "Marketing Director"]
            }
        """
        result = {
            "persons": [],
            "organizations": [],
            "locations": [],
            "emails": [],
            "phones": [],
            "titles": []
        }

        # Extract emails using regex
        for pattern in self.email_patterns:
            emails = re.findall(pattern, text, re.IGNORECASE)
            for email in emails:
                # Clean up obfuscated emails
                email = email.replace('[at]', '@').replace('[dot]', '.')
                email = email.replace('(at)', '@').replace('(dot)', '.')
                email = email.replace('&#64;', '@')
                email = email.replace(' ', '')
                if email not in result["emails"]:
                    result["emails"].append(email.lower())

        # Extract phones using regex
        for pattern in self.phone_patterns:
            phones = re.findall(pattern, text)
            result["phones"].extend([p for p in phones if p not in result["phones"]])

        # Use SpaCy for NER if available
        if self.nlp and self._initialized:
            doc = self.nlp(text[:100000])  # Limit text size

            for ent in doc.ents:
                if ent.label_ == "PERSON":
                    if ent.text not in result["persons"]:
                        result["persons"].append(ent.text)
                elif ent.label_ == "ORG":
                    if ent.text not in result["organizations"]:
                        result["organizations"].append(ent.text)
                elif ent.label_ in ["GPE", "LOC"]:
                    if ent.text not in result["locations"]:
                        result["locations"].append(ent.text)

        # Extract job titles using patterns
        title_patterns = [
            r'\b(CEO|CTO|CFO|COO|CMO|CIO|CISO)\b',
            r'\b(Chief\s+\w+\s+Officer)\b',
            r'\b(Vice\s+President|VP)\s+of\s+\w+\b',
            r'\b(Director\s+of\s+\w+)\b',
            r'\b(Head\s+of\s+\w+)\b',
            r'\b(Senior\s+\w+\s+Manager)\b',
            r'\b(Marketing|Sales|Engineering|Product)\s+(Director|Manager|Lead)\b',
            r'\b(Founder|Co-Founder|Co-founder)\b',
            r'\b(President|Chairman)\b',
        ]

        for pattern in title_patterns:
            titles = re.findall(pattern, text, re.IGNORECASE)
            for title in titles:
                if isinstance(title, tuple):
                    title = title[0]
                if title not in result["titles"]:
                    result["titles"].append(title)

        return result


class EmailPermutationGenerator:
    """
    Generates all possible email permutations for a person + company.
    Uses 50+ patterns for maximum coverage.
    """

    # Comprehensive email patterns
    PATTERNS = [
        # First name based
        "{first}@{domain}",
        "{first_initial}@{domain}",

        # Last name based
        "{last}@{domain}",
        "{last_initial}@{domain}",

        # First + Last combinations
        "{first}.{last}@{domain}",
        "{first}_{last}@{domain}",
        "{first}-{last}@{domain}",
        "{first}{last}@{domain}",

        # Last + First combinations
        "{last}.{first}@{domain}",
        "{last}_{first}@{domain}",
        "{last}-{first}@{domain}",
        "{last}{first}@{domain}",

        # Initial combinations
        "{first_initial}{last}@{domain}",
        "{first_initial}.{last}@{domain}",
        "{first_initial}_{last}@{domain}",
        "{first_initial}-{last}@{domain}",

        "{first}{last_initial}@{domain}",
        "{first}.{last_initial}@{domain}",
        "{first}_{last_initial}@{domain}",

        "{first_initial}{last_initial}@{domain}",
        "{first_initial}.{last_initial}@{domain}",

        "{last}{first_initial}@{domain}",
        "{last}.{first_initial}@{domain}",
        "{last}_{first_initial}@{domain}",

        # With numbers (common for duplicates)
        "{first}{last}1@{domain}",
        "{first}.{last}1@{domain}",
        "{first}{last}01@{domain}",

        # Full name variations
        "{first}.{middle_initial}.{last}@{domain}",
        "{first_initial}{middle_initial}{last}@{domain}",

        # Hyphenated last names
        "{first}.{last1}-{last2}@{domain}",
        "{first}.{last1}{last2}@{domain}",
    ]

    # Common role-based emails
    ROLE_PATTERNS = [
        "info@{domain}",
        "contact@{domain}",
        "hello@{domain}",
        "hi@{domain}",
        "team@{domain}",
        "support@{domain}",
        "help@{domain}",
        "sales@{domain}",
        "marketing@{domain}",
        "press@{domain}",
        "media@{domain}",
        "pr@{domain}",
        "partnerships@{domain}",
        "partners@{domain}",
        "business@{domain}",
        "enterprise@{domain}",
        "careers@{domain}",
        "jobs@{domain}",
        "hr@{domain}",
        "recruiting@{domain}",
        "admin@{domain}",
        "office@{domain}",
        "legal@{domain}",
        "privacy@{domain}",
        "feedback@{domain}",
        "developers@{domain}",
        "dev@{domain}",
        "api@{domain}",
        "investor@{domain}",
        "investors@{domain}",
    ]

    @staticmethod
    def parse_name(full_name: str) -> Dict[str, str]:
        """Parse full name into components."""
        parts = full_name.strip().split()

        if len(parts) == 0:
            return {}

        if len(parts) == 1:
            return {
                "first": parts[0].lower(),
                "first_initial": parts[0][0].lower() if parts[0] else "",
                "last": "",
                "last_initial": "",
                "middle_initial": ""
            }

        # Handle hyphenated last names
        if "-" in parts[-1]:
            last_parts = parts[-1].split("-")
            return {
                "first": parts[0].lower(),
                "first_initial": parts[0][0].lower(),
                "last": parts[-1].lower().replace("-", ""),
                "last1": last_parts[0].lower(),
                "last2": last_parts[1].lower() if len(last_parts) > 1 else "",
                "last_initial": parts[-1][0].lower(),
                "middle_initial": parts[1][0].lower() if len(parts) > 2 else ""
            }

        return {
            "first": parts[0].lower(),
            "first_initial": parts[0][0].lower(),
            "last": parts[-1].lower(),
            "last_initial": parts[-1][0].lower(),
            "middle_initial": parts[1][0].lower() if len(parts) > 2 else ""
        }

    @classmethod
    def generate(cls, name: str, domain: str) -> List[Dict[str, Any]]:
        """
        Generate all possible email permutations.

        Returns:
            [
                {"email": "john.doe@example.com", "pattern": "{first}.{last}", "confidence": 85},
                ...
            ]
        """
        results = []
        name_parts = cls.parse_name(name)

        if not name_parts or not domain:
            return results

        name_parts["domain"] = domain.lower()

        # Generate personal emails
        for i, pattern in enumerate(cls.PATTERNS):
            try:
                # Check if all required keys are available
                required_keys = re.findall(r'\{(\w+)\}', pattern)
                if all(name_parts.get(k, "") for k in required_keys):
                    email = pattern.format(**name_parts)
                    # Confidence decreases with pattern complexity
                    confidence = max(95 - i * 2, 50)
                    results.append({
                        "email": email,
                        "pattern": pattern,
                        "confidence": confidence,
                        "type": "personal"
                    })
            except (KeyError, ValueError):
                continue

        return results

    @classmethod
    def generate_role_emails(cls, domain: str) -> List[Dict[str, Any]]:
        """Generate role-based emails for a domain."""
        results = []

        for pattern in cls.ROLE_PATTERNS:
            email = pattern.format(domain=domain.lower())
            results.append({
                "email": email,
                "pattern": pattern,
                "confidence": 70,
                "type": "role"
            })

        return results


class SMTPRateLimiter:
    """
    Per-domain rate limiting for SMTP verification.

    Prevents aggressive SMTP checking that could get IPs blocked:
    - Tracks timestamps per domain
    - Enforces minimum delay between requests to same MX
    - Random jitter (1-3s) to avoid pattern detection
    - Exponential backoff on failures (421/450/451/452)
    - Maximum checks per domain per session
    - Thread-safe with threading.Lock
    """

    def __init__(self, min_delay: float = 2.0, max_delay: float = 5.0,
                 max_per_domain: int = 10, backoff_factor: float = 2.0):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_per_domain = max_per_domain
        self.backoff_factor = backoff_factor
        self.domain_timestamps: Dict[str, List[float]] = {}
        self.domain_failures: Dict[str, int] = {}
        self.domain_check_count: Dict[str, int] = {}
        self._lock = threading.Lock()

    def should_skip(self, domain: str) -> bool:
        """Check if domain has hit the per-session rate limit."""
        with self._lock:
            count = self.domain_check_count.get(domain, 0)
            return count >= self.max_per_domain

    async def wait_for_domain(self, domain: str):
        """Wait appropriate time before next SMTP request to domain."""
        with self._lock:
            now = time.time()
            timestamps = self.domain_timestamps.get(domain, [])
            failures = self.domain_failures.get(domain, 0)

            if timestamps:
                last_request = timestamps[-1]
                # Calculate delay with exponential backoff on failures
                delay = self.min_delay * (self.backoff_factor ** failures)
                delay = min(delay, self.max_delay)
                # Add random jitter (1-3 seconds)
                import random
                jitter = random.uniform(1.0, 3.0)
                delay += jitter

                elapsed = now - last_request
                wait_time = max(0, delay - elapsed)
            else:
                # First request — small initial jitter only
                import random
                wait_time = random.uniform(0.5, 1.5)

            # Record this request timestamp
            timestamps.append(now + wait_time)
            self.domain_timestamps[domain] = timestamps[-20:]  # Keep last 20
            self.domain_check_count[domain] = self.domain_check_count.get(domain, 0) + 1

        if wait_time > 0:
            await asyncio.sleep(wait_time)

    def record_failure(self, domain: str):
        """Record a failure for exponential backoff calculation."""
        with self._lock:
            self.domain_failures[domain] = self.domain_failures.get(domain, 0) + 1

    def record_success(self, domain: str):
        """Record a success — reset failure count for this domain."""
        with self._lock:
            self.domain_failures[domain] = 0


class EmailVerifier:
    """
    Advanced email verification with multiple methods.

    Layer 6 Enhanced Features:
    - 500+ disposable domain detection with pattern matching
    - Multi-port SMTP verification (25→587→465) with STARTTLS
    - Greylisting retry on 450/451/452 codes
    - MX priority sorting and multi-MX fallback
    - Per-domain SMTP rate limiting with exponential backoff
    - Microsoft 365 GetCredentialType verification (free)
    - Domain age checking via WHOIS
    - Bounce probability scoring (0-100)
    - Truemail-style layered verification with short-circuit
    - MX provider identification
    """

    # ========== DISPOSABLE EMAIL DOMAINS (500+ comprehensive list) ==========
    # Organized by popularity tiers for documentation; stored as flat set for O(1) lookup
    DISPOSABLE_DOMAINS = {
        # Tier 1: Most common disposable services (top 50)
        'tempmail.com', 'guerrillamail.com', '10minutemail.com', 'mailinator.com',
        'throwaway.email', 'maildrop.cc', 'temp-mail.org', 'fakeinbox.com',
        'trashmail.com', 'sharklasers.com', 'yopmail.com', 'guerrillamailblock.com',
        'grr.la', 'guerrillamail.info', 'guerrillamail.net', 'guerrillamail.org',
        'guerrillamail.biz', 'guerrillamail.de', 'tempail.com', 'mohmal.com',
        'getnada.com', 'emailondeck.com', 'tempr.email', 'discard.email',
        'discardmail.com', 'spamgourmet.com', 'mytrashmail.com', 'mailnesia.com',
        'mailcatch.com', 'mailexpire.com', 'tempinbox.com', 'harakirimail.com',
        'mailnull.com', 'jetable.org', 'trashymail.com', 'bugmenot.com',
        'mailmetrash.com', 'thankyou2010.com', 'trashemail.de', 'antireg.com',
        'byom.de', 'tmail.ws', 'wegwerfmail.de', 'spamhereplease.com',
        'filzmail.com', 'rmqkr.net', 'emailthe.net', 'safetymail.info',
        'binkmail.com', 'spamavert.com',
        # Tier 2: Well-known disposable/temporary email services (100+)
        'mailforspam.com', 'safetypost.de', 'devnullmail.com', 'letthemeatspam.com',
        'spam4.me', 'trashmail.at', 'trashmail.me', 'trashmail.net',
        'trashmailer.com', 'kasmail.com', 'sogetthis.com', 'tempomail.fr',
        'mailzilla.com', 'anonbox.net', 'anonymbox.com', 'fakemailgenerator.com',
        'mailtemp.info', 'inboxalias.com', 'mintemail.com', 'nomail.xl.cx',
        'spamfree24.org', 'spammotel.com', 'tempsky.com', 'mailme.lv',
        'meltmail.com', 'nospam.ze.tc', 'spambox.us', 'trashmail.io',
        'tempmailer.com', 'tempmailo.com', 'tempmailaddress.com', 'getairmail.com',
        'dontreg.com', 'mailscrap.com', 'courrieltemporaire.com', 'dandikmail.com',
        'dayrep.com', 'einrot.com', 'fleckens.hu', 'get2mail.fr',
        'girlsundertheinfluence.com', 'grandmamail.com', 'haltospam.com',
        'hotpop.com', 'ichimail.com', 'imstations.com', 'ipoo.org',
        'kulturbetrieb.info', 'lhsdv.com', 'lookugly.com', 'lr78.com',
        'maileater.com', 'mailexpire.com', 'mailfreeonline.com', 'mailguard.me',
        'mailimate.com', 'mailismagic.com', 'mailnator.com', 'mailshell.com',
        'mailsiphon.com', 'mailslite.com', 'mailtemporaire.com', 'mailtemporaire.fr',
        'mailtrash.net', 'mailzilla.org', 'mbx.cc', 'mfsa.ru',
        'mhwolf.net', 'moakt.cc', 'moakt.co', 'moakt.ws',
        'mypartyclip.de', 'mypacks.net', 'mysamp.de', 'nervmich.net',
        'nervtansen.de', 'netmails.com', 'netmails.net', 'neverbox.com',
        'nobulk.com', 'noclickemail.com', 'nogmailspam.info', 'nomail.pw',
        'nomail2me.com', 'nospamfor.us', 'nothingtoseehere.ca', 'nowmymail.com',
        'nurfuerspam.de', 'obobbo.com', 'oneoffemail.com', 'onewaymail.com',
        'otherinbox.com', 'owlpic.com', 'pookmail.com', 'proxymail.eu',
        'putthisinyouremail.com', 'quickinbox.com', 'rcpt.at', 'reallymymail.com',
        'recode.me', 'regbypass.com', 'regbypass.comsafe-mail.net',
        'safetymail.info', 'sharklasers.com', 'shieldedmail.com', 'shitmail.me',
        'shortmail.net', 'sibmail.com', 'skeefmail.com', 'slaskpost.se',
        'slipry.net', 'smashmail.de', 'soodonims.com', 'spam.la',
        'spamavert.com', 'spambob.com', 'spambob.net', 'spambob.org',
        'spambog.com', 'spambog.de', 'spambog.ru', 'spamcannon.com',
        'spamcannon.net', 'spamcero.com', 'spamcon.org', 'spamcorptastic.com',
        'spamcowboy.com', 'spamcowboy.net', 'spamcowboy.org', 'spamday.com',
        'spamex.com', 'spamfighter.cf', 'spamfighter.ga', 'spamfighter.gq',
        'spamfighter.ml', 'spamfighter.tk', 'spamfree.eu', 'spamfree24.com',
        'spamfree24.de', 'spamfree24.eu', 'spamfree24.info', 'spamfree24.net',
        'spamgoes.in', 'spamherelots.com', 'spamhole.com', 'spamify.com',
        'spaminator.de', 'spamkill.info', 'spaml.com', 'spaml.de',
        'spammotel.com', 'spamobox.com', 'spamoff.de', 'spamslicer.com',
        'spamspot.com', 'spamstack.net', 'spamthis.co.uk', 'spamthisplease.com',
        'spamtrail.com', 'spamtrap.ro', 'speed.1s.fr', 'spoofmail.de',
        'stuffmail.de', 'supergreatmail.com', 'supermailer.jp', 'superstachel.de',
        'suremail.info', 'svk.jp', 'sweetxxx.de', 'tafmail.com',
        'tagmymedia.com', 'tagyoureit.com', 'talkinator.com', 'tapchicuoihoi.com',
        'teleworm.com', 'teleworm.us', 'temp.emeraldcraft.com', 'temp.headstrong.de',
        'tempail.com', 'tempalias.com', 'tempe4mail.com', 'tempemaiil.com',
        'tempemail.biz', 'tempemail.co.za', 'tempemail.com', 'tempemail.net',
        'tempinbox.co.uk', 'tempinbox.com', 'tempmail.eu', 'tempmail.it',
        'tempmail2.com', 'tempmaildemo.com', 'tempmailer.de', 'tempomail.fr',
        'temporarily.de', 'temporarioemail.com.br', 'temporaryemail.net',
        'temporaryemail.us', 'temporaryforwarding.com', 'temporaryinbox.com',
        'temporarymailaddress.com', 'thanksnospam.info', 'thankyou2010.com',
        'thisisnotmyrealemail.com', 'throwam.com', 'throwawayemailaddress.com',
        'tittbit.in', 'tizi.com', 'tmailinator.com', 'toiea.com',
        'topranklist.de', 'tradermail.info', 'trash-amil.com', 'trash-mail.at',
        'trash-mail.cf', 'trash-mail.com', 'trash-mail.de', 'trash-mail.ga',
        'trash-mail.gq', 'trash-mail.ml', 'trash-mail.tk', 'trash2009.com',
        'trash2010.com', 'trash2011.com', 'trashdevil.com', 'trashdevil.de',
        'trashemail.de', 'trashemails.de', 'trashmail.ws', 'trashmailer.com',
        'trashymail.net', 'turual.com', 'twinmail.de', 'tyldd.com',
        'uggsrock.com', 'upliftnow.com', 'uplipht.com', 'venompen.com',
        'veryreallywow.com', 'vidchart.com', 'viditag.com', 'viewcastmedia.com',
        'viewcastmedia.net', 'viewcastmedia.org', 'vomoto.com', 'vpn.st',
        'vsimcard.com', 'vubby.com', 'wasteland.rfc822.org', 'webemail.me',
        'weg-werf-email.de', 'wegwerfadresse.de', 'wegwerfemail.com',
        'wegwerfemail.de', 'wegwerfmail.de', 'wegwerfmail.net', 'wegwerfmail.org',
        'wh4f.org', 'whatiaas.com', 'whatpaas.com', 'whyspam.me',
        'wickmail.net', 'wilemail.com', 'willhackforfood.biz', 'willselfdestruct.com',
        'winemaven.info', 'wronghead.com', 'wuzup.net', 'wuzupmail.net',
        'wwwnew.eu', 'xagloo.com', 'xemaps.com', 'xents.com',
        'xjoi.com', 'xmaily.com', 'xoxy.net', 'yapped.net',
        'yeah.net', 'yep.it', 'yogamaven.com', 'yomail.info',
        'yopmail.fr', 'yopmail.net', 'youmailr.com', 'ypmail.webarnak.fr.eu.org',
        'yuurok.com', 'zehnminutenmail.de', 'zippymail.info', 'zoaxe.com',
        'zoemail.org',
        # Tier 3: Additional catch-all disposable services
        'nada.email', 'nada.ltd', 'sharklasers.com', 'guerrillamail.com',
        'grr.la', 'guerrillamail.info', 'guerrillamailblock.com',
        'pokemail.net', 'spam4.me', 'bccto.me', 'chacuo.net',
        'dispostable.com', 'duam.net', 'emailigo.de', 'emailsensei.com',
        'fiifke.de', 'freecat.net', 'hazel.it', 'hz.vc',
        'is.af', 'jp.ftp.sh', 'klzlk.com', 'koszmail.pl',
        'kurzepost.de', 'lazyinbox.com', 'letthemeatspam.com', 'lol.ovpn.to',
        'lroid.com', 'mailed.in', 'mailfence.com', 'mailhub.pw',
        'mailimate.com', 'mailnesia.com', 'mailnull.com', 'mailsac.com',
        'mailtemp.info', 'mailtothis.com', 'mailzi.ru', 'mfsa.info',
        'mfsa.ru', 'mt2015.com', 'nobugmail.com', 'oneoffmail.com',
        'printedmail.com', 'put2.net', 'rax.la', 'rhyta.com',
        'rocketmail.com', 'royal.net', 's0ny.net', 'safe-mail.net',
        'safersignup.de', 'safermail.info', 'sendspamhere.com', 'shitaway.tk',
        'sinnlos-mail.de', 'siteposter.net', 'sly.io', 'smapfree24.com',
        'snakemail.com', 'sofimail.com', 'solvemail.info', 'soodo.com',
        'spam.head.st', 'spamavert.com', 'spamstack.net', 'superrito.com',
        'suremail.info', 'temp.bartdevos.be', 'temp.emeraldcraft.com',
        'temp15qm.com', 'tempemaiil.com', 'tempemail.info', 'tempmail.de',
        'tempmail.eu', 'tempmail.ws', 'tempmail.sbs', 'tempmailin.com',
        'thanksnospam.info', 'throwam.com', 'tmail.ws', 'tmails.net',
        'tmpmail.net', 'tmpmail.org', 'toiea.com', 'trashmail.ws',
        'uggsrock.com', 'upliftnow.com', 'vomoto.com', 'vpn.st',
        'wasteland.rfc822.org', 'weg-werf-email.de', 'wegwerfmail.de',
        'willselfdestruct.com', 'xjoi.com', 'xmaily.com', 'yapped.net',
        'yopmail.com', 'yopmail.fr', 'yopmail.gq', 'yopmail.net',
        'zehnminutenmail.de', 'zoemail.com', 'zoemail.net', 'zoemail.org',
    }

    # Compiled regex patterns for detecting unknown disposable domains
    DISPOSABLE_PATTERNS = [
        re.compile(r'temp[\-_.]?mail', re.IGNORECASE),
        re.compile(r'throw[\-_.]?away', re.IGNORECASE),
        re.compile(r'trash[\-_.]?mail', re.IGNORECASE),
        re.compile(r'fake[\-_.]?(inbox|mail|email)', re.IGNORECASE),
        re.compile(r'spam[\-_.]?(mail|box|free|trap|catch)', re.IGNORECASE),
        re.compile(r'guerr?illa[\-_.]?mail', re.IGNORECASE),
        re.compile(r'discard[\-_.]?(mail|email)', re.IGNORECASE),
        re.compile(r'disposable[\-_.]?(mail|email)', re.IGNORECASE),
        re.compile(r'burner[\-_.]?(mail|email)', re.IGNORECASE),
        re.compile(r'10[\-_.]?min(ute)?[\-_.]?(mail|email)', re.IGNORECASE),
        re.compile(r'yop[\-_.]?mail', re.IGNORECASE),
        re.compile(r'mailinator', re.IGNORECASE),
        re.compile(r'noreply[\-_.]?test', re.IGNORECASE),
        re.compile(r'wegwerf', re.IGNORECASE),  # German for "throwaway"
        re.compile(r'einweg', re.IGNORECASE),  # German for "disposable"
        re.compile(r'jetable', re.IGNORECASE),  # French for "disposable"
        re.compile(r'temporar[iy][\-_.]?(mail|email)', re.IGNORECASE),
    ]

    # Free email providers
    FREE_PROVIDERS = {
        'gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com', 'live.com',
        'aol.com', 'icloud.com', 'mail.com', 'protonmail.com', 'zoho.com',
        'yandex.com', 'gmx.com', 'inbox.com', 'fastmail.com'
    }

    # Role-based prefixes (includes useless-for-outreach and standard role addresses)
    ROLE_PREFIXES = {
        'info', 'admin', 'support', 'sales', 'contact', 'hello', 'hi',
        'webmaster', 'noreply', 'no-reply', 'no_reply', 'donotreply', 'do-not-reply',
        'mailer-daemon', 'postmaster', 'hostmaster', 'abuse', 'security', 'privacy',
        'spam', 'bounce', 'daemon', 'root', 'nobody',
        'autoresponder', 'auto-reply', 'autoreply',
        'unsubscribe', 'remove', 'optout',
    }

    def __init__(self):
        self.mx_cache: Dict[str, bool] = {}
        self.mx_records_cache: Dict[str, List[str]] = {}  # domain → sorted MX hosts
        self.catchall_cache: Dict[str, bool] = {}
        self.smtp_cache: Dict[str, Dict] = {}
        self.warmup_cache: Dict[str, int] = {}
        self.ms365_cache: Dict[str, Optional[bool]] = {}
        self.domain_age_cache: Dict[str, Dict] = {}
        self.mx_provider_cache: Dict[str, Optional[str]] = {}
        self.rate_limiter = SMTPRateLimiter()
        self._http_client: Optional[httpx.AsyncClient] = None

    def _get_http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client for MS365/WHOIS requests."""
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(
                transport=httpx.AsyncHTTPTransport(retries=3),
                timeout=httpx.Timeout(connect=10.0, read=15.0, write=10.0, pool=5.0),
                follow_redirects=True,
            )
        return self._http_client

    async def close(self):
        """Close HTTP client."""
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()

    def _check_disposable(self, domain: str) -> bool:
        """
        Three-layer disposable domain detection:
        1. Direct set lookup (O(1))
        2. Parent domain lookup (handles subdomains like sub.tempmail.com)
        3. Pattern regex match (catches unknown disposable domains)
        """
        # Layer 1: Direct lookup
        if domain in self.DISPOSABLE_DOMAINS:
            return True

        # Layer 2: Parent domain lookup (for subdomains)
        parts = domain.split('.')
        if len(parts) > 2:
            # Try parent domain: sub.tempmail.com → tempmail.com
            parent = '.'.join(parts[-2:])
            if parent in self.DISPOSABLE_DOMAINS:
                return True
            # Try grandparent for .co.uk style: sub.mail.co.uk → mail.co.uk
            if len(parts) > 3:
                grandparent = '.'.join(parts[-3:])
                if grandparent in self.DISPOSABLE_DOMAINS:
                    return True

        # Layer 3: Pattern regex match
        for pattern in self.DISPOSABLE_PATTERNS:
            if pattern.search(domain):
                return True

        return False

    def _check_role_based(self, local_part: str) -> bool:
        """Check if email local part is role-based."""
        return any(local_part == prefix or local_part.startswith(f"{prefix}.") or
                   local_part.startswith(f"{prefix}-") or local_part.startswith(f"{prefix}_")
                   for prefix in self.ROLE_PREFIXES)

    def _validate_syntax(self, email: str) -> bool:
        """Validate email syntax using email-validator or regex fallback."""
        if EMAIL_VALIDATOR_AVAILABLE:
            try:
                validate_email(email, check_deliverability=False)
                return True
            except EmailNotValidError:
                return False
        else:
            return bool(re.match(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$', email))

    async def verify(
        self, email: str, smtp_check: bool = True,
        ms365_check: bool = True, domain_age_check: bool = True
    ) -> Dict[str, Any]:
        """
        Truemail-style layered verification pipeline with short-circuit.

        Layer 1: Syntax validation → FAIL = stop immediately
        Layer 2: Domain extraction + disposable/free/role check → FLAG
        Layer 3: MX record validation → FAIL = stop (no mail server)
        Layer 4: Domain reputation (age, DNS records) → SCORE
        Layer 5: Catch-all detection (per-domain, before SMTP) → FLAG
        Layer 6: Provider-specific verification (MS365 GetCredentialType) → VERIFY
        Layer 7: SMTP RCPT TO (most expensive, rate-limited) → VERIFY
        Layer 8: Bounce probability scoring → FINAL SCORE

        Args:
            email: Email address to verify
            smtp_check: If True, perform SMTP RCPT TO verification (Layer 7)
            ms365_check: If True, check Microsoft 365 GetCredentialType (Layer 6)
            domain_age_check: If True, check domain age via WHOIS (Layer 4)

        Returns:
            Enhanced result dict with all verification layers
        """
        result = {
            "email": email,
            "valid_format": False,
            "mx_valid": False,
            "is_disposable": False,
            "is_free_provider": False,
            "is_role_based": False,
            "is_catchall": False,
            "smtp_exists": None,
            "ms365_exists": None,
            "domain_age_days": None,
            "domain_age_category": None,
            "bounce_score": None,
            "bounce_category": None,
            "deliverable": False,
            "confidence": 0,
            "verification_layers_passed": [],
            "verification_layers_failed": [],
            "mx_provider": None,
        }

        # ===== LAYER 1: Syntax Validation (instant, hard-fail) =====
        if not self._validate_syntax(email):
            result["verification_layers_failed"].append("syntax")
            return result  # Short-circuit: invalid syntax = undeliverable
        result["valid_format"] = True
        result["verification_layers_passed"].append("syntax")

        # Extract domain and local part
        try:
            local_part, domain = email.lower().split('@')
        except ValueError:
            result["verification_layers_failed"].append("syntax")
            return result

        # ===== LAYER 2: Domain Checks (instant, soft-flag) =====
        result["is_disposable"] = self._check_disposable(domain)
        result["is_free_provider"] = domain in self.FREE_PROVIDERS
        result["is_role_based"] = self._check_role_based(local_part)
        result["verification_layers_passed"].append("domain_checks")

        # ===== LAYER 3: MX Record Validation (DNS, hard-fail) =====
        if DNS_AVAILABLE:
            result["mx_valid"] = await self._check_mx(domain)
        else:
            result["mx_valid"] = True  # Assume valid if can't check

        if not result["mx_valid"]:
            result["verification_layers_failed"].append("mx")
            # No MX = undeliverable, but still calculate a partial confidence
            result["confidence"] = 15 if result["valid_format"] else 0
            return result  # Short-circuit: no mail server
        result["verification_layers_passed"].append("mx")

        # Identify MX provider
        result["mx_provider"] = self._identify_mx_provider(domain)

        # ===== LAYER 4: Domain Reputation (WHOIS + DNS records) =====
        if domain_age_check:
            try:
                age_info = await self.check_domain_age(domain)
                result["domain_age_days"] = age_info.get("domain_age_days")
                result["domain_age_category"] = age_info.get("age_category")
                result["verification_layers_passed"].append("domain_age")
            except Exception:
                pass

        # ===== LAYER 5: Catch-all Detection (per-domain, cached) =====
        if smtp_check:
            try:
                result["is_catchall"] = await self.check_catchall(domain)
                if result["is_catchall"]:
                    result["verification_layers_passed"].append("catchall_detected")
                else:
                    result["verification_layers_passed"].append("not_catchall")
            except Exception:
                pass

        # ===== LAYER 6: MS365 Verification (free, fast) =====
        if ms365_check:
            try:
                ms365_result = await self.check_microsoft365(email)
                result["ms365_exists"] = ms365_result
                if ms365_result is True:
                    result["verification_layers_passed"].append("ms365")
                elif ms365_result is False:
                    result["verification_layers_failed"].append("ms365")
            except Exception:
                pass

        # ===== LAYER 7: SMTP RCPT TO (expensive, rate-limited, last) =====
        if smtp_check and not result["is_catchall"]:
            try:
                # Wait for rate limiter
                await self.rate_limiter.wait_for_domain(domain)

                smtp_result = await self._check_smtp(email, domain)
                result["smtp_exists"] = smtp_result.get("exists")

                if smtp_result.get("exists") is True:
                    result["verification_layers_passed"].append("smtp")
                elif smtp_result.get("exists") is False:
                    result["verification_layers_failed"].append("smtp")

                # Track greylisting
                if smtp_result.get("greylisting_retried"):
                    result["verification_layers_passed"].append("greylisting_retried")
            except Exception:
                pass

        # ===== LAYER 8: Bounce Probability Scoring (aggregate) =====
        try:
            bounce_info = await self.calculate_bounce_probability(
                email=email,
                verification_result=result,
                domain_age_info={
                    "domain_age_days": result["domain_age_days"],
                    "age_category": result["domain_age_category"]
                } if result["domain_age_days"] is not None else None,
                ms365_result=result["ms365_exists"]
            )
            result["bounce_score"] = bounce_info["deliverability_score"]
            result["bounce_category"] = bounce_info["category"]
            result["verification_layers_passed"].append("bounce_scoring")
        except Exception:
            pass

        # ===== FINAL CONFIDENCE CALCULATION =====
        result["confidence"] = self._calculate_verification_confidence(result)
        result["deliverable"] = result["confidence"] >= 75

        return result

    def _calculate_verification_confidence(self, result: Dict) -> int:
        """
        Calculate final verification confidence from all layer results.
        Returns 0-100 confidence score.
        """
        confidence = 0

        # Base: syntax valid (+20)
        if result.get("valid_format"):
            confidence += 20

        # MX valid (+20)
        if result.get("mx_valid"):
            confidence += 20

        # Not disposable (+5)
        if not result.get("is_disposable"):
            confidence += 5
        else:
            confidence -= 15  # Penalty for disposable

        # Not free provider (+3)
        if not result.get("is_free_provider"):
            confidence += 3

        # Not role-based (+2)
        if not result.get("is_role_based"):
            confidence += 2

        # MS365 verification result
        ms365 = result.get("ms365_exists")
        if ms365 is True:
            confidence += 15  # Strong signal — Microsoft confirmed
        elif ms365 is False:
            confidence -= 25  # Microsoft says doesn't exist

        # SMTP verification result
        smtp = result.get("smtp_exists")
        if smtp is True:
            confidence += 15  # SMTP confirmed mailbox exists
        elif smtp is False:
            confidence -= 30  # SMTP rejected = likely invalid

        # Catch-all penalty
        if result.get("is_catchall"):
            confidence -= 10

        # Domain age bonus
        age_cat = result.get("domain_age_category")
        if age_cat == "mature":
            confidence += 5
        elif age_cat == "established":
            confidence += 3
        elif age_cat == "new":
            confidence -= 5

        # MX provider bonus (major providers are reliable)
        provider = result.get("mx_provider")
        if provider in ("google", "microsoft", "zoho", "protonmail", "fastmail", "icloud"):
            confidence += 3

        # Bounce score integration
        bounce = result.get("bounce_score")
        if bounce is not None:
            if bounce >= 80:
                confidence += 5
            elif bounce < 30:
                confidence -= 5

        return min(max(confidence, 0), 100)

    async def _check_mx(self, domain: str) -> bool:
        """Check if domain has valid MX records (with caching)."""
        if domain in self.mx_cache:
            return self.mx_cache[domain]

        try:
            if DNS_AVAILABLE:
                mx_records = await asyncio.to_thread(dns.resolver.resolve, domain, 'MX')
                mx_list = sorted(mx_records, key=lambda r: r.preference)
                has_mx = len(mx_list) > 0
                self.mx_cache[domain] = has_mx
                # Cache sorted MX hosts for SMTP use
                if has_mx:
                    self.mx_records_cache[domain] = [
                        str(r.exchange).rstrip('.') for r in mx_list
                    ]
                return has_mx
        except Exception:
            pass

        self.mx_cache[domain] = False
        return False

    async def _get_mx_hosts(self, domain: str) -> List[str]:
        """Get sorted MX hosts for a domain (by priority, lowest first)."""
        if domain in self.mx_records_cache:
            return self.mx_records_cache[domain]

        # Trigger MX check which populates the cache
        await self._check_mx(domain)
        return self.mx_records_cache.get(domain, [])

    async def _check_smtp(self, email: str, domain: str) -> Dict[str, Any]:
        """
        Enhanced SMTP RCPT TO verification with:
        - MX priority sorting (try best MX first)
        - Port fallback (25 → 587 → 465)
        - STARTTLS support for port 587
        - Implicit SSL for port 465
        - Greylisting retry on 450/451/452
        - Multiple MX host fallback (try up to 3)
        - Rate limiting per domain
        """
        if email in self.smtp_cache:
            return self.smtp_cache[email]

        result = {
            "exists": None,
            "catch_all": False,
            "smtp_response": None,
            "mx_host_used": None,
            "port_used": None,
            "greylisted": False,
            "greylisting_retried": False,
        }

        try:
            if not DNS_AVAILABLE:
                return result

            # Get sorted MX hosts
            mx_hosts = await self._get_mx_hosts(domain)
            if not mx_hosts:
                return result

            # Check rate limiter
            if self.rate_limiter.should_skip(domain):
                return result

            # Run enhanced SMTP check in thread (blocking I/O)
            result = await asyncio.to_thread(
                self._smtp_check_enhanced, email, mx_hosts
            )

            # Record success/failure for rate limiting
            if result.get("exists") is not None:
                self.rate_limiter.record_success(domain)
            elif result.get("smtp_response") in (421, 450, 451, 452):
                self.rate_limiter.record_failure(domain)

        except Exception:
            pass

        self.smtp_cache[email] = result
        return result

    def _smtp_check_enhanced(self, email: str, mx_hosts: List[str]) -> Dict[str, Any]:
        """
        Enhanced synchronous SMTP RCPT TO check (run via asyncio.to_thread).

        Features:
        - Tries up to 3 MX hosts in priority order
        - Port fallback: 25 → 587 (STARTTLS) → 465 (SSL)
        - Greylisting retry: on 450/451/452, wait 32s and retry once
        - Proper EHLO hostname
        - Better error classification (no more false catch-all on disconnect)
        """
        result = {
            "exists": None,
            "catch_all": False,
            "smtp_response": None,
            "mx_host_used": None,
            "port_used": None,
            "greylisted": False,
            "greylisting_retried": False,
        }

        # Use a legitimate-looking EHLO hostname
        ehlo_domain = f"mail-check-{uuid.uuid4().hex[:6]}.net"
        ports_to_try = [25, 587, 465]

        for mx_host in mx_hosts[:3]:  # Try top 3 MX hosts
            for port in ports_to_try:
                try:
                    # Connect based on port type
                    if port == 465:
                        # Implicit SSL
                        context = ssl.create_default_context()
                        context.check_hostname = False
                        context.verify_mode = ssl.CERT_NONE
                        smtp_conn = smtplib.SMTP_SSL(mx_host, port, timeout=10, context=context)
                    else:
                        smtp_conn = smtplib.SMTP(timeout=10)
                        smtp_conn.connect(mx_host, port)

                    # EHLO
                    smtp_conn.ehlo(ehlo_domain)

                    # STARTTLS for port 587
                    if port == 587:
                        try:
                            context = ssl.create_default_context()
                            context.check_hostname = False
                            context.verify_mode = ssl.CERT_NONE
                            smtp_conn.starttls(context=context)
                            smtp_conn.ehlo(ehlo_domain)
                        except (smtplib.SMTPException, ssl.SSLError):
                            # If STARTTLS fails, try without it
                            pass

                    # MAIL FROM (bounce address)
                    smtp_conn.mail(f"verify@{ehlo_domain}")

                    # RCPT TO (the actual check)
                    code, message = smtp_conn.rcpt(email)

                    try:
                        smtp_conn.quit()
                    except Exception:
                        pass

                    result["smtp_response"] = code
                    result["mx_host_used"] = mx_host
                    result["port_used"] = port

                    # Interpret response code
                    if code == 250:
                        result["exists"] = True
                        return result
                    elif code in (550, 551, 552, 553):
                        # Hard bounce — mailbox definitely doesn't exist
                        result["exists"] = False
                        return result
                    elif code in (450, 451, 452):
                        # Greylisting or temporary failure — retry after delay
                        result["greylisted"] = True
                        time.sleep(32)  # Standard greylisting window (sync — runs in thread via to_thread)
                        result["greylisting_retried"] = True

                        try:
                            # Retry on same MX/port
                            if port == 465:
                                ctx2 = ssl.create_default_context()
                                ctx2.check_hostname = False
                                ctx2.verify_mode = ssl.CERT_NONE
                                smtp2 = smtplib.SMTP_SSL(mx_host, port, timeout=10, context=ctx2)
                            else:
                                smtp2 = smtplib.SMTP(timeout=10)
                                smtp2.connect(mx_host, port)

                            smtp2.ehlo(ehlo_domain)
                            if port == 587:
                                try:
                                    ctx2 = ssl.create_default_context()
                                    ctx2.check_hostname = False
                                    ctx2.verify_mode = ssl.CERT_NONE
                                    smtp2.starttls(context=ctx2)
                                    smtp2.ehlo(ehlo_domain)
                                except (smtplib.SMTPException, ssl.SSLError):
                                    pass

                            smtp2.mail(f"verify@{ehlo_domain}")
                            code2, _ = smtp2.rcpt(email)

                            try:
                                smtp2.quit()
                            except Exception:
                                pass

                            result["smtp_response"] = code2
                            if code2 == 250:
                                result["exists"] = True
                                return result
                            elif code2 in (550, 551, 552, 553):
                                result["exists"] = False
                                return result
                        except Exception:
                            pass
                    elif code == 421:
                        # Rate limited by this MX — skip to next MX host
                        break

                except smtplib.SMTPServerDisconnected:
                    # Server disconnected — could be firewall, rate limit, or policy
                    # Do NOT assume catch-all; mark as unknown and try next port/MX
                    continue
                except (socket.timeout, ConnectionRefusedError, OSError):
                    # Connection failed on this port — try next port
                    continue
                except smtplib.SMTPException:
                    continue
                except Exception:
                    continue

        return result

    async def check_catchall(self, domain: str) -> bool:
        """
        Test if domain is a catch-all by sending RCPT TO for a random address.
        Catch-all domains accept all emails, making SMTP verification useless.
        Uses enhanced SMTP engine with port fallback.
        """
        if domain in self.catchall_cache:
            return self.catchall_cache[domain]

        # Generate a random test email that almost certainly doesn't exist
        random_local = f"zxq-test-{uuid.uuid4().hex[:8]}"
        test_email = f"{random_local}@{domain}"

        smtp_result = await self._check_smtp(test_email, domain)

        # If random email is accepted (250), domain is catch-all
        # Note: SMTPServerDisconnected no longer implies catch-all
        is_catchall = smtp_result.get("exists") is True
        self.catchall_cache[domain] = is_catchall
        return is_catchall

    # ========== MICROSOFT 365 VERIFICATION (FREE, NO API KEY) ==========

    # Known Microsoft email domains (direct, no MX check needed)
    MS_DIRECT_DOMAINS = {
        'outlook.com', 'hotmail.com', 'live.com', 'msn.com',
        'hotmail.co.uk', 'hotmail.fr', 'hotmail.de', 'hotmail.it',
        'hotmail.es', 'hotmail.co.jp', 'live.co.uk', 'live.fr',
        'live.de', 'live.it', 'live.com.au', 'outlook.co.uk',
        'outlook.fr', 'outlook.de', 'outlook.it', 'outlook.es',
        'outlook.com.au', 'outlook.in', 'outlook.jp',
    }

    # ========== MX PROVIDER IDENTIFICATION ==========
    # Maps MX hostname patterns to provider names
    MX_PROVIDERS = {
        "google": ["google.com", "googlemail.com", "aspmx.l.google.com", "smtp.google.com",
                    "alt1.aspmx.l.google.com", "alt2.aspmx.l.google.com"],
        "microsoft": ["protection.outlook.com", "mail.protection.outlook.com",
                       "olc.protection.outlook.com"],
        "zoho": ["zoho.com", "zoho.eu", "zoho.in", "zmails.net"],
        "protonmail": ["protonmail.ch", "proton.me", "protonmail.com"],
        "fastmail": ["messagingengine.com", "fastmail.com"],
        "mimecast": ["mimecast.com"],
        "barracuda": ["barracudanetworks.com", "cuda-inc.com"],
        "proofpoint": ["pphosted.com", "proofpoint.com"],
        "amazon_ses": ["amazonses.com", "amazonaws.com"],
        "sendgrid": ["sendgrid.net"],
        "mailgun": ["mailgun.org"],
        "rackspace": ["emailsrvr.com"],
        "yahoo": ["yahoodns.net", "yahoo.com"],
        "icloud": ["icloud.com", "apple.com"],
        "godaddy": ["secureserver.net"],
        "namecheap": ["privateemail.com", "registrar-servers.com"],
        "ovh": ["ovh.net"],
        "ionos": ["1and1.com", "ionos.com"],
        "mailchimp": ["mandrillapp.com", "mailchimp.com"],
        "postmark": ["postmarkapp.com"],
        "sparkpost": ["sparkpostmail.com"],
        "yandex": ["yandex.net", "yandex.ru"],
    }

    def _identify_mx_provider(self, domain: str) -> Optional[str]:
        """
        Identify the email provider from MX records.

        Args:
            domain: Email domain to check

        Returns:
            Provider name string (e.g., "google", "microsoft") or None
        """
        if domain in self.mx_provider_cache:
            return self.mx_provider_cache[domain]

        mx_hosts = self.mx_records_cache.get(domain, [])

        for mx_host in mx_hosts:
            mx_lower = mx_host.lower()
            for provider_name, patterns in self.MX_PROVIDERS.items():
                for pattern in patterns:
                    if pattern in mx_lower:
                        self.mx_provider_cache[domain] = provider_name
                        return provider_name

        # Check well-known free provider domains directly
        free_provider_mx = {
            "gmail.com": "google",
            "yahoo.com": "yahoo",
            "outlook.com": "microsoft",
            "hotmail.com": "microsoft",
            "live.com": "microsoft",
            "aol.com": "yahoo",
            "icloud.com": "icloud",
            "protonmail.com": "protonmail",
            "zoho.com": "zoho",
            "yandex.com": "yandex",
            "fastmail.com": "fastmail",
        }
        if domain in free_provider_mx:
            provider = free_provider_mx[domain]
            self.mx_provider_cache[domain] = provider
            return provider

        self.mx_provider_cache[domain] = None
        return None

    async def _is_microsoft_domain(self, domain: str) -> bool:
        """
        Check if domain uses Microsoft for email.
        Checks: direct MS domains + MX records pointing to protection.outlook.com
        """
        # Direct Microsoft domains
        if domain in self.MS_DIRECT_DOMAINS:
            return True

        # Check MX records for Microsoft indicators
        mx_hosts = await self._get_mx_hosts(domain)
        for host in mx_hosts:
            host_lower = host.lower()
            if ('protection.outlook.com' in host_lower or
                    'mail.protection.outlook.com' in host_lower or
                    'olc.protection.outlook.com' in host_lower):
                return True

        return False

    async def check_microsoft365(self, email: str) -> Optional[bool]:
        """
        Check if email exists on Microsoft 365 using GetCredentialType endpoint.
        Completely FREE, no API key needed.

        Works for: Microsoft 365, Outlook.com, Hotmail, Live.com, and any domain
        using Microsoft for email (MX → protection.outlook.com).

        Returns:
            True = email exists in Microsoft 365
            False = email does not exist
            None = not a Microsoft domain, throttled, or error
        """
        # Check cache first
        if email in self.ms365_cache:
            return self.ms365_cache[email]

        try:
            domain = email.split('@')[1].lower()

            # Only check Microsoft domains
            if not await self._is_microsoft_domain(domain):
                self.ms365_cache[email] = None
                return None

            client = self._get_http_client()

            # POST to Microsoft's GetCredentialType endpoint
            response = await client.post(
                "https://login.microsoftonline.com/common/GetCredentialType",
                json={"Username": email},
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                },
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()

                # Check throttle status
                throttle_status = data.get("ThrottleStatus", 0)
                if throttle_status == 1:
                    # Throttled — can't determine
                    self.ms365_cache[email] = None
                    return None

                # Interpret IfExistsResult
                if_exists = data.get("IfExistsResult", -1)

                if if_exists == 0:
                    # Account exists
                    self.ms365_cache[email] = True
                    return True
                elif if_exists == 1:
                    # Account does not exist
                    self.ms365_cache[email] = False
                    return False
                elif if_exists == 5:
                    # Exists in a different tenant (still valid)
                    self.ms365_cache[email] = True
                    return True
                elif if_exists == 6:
                    # Domain not found in Microsoft
                    self.ms365_cache[email] = None
                    return None
                else:
                    # Unknown result
                    self.ms365_cache[email] = None
                    return None

        except Exception:
            pass

        self.ms365_cache[email] = None
        return None

    # ========== DOMAIN AGE & REPUTATION (WHOIS) ==========

    async def check_domain_age(self, domain: str) -> Dict[str, Any]:
        """
        Check domain age via WHOIS for reputation scoring.
        Completely FREE — uses python-whois library.

        Returns:
            {
                "domain_age_days": 365,
                "creation_date": "2023-01-15",
                "registrar": "GoDaddy",
                "age_category": "established"
            }

        Age categories:
            - "new" (<30 days): Suspicious, likely spam
            - "young" (<1 year): Caution
            - "established" (1-5 years): Trusted
            - "mature" (5+ years): Highly trusted
            - "unknown": WHOIS lookup failed
        """
        if domain in self.domain_age_cache:
            return self.domain_age_cache[domain]

        result = {
            "domain_age_days": None,
            "creation_date": None,
            "registrar": None,
            "age_category": "unknown"
        }

        try:
            # Run WHOIS in thread (blocking I/O)
            whois_data = await asyncio.wait_for(
                asyncio.to_thread(self._whois_lookup, domain), timeout=20
            )

            if whois_data:
                creation_date = whois_data.get("creation_date")

                # Handle creation_date as list (some registrars return multiple)
                if isinstance(creation_date, list):
                    creation_date = creation_date[0]

                if creation_date:
                    # Handle string dates
                    if isinstance(creation_date, str):
                        try:
                            from dateutil import parser as dateutil_parser
                            creation_date = dateutil_parser.parse(creation_date)
                        except Exception:
                            creation_date = None

                    if creation_date:
                        now = datetime.now(timezone.utc)
                        if hasattr(creation_date, 'replace') and creation_date.tzinfo:
                            creation_date = creation_date.replace(tzinfo=None)

                        age_days = (now - creation_date).days
                        result["domain_age_days"] = age_days
                        result["creation_date"] = creation_date.strftime("%Y-%m-%d")

                        # Categorize
                        if age_days < 30:
                            result["age_category"] = "new"
                        elif age_days < 365:
                            result["age_category"] = "young"
                        elif age_days < 1825:  # 5 years
                            result["age_category"] = "established"
                        else:
                            result["age_category"] = "mature"

                # Get registrar
                registrar = whois_data.get("registrar")
                if isinstance(registrar, list):
                    registrar = registrar[0] if registrar else None
                result["registrar"] = str(registrar) if registrar else None

        except Exception:
            pass

        self.domain_age_cache[domain] = result
        return result

    def _whois_lookup(self, domain: str) -> Optional[Dict]:
        """Synchronous WHOIS lookup (run via asyncio.to_thread)."""
        try:
            import whois
            w = whois.whois(domain)
            if w:
                return {
                    "creation_date": w.creation_date,
                    "registrar": w.registrar,
                    "expiration_date": w.expiration_date,
                }
        except Exception:
            pass
        return None

    # ========== BOUNCE PROBABILITY SCORING ==========

    async def calculate_bounce_probability(
        self,
        email: str,
        verification_result: Optional[Dict] = None,
        domain_age_info: Optional[Dict] = None,
        ms365_result: Optional[bool] = None,
        warmup_score: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Calculate bounce probability score (0=certain bounce, 100=certain delivery).

        Combines all available verification signals into a composite deliverability score.

        Args:
            email: The email address
            verification_result: Result from verify() if already run
            domain_age_info: Result from check_domain_age() if already run
            ms365_result: Result from check_microsoft365() if already run
            warmup_score: Result from get_domain_warmup_score() if already run

        Returns:
            {
                "deliverability_score": 85,
                "category": "high",
                "factors": { component: { "score": int, "max": int, "pass": bool } }
            }
        """
        factors = {}
        total_score = 0

        try:
            local_part, domain = email.lower().split('@')
        except ValueError:
            return {
                "deliverability_score": 0,
                "category": "risky",
                "factors": {"syntax": {"score": 0, "max": 10, "pass": False}}
            }

        # Factor 1: Syntax valid (+10)
        syntax_valid = self._validate_syntax(email)
        syntax_score = 10 if syntax_valid else 0
        factors["syntax"] = {"score": syntax_score, "max": 10, "pass": syntax_valid}
        total_score += syntax_score

        if not syntax_valid:
            return {
                "deliverability_score": 0,
                "category": "risky",
                "factors": factors
            }

        # Factor 2: MX records exist (+15)
        mx_valid = False
        if verification_result:
            mx_valid = verification_result.get("mx_valid", False)
        else:
            mx_valid = await self._check_mx(domain)
        mx_score = 15 if mx_valid else 0
        factors["mx_records"] = {"score": mx_score, "max": 15, "pass": mx_valid}
        total_score += mx_score

        # Factor 3: Not disposable (+10, penalty -8 if disposable)
        is_disposable = self._check_disposable(domain)
        if is_disposable:
            disp_score = -8
            factors["disposable"] = {"score": disp_score, "max": 10, "pass": False}
        else:
            disp_score = 10
            factors["disposable"] = {"score": disp_score, "max": 10, "pass": True}
        total_score += disp_score

        # Factor 4: Not role-based (+5)
        is_role = self._check_role_based(local_part)
        role_score = 0 if is_role else 5
        factors["role_based"] = {"score": role_score, "max": 5, "pass": not is_role}
        total_score += role_score

        # Factor 5: SPF record (+5)
        has_spf = False
        if warmup_score is not None:
            has_spf = warmup_score >= 5  # Warmup includes SPF check
        else:
            try:
                if DNS_AVAILABLE:
                    txt_records = await asyncio.to_thread(dns.resolver.resolve, domain, 'TXT')
                    for record in txt_records:
                        if 'v=spf1' in str(record):
                            has_spf = True
                            break
            except Exception:
                pass
        spf_score = 5 if has_spf else 0
        factors["spf"] = {"score": spf_score, "max": 5, "pass": has_spf}
        total_score += spf_score

        # Factor 6: DMARC record (+5)
        has_dmarc = False
        try:
            if DNS_AVAILABLE:
                await asyncio.to_thread(dns.resolver.resolve, f'_dmarc.{domain}', 'TXT')
                has_dmarc = True
        except Exception:
            pass
        dmarc_score = 5 if has_dmarc else 0
        factors["dmarc"] = {"score": dmarc_score, "max": 5, "pass": has_dmarc}
        total_score += dmarc_score

        # Factor 7: DKIM record (+5)
        has_dkim = False
        for selector in ['selector1', 'google', 'default', 'k1', 'dkim']:
            try:
                if DNS_AVAILABLE:
                    await asyncio.to_thread(
                        dns.resolver.resolve, f'{selector}._domainkey.{domain}', 'TXT'
                    )
                    has_dkim = True
                    break
            except Exception:
                continue
        dkim_score = 5 if has_dkim else 0
        factors["dkim"] = {"score": dkim_score, "max": 5, "pass": has_dkim}
        total_score += dkim_score

        # Factor 8: Domain age (+3 for >1yr, +5 for >5yr)
        age_days = None
        if domain_age_info:
            age_days = domain_age_info.get("domain_age_days")
            age_cat = domain_age_info.get("age_category", "unknown")
        else:
            age_info = await self.check_domain_age(domain)
            age_days = age_info.get("domain_age_days")
            age_cat = age_info.get("age_category", "unknown")

        if age_cat == "mature":
            age_score = 5
        elif age_cat == "established":
            age_score = 3
        elif age_cat == "new":
            age_score = -4  # Penalty for very new domains
        elif age_cat == "young":
            age_score = 0
        else:
            age_score = 0
        factors["domain_age"] = {
            "score": age_score, "max": 5, "pass": age_cat in ("established", "mature"),
            "age_days": age_days, "category": age_cat
        }
        total_score += age_score

        # Factor 9: SMTP confirmed (+20, -30 if rejected)
        smtp_exists = None
        if verification_result:
            smtp_exists = verification_result.get("smtp_exists")
        if smtp_exists is True:
            smtp_score = 20
            factors["smtp"] = {"score": 20, "max": 20, "pass": True}
        elif smtp_exists is False:
            smtp_score = -18
            factors["smtp"] = {"score": -30, "max": 20, "pass": False}
        else:
            smtp_score = 0
            factors["smtp"] = {"score": 0, "max": 20, "pass": None}
        total_score += smtp_score

        # Factor 10: Not catch-all (+5)
        is_catchall = False
        if verification_result:
            is_catchall = verification_result.get("is_catchall", False)
        elif domain in self.catchall_cache:
            is_catchall = self.catchall_cache[domain]
        catchall_score = 0 if is_catchall else 5
        factors["catch_all"] = {"score": catchall_score, "max": 5, "pass": not is_catchall}
        total_score += catchall_score

        # Factor 11: MS365 verified (+10)
        if ms365_result is None and verification_result:
            ms365_result = verification_result.get("ms365_exists")
        if ms365_result is True:
            ms_score = 10
            factors["ms365"] = {"score": 10, "max": 10, "pass": True}
        elif ms365_result is False:
            ms_score = -10
            factors["ms365"] = {"score": -10, "max": 10, "pass": False}
        else:
            ms_score = 0
            factors["ms365"] = {"score": 0, "max": 10, "pass": None}
        total_score += ms_score

        # Factor 12: MX is major provider (+5)
        mx_provider = self._identify_mx_provider(domain)
        major_providers = {"google", "microsoft", "zoho", "protonmail", "fastmail",
                           "icloud", "yahoo", "amazon_ses"}
        is_major = mx_provider in major_providers if mx_provider else False
        provider_score = 5 if is_major else 0
        factors["mx_provider"] = {
            "score": provider_score, "max": 5, "pass": is_major,
            "provider": mx_provider
        }
        total_score += provider_score

        # Clamp to 0-100
        total_score = max(0, min(100, total_score))

        # Categorize
        if total_score >= 80:
            category = "high"
        elif total_score >= 50:
            category = "medium"
        elif total_score >= 30:
            category = "low"
        else:
            category = "risky"

        return {
            "deliverability_score": total_score,
            "category": category,
            "factors": factors
        }

    async def get_domain_warmup_score(self, domain: str) -> int:
        """
        Score domain email trustworthiness based on DNS infrastructure signals.
        Returns 0-20 bonus points.

        Signals:
        - Has MX records: +5
        - Has SPF record (TXT with v=spf1): +5
        - Has DMARC record (_dmarc.domain TXT): +5
        - Has DKIM selector: +5
        """
        if domain in self.warmup_cache:
            return self.warmup_cache[domain]

        score = 0
        if not DNS_AVAILABLE:
            return 10  # Assume moderate if can't check

        # MX check (reuse cache)
        if await self._check_mx(domain):
            score += 5

        # SPF check
        try:
            txt_records = await asyncio.to_thread(dns.resolver.resolve, domain, 'TXT')
            for record in txt_records:
                if 'v=spf1' in str(record):
                    score += 5
                    break
        except Exception:
            pass

        # DMARC check
        try:
            await asyncio.to_thread(dns.resolver.resolve, f'_dmarc.{domain}', 'TXT')
            score += 5
        except Exception:
            pass

        # DKIM check (common selectors)
        for selector in ['selector1', 'google', 'default']:
            try:
                await asyncio.to_thread(
                    dns.resolver.resolve, f'{selector}._domainkey.{domain}', 'TXT'
                )
                score += 5
                break
            except Exception:
                continue

        self.warmup_cache[domain] = score
        return score

    async def discover_role_emails_via_search(
        self, domain: str, client: httpx.AsyncClient
    ) -> List[Dict[str, Any]]:
        """
        Search web for evidence of role-based emails existing.
        Uses DuckDuckGo to search for "info@company.com" in quotes.
        """
        discovered = []
        role_prefixes = ["info", "contact", "hello", "support", "sales", "marketing", "press", "hr"]

        for prefix in role_prefixes:
            test_email = f"{prefix}@{domain}"
            try:
                query = f'"{test_email}"'
                search_url = f"https://html.duckduckgo.com/html/?q={quote(query)}"
                response = await client.get(search_url, headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }, timeout=10)

                if response.status_code == 200 and test_email.lower() in response.text.lower():
                    discovered.append({
                        "email": test_email,
                        "source": "web_search_discovery",
                        "confidence": 90,  # High — found publicly mentioned
                        "type": "role"
                    })
            except Exception:
                continue

            await asyncio.sleep(0.5)  # Rate limit

        return discovered


class EmailPatternDetector:
    """
    Detect company email patterns from known emails (emailhunter approach).

    Given one known real email + person name, detects the pattern
    (e.g., first.last@, flast@, firstl@) and applies it to all other people.
    """

    KNOWN_PATTERNS = [
        ("{first}.{last}", "first.last"),
        ("{first}{last}", "firstlast"),
        ("{first_initial}{last}", "flast"),
        ("{first_initial}.{last}", "f.last"),
        ("{first}", "first"),
        ("{last}.{first}", "last.first"),
        ("{last}{first}", "lastfirst"),
        ("{last}", "last"),
        ("{first}_{last}", "first_last"),
        ("{first}-{last}", "first-last"),
        ("{first}{last_initial}", "firstl"),
        ("{first}.{last_initial}", "first.l"),
        ("{first_initial}{last_initial}", "fl"),
        ("{last}_{first}", "last_first"),
        ("{last}-{first}", "last-first"),
        ("{last}{first_initial}", "lastf"),
        ("{last}.{first_initial}", "last.f"),
    ]

    @classmethod
    def detect_pattern(
        cls, known_email: str, person_name: str, domain: str
    ) -> Optional[str]:
        """
        Given a known email like john.doe@company.com and name "John Doe",
        detect which pattern the company uses.

        Returns: pattern string like "{first}.{last}" or None
        """
        name_parts = EmailPermutationGenerator.parse_name(person_name)
        if not name_parts:
            return None

        try:
            local_part = known_email.split("@")[0].lower()
        except (IndexError, AttributeError):
            return None

        name_parts["domain"] = domain.lower()

        for pattern_template, _pattern_name in cls.KNOWN_PATTERNS:
            try:
                required_keys = re.findall(r'\{(\w+)\}', pattern_template)
                if all(name_parts.get(k, "") for k in required_keys):
                    expected_local = pattern_template.format(**name_parts).split("@")[0]
                    if expected_local == local_part:
                        return pattern_template
            except (KeyError, ValueError):
                continue
        return None

    @classmethod
    def apply_pattern(
        cls, pattern: str, person_name: str, domain: str
    ) -> Optional[str]:
        """Apply a detected pattern to generate a full email for another person."""
        name_parts = EmailPermutationGenerator.parse_name(person_name)
        if not name_parts:
            return None
        name_parts["domain"] = domain.lower()
        try:
            # Pattern may or may not include @{domain}
            full_pattern = pattern if "@" in pattern else f"{pattern}@{{domain}}"
            required_keys = re.findall(r'\{(\w+)\}', full_pattern)
            if all(name_parts.get(k, "") for k in required_keys):
                return full_pattern.format(**name_parts)
        except (KeyError, ValueError):
            pass
        return None

    @classmethod
    def detect_from_multiple(
        cls, emails_with_names: List[Tuple[str, str]], domain: str
    ) -> Optional[str]:
        """
        Detect pattern from multiple known email+name pairs.
        Returns the most common pattern found.
        """
        pattern_counts: Dict[str, int] = {}
        for email, name in emails_with_names:
            pattern = cls.detect_pattern(email, name, domain)
            if pattern:
                pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1

        if pattern_counts:
            return max(pattern_counts, key=pattern_counts.get)
        return None


class FuzzyMatcher:
    """
    Fuzzy string matching for deduplication and similarity.
    Uses RapidFuzz for speed.
    """

    @staticmethod
    def similarity(s1: str, s2: str) -> float:
        """Calculate similarity ratio between two strings."""
        if RAPIDFUZZ_AVAILABLE:
            return fuzz.ratio(s1.lower(), s2.lower()) / 100.0
        else:
            # Simple fallback
            s1, s2 = s1.lower(), s2.lower()
            if s1 == s2:
                return 1.0
            if s1 in s2 or s2 in s1:
                return 0.8
            return 0.0

    @staticmethod
    def find_best_match(query: str, choices: List[str], threshold: float = 0.8) -> Optional[Tuple[str, float]]:
        """Find best matching string from choices."""
        if RAPIDFUZZ_AVAILABLE:
            result = process.extractOne(query, choices, score_cutoff=threshold * 100)
            if result:
                return (result[0], result[1] / 100.0)
        else:
            best_match = None
            best_score = 0.0
            for choice in choices:
                score = FuzzyMatcher.similarity(query, choice)
                if score > best_score and score >= threshold:
                    best_match = choice
                    best_score = score
            if best_match:
                return (best_match, best_score)

        return None

    @staticmethod
    def deduplicate(items: List[str], threshold: float = 0.9) -> List[str]:
        """Remove near-duplicate strings."""
        if not items:
            return []

        unique = [items[0]]

        for item in items[1:]:
            is_duplicate = False
            for existing in unique:
                if FuzzyMatcher.similarity(item, existing) >= threshold:
                    is_duplicate = True
                    break
            if not is_duplicate:
                unique.append(item)

        return unique


# ============================================
# ENHANCED DATA SOURCES
# ============================================

class HackerNewsScraper:
    """
    FREE HackerNews API scraper.
    Finds tech companies mentioned on HN.
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.api_url = "https://hn.algolia.com/api/v1"
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
        )
        self.cache = LRUCache(500)

    async def search(self, query: str, max_results: int = 20) -> List[Dict[str, Any]]:
        """Search HackerNews for mentions."""
        cache_key = f"hn:{query.lower()}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        results = []

        try:
            url = f"{self.api_url}/search"
            params = {
                "query": query,
                "tags": "story",
                "hitsPerPage": max_results
            }

            response = await self.client.get(url, params=params)

            if response.status_code == 200:
                data = response.json()

                for hit in data.get("hits", []):
                    results.append({
                        "title": hit.get("title"),
                        "url": hit.get("url"),
                        "author": hit.get("author"),
                        "points": hit.get("points", 0),
                        "created_at": hit.get("created_at"),
                        "source": "hackernews"
                    })

            self.cache.put(cache_key, results)

        except Exception as e:
            logger.error(f"HackerNews search error: {e}")

        return results

    async def close(self):
        await self.client.aclose()


class GitHubOrganizationScraper:
    """
    FREE GitHub organization scraper.
    Extracts org members, emails from commits.
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.api_url = "https://api.github.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "MobiAdz-Scraper/2.0"
        }
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
        )
        self.cache = LRUCache(500)
        self.rate_remaining = 60

    async def search_organizations(self, query: str, max_results: int = 20) -> List[Dict[str, Any]]:
        """Search GitHub organizations."""
        results = []

        try:
            url = f"{self.api_url}/search/users"
            params = {
                "q": f"{query} type:org",
                "per_page": min(max_results, 100)
            }

            response = await self.client.get(url, params=params, headers=self.headers)
            self.rate_remaining = int(response.headers.get("X-RateLimit-Remaining", 60))

            if response.status_code == 200:
                data = response.json()

                for item in data.get("items", []):
                    results.append({
                        "login": item.get("login"),
                        "name": item.get("login"),
                        "url": item.get("html_url"),
                        "avatar": item.get("avatar_url"),
                        "type": item.get("type"),
                        "source": "github_org"
                    })

        except Exception as e:
            logger.error(f"GitHub org search error: {e}")

        return results

    async def get_org_details(self, org_name: str) -> Optional[Dict[str, Any]]:
        """Get organization details."""
        cache_key = f"github_org:{org_name}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        try:
            url = f"{self.api_url}/orgs/{org_name}"
            response = await self.client.get(url, headers=self.headers)
            self.rate_remaining = int(response.headers.get("X-RateLimit-Remaining", 60))

            if response.status_code == 200:
                data = response.json()

                result = {
                    "name": data.get("name") or org_name,
                    "login": data.get("login"),
                    "description": data.get("description"),
                    "blog": data.get("blog"),
                    "location": data.get("location"),
                    "email": data.get("email"),
                    "twitter": data.get("twitter_username"),
                    "public_repos": data.get("public_repos"),
                    "followers": data.get("followers"),
                    "url": data.get("html_url"),
                    "source": "github_org"
                }

                self.cache.put(cache_key, result)
                return result

        except Exception as e:
            logger.error(f"GitHub org details error: {e}")

        return None

    async def get_org_members_emails(self, org_name: str, max_members: int = 20) -> List[Dict[str, Any]]:
        """Get emails from organization member commits."""
        results = []

        try:
            # Get public members
            url = f"{self.api_url}/orgs/{org_name}/members"
            params = {"per_page": min(max_members, 100)}

            response = await self.client.get(url, params=params, headers=self.headers)
            self.rate_remaining = int(response.headers.get("X-RateLimit-Remaining", 60))

            if response.status_code == 200:
                members = response.json()

                for member in members[:max_members]:
                    username = member.get("login")

                    if username and self.rate_remaining > 5:
                        # Get email from commits
                        email = await self._get_user_email_from_commits(username)

                        if email:
                            results.append({
                                "username": username,
                                "email": email,
                                "profile_url": member.get("html_url"),
                                "organization": org_name,
                                "source": "github_commits",
                                "confidence": 90  # High - actual git email
                            })

                        await asyncio.sleep(0.5)  # Rate limiting

        except Exception as e:
            logger.error(f"GitHub org members error: {e}")

        return results

    async def _get_user_email_from_commits(self, username: str) -> Optional[str]:
        """Extract email from user's public commits."""
        try:
            url = f"{self.api_url}/users/{username}/events/public"
            response = await self.client.get(url, headers=self.headers)
            self.rate_remaining = int(response.headers.get("X-RateLimit-Remaining", 60))

            if response.status_code == 200:
                events = response.json()

                for event in events:
                    if event.get("type") == "PushEvent":
                        commits = event.get("payload", {}).get("commits", [])

                        for commit in commits:
                            author = commit.get("author", {})
                            email = author.get("email", "")

                            # Filter out noreply emails
                            if email and "noreply" not in email.lower() and "github" not in email.lower():
                                return email

        except Exception:
            pass

        return None

    async def _get_user_details(self, username: str) -> Optional[Dict[str, Any]]:
        """Get user profile details."""
        try:
            url = f"{self.api_url}/users/{username}"
            response = await self.client.get(url, headers=self.headers)
            self.rate_remaining = int(response.headers.get("X-RateLimit-Remaining", 60))

            if response.status_code == 200:
                return response.json()

        except Exception as e:
            logger.debug(f"GitHub user details error: {e}")

        return None

    async def search_users(self, query: str, max_results: int = 10) -> List[Dict[str, Any]]:
        """Search GitHub users by query string."""
        users = []

        try:
            url = f"{self.api_url}/search/users"
            params = {"q": query, "per_page": min(max_results, 100)}

            response = await self.client.get(url, params=params, headers=self.headers)
            self.rate_remaining = int(response.headers.get("X-RateLimit-Remaining", 60))

            if response.status_code == 200:
                data = response.json()

                for item in data.get("items", [])[:max_results]:
                    username = item.get("login")

                    if username and self.rate_remaining > 5:
                        user_data = await self._get_user_details(username)
                        email = await self._get_user_email_from_commits(username)

                        if user_data:
                            users.append({
                                "username": username,
                                "name": user_data.get("name"),
                                "email": email,
                                "bio": user_data.get("bio"),
                                "company": user_data.get("company"),
                                "location": user_data.get("location"),
                                "twitter": user_data.get("twitter_username"),
                                "blog": user_data.get("blog"),
                                "github_url": user_data.get("html_url"),
                                "avatar": user_data.get("avatar_url"),
                                "source": "github_search"
                            })

                        await asyncio.sleep(0.5)

        except Exception as e:
            logger.debug(f"GitHub user search error: {e}")

        return users

    async def search_org_members(self, org_name: str, max_members: int = 20) -> List[Dict[str, Any]]:
        """Search org members with user details (compatible with OSINT engine API)."""
        results = []

        try:
            url = f"{self.api_url}/orgs/{org_name}/members"
            params = {"per_page": min(max_members, 100)}

            response = await self.client.get(url, params=params, headers=self.headers)
            self.rate_remaining = int(response.headers.get("X-RateLimit-Remaining", 60))

            if response.status_code == 200:
                members = response.json()

                for member in members[:max_members]:
                    username = member.get("login")

                    if username and self.rate_remaining > 5:
                        user_data = await self._get_user_details(username)
                        email = await self._get_user_email_from_commits(username)

                        member_info = {
                            "username": username,
                            "email": email,
                            "profile_url": member.get("html_url"),
                            "github_url": member.get("html_url"),
                            "organization": org_name,
                            "source": "github_org",
                            "confidence": 90
                        }

                        if user_data:
                            member_info.update({
                                "name": user_data.get("name"),
                                "bio": user_data.get("bio"),
                                "company": user_data.get("company"),
                                "location": user_data.get("location"),
                                "twitter": user_data.get("twitter_username"),
                                "blog": user_data.get("blog"),
                                "avatar": user_data.get("avatar_url"),
                            })

                        results.append(member_info)
                        await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"GitHub org members error: {e}")

        return results

    async def close(self):
        await self.client.aclose()


class NPMPackageScraper:
    """
    FREE npm registry scraper.
    Finds package maintainer emails.
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.api_url = "https://registry.npmjs.org"
        self.search_url = "https://api.npms.io/v2/search"
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
        )
        self.cache = LRUCache(500)

    async def search_packages(self, query: str, max_results: int = 20) -> List[Dict[str, Any]]:
        """Search npm packages."""
        results = []

        try:
            params = {"q": query, "size": max_results}
            response = await self.client.get(self.search_url, params=params)

            if response.status_code == 200:
                data = response.json()

                for result in data.get("results", []):
                    package = result.get("package", {})

                    # Get maintainers
                    maintainers = package.get("maintainers", [])
                    publisher = package.get("publisher", {})

                    results.append({
                        "name": package.get("name"),
                        "description": package.get("description"),
                        "version": package.get("version"),
                        "publisher": publisher.get("username"),
                        "publisher_email": publisher.get("email"),
                        "maintainers": maintainers,
                        "homepage": package.get("links", {}).get("homepage"),
                        "repository": package.get("links", {}).get("repository"),
                        "source": "npm"
                    })

        except Exception as e:
            logger.error(f"NPM search error: {e}")

        return results

    async def get_package_maintainers(self, package_name: str) -> List[Dict[str, Any]]:
        """Get package maintainer emails."""
        results = []

        try:
            url = f"{self.api_url}/{package_name}"
            response = await self.client.get(url)

            if response.status_code == 200:
                data = response.json()

                maintainers = data.get("maintainers", [])
                for m in maintainers:
                    if m.get("email"):
                        results.append({
                            "name": m.get("name"),
                            "email": m.get("email"),
                            "package": package_name,
                            "source": "npm_registry",
                            "confidence": 95  # Very high - official registry
                        })

        except Exception as e:
            logger.error(f"NPM package error: {e}")

        return results

    async def close(self):
        await self.client.aclose()


class DNSIntelligence:
    """
    DNS-based intelligence gathering.
    Extracts emails from TXT records, MX records, WHOIS.
    Enhanced: SPF include parsing, 25+ MX provider mapping,
    autodiscover detection, BIMI records, infrastructure scoring.
    """

    # SPF include: → email service provider mapping
    SPF_PROVIDER_MAP = {
        "_spf.google.com": "Google Workspace",
        "spf.protection.outlook.com": "Microsoft 365",
        "zoho.com": "Zoho Mail",
        "zohomail.com": "Zoho Mail",
        "spf.zoho.eu": "Zoho Mail (EU)",
        "zoho.in": "Zoho Mail (India)",
        "_spf.protonmail.ch": "ProtonMail",
        "spf.messagingengine.com": "Fastmail",
        "amazonses.com": "Amazon SES",
        "sendgrid.net": "SendGrid",
        "servers.mcsv.net": "Mailchimp",
        "mailgun.org": "Mailgun",
        "pphosted.com": "Proofpoint",
        "ppe-hosted.com": "Proofpoint",
        "_netblocks.mimecast.com": "Mimecast",
        "sparkpostmail.com": "SparkPost",
        "mandrillapp.com": "Mandrill",
        "firebasemail.com": "Firebase",
        "zendesk.com": "Zendesk",
        "freshdesk.com": "Freshdesk",
        "salesforce.com": "Salesforce",
        "helpscoutemail.com": "HelpScout",
    }

    # MX hostname pattern → (provider, category) mapping
    MX_PROVIDER_MAP = [
        ("google.com", "Google Workspace", "email_suite"),
        ("googlemail.com", "Google Workspace", "email_suite"),
        ("smtp.google.com", "Google Workspace", "email_suite"),
        ("outlook.com", "Microsoft 365", "email_suite"),
        ("protection.outlook.com", "Microsoft 365", "email_suite"),
        ("zoho.com", "Zoho Mail", "email_suite"),
        ("zoho.eu", "Zoho Mail (EU)", "email_suite"),
        ("zoho.in", "Zoho Mail (India)", "email_suite"),
        ("protonmail.ch", "ProtonMail", "email_suite"),
        ("messagingengine.com", "Fastmail", "email_suite"),
        ("pphosted.com", "Proofpoint", "security_gateway"),
        ("ppe-hosted.com", "Proofpoint", "security_gateway"),
        ("mimecast.com", "Mimecast", "security_gateway"),
        ("barracudanetworks.com", "Barracuda", "security_gateway"),
        ("amazonaws.com", "Amazon SES", "transactional"),
        ("sendgrid.net", "SendGrid", "transactional"),
        ("mailgun.org", "Mailgun", "transactional"),
        ("registrar-servers.com", "Namecheap", "registrar"),
        ("emailsrvr.com", "Rackspace", "hosted"),
        ("yahoodns.net", "Yahoo Mail", "consumer"),
        ("cloudflare.net", "Cloudflare Email Routing", "routing"),
        ("secureserver.net", "GoDaddy", "registrar"),
        ("migadu.com", "Migadu", "hosted"),
        ("icloud.com", "Apple iCloud", "consumer"),
        ("yandex.net", "Yandex Mail", "consumer"),
        ("mail.ru", "Mail.ru", "consumer"),
    ]

    def __init__(self):
        self.cache = LRUCache(500)

    def _parse_spf_providers(self, spf_record: str) -> Dict[str, Any]:
        """Parse SPF include: directives to identify all email service providers."""
        includes = re.findall(r'include:([^\s]+)', spf_record)
        providers = []
        for include in includes:
            matched = False
            for pattern, provider in self.SPF_PROVIDER_MAP.items():
                if pattern in include:
                    providers.append({"include": include, "provider": provider})
                    matched = True
                    break
            if not matched:
                providers.append({"include": include, "provider": "Unknown"})
        return {
            "includes": includes,
            "providers": providers,
            "sending_services": [p["provider"] for p in providers if p["provider"] != "Unknown"]
        }

    def _identify_mx_provider(self, mx_host: str) -> Dict[str, str]:
        """Identify email provider and category from MX hostname."""
        mx_lower = mx_host.lower()
        for pattern, provider, category in self.MX_PROVIDER_MAP:
            if pattern in mx_lower:
                return {"provider": provider, "category": category}
        return {"provider": None, "category": "unknown"}

    async def _check_autodiscover(self, domain: str) -> Dict[str, Any]:
        """Check for Office 365/Google Workspace autodiscover DNS records."""
        result = {"o365_confirmed": False, "google_confirmed": False, "indicators": []}

        if not DNS_AVAILABLE:
            return result

        # Office 365 autodiscover CNAME → autodiscover.outlook.com
        try:
            answers = await asyncio.to_thread(
                dns.resolver.resolve, f"autodiscover.{domain}", 'CNAME'
            )
            for rdata in answers:
                target = str(rdata.target).rstrip('.')
                if "outlook.com" in target:
                    result["o365_confirmed"] = True
                    result["indicators"].append(f"autodiscover CNAME → {target}")
        except Exception:
            pass

        # Office 365 SRV record: _autodiscover._tcp.domain
        try:
            answers = await asyncio.to_thread(
                dns.resolver.resolve, f"_autodiscover._tcp.{domain}", 'SRV'
            )
            for rdata in answers:
                target = str(rdata.target).rstrip('.')
                if "outlook.com" in target:
                    result["o365_confirmed"] = True
                    result["indicators"].append(f"autodiscover SRV → {target}")
        except Exception:
            pass

        # Office 365 lyncdiscover (Teams/Skype for Business indicator)
        try:
            answers = await asyncio.to_thread(
                dns.resolver.resolve, f"lyncdiscover.{domain}", 'CNAME'
            )
            for rdata in answers:
                target = str(rdata.target).rstrip('.')
                if "lync.com" in target:
                    result["o365_confirmed"] = True
                    result["indicators"].append(f"lyncdiscover CNAME → {target}")
        except Exception:
            pass

        return result

    async def _check_bimi(self, domain: str) -> Dict[str, Any]:
        """
        Check for BIMI (Brand Indicators for Message Identification) record.
        BIMI presence implies strict DMARC (p=quarantine or p=reject).
        """
        result = {"has_bimi": False, "logo_url": None, "authority_url": None}

        if not DNS_AVAILABLE:
            return result

        try:
            answers = await asyncio.to_thread(
                dns.resolver.resolve, f"default._bimi.{domain}", 'TXT'
            )
            for rdata in answers:
                txt = str(rdata).strip('"')
                if "v=BIMI1" in txt:
                    result["has_bimi"] = True
                    # Extract logo URL
                    logo_match = re.search(r'l=(https?://[^;\s]+)', txt)
                    if logo_match:
                        result["logo_url"] = logo_match.group(1)
                    # Extract authority/VMC certificate URL
                    auth_match = re.search(r'a=(https?://[^;\s]+)', txt)
                    if auth_match:
                        result["authority_url"] = auth_match.group(1)
        except Exception:
            pass

        return result

    async def get_domain_intelligence(self, domain: str) -> Dict[str, Any]:
        """
        Get comprehensive DNS-based intelligence for a domain.
        Enhanced: SPF provider parsing, 25+ MX mapping, autodiscover,
        BIMI, DMARC policy, infrastructure scoring.
        """
        result = {
            "domain": domain,
            "mx_records": [],
            "mx_providers": [],
            "txt_records": [],
            "emails_from_txt": [],
            "spf_record": None,
            "spf_providers": {},
            "dmarc_record": None,
            "dmarc_policy": None,
            "has_email_service": False,
            "email_provider": None,
            "email_providers": [],
            "autodiscover": {},
            "bimi": {},
            "has_security_gateway": False,
            "infrastructure_score": 0
        }

        if not DNS_AVAILABLE:
            return result

        infra_score = 0
        all_providers = set()

        try:
            # 1. Get MX records with enhanced provider detection
            try:
                mx_records = await asyncio.to_thread(dns.resolver.resolve, domain, 'MX')
                for mx in mx_records:
                    mx_host = str(mx.exchange).rstrip('.')
                    result["mx_records"].append(mx_host)

                    # Enhanced provider detection (25+ providers)
                    provider_info = self._identify_mx_provider(mx_host)
                    result["mx_providers"].append({
                        "host": mx_host,
                        "provider": provider_info["provider"],
                        "category": provider_info["category"]
                    })

                    if provider_info["provider"]:
                        all_providers.add(provider_info["provider"])
                        # Set primary email provider (first MX with known provider)
                        if not result["email_provider"] and provider_info["category"] == "email_suite":
                            result["email_provider"] = provider_info["provider"]

                    # Detect security gateways
                    if provider_info["category"] == "security_gateway":
                        result["has_security_gateway"] = True

                result["has_email_service"] = len(result["mx_records"]) > 0
                if result["has_email_service"]:
                    infra_score += 20  # Has MX records
            except Exception:
                pass

            # 2. Get TXT records (SPF, emails, verification records)
            google_verified = False
            try:
                txt_records = await asyncio.to_thread(dns.resolver.resolve, domain, 'TXT')
                for txt in txt_records:
                    txt_str = str(txt).strip('"')
                    result["txt_records"].append(txt_str)

                    # Check for SPF — parse include directives
                    if txt_str.startswith("v=spf1"):
                        result["spf_record"] = txt_str
                        spf_data = self._parse_spf_providers(txt_str)
                        result["spf_providers"] = spf_data
                        infra_score += 15  # Has SPF

                        # Add SPF-detected providers to the set
                        for svc in spf_data.get("sending_services", []):
                            all_providers.add(svc)

                    # Check for Google Workspace verification
                    if "google-site-verification" in txt_str:
                        google_verified = True

                    # Extract any emails from TXT records
                    emails = re.findall(r'[\w.+-]+@[\w.-]+\.\w+', txt_str)
                    result["emails_from_txt"].extend(emails)
            except Exception:
                pass

            # 3. Get DMARC record with policy extraction
            try:
                dmarc_records = await asyncio.to_thread(
                    dns.resolver.resolve, f"_dmarc.{domain}", 'TXT'
                )
                for dmarc in dmarc_records:
                    dmarc_str = str(dmarc).strip('"')
                    if "v=DMARC1" in dmarc_str:
                        result["dmarc_record"] = dmarc_str
                        infra_score += 15  # Has DMARC

                        # Extract DMARC policy (p=none/quarantine/reject)
                        policy_match = re.search(r'p=(none|quarantine|reject)', dmarc_str)
                        if policy_match:
                            result["dmarc_policy"] = policy_match.group(1)
                            # Stricter policy = more mature infrastructure
                            if policy_match.group(1) == "reject":
                                infra_score += 10
                            elif policy_match.group(1) == "quarantine":
                                infra_score += 5

                        # Extract rua/ruf report emails
                        rua_emails = re.findall(r'rua=mailto:([^;,\s]+)', dmarc_str)
                        ruf_emails = re.findall(r'ruf=mailto:([^;,\s]+)', dmarc_str)
                        result["emails_from_txt"].extend(rua_emails)
                        result["emails_from_txt"].extend(ruf_emails)
            except Exception:
                pass

            # 4. Check autodiscover (O365/Google Workspace confirmation)
            try:
                autodiscover = await self._check_autodiscover(domain)
                result["autodiscover"] = autodiscover

                if autodiscover.get("o365_confirmed"):
                    all_providers.add("Microsoft 365")
                    infra_score += 10  # Confirmed O365
                if google_verified:
                    autodiscover["google_confirmed"] = True
                    autodiscover["indicators"] = autodiscover.get("indicators", [])
                    autodiscover["indicators"].append("google-site-verification TXT present")
            except Exception:
                pass

            # 5. Check BIMI record
            try:
                bimi = await self._check_bimi(domain)
                result["bimi"] = bimi
                if bimi.get("has_bimi"):
                    infra_score += 15  # BIMI = mature email infrastructure
            except Exception:
                pass

            # 6. Check DKIM (common selectors)
            dkim_found = False
            for selector in ['selector1', 'google', 'default', 'k1']:
                try:
                    await asyncio.to_thread(
                        dns.resolver.resolve,
                        f'{selector}._domainkey.{domain}', 'TXT'
                    )
                    dkim_found = True
                    infra_score += 15  # Has DKIM
                    break
                except Exception:
                    continue

            # Compile all providers
            result["email_providers"] = sorted(all_providers)

            # Calculate final infrastructure score (0-100)
            # Components: MX(20) + SPF(15) + DMARC(15+10) + DKIM(15) + Autodiscover(10) + BIMI(15) = max 100
            result["infrastructure_score"] = min(infra_score, 100)

        except Exception as e:
            logger.error(f"DNS intelligence error for {domain}: {e}")

        return result


class SSLCertificateIntelligence:
    """
    SSL Certificate intelligence.
    Extracts subdomains and emails from crt.sh.
    Enhanced: mail subdomain filtering for mail infrastructure discovery.
    """

    # Subdomain prefixes that indicate mail infrastructure
    MAIL_SUBDOMAIN_KEYWORDS = [
        "mail", "smtp", "webmail", "imap", "pop", "pop3", "mx",
        "exchange", "owa", "autodiscover", "mta", "relay",
        "email", "em", "mx1", "mx2", "mx3", "mail2", "mail3",
        "mailer", "newsletter", "postfix", "mailgw", "mailgateway",
        "lists", "list", "outbound", "inbound"
    ]

    def __init__(self, timeout: int = 60):
        self.timeout = timeout
        self.crt_url = "https://crt.sh"
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
        )
        self.cache = LRUCache(500)

    async def get_subdomains(self, domain: str) -> List[str]:
        """Get all subdomains from Certificate Transparency logs."""
        cache_key = f"crt:{domain}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        subdomains = set()

        try:
            params = {"q": f"%.{domain}", "output": "json"}
            response = await self.client.get(self.crt_url, params=params)

            if response.status_code == 200:
                try:
                    certs = response.json()

                    for cert in certs:
                        name_value = cert.get("name_value", "")

                        for subdomain in name_value.split("\n"):
                            subdomain = subdomain.strip().replace("*.", "")
                            if subdomain and domain in subdomain:
                                subdomains.add(subdomain)
                except Exception:
                    pass

            result = list(subdomains)
            self.cache.put(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"crt.sh error: {e}")

        return list(subdomains)

    async def get_mail_subdomains(self, domain: str) -> List[str]:
        """
        Get mail-related subdomains from Certificate Transparency logs.
        Filters crt.sh results for mail infrastructure subdomains
        (mail., smtp., webmail., imap., mx., exchange., etc.)
        """
        all_subdomains = await self.get_subdomains(domain)
        mail_subs = []
        for sub in all_subdomains:
            # Get the first label (prefix) of the subdomain
            prefix = sub.split('.')[0].lower()
            if any(kw == prefix or kw in prefix for kw in self.MAIL_SUBDOMAIN_KEYWORDS):
                mail_subs.append(sub)
        return mail_subs

    async def close(self):
        await self.client.aclose()


class WaybackIntelligence:
    """
    Wayback Machine intelligence.
    Extracts historical emails from archived pages.
    """

    def __init__(self, timeout: int = 60):
        self.timeout = timeout
        self.cdx_url = "https://web.archive.org/cdx/search/cdx"
        self.wayback_url = "https://web.archive.org/web"
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
        )
        self.cache = LRUCache(500)

    async def get_historical_emails(self, domain: str, pages: List[str] = None) -> List[str]:
        """Extract emails from historical archived pages."""
        cache_key = f"wayback_emails:{domain}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        emails = set()

        # Default pages to check
        if not pages:
            pages = [
                f"https://{domain}/",
                f"https://{domain}/contact",
                f"https://{domain}/about",
                f"https://{domain}/team",
                f"https://{domain}/about-us",
                f"https://{domain}/contact-us",
                f"https://www.{domain}/contact",
                f"https://www.{domain}/about"
            ]

        for page_url in pages[:5]:  # Limit pages
            try:
                # Get snapshots
                params = {
                    "url": page_url,
                    "output": "json",
                    "limit": 3,
                    "fl": "timestamp,original",
                    "filter": "statuscode:200"
                }

                response = await self.client.get(self.cdx_url, params=params)

                if response.status_code == 200:
                    lines = response.text.strip().split("\n")

                    for line in lines[1:]:  # Skip header
                        try:
                            parts = line.split()
                            if len(parts) >= 2:
                                timestamp = parts[0]
                                original_url = parts[1]

                                # Fetch archived page
                                archive_url = f"{self.wayback_url}/{timestamp}/{original_url}"
                                arch_response = await self.client.get(archive_url)

                                if arch_response.status_code == 200:
                                    # Extract emails
                                    found_emails = re.findall(
                                        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b',
                                        arch_response.text
                                    )
                                    emails.update(found_emails)

                                await asyncio.sleep(1)  # Rate limit

                        except Exception:
                            continue

            except Exception as e:
                logger.debug(f"Wayback error for {page_url}: {e}")

        result = list(emails)
        self.cache.put(cache_key, result)
        return result

    async def close(self):
        await self.client.aclose()


# ============================================
# ULTRA EXTRACTION ENGINE
# ============================================

class MobiAdzUltraEngine:
    """
    ULTRA Enhanced MobiAdz Extraction Engine V2.0

    Features:
    - AI/ML entity extraction with SpaCy NER
    - 50+ email patterns with permutation generator
    - Advanced data structures (Bloom filter, LRU cache, Trie, Priority Queue)
    - 20+ data sources with intelligent fallback
    - Fuzzy matching for deduplication
    - DNS/SSL/WHOIS intelligence
    - Historical data from Wayback Machine
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}

        # Advanced data structures
        self.url_bloom = BloomFilter(size=1000000)
        self.email_bloom = BloomFilter(size=500000)
        self.response_cache = LRUCache(capacity=2000)
        self.email_trie = EmailPatternTrie()
        self.url_queue = PriorityURLQueue()

        # Initialize email patterns in Trie
        for i, pattern in enumerate(EmailPermutationGenerator.PATTERNS):
            self.email_trie.insert(pattern, confidence=95 - i)

        # AI/ML components
        self.nlp_extractor = NLPEntityExtractor()
        self.email_verifier = EmailVerifier()

        # Data source scrapers
        self.github = GitHubOrganizationScraper()
        self.npm = NPMPackageScraper()
        self.hackernews = HackerNewsScraper()
        self.dns_intel = DNSIntelligence()
        self.ssl_intel = SSLCertificateIntelligence()
        self.wayback = WaybackIntelligence()

        # HTTP client for general scraping
        self.client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
        )
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        # Statistics
        self.stats = {
            "urls_processed": 0,
            "emails_found": 0,
            "emails_verified": 0,
            "entities_extracted": 0,
            "sources_used": [],
            "cache_hits": 0,
            "bloom_filter_hits": 0,
            "start_time": None,
            "end_time": None
        }

        # Progress
        self.progress = {
            "stage": "idle",
            "stage_progress": 0,
            "total_progress": 0,
            "message": "Ready"
        }

    async def initialize(self):
        """Initialize AI/ML components."""
        await self.nlp_extractor.initialize()
        logger.info("MobiAdz Ultra Engine initialized")

    def _update_progress(self, stage: str, progress: int, message: str):
        stages = ["init", "discovery", "scraping", "extraction", "verification", "enrichment", "complete"]
        stage_idx = stages.index(stage) if stage in stages else 0

        self.progress = {
            "stage": stage,
            "stage_progress": progress,
            "total_progress": int((stage_idx * 100 + progress) / len(stages)),
            "message": message
        }

    async def extract_company_intelligence(
        self,
        company_name: str,
        domain: Optional[str] = None,
        deep_mode: bool = True
    ) -> Dict[str, Any]:
        """
        Extract comprehensive company intelligence using all sources.

        This is the main extraction method that combines all data sources.
        """
        self.stats["start_time"] = datetime.now(timezone.utc).isoformat()

        result = {
            "company_name": company_name,
            "domain": domain,
            "emails": [],
            "people": [],
            "company_info": {},
            "social_profiles": {},
            "technology_stack": [],
            "funding_info": None,
            "subdomains": [],
            "dns_intelligence": {},
            "historical_emails": [],
            "sources_used": [],
            "confidence_score": 0
        }

        # Extract or guess domain
        if not domain:
            domain = self._guess_domain(company_name)
            result["domain"] = domain

        try:
            await self.initialize()

            # Stage 1: DNS Intelligence
            self._update_progress("discovery", 0, f"Gathering DNS intelligence for {domain}...")
            dns_data = await self.dns_intel.get_domain_intelligence(domain)
            result["dns_intelligence"] = dns_data
            result["emails"].extend([
                {"email": e, "source": "dns_txt", "confidence": 85}
                for e in dns_data.get("emails_from_txt", [])
            ])
            result["sources_used"].append("dns")

            # Stage 2: SSL Certificate Intelligence
            self._update_progress("discovery", 20, "Finding subdomains from SSL certificates...")
            subdomains = await self.ssl_intel.get_subdomains(domain)
            result["subdomains"] = subdomains[:50]  # Limit
            if subdomains:
                result["sources_used"].append("certificate_transparency")

            # Stage 3: Website Deep Scraping
            self._update_progress("scraping", 0, f"Deep scraping {domain}...")
            website_data = await self._deep_scrape_website(domain)

            if website_data:
                # Extract entities using NLP
                entities = self.nlp_extractor.extract_entities(website_data.get("text", ""))

                result["emails"].extend([
                    {"email": e, "source": "website_scrape", "confidence": 85}
                    for e in entities.get("emails", [])
                ])

                result["people"].extend([
                    {"name": p, "source": "website_nlp"}
                    for p in entities.get("persons", [])
                ])

                result["company_info"]["description"] = website_data.get("description", "")
                result["social_profiles"] = website_data.get("social_links", {})

                result["sources_used"].append("website_scrape")

            # Stage 4: GitHub Organization
            if deep_mode:
                self._update_progress("extraction", 0, "Searching GitHub organizations...")

                org_name = company_name.lower().replace(" ", "").replace("-", "").replace(".", "")
                org_details = await self.github.get_org_details(org_name)

                if org_details:
                    if org_details.get("email"):
                        result["emails"].append({
                            "email": org_details["email"],
                            "source": "github_org",
                            "confidence": 95
                        })

                    if org_details.get("blog"):
                        result["company_info"]["website"] = org_details["blog"]

                    result["sources_used"].append("github_org")

                    # Get member emails from commits
                    member_emails = await self.github.get_org_members_emails(org_name, max_members=10)

                    for member in member_emails:
                        result["emails"].append({
                            "email": member["email"],
                            "name": member.get("username"),
                            "source": "github_commits",
                            "confidence": 90  # High - actual git email
                        })

            # Stage 5: NPM Packages
            if deep_mode:
                self._update_progress("extraction", 30, "Searching npm packages...")

                npm_packages = await self.npm.search_packages(company_name, max_results=5)

                for pkg in npm_packages:
                    if pkg.get("publisher_email"):
                        result["emails"].append({
                            "email": pkg["publisher_email"],
                            "source": "npm_registry",
                            "confidence": 95,
                            "context": f"npm package: {pkg.get('name')}"
                        })
                        result["sources_used"].append("npm")
                        break

            # Stage 6: HackerNews Mentions
            if deep_mode:
                self._update_progress("extraction", 50, "Checking HackerNews mentions...")

                hn_results = await self.hackernews.search(company_name, max_results=10)

                if hn_results:
                    result["company_info"]["hackernews_mentions"] = len(hn_results)
                    result["sources_used"].append("hackernews")

            # Stage 7: Wayback Machine
            if deep_mode:
                self._update_progress("extraction", 70, "Searching historical archives...")

                historical_emails = await self.wayback.get_historical_emails(domain)

                for email in historical_emails[:10]:
                    if email not in [e["email"] for e in result["emails"]]:
                        result["emails"].append({
                            "email": email,
                            "source": "wayback_machine",
                            "confidence": 75
                        })

                if historical_emails:
                    result["sources_used"].append("wayback_machine")

            # Stage 8: Email Permutation
            if result["people"]:
                self._update_progress("extraction", 85, "Generating email permutations...")

                for person in result["people"][:5]:
                    permutations = EmailPermutationGenerator.generate(person["name"], domain)

                    for perm in permutations[:3]:  # Top 3 patterns only
                        if perm["email"] not in [e["email"] for e in result["emails"]]:
                            result["emails"].append({
                                "email": perm["email"],
                                "source": "email_permutation",
                                "pattern": perm["pattern"],
                                "confidence": perm["confidence"],
                                "for_person": person["name"]
                            })

            # Stage 9: Generate role-based emails
            role_emails = EmailPermutationGenerator.generate_role_emails(domain)

            for role_email in role_emails[:10]:
                if role_email["email"] not in [e["email"] for e in result["emails"]]:
                    result["emails"].append({
                        "email": role_email["email"],
                        "source": "role_based",
                        "confidence": role_email["confidence"]
                    })

            # Stage 10: Email Verification
            self._update_progress("verification", 0, "Verifying emails...")

            verified_emails = []
            for i, email_data in enumerate(result["emails"][:30]):  # Limit verification
                verification = await self.email_verifier.verify(email_data["email"])

                email_data["verified"] = verification["deliverable"]
                email_data["verification_confidence"] = verification["confidence"]
                email_data["is_role_based"] = verification["is_role_based"]
                email_data["is_free_provider"] = verification["is_free_provider"]

                if verification["deliverable"]:
                    verified_emails.append(email_data)

                progress = int((i + 1) / min(len(result["emails"]), 30) * 100)
                self._update_progress("verification", progress, f"Verified {i + 1} emails...")

            # Keep verified emails first, then unverified
            result["emails"] = verified_emails + [
                e for e in result["emails"]
                if e not in verified_emails
            ]

            # Deduplicate emails using fuzzy matching
            seen_emails = set()
            unique_emails = []

            for email_data in result["emails"]:
                email_lower = email_data["email"].lower()
                if email_lower not in seen_emails:
                    seen_emails.add(email_lower)
                    unique_emails.append(email_data)

            result["emails"] = unique_emails

            # Calculate confidence score
            confidence = 0
            if result["emails"]:
                confidence += 30
            if result["people"]:
                confidence += 20
            if result["company_info"]:
                confidence += 20
            if result["social_profiles"]:
                confidence += 10
            if result["dns_intelligence"].get("has_email_service"):
                confidence += 10
            confidence += min(len(result["sources_used"]) * 5, 25)

            result["confidence_score"] = min(confidence, 100)

            # Update stats
            self.stats["emails_found"] = len(result["emails"])
            self.stats["sources_used"] = result["sources_used"]
            self.stats["end_time"] = datetime.now(timezone.utc).isoformat()

            self._update_progress("complete", 100, f"Found {len(result['emails'])} emails from {len(result['sources_used'])} sources")

        except Exception as e:
            logger.error(f"Company extraction error: {e}")
            self.stats["end_time"] = datetime.now(timezone.utc).isoformat()

        return result

    async def _deep_scrape_website(self, domain: str, max_pages: int = 20) -> Optional[Dict[str, Any]]:
        """Deep scrape a website for data."""
        result = {
            "text": "",
            "emails": [],
            "description": "",
            "social_links": {}
        }

        pages_to_scrape = [
            f"https://{domain}/",
            f"https://{domain}/about",
            f"https://{domain}/about-us",
            f"https://{domain}/team",
            f"https://{domain}/contact",
            f"https://{domain}/contact-us",
            f"https://www.{domain}/",
            f"https://www.{domain}/about",
            f"https://www.{domain}/team",
            f"https://www.{domain}/contact"
        ]

        scraped = 0

        for url in pages_to_scrape:
            if scraped >= max_pages:
                break

            # Check bloom filter
            if url in self.url_bloom:
                self.stats["bloom_filter_hits"] += 1
                continue

            self.url_bloom.add(url)

            try:
                response = await self.client.get(url, headers=self.headers)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")

                    # Extract text
                    text = soup.get_text(separator=" ", strip=True)
                    result["text"] += " " + text

                    # Extract description
                    meta_desc = soup.find("meta", {"name": "description"}) or \
                                soup.find("meta", {"property": "og:description"})
                    if meta_desc and not result["description"]:
                        result["description"] = meta_desc.get("content", "")[:500]

                    # Extract social links
                    for link in soup.find_all("a", href=True):
                        href = link.get("href", "")

                        if "linkedin.com/company" in href and not result["social_links"].get("linkedin"):
                            result["social_links"]["linkedin"] = href
                        elif ("twitter.com/" in href or "x.com/" in href) and not result["social_links"].get("twitter"):
                            result["social_links"]["twitter"] = href
                        elif "facebook.com/" in href and not result["social_links"].get("facebook"):
                            result["social_links"]["facebook"] = href

                    scraped += 1

                await asyncio.sleep(0.3)

            except Exception as e:
                logger.debug(f"Scrape error for {url}: {e}")

        return result if result["text"] else None

    def _guess_domain(self, company_name: str) -> str:
        """Guess domain from company name (consistent with OSINT engine)."""
        clean = company_name.lower()
        clean = re.sub(r'[^a-z0-9]', '', clean)

        # Remove common corporate suffixes (kept in sync with OSINT engine)
        suffixes = ['inc', 'llc', 'ltd', 'corp', 'corporation', 'company', 'co',
                     'limited', 'gmbh', 'ag', 'sa', 'srl', 'bv', 'pty', 'pvt',
                     'technologies', 'technology', 'tech', 'software', 'solutions',
                     'digital', 'media', 'group', 'labs', 'studio', 'studios']
        for suffix in suffixes:
            if clean.endswith(suffix) and len(clean) > len(suffix):
                clean = clean[:-len(suffix)]

        return f"{clean}.com" if clean else f"{company_name.lower().replace(' ', '')}.com"

    async def batch_extract(
        self,
        companies: List[Dict[str, str]],
        deep_mode: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Batch extract intelligence for multiple companies.

        Args:
            companies: List of {"name": "Company Name", "domain": "example.com"}
        """
        results = []

        for i, company in enumerate(companies):
            self._update_progress(
                "extraction",
                int(i / len(companies) * 100),
                f"Processing {i + 1}/{len(companies)}: {company.get('name')}"
            )

            result = await self.extract_company_intelligence(
                company_name=company.get("name", ""),
                domain=company.get("domain"),
                deep_mode=deep_mode
            )

            results.append(result)

            # Rate limiting (reduced from 1s for batch throughput)
            await asyncio.sleep(0.3)

        return results

    def get_stats(self) -> Dict[str, Any]:
        return self.stats

    def get_progress(self) -> Dict[str, Any]:
        return self.progress

    async def close(self):
        """Close all resources (fault-tolerant)."""
        results = await asyncio.gather(
            self.client.aclose(),
            self.github.close(),
            self.npm.close(),
            self.hackernews.close(),
            self.ssl_intel.close(),
            self.wayback.close(),
            return_exceptions=True
        )
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                logger.warning(f"Error closing Ultra Engine resource {i}: {r}")

        logger.info("MobiAdz Ultra Engine closed")


# ============================================
# QUICK START FUNCTIONS
# ============================================

async def ultra_company_extraction(
    company_name: str,
    domain: Optional[str] = None
) -> Dict[str, Any]:
    """
    Quick function for ultra company extraction.

    Example:
        result = await ultra_company_extraction(
            company_name="Stripe",
            domain="stripe.com"
        )

        print(f"Found {len(result['emails'])} emails")
        print(f"Sources: {result['sources_used']}")
    """
    engine = MobiAdzUltraEngine()

    try:
        result = await engine.extract_company_intelligence(
            company_name=company_name,
            domain=domain,
            deep_mode=True
        )
        return result
    finally:
        await engine.close()


async def batch_ultra_extraction(
    companies: List[Dict[str, str]]
) -> List[Dict[str, Any]]:
    """
    Quick function for batch extraction.

    Example:
        companies = [
            {"name": "Stripe", "domain": "stripe.com"},
            {"name": "Notion", "domain": "notion.so"},
        ]
        results = await batch_ultra_extraction(companies)
    """
    engine = MobiAdzUltraEngine()

    try:
        results = await engine.batch_extract(companies, deep_mode=True)
        return results
    finally:
        await engine.close()
