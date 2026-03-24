"""
Trie Data Structure for Efficient URL Pattern Matching

Uses Trie (Prefix Tree) for:
- Fast URL pattern matching O(m) where m = URL length
- Domain grouping and categorization
- robots.txt rule matching
- Visited URL tracking with patterns

Space Complexity: O(ALPHABET_SIZE * N * M) where N = num URLs, M = avg length
Time Complexity: Insert/Search = O(M) where M = URL length
"""

from typing import Optional, List, Set, Dict, Any
from dataclasses import dataclass, field
from urllib.parse import urlparse


@dataclass
class TrieNode:
    """Node in the Trie"""
    char: str = ""
    is_end_of_url: bool = False
    children: Dict[str, 'TrieNode'] = field(default_factory=dict)
    url_data: Optional[Dict[str, Any]] = None  # Metadata for complete URLs
    count: int = 0  # Number of URLs passing through this node


class URLTrie:
    """
    Trie optimized for URL storage and pattern matching

    Features:
    - Fast insertion and lookup O(m)
    - Pattern matching (e.g., all URLs under /blog/*)
    - Domain grouping
    - Memory-efficient for millions of URLs
    """

    def __init__(self):
        self.root = TrieNode()
        self.total_urls = 0

    def insert(self, url: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Insert URL into trie

        Time Complexity: O(m) where m = len(url)
        """
        node = self.root

        for char in url:
            if char not in node.children:
                node.children[char] = TrieNode(char=char)

            node = node.children[char]
            node.count += 1

        node.is_end_of_url = True
        node.url_data = metadata or {}
        self.total_urls += 1

    def search(self, url: str) -> bool:
        """
        Check if exact URL exists

        Time Complexity: O(m)
        """
        node = self._traverse(url)
        return node is not None and node.is_end_of_url

    def starts_with(self, prefix: str) -> List[str]:
        """
        Find all URLs starting with prefix

        Example: starts_with("https://example.com/blog/")
        Returns: all blog post URLs

        Time Complexity: O(m + n) where m = prefix length, n = num results
        """
        node = self._traverse(prefix)
        if node is None:
            return []

        results = []
        self._collect_urls(node, prefix, results)
        return results

    def _traverse(self, path: str) -> Optional[TrieNode]:
        """Traverse trie following path"""
        node = self.root

        for char in path:
            if char not in node.children:
                return None
            node = node.children[char]

        return node

    def _collect_urls(self, node: TrieNode, current_path: str, results: List[str]) -> None:
        """Recursively collect all URLs under node"""
        if node.is_end_of_url:
            results.append(current_path)

        for char, child in node.children.items():
            self._collect_urls(child, current_path + char, results)

    def get_metadata(self, url: str) -> Optional[Dict[str, Any]]:
        """Get metadata for URL"""
        node = self._traverse(url)
        if node and node.is_end_of_url:
            return node.url_data
        return None

    def count_prefix(self, prefix: str) -> int:
        """
        Count URLs with given prefix

        Much faster than len(starts_with()) as it doesn't collect all URLs

        Time Complexity: O(m) where m = len(prefix)
        """
        node = self._traverse(prefix)
        return node.count if node else 0

    def delete(self, url: str) -> bool:
        """
        Delete URL from trie

        Time Complexity: O(m)
        """
        def _delete_recursive(node: TrieNode, url: str, index: int) -> bool:
            if index == len(url):
                if not node.is_end_of_url:
                    return False
                node.is_end_of_url = False
                node.url_data = None
                return len(node.children) == 0

            char = url[index]
            if char not in node.children:
                return False

            child = node.children[char]
            should_delete_child = _delete_recursive(child, url, index + 1)

            if should_delete_child:
                del node.children[char]
                return len(node.children) == 0 and not node.is_end_of_url

            return False

        result = _delete_recursive(self.root, url, 0)
        if result or self._traverse(url) is None:
            self.total_urls -= 1
            return True
        return False

    def __len__(self) -> int:
        return self.total_urls

    def __contains__(self, url: str) -> bool:
        return self.search(url)


class DomainTrie:
    """
    Specialized Trie for domain-based URL organization

    Groups URLs by domain for efficient:
    - Rate limiting per domain
    - Robots.txt rule checking
    - Domain statistics
    """

    def __init__(self):
        self.domains: Dict[str, URLTrie] = {}

    def insert(self, url: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Insert URL organized by domain"""
        domain = self._extract_domain(url)

        if domain not in self.domains:
            self.domains[domain] = URLTrie()

        self.domains[domain].insert(url, metadata)

    def get_domain_urls(self, domain: str) -> List[str]:
        """Get all URLs for a domain"""
        if domain not in self.domains:
            return []

        trie = self.domains[domain]
        return trie.starts_with("")  # Get all URLs

    def get_domain_count(self, domain: str) -> int:
        """Count URLs for domain"""
        if domain not in self.domains:
            return 0
        return len(self.domains[domain])

    def get_all_domains(self) -> List[str]:
        """Get list of all domains"""
        return list(self.domains.keys())

    def search(self, url: str) -> bool:
        """Check if URL exists"""
        domain = self._extract_domain(url)
        if domain not in self.domains:
            return False
        return self.domains[domain].search(url)

    @staticmethod
    def _extract_domain(url: str) -> str:
        """Extract domain from URL"""
        parsed = urlparse(url)
        return parsed.netloc

    def stats(self) -> Dict[str, Any]:
        """Get statistics"""
        return {
            "total_domains": len(self.domains),
            "total_urls": sum(len(trie) for trie in self.domains.values()),
            "domains_by_url_count": {
                domain: len(trie)
                for domain, trie in sorted(
                    self.domains.items(),
                    key=lambda x: len(x[1]),
                    reverse=True
                )[:20]  # Top 20 domains
            }
        }

    def __contains__(self, url: str) -> bool:
        return self.search(url)


class RobotsTrie:
    """
    Specialized Trie for robots.txt rule matching

    Efficiently matches URL paths against disallow rules
    """

    def __init__(self):
        self.disallow_trie = URLTrie()
        self.allow_trie = URLTrie()

    def add_disallow_rule(self, path: str) -> None:
        """Add disallow rule from robots.txt"""
        self.disallow_trie.insert(path)

    def add_allow_rule(self, path: str) -> None:
        """Add allow rule (overrides disallow)"""
        self.allow_trie.insert(path)

    def is_allowed(self, url_path: str) -> bool:
        """
        Check if URL path is allowed

        Returns True if allowed, False if disallowed
        Follows robots.txt precedence rules
        """
        # Check allow rules first (they override disallow)
        for allow_rule in self._get_matching_rules(url_path, self.allow_trie):
            return True

        # Check disallow rules
        for disallow_rule in self._get_matching_rules(url_path, self.disallow_trie):
            return False

        # No matching rules = allowed
        return True

    def _get_matching_rules(self, url_path: str, trie: URLTrie) -> List[str]:
        """Get all rules that match the URL path"""
        matching = []

        # Check all prefixes of the URL
        for i in range(1, len(url_path) + 1):
            prefix = url_path[:i]
            if prefix in trie:
                matching.append(prefix)

        return matching


# Usage Examples:
"""
# 1. Basic URL storage and lookup
url_trie = URLTrie()

# Insert URLs with metadata
url_trie.insert("https://example.com/page1", {"title": "Page 1", "status": 200})
url_trie.insert("https://example.com/page2", {"title": "Page 2", "status": 200})
url_trie.insert("https://example.com/blog/post1", {"title": "Post 1"})

# Fast lookup O(m)
if "https://example.com/page1" in url_trie:
    print("URL exists!")

# Pattern matching - get all blog posts
blog_posts = url_trie.starts_with("https://example.com/blog/")
print(f"Found {len(blog_posts)} blog posts")

# Count without fetching all URLs
count = url_trie.count_prefix("https://example.com/blog/")
print(f"Blog posts count: {count}")

# 2. Domain-based organization
domain_trie = DomainTrie()

urls = [
    "https://example.com/page1",
    "https://example.com/page2",
    "https://another.com/page1",
]

for url in urls:
    domain_trie.insert(url)

# Get all URLs for a domain
example_urls = domain_trie.get_domain_urls("example.com")

# Statistics
print(domain_trie.stats())
# {
#     'total_domains': 2,
#     'total_urls': 3,
#     'domains_by_url_count': {
#         'example.com': 2,
#         'another.com': 1
#     }
# }

# 3. Robots.txt rule matching
robots = RobotsTrie()
robots.add_disallow_rule("/admin/")
robots.add_disallow_rule("/private/")
robots.add_allow_rule("/admin/public/")  # Exception

print(robots.is_allowed("/page"))  # True
print(robots.is_allowed("/admin/dashboard"))  # False
print(robots.is_allowed("/admin/public/docs"))  # True (allow overrides)

# 4. Visited URL tracking for crawling
visited = URLTrie()

def crawl_website(start_url: str, max_depth: int = 3):
    queue = [(start_url, 0)]

    while queue:
        url, depth = queue.pop(0)

        if url in visited or depth > max_depth:
            continue

        # Mark as visited
        visited.insert(url, {"depth": depth})

        # Scrape and find links
        links = scrape_page(url)

        for link in links:
            queue.append((link, depth + 1))

    print(f"Crawled {len(visited)} unique URLs")

# Memory comparison:
# - Set of 1M URLs: ~50-70 MB
# - Trie of 1M URLs: ~20-30 MB (shared prefixes!)
# - Trie provides pattern matching for free!
"""
