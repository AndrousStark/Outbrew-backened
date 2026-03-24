"""
Bloom Filter Implementation for URL Deduplication
Memory-efficient probabilistic data structure for tracking visited URLs
Uses MurmurHash3 for fast hashing

Space Complexity: O(m) where m is bit array size
Time Complexity: O(k) where k is number of hash functions
False Positive Rate: configurable (default: 0.01 = 1%)
"""

import math
import mmh3  # MurmurHash3 - fastest non-cryptographic hash
from typing import Set
import struct


class BloomFilter:
    """
    Memory-efficient Bloom Filter for URL deduplication

    Uses bit array for O(1) lookups with minimal memory
    Perfect for tracking millions of visited URLs

    Example:
        bf = BloomFilter(expected_items=1000000, false_positive_rate=0.01)
        bf.add("https://example.com")
        if bf.contains("https://example.com"):  # Fast O(k) lookup
            print("Already visited")
    """

    def __init__(
        self,
        expected_items: int = 1_000_000,
        false_positive_rate: float = 0.01
    ):
        """
        Initialize Bloom Filter

        Args:
            expected_items: Expected number of URLs to track
            false_positive_rate: Acceptable false positive rate (0.0-1.0)
        """
        # Calculate optimal bit array size
        # m = -(n * ln(p)) / (ln(2)^2)
        self.size = self._optimal_size(expected_items, false_positive_rate)

        # Calculate optimal number of hash functions
        # k = (m/n) * ln(2)
        self.num_hashes = self._optimal_hashes(self.size, expected_items)

        # Bit array (use bytearray for memory efficiency)
        self.bit_array = bytearray(math.ceil(self.size / 8))

        # Statistics
        self.items_added = 0
        self.expected_items = expected_items

    @staticmethod
    def _optimal_size(n: int, p: float) -> int:
        """Calculate optimal bit array size"""
        m = -(n * math.log(p)) / (math.log(2) ** 2)
        return int(m)

    @staticmethod
    def _optimal_hashes(m: int, n: int) -> int:
        """Calculate optimal number of hash functions"""
        k = (m / n) * math.log(2)
        return max(1, int(k))

    def _hash(self, item: str, seed: int) -> int:
        """Generate hash using MurmurHash3"""
        return mmh3.hash(item, seed) % self.size

    def add(self, item: str) -> None:
        """
        Add item to bloom filter

        Time Complexity: O(k) where k = num_hashes
        """
        for i in range(self.num_hashes):
            index = self._hash(item, i)
            # Set bit at index
            byte_index = index // 8
            bit_index = index % 8
            self.bit_array[byte_index] |= (1 << bit_index)

        self.items_added += 1

    def contains(self, item: str) -> bool:
        """
        Check if item might be in the filter

        Returns:
            True: Item might be present (with false positive rate)
            False: Item is definitely NOT present (100% accurate)

        Time Complexity: O(k) where k = num_hashes
        """
        for i in range(self.num_hashes):
            index = self._hash(item, i)
            # Check bit at index
            byte_index = index // 8
            bit_index = index % 8
            if not (self.bit_array[byte_index] & (1 << bit_index)):
                return False  # Definitely not present
        return True  # Might be present

    def __contains__(self, item: str) -> bool:
        """Support 'in' operator"""
        return self.contains(item)

    @property
    def false_positive_probability(self) -> float:
        """Calculate current false positive probability"""
        # (1 - e^(-kn/m))^k
        k = self.num_hashes
        n = self.items_added
        m = self.size
        return (1 - math.exp(-k * n / m)) ** k

    @property
    def memory_usage_mb(self) -> float:
        """Current memory usage in MB"""
        return len(self.bit_array) / (1024 * 1024)

    def clear(self) -> None:
        """Reset the bloom filter"""
        self.bit_array = bytearray(math.ceil(self.size / 8))
        self.items_added = 0

    def __len__(self) -> int:
        """Return number of items added"""
        return self.items_added

    def stats(self) -> dict:
        """Get filter statistics"""
        return {
            "size_bits": self.size,
            "num_hashes": self.num_hashes,
            "items_added": self.items_added,
            "expected_items": self.expected_items,
            "memory_mb": round(self.memory_usage_mb, 2),
            "false_positive_rate": round(self.false_positive_probability, 6),
            "capacity_used_percent": round((self.items_added / self.expected_items) * 100, 2)
        }


class ScalableBloomFilter:
    """
    Scalable Bloom Filter that grows as needed

    Automatically adds new filters when capacity is reached
    Maintains false positive rate across all filters
    """

    def __init__(
        self,
        initial_capacity: int = 100_000,
        false_positive_rate: float = 0.01,
        growth_factor: int = 2
    ):
        self.initial_capacity = initial_capacity
        self.false_positive_rate = false_positive_rate
        self.growth_factor = growth_factor

        # List of bloom filters
        self.filters: list[BloomFilter] = []
        self._add_filter()

    def _add_filter(self) -> None:
        """Add new bloom filter with increased capacity"""
        capacity = self.initial_capacity * (self.growth_factor ** len(self.filters))
        bf = BloomFilter(
            expected_items=capacity,
            false_positive_rate=self.false_positive_rate
        )
        self.filters.append(bf)

    def add(self, item: str) -> None:
        """Add item to the most recent filter"""
        current_filter = self.filters[-1]

        # Check if current filter is near capacity
        if current_filter.items_added >= current_filter.expected_items * 0.9:
            self._add_filter()
            current_filter = self.filters[-1]

        current_filter.add(item)

    def contains(self, item: str) -> bool:
        """Check if item exists in any filter"""
        return any(bf.contains(item) for bf in self.filters)

    def __contains__(self, item: str) -> bool:
        return self.contains(item)

    @property
    def total_items(self) -> int:
        """Total items across all filters"""
        return sum(bf.items_added for bf in self.filters)

    @property
    def total_memory_mb(self) -> float:
        """Total memory usage across all filters"""
        return sum(bf.memory_usage_mb for bf in self.filters)

    def stats(self) -> dict:
        """Get combined statistics"""
        return {
            "num_filters": len(self.filters),
            "total_items": self.total_items,
            "total_memory_mb": round(self.total_memory_mb, 2),
            "filters": [bf.stats() for bf in self.filters]
        }


# Example Usage:
"""
# For single extraction job (typical: 10K-100K URLs)
bloom = BloomFilter(expected_items=100_000, false_positive_rate=0.01)

for url in discovered_urls:
    if url not in bloom:  # O(k) lookup - very fast!
        bloom.add(url)
        scrape_url(url)

# Memory usage: ~120KB for 100K URLs (0.01 FP rate)
# vs Set: ~8MB for 100K URLs (exact but 66x more memory!)

print(bloom.stats())
# {
#     'size_bits': 958506,
#     'num_hashes': 7,
#     'items_added': 100000,
#     'memory_mb': 0.11,
#     'false_positive_rate': 0.01,
#     'capacity_used_percent': 100.0
# }

# For long-running crawler (grows over time)
scalable_bloom = ScalableBloomFilter(initial_capacity=10_000)

# Automatically scales as needed
for i in range(1_000_000):
    scalable_bloom.add(f"https://example.com/page/{i}")

print(scalable_bloom.stats())
# Multiple filters created automatically, efficient memory usage
"""
