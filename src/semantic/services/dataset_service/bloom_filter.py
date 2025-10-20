"""
Simple Bloom Filter for fast corpus_id existence check
Reduces database queries by pre-filtering IDs that definitely don't exist
"""

import mmh3
from typing import List


class BloomFilter:
    """
    Simple Bloom Filter implementation using MurmurHash3

    False positive rate: ~1% with optimal parameters
    Memory usage: ~1.2MB per million items
    """

    def __init__(self, expected_items: int = 5_000_000, false_positive_rate: float = 0.01):
        """
        Initialize Bloom Filter

        Args:
            expected_items: Expected number of items to store
            false_positive_rate: Desired false positive rate (default 1%)
        """
        # Calculate optimal bit array size
        self.size = self._optimal_size(expected_items, false_positive_rate)

        # Calculate optimal number of hash functions
        self.hash_count = self._optimal_hash_count(self.size, expected_items)

        # Initialize bit array (using bytearray for memory efficiency)
        self.bit_array = bytearray(self.size // 8 + 1)

        self.item_count = 0

    def _optimal_size(self, n: int, p: float) -> int:
        """
        Calculate optimal bit array size
        m = -(n * ln(p)) / (ln(2)^2)
        """
        import math
        m = -(n * math.log(p)) / (math.log(2) ** 2)
        return int(m)

    def _optimal_hash_count(self, m: int, n: int) -> int:
        """
        Calculate optimal number of hash functions
        k = (m/n) * ln(2)
        """
        import math
        k = (m / n) * math.log(2)
        return int(k) + 1

    def _get_bit_positions(self, item: int) -> List[int]:
        """Get bit positions for an item using multiple hash functions"""
        positions = []
        for i in range(self.hash_count):
            # Use mmh3 with different seeds
            hash_value = mmh3.hash(str(item), seed=i)
            position = abs(hash_value) % self.size
            positions.append(position)
        return positions

    def add(self, item: int) -> None:
        """Add an item to the Bloom Filter"""
        for position in self._get_bit_positions(item):
            byte_index = position // 8
            bit_index = position % 8
            self.bit_array[byte_index] |= (1 << bit_index)
        self.item_count += 1

    def add_batch(self, items: List[int]) -> None:
        """Add multiple items efficiently"""
        for item in items:
            self.add(item)

    def contains(self, item: int) -> bool:
        """
        Check if an item might be in the set

        Returns:
            True: Item MIGHT be in the set (could be false positive)
            False: Item is DEFINITELY NOT in the set (100% certain)
        """
        for position in self._get_bit_positions(item):
            byte_index = position // 8
            bit_index = position % 8
            if not (self.bit_array[byte_index] & (1 << bit_index)):
                return False  # Definitely not in set
        return True  # Might be in set (or false positive)

    def get_stats(self) -> dict:
        """Get Bloom Filter statistics"""
        import math

        # Calculate approximate false positive rate
        k = self.hash_count
        m = self.size
        n = self.item_count

        if n > 0:
            # P(false positive) ≈ (1 - e^(-kn/m))^k
            fpp = (1 - math.exp(-k * n / m)) ** k
        else:
            fpp = 0

        memory_mb = len(self.bit_array) / (1024 * 1024)

        return {
            'items': self.item_count,
            'size_bits': self.size,
            'memory_mb': round(memory_mb, 2),
            'hash_functions': self.hash_count,
            'false_positive_probability': round(fpp, 4)
        }
