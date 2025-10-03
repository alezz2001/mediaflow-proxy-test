"""
Segment URL normalizer for HLS streams with unconventional file extensions.

This module provides an LRU (Least Recently Used) mapping to store normalized segment IDs
and their original URLs, allowing unconventional segment extensions (e.g., .css) to be
rewritten for player compatibility while preserving the original URLs for fetching.

Example:
    Original segment URL: https://example.com/segment1.css
    Normalized ID: abc123def4567890 (first 16 hex chars of SHA1)
    Proxy URL: /proxy/hls/segment?segment_url=https%3A%2F%2Fexample.com%2Fsegment1.css&id=abc123def4567890&norm_ext=ts

    The mapping stores: abc123def4567890 -> https://example.com/segment1.css
"""

import logging
from collections import OrderedDict
from typing import Optional

from mediaflow_proxy.configs import settings

logger = logging.getLogger(__name__)


class SegmentNormalizer:
    """
    Thread-safe LRU cache for normalized segment URL mappings.

    This class maintains a mapping between normalized IDs (short hashes) and original
    segment URLs. The mapping is capped at a configurable size and uses LRU eviction.

    Attributes:
        _mapping (OrderedDict): LRU cache mapping normalized_id -> original_url
        _max_size (int): Maximum number of entries in the mapping
    """

    def __init__(self, max_size: Optional[int] = None):
        """
        Initialize the segment normalizer with an LRU mapping.

        Args:
            max_size (int, optional): Maximum number of mapping entries.
                                     Defaults to settings.hls_normalized_map_size.
        """
        self._max_size = max_size or settings.hls_normalized_map_size
        self._mapping: OrderedDict[str, str] = OrderedDict()
        logger.debug(f"SegmentNormalizer initialized with max_size={self._max_size}")

    def store_mapping(self, norm_id: str, original_url: str) -> None:
        """
        Store a normalized ID to original URL mapping.

        If the mapping is at capacity, removes the least recently used entry.
        If the norm_id already exists, it's moved to the end (most recent).

        Args:
            norm_id (str): The normalized segment ID (hash)
            original_url (str): The original segment URL
        """
        # Move to end if exists, or add new entry
        if norm_id in self._mapping:
            self._mapping.move_to_end(norm_id)
        else:
            self._mapping[norm_id] = original_url

            # Evict oldest if over capacity
            if len(self._mapping) > self._max_size:
                evicted_id = next(iter(self._mapping))
                evicted_url = self._mapping.pop(evicted_id)
                logger.debug(f"Evicted LRU mapping: {evicted_id} -> {evicted_url[:50]}...")

        logger.debug(f"Stored mapping: {norm_id} -> {original_url[:50]}... (total: {len(self._mapping)})")

    def get_original(self, norm_id: str) -> Optional[str]:
        """
        Retrieve the original URL for a normalized ID.

        Args:
            norm_id (str): The normalized segment ID

        Returns:
            str | None: The original URL if found, None otherwise
        """
        original_url = self._mapping.get(norm_id)
        if original_url:
            # Move to end to mark as recently used
            self._mapping.move_to_end(norm_id)
            logger.debug(f"Retrieved mapping: {norm_id} -> {original_url[:50]}...")
        else:
            logger.debug(f"Mapping not found for norm_id: {norm_id}")
        return original_url


# Global instance
_normalizer_instance: Optional[SegmentNormalizer] = None


def get_normalizer() -> SegmentNormalizer:
    """
    Get the global SegmentNormalizer instance (singleton pattern).

    Returns:
        SegmentNormalizer: The global normalizer instance
    """
    global _normalizer_instance
    if _normalizer_instance is None:
        _normalizer_instance = SegmentNormalizer()
    return _normalizer_instance


def store_mapping(norm_id: str, original_url: str) -> None:
    """
    Store a normalized ID to original URL mapping in the global normalizer.

    Args:
        norm_id (str): The normalized segment ID (hash)
        original_url (str): The original segment URL
    """
    get_normalizer().store_mapping(norm_id, original_url)


def get_original(norm_id: str) -> Optional[str]:
    """
    Retrieve the original URL for a normalized ID from the global normalizer.

    Args:
        norm_id (str): The normalized segment ID

    Returns:
        str | None: The original URL if found, None otherwise
    """
    return get_normalizer().get_original(norm_id)
