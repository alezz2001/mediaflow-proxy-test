import asyncio
import logging
import psutil
from typing import Dict, Optional, List
from urllib.parse import urlparse, urljoin
from collections import OrderedDict

import httpx
from mediaflow_proxy.utils.http_utils import create_httpx_client
from mediaflow_proxy.configs import settings

logger = logging.getLogger(__name__)


class HLSPreBuffer:
    """
    Pre-buffer system for HLS (HTTP Live Streaming) streams with proactive garbage collection
    and input validation to ensure system stability.
    
    This class implements an intelligent caching mechanism that:
    - Pre-downloads video segments before they're requested
    - Maintains an LRU (Least Recently Used) cache
    - Monitors system memory usage
    - Automatically refreshes live playlists
    - Cleans up inactive streams to prevent memory leaks
    """
    
    def __init__(self, max_cache_size: Optional[int] = None, prebuffer_segments: Optional[int] = None):
        """
        Initialize the HLS pre-buffer system.
        
        Args:
            max_cache_size: Maximum number of segments to keep in cache (defaults to settings)
            prebuffer_segments: Number of segments to download ahead of playback (defaults to settings)
        """
        # Configuration from settings or defaults
        self.max_cache_size = max_cache_size or settings.hls_prebuffer_cache_size
        self.prebuffer_segments = prebuffer_segments or settings.hls_prebuffer_segments
        self.max_memory_percent = settings.hls_prebuffer_max_memory_percent
        self.emergency_threshold = settings.hls_prebuffer_emergency_threshold
        
        # LRU cache: stores segment URLs -> binary content
        # OrderedDict maintains insertion order for LRU eviction
        self.segment_cache: "OrderedDict[str, bytes]" = OrderedDict()
        
        # Maps playlist URL -> list of segment URLs (current state)
        self.segment_urls: Dict[str, List[str]] = {}
        
        # Reverse mapping: segment URL -> (playlist_url, index_in_playlist)
        # Allows finding which playlist a segment belongs to
        self.segment_to_playlist: Dict[str, tuple[str, int]] = {}
        
        # Per-playlist metadata and state management
        # Stores: headers, last_access timestamp, refresh_task, target_duration, all_known_segments
        self.playlist_state: Dict[str, dict] = {}
        
        # HTTP client with connection pooling
        self.client = create_httpx_client()

    async def prebuffer_playlist(self, playlist_url: str, headers: Dict[str, str]) -> None:
        """
        Main entry point: Start pre-buffering segments from an HLS playlist.
        
        This method handles both master playlists (which contain variants) and
        media playlists (which contain actual video segments).
        
        Args:
            playlist_url: URL of the HLS playlist (m3u8 file)
            headers: HTTP headers to use for requests (authentication, user-agent, etc.)
        """
        # --- CRITICAL INPUT VALIDATION ---
        # Prevents the "ghost stream" bug: stops the system from treating video segments
        # (which end in .ts, .mp4, etc.) as if they were playlists.
        # Without this check, the system would try to parse binary video data as text,
        # creating phantom streams that never clean up.
        if playlist_url.endswith(('.ts', '.mp4', '.m4s', '.vtt', '.aac', '.mp3')):
            logger.warning(f"Attempted to prebuffer a segment URL as a playlist. Skipping: {playlist_url}")
            return

        try:
            logger.debug(f"Starting pre-buffer for playlist: {playlist_url}")
            # Download the playlist file with explicit timeout
            response = await self.client.get(playlist_url, headers=headers, timeout=15)
            response.raise_for_status()
            playlist_content = response.text

            # Check if this is a master playlist (contains multiple quality variants)
            if "#EXT-X-STREAM-INF" in playlist_content:
                logger.debug("Master playlist detected, finding first variant.")
                # Extract all variant URLs (different quality streams)
                variant_urls = self._extract_variant_urls(playlist_content, playlist_url)
                if variant_urls:
                    # Recursively process the first variant as a media playlist
                    await self.prebuffer_playlist(variant_urls[0], headers)
                else:
                    logger.warning("No variants found in master playlist.")
                return

            # This is a media playlist - contains actual segment URLs
            segment_urls = self._extract_segment_urls(playlist_content, playlist_url)
            if not segment_urls:
                logger.warning(f"No segments found in media playlist: {playlist_url}")
                return

            # Store the segment list for this playlist
            self.segment_urls[playlist_url] = segment_urls
            
            # Build reverse mapping: each segment knows its playlist and index
            for idx, u in enumerate(segment_urls):
                self.segment_to_playlist[u] = (playlist_url, idx)

            # Pre-download the first N segments (configurable via settings)
            await self._prebuffer_segments(segment_urls[:self.prebuffer_segments], headers)
            logger.info(f"Pre-buffered {min(self.prebuffer_segments, len(segment_urls))} segments for {playlist_url}")

            # Parse the target duration (typical segment length in seconds)
            # Used to determine refresh interval for live streams
            target_duration = self._parse_target_duration(playlist_content) or 6
            
            # Initialize or update playlist state
            st = self.playlist_state.get(playlist_url, {})
            
            # Start a background refresh task if not already running
            # This continuously updates live playlists (sliding window)
            if not st.get("refresh_task") or st["refresh_task"].done():
                task = asyncio.create_task(self._refresh_playlist_loop(playlist_url, headers, target_duration))
                self.playlist_state[playlist_url] = {
                    "headers": headers,
                    "last_access": asyncio.get_event_loop().time(),
                    "refresh_task": task,
                    "target_duration": target_duration,
                    # Track ALL segments ever seen for this playlist (for cleanup)
                    "all_known_segments": set(segment_urls)
                }
        except Exception as e:
            logger.error(f"Failed to pre-buffer playlist {playlist_url}: {e}")

    def _extract_segment_urls(self, playlist_content: str, base_url: str) -> List[str]:
        """
        Parse an HLS media playlist and extract all video segment URLs.
        
        HLS playlists contain metadata lines (starting with #) and segment URLs.
        This method filters out metadata and resolves relative URLs.
        
        Args:
            playlist_content: Raw text content of the m3u8 file
            base_url: Base URL of the playlist (for resolving relative paths)
            
        Returns:
            List of absolute segment URLs
        """
        segment_urls = []
        for line in playlist_content.splitlines():
            line = line.strip()
            # Skip empty lines and metadata (comments starting with #)
            if line and not line.startswith('#'):
                # Use urljoin to handle both absolute and relative URLs correctly
                # urljoin("http://example.com/path/playlist.m3u8", "segment.ts")
                # -> "http://example.com/path/segment.ts"
                segment_urls.append(urljoin(base_url, line))
        return segment_urls

    def _extract_variant_urls(self, playlist_content: str, base_url: str) -> List[str]:
        """
        Parse a master playlist and extract URLs of all quality variants.
        
        Master playlists contain #EXT-X-STREAM-INF tags followed by variant URLs.
        Example:
            #EXT-X-STREAM-INF:BANDWIDTH=1280000
            low/index.m3u8
            #EXT-X-STREAM-INF:BANDWIDTH=2560000
            high/index.m3u8
        
        Args:
            playlist_content: Raw text of the master playlist
            base_url: Base URL for resolving relative paths
            
        Returns:
            List of absolute variant playlist URLs
        """
        variant_urls = []
        lines = [l.strip() for l in playlist_content.splitlines()]
        
        # Iterate through lines looking for STREAM-INF tags
        for i, line in enumerate(lines):
            if line.startswith("#EXT-X-STREAM-INF") and i + 1 < len(lines):
                next_line = lines[i+1]
                # The line immediately after STREAM-INF is the variant URL
                if next_line and not next_line.startswith('#'):
                    variant_urls.append(urljoin(base_url, next_line))
        return variant_urls

    def _parse_target_duration(self, playlist_content: str) -> Optional[int]:
        """
        Extract the target duration from a media playlist.
        
        The #EXT-X-TARGETDURATION tag specifies the maximum segment duration.
        This is used to determine how often to refresh the playlist for live streams.
        
        Args:
            playlist_content: Raw text of the media playlist
            
        Returns:
            Target duration in seconds, or None if not found
        """
        for line in playlist_content.splitlines():
            if line.startswith("#EXT-X-TARGETDURATION:"):
                try:
                    # Extract the numeric value after the colon
                    return int(float(line.split(":", 1)[1].strip()))
                except (ValueError, IndexError):
                    return None
        return None

    async def _prebuffer_segments(self, segment_urls: List[str], headers: Dict[str, str]) -> None:
        """
        Download multiple segments concurrently.
        
        Only downloads segments that aren't already cached.
        Uses asyncio.gather for parallel downloads.
        
        Args:
            segment_urls: List of segment URLs to download
            headers: HTTP headers for requests
        """
        # Build list of download tasks, skipping already-cached segments
        tasks = [self._download_segment(url, headers) for url in segment_urls if url not in self.segment_cache]
        if tasks:
            # Execute all downloads in parallel, catching exceptions individually
            await asyncio.gather(*tasks, return_exceptions=True)

    def _get_memory_usage_percent(self) -> float:
        """
        Get current system memory usage as a percentage.
        
        Uses psutil to query system memory. Returns 0.0 on failure to avoid
        blocking cache operations if monitoring fails.
        
        Returns:
            Memory usage percentage (0-100)
        """
        try:
            return psutil.virtual_memory().percent
        except Exception as e:
            logger.warning(f"Failed to get memory usage: {e}")
            return 0.0

    def _enforce_cache_limits(self) -> None:
        """
        Enforce cache size and memory limits using LRU eviction.
        
        Two-tier eviction strategy:
        1. Emergency cleanup: If memory exceeds critical threshold, remove 50% of cache
        2. Normal cleanup: Remove oldest entries until cache size is within limit
        
        This prevents unbounded memory growth and OOM crashes.
        """
        # Emergency cleanup at critical memory threshold
        if self._get_memory_usage_percent() > self.emergency_threshold:
            logger.warning(f"Emergency cache cleanup triggered at {self._get_memory_usage_percent()}% memory usage.")
            items_to_remove = len(self.segment_cache) // 2
            for _ in range(items_to_remove):
                if self.segment_cache:
                    # popitem(last=False) removes the oldest (least recently used) item
                    self.segment_cache.popitem(last=False)
            logger.info(f"Emergency cleanup removed {items_to_remove} oldest segments.")
        
        # Normal size-based cleanup
        while len(self.segment_cache) > self.max_cache_size:
            self.segment_cache.popitem(last=False)

    async def _download_segment(self, segment_url: str, headers: Dict[str, str]) -> None:
        """
        Download a single video segment and add it to the cache.
        
        Implements memory-aware downloading: skips download if system memory is high.
        Uses LRU caching with automatic eviction.
        
        Args:
            segment_url: URL of the segment to download
            headers: HTTP headers for the request
        """
        try:
            # Check memory before downloading to prevent OOM
            if self._get_memory_usage_percent() > self.max_memory_percent:
                logger.warning(f"Memory usage high ({self._get_memory_usage_percent()}%). Skipping download of {segment_url}.")
                return

            # Download with timeout to prevent hanging
            response = await self.client.get(segment_url, headers=headers, timeout=20)
            response.raise_for_status()

            # Add to cache and mark as most recently used
            self.segment_cache[segment_url] = response.content
            self.segment_cache.move_to_end(segment_url)
            
            # Enforce cache limits after adding new segment
            self._enforce_cache_limits()
            logger.debug(f"Cached segment: {segment_url}")

        except Exception as e:
            logger.warning(f"Failed to download segment {segment_url}: {e}")

    async def get_segment(self, segment_url: str, headers: Dict[str, str]) -> Optional[bytes]:
        """
        Retrieve a segment from cache or download it on-demand.
        
        This is the main method called by the streaming proxy. It:
        1. Checks cache first (fast path)
        2. Downloads if not cached (slow path)
        3. Updates access time for inactivity tracking
        
        Args:
            segment_url: URL of the requested segment
            headers: HTTP headers for download (if needed)
            
        Returns:
            Segment binary data, or None if download failed
        """
        # Update last access time for the playlist containing this segment
        # This keeps the refresh task alive for active streams
        playlist_info = self.segment_to_playlist.get(segment_url)
        if playlist_info:
            playlist_url, _ = playlist_info
            if playlist_url in self.playlist_state:
                self.playlist_state[playlist_url]["last_access"] = asyncio.get_event_loop().time()

        # Fast path: cache hit
        if segment_url in self.segment_cache:
            logger.debug(f"Cache hit for segment: {segment_url}")
            # Move to end = mark as most recently used
            self.segment_cache.move_to_end(segment_url)
            return self.segment_cache[segment_url]

        # Slow path: cache miss, download on-demand
        logger.debug(f"Cache miss for segment: {segment_url}. Downloading...")
        await self._download_segment(segment_url, headers)
        return self.segment_cache.get(segment_url)

    async def prebuffer_from_segment(self, segment_url: str, headers: Dict[str, str]) -> None:
        """
        Proactively pre-buffer upcoming segments based on current playback position.
        
        When the player requests segment N, this method pre-downloads segments N+1, N+2, etc.
        This reduces buffering/stuttering during playback.
        
        Args:
            segment_url: URL of the segment currently being played
            headers: HTTP headers for pre-buffering requests
        """
        # Find which playlist and index this segment belongs to
        playlist_info = self.segment_to_playlist.get(segment_url)
        if not playlist_info:
            return
        
        playlist_url, current_index = playlist_info
        
        # Update activity timestamp
        if playlist_url in self.playlist_state:
            self.playlist_state[playlist_url]["last_access"] = asyncio.get_event_loop().time()

        # Get the full segment list and calculate which segments to pre-buffer
        all_segments = self.segment_urls.get(playlist_url, [])
        segments_to_prebuffer = all_segments[current_index + 1 : current_index + 1 + self.prebuffer_segments]
        
        if segments_to_prebuffer:
            await self._prebuffer_segments(segments_to_prebuffer, headers)

    def _cleanup_playlist_resources(self, playlist_url: str):
        """
        Completely remove all resources associated with a playlist.
        
        This is critical for preventing memory leaks in long-running servers.
        Cleans up:
        - All cached segments (including historical ones)
        - Segment-to-playlist mappings
        - Playlist state and metadata
        
        Args:
            playlist_url: URL of the playlist to clean up
        """
        logger.info(f"Cleaning up ALL resources for playlist: {playlist_url}")
        
        # Remove playlist state and extract historical segment list
        state = self.playlist_state.pop(playlist_url, None)
        if not state:
            return

        # Get ALL segments ever seen for this playlist (not just current ones)
        # This catches segments from earlier in live streams
        all_segments_for_playlist = state.get('all_known_segments', [])
        
        # Remove every segment from cache and reverse mapping
        for url in all_segments_for_playlist:
            self.segment_cache.pop(url, None)
            self.segment_to_playlist.pop(url, None)
        
        # Remove playlist's segment list
        self.segment_urls.pop(playlist_url, None)
        
        logger.info(f"Cleaned up {len(all_segments_for_playlist)} historical segments for playlist: {playlist_url}")

    async def _refresh_playlist_loop(self, playlist_url: str, headers: Dict[str, str], target_duration: int) -> None:
        """
        Background task that continuously refreshes a live playlist.
        
        Live HLS streams use a "sliding window" - old segments disappear and new ones
        appear as the stream progresses. This task:
        1. Periodically re-downloads the playlist
        2. Updates the segment list
        3. Removes stale segments from cache
        4. Terminates after inactivity or repeated failures
        
        Args:
            playlist_url: URL of the playlist to refresh
            headers: HTTP headers for requests
            target_duration: Segment duration (determines refresh interval)
        """
        # Refresh interval: based on segment duration, clamped between 2-15 seconds
        sleep_s = max(2, min(15, target_duration))
        
        # If no segments are requested for this long, assume stream is dead
        inactivity_timeout_s = 90
        
        # Terminate after this many consecutive refresh failures
        max_consecutive_failures = 5
        consecutive_failures = 0

        while True:
            await asyncio.sleep(sleep_s)

            # Check if playlist state still exists
            state = self.playlist_state.get(playlist_url)
            if not state:
                logger.debug(f"State for {playlist_url} removed. Terminating refresh task.")
                return

            # Check for inactivity (no segments requested recently)
            now = asyncio.get_event_loop().time()
            if now - state.get("last_access", now) > inactivity_timeout_s:
                logger.info(f"Playlist {playlist_url} inactive for over {inactivity_timeout_s}s. Cleaning up.")
                self._cleanup_playlist_resources(playlist_url)
                return

            try:
                # Re-download the playlist
                resp = await self.client.get(playlist_url, headers=headers, timeout=15)
                resp.raise_for_status()
                content = resp.text
                consecutive_failures = 0  # Reset failure counter on success

                # Update refresh interval if target duration changed
                new_target_duration = self._parse_target_duration(content)
                if new_target_duration:
                    sleep_s = max(2, min(15, new_target_duration))
                
                # Compare old and new segment lists
                old_urls_set = set(self.segment_urls.get(playlist_url, []))
                new_urls = self._extract_segment_urls(content, playlist_url)
                new_urls_set = set(new_urls)

                if new_urls:
                    # Find segments that disappeared (sliding window moved)
                    segments_to_remove = old_urls_set - new_urls_set
                    if segments_to_remove:
                        logger.debug(f"Removing {len(segments_to_remove)} stale segments for {playlist_url}")
                        for url in segments_to_remove:
                            # Remove from cache and reverse mapping
                            self.segment_cache.pop(url, None)
                            self.segment_to_playlist.pop(url, None)

                    # Update segment list
                    self.segment_urls[playlist_url] = new_urls
                    
                    # Rebuild reverse mapping for new segments
                    for idx, u in enumerate(new_urls):
                        self.segment_to_playlist[u] = (playlist_url, idx)
                    
                    # Add new segments to historical tracking (for cleanup)
                    if 'all_known_segments' in state:
                        state['all_known_segments'].update(new_urls)

            except Exception as e:
                consecutive_failures += 1
                logger.warning(f"Refresh failed for {playlist_url} ({consecutive_failures}/{max_consecutive_failures}): {e}")
                
                # Terminate and clean up after repeated failures
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"Max refresh failures reached for {playlist_url}. Terminating and cleaning up.")
                    self._cleanup_playlist_resources(playlist_url)
                    return

    def clear_cache(self) -> None:
        """
        Completely clear all cached data.
        
        Nuclear option: removes everything. Used for shutdown or manual cache reset.
        """
        self.segment_cache.clear()
        self.segment_urls.clear()
        self.segment_to_playlist.clear()
        self.playlist_state.clear()
        logger.info("HLS pre-buffer cache completely cleared.")
    
    async def close(self) -> None:
        """
        Gracefully shut down the pre-buffer system.
        
        Closes the HTTP client and releases resources.
        """
        await self.client.aclose()


# Global singleton instance
# Used across the application to maintain a single cache
hls_prebuffer = HLSPreBuffer()
