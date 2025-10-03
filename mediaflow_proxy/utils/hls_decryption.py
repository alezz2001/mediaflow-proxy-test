"""
HLS AES-128 decryption utilities for encrypted segments.

This module provides functionality to decrypt HLS segments encrypted with AES-128-CBC,
which is commonly used in HLS streams for content protection.
"""

import logging
import re
from typing import Optional, Dict, Tuple
from urllib.parse import urljoin, urlparse

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

logger = logging.getLogger(__name__)


class HLSDecryptor:
    """Handles decryption of HLS segments encrypted with AES-128."""
    
    def __init__(self):
        """Initialize the HLS decryptor."""
        self.key_cache: Dict[str, bytes] = {}  # Cache decryption keys by URL
    
    async def get_decryption_key(self, key_url: str, headers: Dict[str, str], http_client) -> bytes:
        """
        Fetch and cache the decryption key from the key URL.
        
        Args:
            key_url: URL to fetch the decryption key from
            headers: HTTP headers to use for the request
            http_client: HTTP client instance to use for the request
            
        Returns:
            The decryption key as bytes
        """
        if key_url in self.key_cache:
            logger.debug(f"Using cached decryption key for: {key_url}")
            return self.key_cache[key_url]
        
        try:
            response = await http_client.get(key_url, headers=headers)
            response.raise_for_status()
            key = response.content
            
            # HLS AES-128 keys should be exactly 16 bytes
            if len(key) != 16:
                logger.warning(f"Key length is {len(key)} bytes, expected 16 bytes. Will use as-is.")
            
            self.key_cache[key_url] = key
            logger.info(f"Fetched and cached decryption key from: {key_url}")
            return key
            
        except Exception as e:
            logger.error(f"Failed to fetch decryption key from {key_url}: {e}")
            raise
    
    def decrypt_segment(self, encrypted_data: bytes, key: bytes, iv: Optional[bytes] = None) -> bytes:
        """
        Decrypt an HLS segment encrypted with AES-128-CBC.
        
        Args:
            encrypted_data: The encrypted segment data
            key: The 16-byte decryption key
            iv: The initialization vector (IV). If None, uses a zero IV or sequence number.
            
        Returns:
            The decrypted segment data
        """
        try:
            # If no IV is provided, use a zero IV (common in HLS)
            if iv is None:
                iv = bytes([0] * 16)
            
            # Ensure IV is exactly 16 bytes
            if len(iv) != 16:
                logger.warning(f"IV length is {len(iv)} bytes, expected 16. Padding/truncating.")
                if len(iv) < 16:
                    iv = iv + bytes([0] * (16 - len(iv)))
                else:
                    iv = iv[:16]
            
            # Create AES cipher in CBC mode
            cipher = AES.new(key, AES.MODE_CBC, iv)
            
            # Decrypt the data
            decrypted_data = cipher.decrypt(encrypted_data)
            
            # Try to remove PKCS7 padding
            # Some HLS segments may not be padded, so handle both cases
            try:
                decrypted_data = unpad(decrypted_data, AES.block_size)
            except ValueError:
                # If unpadding fails, the data might not be padded
                logger.debug("Segment appears to not use PKCS7 padding")
            
            return decrypted_data
            
        except Exception as e:
            logger.error(f"Failed to decrypt segment: {e}")
            raise
    
    def clear_key_cache(self):
        """Clear the cached decryption keys."""
        self.key_cache.clear()
        logger.info("Cleared decryption key cache")


def parse_m3u8_encryption_info(m3u8_content: str, base_url: str) -> Optional[Tuple[str, Optional[str]]]:
    """
    Parse M3U8 content to extract encryption information.
    
    Args:
        m3u8_content: The M3U8 playlist content
        base_url: Base URL to resolve relative key URLs
        
    Returns:
        Tuple of (key_url, iv) if encryption is found, None otherwise.
        iv can be None if not specified in the playlist.
    """
    # Look for EXT-X-KEY tags
    key_pattern = r'#EXT-X-KEY:([^\n]+)'
    key_matches = re.findall(key_pattern, m3u8_content)
    
    if not key_matches:
        return None
    
    # Parse the first key tag (most common case)
    key_line = key_matches[0]
    
    # Check if encryption method is AES-128
    if 'METHOD=AES-128' not in key_line:
        logger.debug(f"Encryption method is not AES-128: {key_line}")
        return None
    
    # Extract key URI
    uri_match = re.search(r'URI="([^"]+)"', key_line)
    if not uri_match:
        logger.warning("No URI found in EXT-X-KEY tag")
        return None
    
    key_url = uri_match.group(1)
    # Resolve relative URLs
    key_url = urljoin(base_url, key_url)
    
    # Extract IV if present
    iv_match = re.search(r'IV=0x([0-9A-Fa-f]+)', key_line)
    iv = iv_match.group(1) if iv_match else None
    
    logger.info(f"Found AES-128 encryption - Key URL: {key_url}, IV: {iv or 'not specified'}")
    
    return (key_url, iv)


def has_encrypted_segments(m3u8_content: str) -> bool:
    """
    Check if an M3U8 playlist contains encrypted segments.
    
    Args:
        m3u8_content: The M3U8 playlist content
        
    Returns:
        True if encryption is detected, False otherwise
    """
    return '#EXT-X-KEY:' in m3u8_content and 'METHOD=AES-128' in m3u8_content


# Global instance for reuse
hls_decryptor = HLSDecryptor()
