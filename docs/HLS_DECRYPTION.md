# HLS AES-128 Decryption Feature

## Overview

MediaFlow Proxy now supports automatic decryption of HLS segments encrypted with AES-128-CBC. This feature is particularly useful for streams that use encrypted `.css` segment files or other encrypted formats that some media players cannot handle directly.

## How It Works

### Automatic Detection

When you request a stream through the DLHD extractor, the proxy automatically:

1. Fetches the M3U8 manifest
2. Detects if segments are encrypted (checks for `#EXT-X-KEY` tags)
3. Detects if segments use non-standard extensions like `.css`
4. If encryption is found, enables automatic decryption

### Decryption Process

1. **Key Fetching**: The decryption key is automatically downloaded from the key URL specified in the M3U8 manifest
2. **Segment Download**: Encrypted segments are downloaded and cached
3. **Decryption**: Segments are decrypted using AES-128-CBC with the fetched key
4. **Serving**: Decrypted segments are served to the media player as standard TS/MP4 files

### Prebuffering Integration

Decryption is integrated with the HLS prebuffering system:
- Decrypted segments are cached for fast access
- Upcoming segments are pre-downloaded and pre-decrypted
- Memory limits are respected
- Automatic cleanup prevents memory leaks

## Usage

### Via DLHD Extractor (Automatic)

The simplest way is to use the DLHD extractor with `redirect_stream=true`:

```bash
curl "http://localhost:8888/extractor/video?host=DLHD&redirect_stream=true&d=https://dlhd.dad/watch.php?id=881&api_password=your_password"
```

This will:
- Extract the stream URL
- Detect encryption automatically
- Redirect to the HLS manifest proxy with decryption enabled

### Manual Activation

You can also manually enable decryption for any HLS stream:

```bash
mpv "http://localhost:8888/proxy/hls/manifest.m3u8?d=https://example.com/encrypted.m3u8&enable_hls_decryption=true&api_password=your_password"
```

## Technical Details

### Supported Encryption

- **Method**: AES-128-CBC (standard HLS encryption)
- **Key Size**: 16 bytes (128 bits)
- **IV Support**: 
  - Explicit IV from M3U8 (`IV=0x...`)
  - Sequence-number-based IV (when not specified)

### Performance Considerations

- **Key Caching**: Decryption keys are cached to avoid repeated downloads
- **Segment Caching**: Decrypted segments are cached using LRU eviction
- **Memory Management**: Respects `HLS_PREBUFFER_CACHE_SIZE` and memory limits
- **Parallel Processing**: Multiple segments can be decrypted concurrently

### Configuration

Decryption works with existing HLS prebuffer settings:

```env
# Enable HLS prebuffering (optional - auto-enabled when decryption is needed)
ENABLE_HLS_PREBUFFER=true

# Number of segments to pre-decrypt ahead
HLS_PREBUFFER_SEGMENTS=5

# Maximum number of decrypted segments to cache
HLS_PREBUFFER_CACHE_SIZE=50

# Memory limits
HLS_PREBUFFER_MAX_MEMORY_PERCENT=80
HLS_PREBUFFER_EMERGENCY_THRESHOLD=90
```

## Examples

### Example 1: DLHD Stream with .css Segments

```bash
# Request stream via extractor
curl "http://localhost:8888/extractor/video?host=DLHD&redirect_stream=true&d=https://dlhd.dad/watch.php?id=881&api_password=mypass"

# Response redirects to:
# http://localhost:8888/proxy/hls/manifest.m3u8?d=https://...newkso.ru/.../mono.m3u8&enable_hls_decryption=true&...
```

### Example 2: Direct HLS Stream with Encryption

```bash
# If you have a direct M3U8 URL with encryption
mpv "http://localhost:8888/proxy/hls/manifest.m3u8?d=https://example.com/encrypted/playlist.m3u8&enable_hls_decryption=true&api_password=mypass"
```

### Example 3: Generate Encrypted URL

```python
import requests

# Generate a proxied URL with decryption
response = requests.post("http://localhost:8888/generate_url", json={
    "mediaflow_proxy_url": "http://localhost:8888",
    "endpoint": "/proxy/hls/manifest.m3u8",
    "destination_url": "https://example.com/encrypted/playlist.m3u8",
    "query_params": {
        "enable_hls_decryption": "true",
        "api_password": "mypass"
    }
})

print(response.json()["url"])
```

## Troubleshooting

### Issue: Segments not decrypting

**Possible causes:**
- The key URL is not accessible
- The key format is incorrect (must be exactly 16 bytes)
- The encryption method is not AES-128

**Solution:** Check the logs for decryption errors and verify the M3U8 manifest.

### Issue: Playback stuttering

**Possible causes:**
- Insufficient prebuffer cache
- Memory limits too low
- Network latency for key/segment downloads

**Solution:** 
- Increase `HLS_PREBUFFER_SEGMENTS` to pre-decrypt more segments
- Increase `HLS_PREBUFFER_CACHE_SIZE` to cache more decrypted segments
- Adjust memory limits if needed

### Issue: High memory usage

**Possible causes:**
- Too many segments cached
- Large segment sizes
- Multiple simultaneous streams

**Solution:**
- Decrease `HLS_PREBUFFER_CACHE_SIZE`
- Lower `HLS_PREBUFFER_MAX_MEMORY_PERCENT`
- The emergency cleanup will trigger automatically at 90% by default

## Security Considerations

### Data Privacy
- Decryption keys are temporarily cached in memory
- Keys are not logged or persisted to disk
- Decrypted segments are only cached temporarily

### HTTPS Requirement
- Key URLs should use HTTPS to prevent key interception
- The proxy respects SSL verification settings per domain

### Access Control
- API password protection applies to all endpoints
- Encrypted URL tokens can be used for additional security

## Limitations

1. **Encryption Methods**: Only AES-128-CBC is supported (standard HLS encryption)
2. **DRM**: This is NOT a DRM solution - it only handles standard HLS encryption
3. **Key Accessibility**: The encryption key must be accessible via HTTP(S)
4. **Performance**: Decryption adds CPU overhead, though it's generally negligible
