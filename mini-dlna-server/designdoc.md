# Mini DLNA Server

## Overview
A production-ready DLNA media server with Samsung TV support and Windows 11 compatibility.

## Requirements

### Compatibility
- Windows 11 (2025 edition) compatible
- Samsung TV support (2022 and later editions)
- DLNA/UPnP 1.5 compliant

### Media Support
- Video: .mp4, .mkv, .avi
- Audio: .mp3, .flac, .wav
- Images: .jpg, .png, .gif

## Architecture

### Components
1. SSDP Service
   - Device discovery via SSDP protocol
   - Periodic alive notifications with exponential backoff
   - M-SEARCH request handling with filtering
   - Multiple network interface support
   - Windows-specific socket handling

2. HTTP Server
   - Media streaming with range support
   - DLNA/UPnP descriptor XML serving
   - Content type negotiation
   - Byte range and time-based seeking
   - Thumbnail generation and caching

3. AVTransport Service
   - Media transport controls (play, pause, stop)
   - Time-based seeking support
   - Playlist management
   - State change notifications

4. Content Directory Service
   - Media indexing with file system monitoring
   - Content browsing interface with sorting
   - Metadata extraction and caching
   - Advanced search capabilities
   - Playlist support for audio files

### Protocol Support
- SSDP for device discovery
- HTTP for content delivery
- UPnP for device control
- DLNA guidelines 1.5 for media format profiles

### Debug System

#### Logging Levels
1. DEBUG: Detailed operation logging
   - SOAP request/response content
   - File operations
   - Browse request parameters
   - Child item counting
   - DIDL-Lite XML generation

2. INFO: Operational status
   - Server start/stop events
   - Client connections
   - Media streaming events
   - Device discovery events

3. WARNING: Non-critical issues
   - Network timeouts
   - Media format issues
   - Cache misses
   - Retry attempts

4. ERROR: Critical issues
   - File access failures
   - XML parsing errors
   - Network socket errors
   - Protocol violations

#### Log File Structure
- dlna_server_debug.log: Full debug information with thread IDs
- dlna_server.log: General operational logs
- non_compatible_files.log: Media format compatibility issues

#### Debug Points
1. Content Directory Service
   - BrowseMetadata requests (object_id, browse_flag)
   - Child item counting results
   - DIDL generation steps
   - File path resolution

2. SSDP Discovery
   - M-SEARCH request details
   - Device announcement timing
   - Interface binding status
   - Client tracking

3. Media Streaming
   - Range request handling
   - Buffer management
   - Network transfer rates
   - Client disconnections

4. Error Recovery
   - Socket error handling
   - Network retry logic
   - Resource cleanup
   - Connection management

## Performance Features

### Network Optimization
- Socket buffer sizing (256KB)
- Connection pooling
- Keep-alive support
- Chunked transfer encoding

### Caching System
- Thumbnail caching (100 items)
- Metadata caching
- File system index caching
- DLNA profile caching

### Error Handling
- Network error recovery (3 retries)
- Socket timeout management
- Client disconnection handling
- Resource cleanup

### Samsung TV Compatibility
- DLNA 1.5 profile support
- Samsung-specific extensions
- Extended content types
- Thumbnail generation
