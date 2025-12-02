# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DolphinScheduler file upload automation system consisting of:
- **Core uploader** (`file_upload.py`): Batch file upload to DolphinScheduler with deduplication
- **File listener service** (`file_listener_service.py`): Real-time file monitoring and auto-upload
- **Configuration system** (`config.py`): Centralized configuration with validation
- **Deployment scripts** (`start_listener.sh`): Service management and automation

## Core Commands & Development Workflow

### File Upload Operations
```bash
# Upload directory to DolphinScheduler (main functionality)
python3 file_upload.py /path/to/directory

# Test connection before upload
python3 file_upload.py --test-connection

# Create sample config file
python3 file_upload.py --create-config

# Use traditional JSON config instead of config.py
python3 file_upload.py /path/to/directory --use-config-file -c config.json

# Custom concurrency (default: 5 workers)
python3 file_upload.py /path/to/directory -w 10
```

### File Listener Service (Real-time Monitoring)
```bash
# Start monitoring service with config.py (recommended)
python3 file_listener_service.py /usr/local/src/dolphinscheduler/test_files

# Start with JSON configuration
python3 file_listener_service.py /path/to/watch --use-config-file -c config.json

# Custom worker count
python3 file_listener_service.py /path/to/watch -w 3

# Test connection only
python3 file_listener_service.py /path/to/watch --test-connection
```

### Quick Start with Scripts
```bash
# Automated startup script (handles dependencies and testing)
./start_listener.sh

# Custom parameters
./start_listener.sh /path/to/watch config.json 3 false
```

### Configuration Validation
```bash
# Validate current config.py
python3 -c "import config; print(config.validate_config())"

# Test config module directly
python3 config.py
```

## Architecture Overview

### Configuration Management
The system supports **two configuration approaches**:

1. **config.py (recommended)**: Modern Python module with validation
   - `get_auth_config()`: Token-based authentication
   - `get_upload_config()`: API endpoints and upload parameters
   - `get_request_config()`: Timeout and SSL settings
   - `validate_config()`: Parameter validation

2. **JSON config file**: Legacy support for existing deployments
   - Simple key-value structure
   - Used with `--use-config-file` flag

### File Upload Workflow
1. **File Collection**: Recursive directory traversal with relative path preservation
2. **Existence Check**: MD5-based deduplication against DolphinScheduler resources
3. **Content Upload**: Direct binary upload via `/resources/online-create` endpoint
4. **Suffix Handling**: Automatic file type detection and DolphinScheduler-compatible suffix mapping

### API Integration Pattern
- **Authentication**: Token-based via `token` header (primary method)
- **API Base**: `{BASE_URL}/dolphinscheduler`
- **File Endpoint**: `/resources/online-create` with form data payload
- **Existence Check**: `/resources` with search parameters
- **Tenant ID**: Always `21` (hardcoded in config)

### File Monitoring Architecture
- **Event Handler**: Uses `watchdog` library for filesystem monitoring
- **Upload Queue**: Thread-safe queue for batching file changes
- **Worker Pool**: Configurable concurrent upload threads (default: 3)
- **Stability Check**: Ensures file writes complete before upload
- **Filtering**: Intelligent filtering of temp files, system files, and logs

## Key Technical Details

### Authentication Patterns
```python
# Primary: Token header authentication
headers = {'token': ACCESS_TOKEN}

# Fallback: Cookie-based (for legacy support)
cookies = {'sessionId': SESSION_ID}
```

### File Type Mapping
DolphinScheduler requires specific lowercase suffixes. The system maps extensions to supported types:
- **Supported**: `['jar', 'zip', 'tar', 'gz', 'py', 'sql', 'json', 'xml', 'properties', 'yml', 'yaml', 'sh', 'bat', 'md', 'txt']`
- **Default**: Files with unsupported extensions use `txt` suffix

### Upload Payload Structure
```python
form_data = {
    "currentDir": "",
    "description": f"Uploaded via File API - {relative_path}",
    "fileName": filename_without_extension,  # Extension handled by suffix param
    "pid": str(parent_id),
    "type": "FILE",
    "tenantId": str(tenant_id),
    "suffix": mapped_suffix  # Critical: must be lowercase
}
```

### Error Handling Strategy
- **Network Errors**: Configurable retry with exponential backoff
- **HTTP 401**: Token authentication failure
- **HTTP 413**: File size limit exceeded
- **Connection Issues**: Graceful degradation with local fallback

## Configuration Parameters

### Essential Settings in config.py
- `BASE_URL`: DolphinScheduler server URL with `/dolphinscheduler` path
- `ACCESS_TOKEN`: Authentication token (get from DolphinScheduler user profile)
- `TENANT_ID`: Always `21` for this deployment
- `PARENT_DIR_ID`: Target directory ID (`-1` for root)
- `REQUEST_TIMEOUT`: HTTP timeout in seconds (default: 300)

### Performance Tuning
- `MAX_CONCURRENT_UPLOADS`: Upload thread pool size (default: 5)
- `CHUNK_SIZE`: File read buffer size (default: 8192 bytes)
- `MAX_RETRIES`: Upload retry attempts (default: 3)
- `RETRY_DELAY`: Delay between retries (default: 1 second)

## Development Notes

### Testing Approach
1. **Connection Test**: Always run `--test-connection` before batch operations
2. **Single File Test**: Use small test directory for debugging
3. **Log Analysis**: Check `uploader.log` for detailed HTTP responses

### Common Modifications
- **Add File Types**: Update `SUPPORTED_EXTENSIONS` in config.py
- **Change Endpoints**: Modify `UPLOAD_PATH` for different API versions
- **Adjust Concurrency**: Tune `MAX_CONCURRENT_UPLOADS` based on server capacity
- **Custom Filtering**: Extend `_should_skip_file()` in `FileUploadHandler`

### Integration Points
- **DolphinScheduler API**: Uses standard REST endpoints
- **File System**: Cross-platform path handling with `pathlib`
- **Configuration**: Environment variable support via `config.py` module