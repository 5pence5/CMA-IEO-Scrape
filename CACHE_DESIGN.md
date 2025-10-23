# File Cache Design

This document describes the file caching system implemented for the CMA-IEO-Scrape project.

## Overview

The file cache prevents redundant downloads by tracking downloaded files in a SQLite database. It includes:
- File metadata (URL, path, size, hash)
- HTTP caching headers (ETag, Last-Modified)
- Document classification (case, type, title, dates)
- Integrity verification via SHA256 hashes
- Orphaned entry cleanup

## Architecture

### Database Schema

The cache uses a single SQLite database with one main table:

```sql
CREATE TABLE file_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT UNIQUE NOT NULL,                -- Source URL (unique identifier)
    local_path TEXT NOT NULL,                -- Local filesystem path
    file_hash TEXT,                          -- SHA256 hash for integrity
    file_size INTEGER,                       -- File size in bytes
    etag TEXT,                               -- HTTP ETag header
    last_modified TEXT,                      -- HTTP Last-Modified header
    download_timestamp TEXT NOT NULL,        -- When downloaded (ISO 8601)
    case_title TEXT,                         -- Case title
    case_url TEXT,                           -- Case page URL
    case_path TEXT,                          -- Case path component
    doc_type TEXT,                           -- Document classification
    doc_title TEXT,                          -- Document title
    doc_date_display TEXT,                   -- Display date
    last_verified TEXT                       -- Last verification timestamp
)
```

### Indexes

Three indexes optimize common queries:
- `idx_url`: Fast lookups by URL
- `idx_local_path`: Find cache entries by file path
- `idx_case_url`: Query all documents for a case

## Cache Operations

### Adding Files

When a file is downloaded:
1. File is saved to disk
2. SHA256 hash is computed
3. Metadata is stored in database with `INSERT OR REPLACE`
4. Timestamps are recorded in ISO 8601 format

### Cache Lookup

Before downloading:
1. Query database by URL
2. If found, verify file still exists
3. Check file size matches
4. Verify SHA256 hash matches
5. Update `last_verified` timestamp
6. Return cached metadata if valid

### Conditional Requests

If cached metadata includes ETag or Last-Modified headers:
1. Add `If-None-Match` or `If-Modified-Since` headers to request
2. Server may return `304 Not Modified` to save bandwidth
3. Currently not fully utilized (file deleted before redownload in current implementation)

### Cache Invalidation

Files are invalidated when:
- Verification fails (file missing or hash mismatch)
- User explicitly cleans orphaned entries
- File is manually deleted from database

## Integration with Scraper

### Download Flow

```python
for document in documents:
    if cache and cache.verify_cached_file(url):
        # Use cached file
        use_existing_file()
    else:
        # Download from network
        download_file()
        # Add to cache
        cache.add_or_update_file(url, path, ...)
```

### Statistics Tracking

During download, the system tracks:
- **Cache hits**: Files served from cache
- **Cache misses**: Files needing download
- **New downloads**: Actual HTTP requests made

## Command-Line Interface

### Cache Options

```bash
# Disable caching
--no-cache

# Custom database location
--cache-db /path/to/cache.db

# View statistics
--cache-stats

# Clean orphaned entries
--cache-clean
```

### Cache Statistics Output

```
=== Cache Statistics ===
Total files: 1,234
Total size: 456.78 MB (478,849,024 bytes)
Verified files: 1,200

Files by type:
  Derogation: 456
  IEO: 234
  Revocation: 123
  Other: 421
```

## Future MCP Integration

The cache database provides a foundation for Model Context Protocol (MCP) integration:

### Database as Knowledge Base

The SQLite database contains structured metadata suitable for:
- Full-text search across document titles and types
- Filtering by case, date, or document type
- Querying relationships between documents
- Building a searchable document index

### Potential MCP Features

1. **Document Search API**
   - Query by case name, document type, date range
   - Full-text search across metadata
   - Return file paths for retrieval

2. **Content Indexing**
   - Extract text from PDFs
   - Store text in additional table
   - Enable semantic search

3. **Change Detection**
   - Track document versions
   - Notify when new documents appear
   - Monitor for updates

4. **Analytics**
   - Document type distribution
   - Timeline of cases
   - Derogation patterns

### Example MCP Schema Extension

```sql
-- Future table for full-text content
CREATE TABLE document_content (
    url TEXT PRIMARY KEY,
    content_text TEXT,
    content_hash TEXT,
    extracted_at TEXT,
    FOREIGN KEY (url) REFERENCES file_cache(url)
)

-- Future table for version history
CREATE TABLE document_versions (
    url TEXT,
    version_num INTEGER,
    file_hash TEXT,
    download_timestamp TEXT,
    PRIMARY KEY (url, version_num),
    FOREIGN KEY (url) REFERENCES file_cache(url)
)
```

## Performance Considerations

### Hash Computation

- Uses SHA256 for integrity verification
- Reads files in 8KB chunks to handle large PDFs
- Hash computation adds ~10-50ms per file
- Hashes are stored to avoid recomputation

### Database Size

- Metadata: ~1KB per file
- 10,000 files ≈ 10MB database
- SQLite handles millions of rows efficiently
- Regular VACUUM recommended for maintenance

### Cache Hit Ratio

Expected performance:
- First run: 0% cache hits, all downloads
- Second run: 95%+ cache hits if files unchanged
- Subsequent runs: Near 100% cache hits

### Bandwidth Savings

Example scenario:
- 1,000 PDFs × 500KB average = 500MB total
- First run: 500MB downloaded
- Second run: ~0MB (all cached)
- Annual savings: Depends on run frequency

## Maintenance

### Cleaning Orphaned Entries

```bash
python Scrape.py --out ./output --cache-clean
```

Removes cache entries where local file no longer exists.

### Manual Database Inspection

```bash
sqlite3 output/file_cache.db
sqlite> SELECT COUNT(*) FROM file_cache;
sqlite> SELECT doc_type, COUNT(*) FROM file_cache GROUP BY doc_type;
sqlite> .exit
```

### Resetting Cache

```bash
rm output/file_cache.db
# Next run will rebuild cache from scratch
```

## Testing

### Unit Tests

See `test_cache_integration.py` for cache module tests:
- Basic operations (add, retrieve, verify)
- Hash computation
- Persistence across sessions

### Integration Tests

See `tests/test_download_documents.py`:
- Cache hit behavior
- Backward compatibility (cache optional)
- Mock cache for isolated testing

## File Structure

```
CMA-IEO-Scrape/
├── cache_db.py                    # Cache module
├── Scrape.py                      # Main scraper (uses cache)
├── test_cache_integration.py      # Cache unit tests
├── tests/
│   └── test_download_documents.py # Integration tests
├── CACHE_DESIGN.md                # This file
└── README.md                      # User documentation
```

## Design Decisions

### Why SQLite?

- **Portable**: Single file, no server required
- **Reliable**: ACID transactions, corruption-resistant
- **Fast**: Optimized for local queries
- **Queryable**: Standard SQL interface
- **Python stdlib**: No external dependencies

### Why SHA256?

- **Standard**: Widely used cryptographic hash
- **Fast**: ~500MB/s on modern hardware
- **Reliable**: Collision-resistant
- **Compact**: 64-character hex string

### Why Optional Cache?

- **Backward compatibility**: Existing workflows unaffected
- **Flexibility**: Users can disable for testing
- **Debugging**: Easier to diagnose issues
- **Migration**: Gradual adoption path

## Known Limitations

1. **No version tracking**: Only stores current version
2. **No deduplication**: Same content at different URLs stored separately
3. **No partial downloads**: Resume not supported
4. **No compression**: Database not compressed (VACUUM helps)
5. **No distributed cache**: Single-machine only

## Future Enhancements

1. **Content extraction**: Store PDF text in database
2. **Semantic search**: Vector embeddings for similarity
3. **Version history**: Track changes over time
4. **Deduplication**: Link identical files by hash
5. **MCP server**: Expose database via protocol
6. **Web interface**: Browse cached documents
7. **Export formats**: JSON, Parquet, etc.
