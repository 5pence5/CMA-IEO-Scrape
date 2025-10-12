# CMA IEO Scraper

A Python script to scrape CMA (Competition and Markets Authority) merger case documents, specifically Initial Enforcement Orders (IEOs), Derogations, and Revocations.

## Installation

Install required dependencies:

```bash
pip install requests beautifulsoup4 lxml pandas openpyxl
```

## Usage

### Option 1: Query IEO cases only (faster)

```bash
python Scrape.py --out ./output --query-ieo-only
```

### Option 2: All merger cases (slower, more complete)

```bash
python Scrape.py --out ./output --all-merger-cases
```

### Limit cases for testing

```bash
python Scrape.py --out ./output --max-cases 10 --query-ieo-only
```

## Output Structure

The script creates:

1. **CSV Index**: `cma_ieo_derogs_revocations_index.csv`
2. **Excel Index**: `cma_ieo_derogs_revocations_index.xlsx`
3. **ZIP Bundle**: `cma_initial_orders_derogs_revocations.zip`
   - Contains index files (CSV & XLSX)
   - Contains PDFs organized by: `{Case}/{Category}/{filename}.pdf`
    - Categories: `IEOs`, `Derogations`, `Revocations`, `Other`

4. **Downloads folder**: `downloads/` (intermediate storage for PDFs)
5. **SQLite cache**: `cma_scrape_cache.sqlite` (created alongside `--out` unless `--db-path` overrides it)

## Important Notes

### Download Failure Handling

- If a download fails, it will appear in the manifest (CSV/XLSX) with an empty `local_path`
- Failed downloads are NOT included in the ZIP file
- Warning messages are printed to stderr for failed downloads
- The manifest provides complete visibility into all discovered documents

### Reusing Output Directory

The script reuses the `--out` directory. To avoid confusion:
- Remove old artifacts with `rm -rf ./output` before running fresh tests
- Or use a different output directory each time

### Persistent cache & incremental updates

- Each run populates a SQLite database (`cma_scrape_cache.sqlite` by default) that records case metadata, document URLs, and local file paths.
- Rerunning the scraper against the same database will skip network downloads for documents that already exist locally.
- Use `--db-path` to point at a shared cache location or keep separate caches per output directory.
- Pass `--refresh-cache` to force redownloading cached files when you need to refresh existing PDFs.
- `--skip-downloads` still updates the manifest/database but will not fetch missing files.

## Recent Fixes

### ZIP Only Contains Index (Fixed 2025-02-15)

**Issue**: When all downloads failed (e.g., network issues), the ZIP file only contained index files with no PDFs.

**Root Cause**: Failed downloads were not tracked in the manifest.

**Fix**: Failed downloads now appear in the manifest with empty `local_path`, providing visibility into what failed while keeping the ZIP clean (only successful downloads).

## Examples

```bash
# Quick test with 5 cases
python Scrape.py --out ./test --max-cases 5 --query-ieo-only

# Full scrape of all IEO-related cases
python Scrape.py --out ./full_output --query-ieo-only

# Full scrape of ALL merger cases (comprehensive but slow)
python Scrape.py --out ./complete --all-merger-cases
```

## Troubleshooting

1. **Missing dependencies**: Run `pip install requests beautifulsoup4 lxml pandas openpyxl`
2. **Empty ZIP**: Check the manifest CSV/XLSX to see if downloads failed (empty `local_path`)
3. **Network errors**: The script prints warnings to stderr; check for connection issues
