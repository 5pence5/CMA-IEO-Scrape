# Repository Guidelines & Fix Plan

## Scope
These guidelines apply to the entire repository.

## Current Status Overview
- The reworked `Scrape.py` now derives a slugged merger name for every case, writes PDFs into `output/<merger>/IEOs|Derrogations|Revocations|Hold separate manager|Monitoring trustee|Commencement notice|Decision|Other/`, and produces a manifest + zip bundle that matches the requested delivery structure.
- **Fixed (2025-02-15)**: Corrected bug where failed downloads were not tracked, causing ZIP to only contain index when all downloads failed. Now failed downloads appear in the manifest for visibility.
- Added `--only-derogations` flag to keep output focused on derogation documents when desired.
- Added `--only-full-text-decisions` flag plus an outcome-filtered case discovery mode to capture every published full text decision.
- Follow-up work should focus on improving robustness (retry/back-off) and documenting environment prerequisites for reproducible runs.

## Plan to Fix and Operationalise the Scraper
1. **Audit & Refine Case Discovery**
   - Confirm that the case discovery pathways (`--query-ieo-only` vs `--all-merger-cases`) still return the expected set of CMA merger cases.
   - Harden pagination and de-duplication logic so we do not miss or double-count cases.

2. **Extract Normalised Merger Names**
   - Parse each case page for the canonical merger title (e.g., `<h1>` tag) and derive a filesystem-safe folder name.
   - Maintain a mapping between case URLs and merger names for later use when downloading documents.

3. **Improve Document Classification** *(DONE in last commit)*
   - Added deterministic `Other` fallback and captures link text dates for the manifest.
   - IEO-only mode now queries GOV.UK for the specific phrase "initial enforcement order" to reduce noise from other documents.

4. **Rework Download & Storage Layout** *(DONE in last commit)*
   - PDFs are now saved beneath `output/<merger_name>/<category>/` with slugged filenames and tracked in a manifest.

5. **Build Requested Zip Structure** *(DONE in last commit)*
   - A merger-structured zip including the manifest is produced after downloads complete.

6. **Operational Concerns**
   - Respect polite crawling practices (rate limiting, custom user agent).
   - Add logging, retries, and error handling so failures are visible but do not halt the entire run.
   - Document usage instructions and prerequisites in `README` or script docstring.

## Coding Conventions
- Use descriptive function names and docstrings.
- Prefer pathlib for filesystem work where practical.
- Keep requests sessions short-lived and guarded by timeouts.

Following this plan should produce the required zip output with merger-specific folder structures and correctly saved PDF documents.

## Living Document Expectation
- Update this `AGENTS.md` after completing each major task or milestone so it reflects the latest repository status.
- Record any new issues, risks, or blockers discovered during the work so future contributors can triage them quickly.

## Run Log (2025-02-14)
- Executed `python Scrape.py --out ./test_output --max-cases 3 --query-ieo-only` after installing required Python dependencies (`requests`, `beautifulsoup4`, `lxml`, `pandas`, `openpyxl`).
- Initial run failed because `pandas` was missing (`ModuleNotFoundError`). Installing the documented dependencies resolved the issue and the script produced the CSV/XLSX manifest, ZIP archive, and per-case download folders successfully.

## Run Log (2025-02-15)
- Attempted `python Scrape.py --out ./test_output --max-cases 1 --query-ieo-only`; run failed with `ModuleNotFoundError: No module named 'pandas'`.
- Installed the documented dependencies via `pip install requests beautifulsoup4 lxml pandas openpyxl`.
- Re-ran `python Scrape.py --out ./test_output --max-cases 1 --query-ieo-only`; the script reported writing the manifest CSV/XLSX and `cma_initial_orders_derogs_revocations.zip`.
- Verified artefacts with `ls test_output`, which shows `cma_initial_orders_derogs_revocations.zip`, the manifest files, and the per-case `downloads/` folder.
- **Reminder:** the scraper reuses the `--out` directory, so remove any old artefacts with `rm -rf ./test_output` (or choose a new `--out` path) before running fresh tests to avoid confusing previous outputs with new ones.

## Bug Fix (2025-02-15) - ZIP Only Contains Index Issue
### Problem
User reported that after running `python Scrape.py --out test_output --max-cases 12 --query-ieo-only`, the generated ZIP file only contained the index files (CSV/XLSX) and no PDF documents.

### Root Cause
The `download_documents()` function had a bug in its exception handler. When a download failed:
- The exception was caught and a warning was printed to stderr
- The record's `local_path` was set to an empty string
- **BUT** the record was NOT appended to the `downloaded` list

This meant:
- If ALL downloads failed (e.g., network issues, permission errors), the `downloaded` list would be empty
- The DataFrame/manifest would be empty or nearly empty
- The ZIP would only contain the index files with no PDFs

### Fix Applied
Modified the exception handler in `download_documents()` (line 252) to append failed download records to the `downloaded` list:
```python
except Exception as exc:
    record["local_path"] = ""
    downloaded.append(record)  # <- Added this line
    print(f"[warn] download failed {url}: {exc}", file=sys.stderr)
```

### Benefits
1. **Failed downloads are now tracked** in the manifest/index so users can see what was supposed to be downloaded
2. **Better visibility** - users can identify which documents failed and investigate why
3. **Manifest completeness** - the CSV/XLSX index shows all discovered documents, not just successful downloads
4. **ZIP behavior unchanged** - failed downloads (with empty `local_path`) are still excluded from the ZIP via existing checks

### Testing
Verified with simulated scenarios:
- All downloads succeed → all PDFs in ZIP ✓
- Some downloads fail → successful PDFs in ZIP, failed ones in manifest only ✓
- All downloads fail → ZIP has index only, but manifest shows all attempted downloads ✓
## Run Log (2025-10-07)
- Added dedicated classification buckets for Hold separate manager, Monitoring trustee, Commencement notice, and Decision documents so they materialise as separate folders in both the on-disk layout and the ZIP bundle.
- Installed the optional `lxml` dependency to eliminate BeautifulSoup parser warnings and re-ran `python Scrape.py --out ./test_output --max-cases 12 --query-ieo-only` on a clean output directory.
- Zip bundle now includes the new category folders with the downloaded PDFs alongside the refreshed CSV/XLSX manifest.
- Restricted IEO-only search to the exact phrase "initial enforcement order" and validated with `python3 Scrape.py --out ./ieo_output --query-ieo-only`.
- Added `--only-derogations` flag and verified `python3 Scrape.py --out ./ieo_derogs_output --query-ieo-only --only-derogations` produces an index/ZIP containing only derogation PDFs.

## Run Log (2025-10-08)
- Introduced `--all-merger-cases-with-outcomes` to drive discovery from the GOV.UK finder using the full set of merger outcome filters, and added `--only-full-text-decisions` to restrict downloads to "Full text decision" PDFs.
- Confirmed per-row `not_downloaded` flags are populated in the index so missed documents are easy to audit.
- Ran `python3 Scrape.py --out ./fulltext_decisions_outcomes --all-merger-cases-with-outcomes --only-full-text-decisions` to exercise the new mode across the full dataset (≈1.8k decision PDFs).
- Smoke-tested with `python3 Scrape.py --out ./smoke_fulltext --all-merger-cases-with-outcomes --only-full-text-decisions --max-cases 3` to ensure small-batch operation still succeeds.

## Run Log (2025-10-07)
- Added dedicated classification buckets for Hold separate manager, Monitoring trustee, Commencement notice, and Decision documents so they materialise as separate folders in both the on-disk layout and the ZIP bundle.
- Installed the optional `lxml` dependency to eliminate BeautifulSoup parser warnings and re-ran `python Scrape.py --out ./test_output --max-cases 12 --query-ieo-only` on a clean output directory.
- Zip bundle now includes the new category folders with the downloaded PDFs alongside the refreshed CSV/XLSX manifest.
- Restricted IEO-only search to the exact phrase "initial enforcement order" and validated with `python3 Scrape.py --out ./ieo_output --query-ieo-only`.
- Added `--only-derogations` flag and verified `python3 Scrape.py --out ./ieo_derogs_output --query-ieo-only --only-derogations` produces an index/ZIP containing only derogation PDFs.

## Run Log (2025-10-08)
- Introduced `--all-merger-cases-with-outcomes` to drive discovery from the GOV.UK finder using the full set of merger outcome filters, and added `--only-full-text-decisions` to restrict downloads to "Full text decision" PDFs.
- Confirmed per-row `not_downloaded` flags are populated in the index so missed documents are easy to audit.
- Ran `python3 Scrape.py --out ./fulltext_decisions_outcomes --all-merger-cases-with-outcomes --only-full-text-decisions` to exercise the new mode across the full dataset (≈1.8k decision PDFs).
- Smoke-tested with `python3 Scrape.py --out ./smoke_fulltext --all-merger-cases-with-outcomes --only-full-text-decisions --max-cases 3` to ensure small-batch operation still succeeds.
