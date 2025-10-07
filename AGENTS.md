# Repository Guidelines & Fix Plan

## Scope
These guidelines apply to the entire repository.

## Current Status Overview
The existing `Scrape.py` script aggregates CMA merger case documents into a single `docs/` directory and bundles them into one zip file. It does not organise downloads by merger name, nor does it separate IEOs, Derogations, Revocations, and other documents into dedicated folders. As a result, the deliverable structure described by the user cannot be produced.

## Plan to Fix and Operationalise the Scraper
1. **Audit & Refine Case Discovery**
   - Confirm that the case discovery pathways (`--query-ieo-only` vs `--all-merger-cases`) still return the expected set of CMA merger cases.
   - Harden pagination and de-duplication logic so we do not miss or double-count cases.

2. **Extract Normalised Merger Names**
   - Parse each case page for the canonical merger title (e.g., `<h1>` tag) and derive a filesystem-safe folder name.
   - Maintain a mapping between case URLs and merger names for later use when downloading documents.

3. **Improve Document Classification**
   - Extend `classify_type` with a deterministic fallback category (`Other`) for items that do not match IEO, Derogation, or Revocation rules but should still be archived.
   - Capture the published date and any document metadata that can help confirm classification.

4. **Rework Download & Storage Layout**
   - When fetching each PDF, save it under `output/<merger_name>/<category>/` using a consistent filename (e.g., `{date}_{slugified_title}.pdf`).
   - Keep a manifest (DataFrame) that records the final archive path in addition to source metadata.

5. **Build Requested Zip Structure**
   - After downloads finish, create a zip archive where the root is the merger name and each merger contains subfolders `IEOs`, `Derrogations`, `Revocations`, and `Other` populated with the corresponding PDFs.
   - Include the manifest (CSV/XLSX) at the top level of the zip for reference.

6. **Operational Concerns**
   - Respect polite crawling practices (rate limiting, custom user agent).
   - Add logging and error handling so failures are visible but do not halt the entire run.
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
