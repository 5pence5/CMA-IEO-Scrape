# Repository Guidelines & Fix Plan

## Scope
These guidelines apply to the entire repository.

## Current Status Overview
- The reworked `Scrape.py` now derives a slugged merger name for every case, writes PDFs into `output/<merger>/IEOs|Derrogations|Revocations|Other/`, and produces a manifest + zip bundle that matches the requested delivery structure.
- Search flows for both the IEO keyword query and the all-merger index remain in place, and attachments are categorised with the extended heuristics from the last change.
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
