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
