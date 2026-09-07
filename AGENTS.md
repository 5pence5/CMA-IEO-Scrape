# Repository Guidelines & Fix Plan

## Scope
These guidelines apply to the entire repository.

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
- Record run logs and temporary project status in the history document linked below; keep `AGENTS.md` for durable operating guidance.
- Record any new issues, risks, or blockers discovered during the work so future contributors can triage them quickly.

## Historical records

Previous run logs and status snapshots are preserved in [docs/agent-history-2026-09-07.md](docs/agent-history-2026-09-07.md). Consult them only for historical context; they are not current task instructions.
