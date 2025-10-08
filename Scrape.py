#!/usr/bin/env python3
"""
Scrape CMA merger-case Initial Enforcement Orders (IEOs), Derogations, and Revocations
and build an index + zip of all matching PDFs.

Usage:
  pip install requests beautifulsoup4 lxml pandas openpyxl
  python cma_ieo_scraper.py --out ./cma_ieo_bundle --all-merger-cases
    # or restrict to cases that mention IEO explicitly:
  python cma_ieo_scraper.py --out ./cma_ieo_bundle --query-ieo-only
"""

import argparse
import os
import re
import sys
import time
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, FeatureNotFound

try:
    BeautifulSoup("", "lxml")
    BS_PARSER = "lxml"
except FeatureNotFound:
    BS_PARSER = "html.parser"
    print(
        "[info] Optional dependency 'lxml' not found; falling back to Python's built-in HTML parser.",
        file=sys.stderr,
    )

BASE = "https://www.gov.uk"
SEARCH_API = f"{BASE}/api/search.json"
HEADERS = {"User-Agent": "CMA-IEO-scraper (legal research; contact: your-email@example.com)"}

CMA_ORG_SLUG = "competition-and-markets-authority"
CMA_CASE_FORMAT = "cma_case"
IEO_KEYWORDS = [
    "initial enforcement order",
]

# Heuristics for classifying attachment type from link text
TYPE_RULES = [
    (re.compile(r"\binitial enforcement order\b", re.I), "Initial enforcement order"),
    (re.compile(r"\bieo\b", re.I), "Initial enforcement order"),
    (re.compile(r"\brevocation\b", re.I), "Revocation order"),
    (re.compile(r"\bderogation\b", re.I), "Derogation"),
    (re.compile(r"\bconsent\b", re.I), "Derogation"),
    (re.compile(r"hold[-\s]?separate manager", re.I), "Hold separate manager"),
    (re.compile(r"monitoring trustee", re.I), "Monitoring trustee"),
    (re.compile(r"commencement", re.I), "Commencement notice"),
    (re.compile(r"decision", re.I), "Decision"),
]

CATEGORY_TO_FOLDER = {
    "Initial enforcement order": "IEOs",
    "Derogation": "Derrogations",
    "Revocation order": "Revocations",
    "Hold separate manager": "Hold separate manager",
    "Monitoring trustee": "Monitoring trustee",
    "Commencement notice": "Commencement notice",
    "Decision": "Decision",
    "Other": "Other",
}

MAX_CASE_DIR_LEN = 80
MAX_FILE_STEM_LEN = 96


def classify_type(text: str, href: str) -> str:
    """Return the best-guess document category for an attachment."""

    text = text or ""
    href = href or ""
    for rx, label in TYPE_RULES:
        if rx.search(text):
            return label

    # Fall back to hints in the URL itself – many attachments include the type there.
    href_lower = href.lower()
    if any(k in href_lower for k in ("initial-enforcement-order", "initial_enforcement_order", "ieo")):
        return "Initial enforcement order"
    if any(k in href_lower for k in ("derogation", "consent")):
        return "Derogation"
    if "revocation" in href_lower:
        return "Revocation order"
    if "hold" in href_lower and "separate" in href_lower and "manager" in href_lower:
        return "Hold separate manager"
    if "monitoring" in href_lower and "trustee" in href_lower:
        return "Monitoring trustee"
    if "commencement" in href_lower and "notice" in href_lower:
        return "Commencement notice"
    if "decision" in href_lower:
        return "Decision"

    # As a final heuristic, sometimes the link text just references "order" alongside IEO keywords.
    text_lower = text.lower()
    if "order" in text_lower and "enforcement" in text_lower:
        return "Initial enforcement order"
    if "hold" in text_lower and "separate" in text_lower and "manager" in text_lower:
        return "Hold separate manager"
    if "monitoring" in text_lower and "trustee" in text_lower:
        return "Monitoring trustee"
    if "commencement" in text_lower and "notice" in text_lower:
        return "Commencement notice"
    if "decision" in text_lower:
        return "Decision"

    return "Other"

def search_cases_ieo_only(session):
    """Use Search API to find CMA cases mentioning IEO (or related terms)."""
    q = " OR ".join([f'"{k}"' for k in IEO_KEYWORDS])
    params = {
        "q": q,
        "filter_format": CMA_CASE_FORMAT,
        "filter_organisations": CMA_ORG_SLUG,
        "count": 1500,
        # we can ask for extra fields but link+title suffice:
        "fields": "title,link,public_timestamp",
        "order": "-public_timestamp",
    }
    r = session.get(SEARCH_API, params=params, headers=HEADERS, timeout=60)
    r.raise_for_status()
    data = r.json()
    results = data.get("results", [])
    # de-duplicate by link
    seen, unique = set(), []
    for it in results:
        link = it.get("link")
        if link and link not in seen:
            seen.add(link)
            unique.append(it)
    return unique

def search_all_merger_cases(session, outcome_types=None):
    """
    Use the finder page HTML with filters for case_type and outcome_type, then paginate and collect case links.
    """
    links = []
    page = 1
    # Build the filter string
    base_url = f"{BASE}/cma-cases?case_type[]=mergers"
    if outcome_types:
        for ot in outcome_types:
            base_url += f"&outcome_type[]={ot}"
    while True:
        url = f"{base_url}&page={page}"
        resp = session.get(url, headers=HEADERS, timeout=60)
        if resp.status_code != 200:
            break
        soup = BeautifulSoup(resp.text, BS_PARSER)
        for a in soup.select("a.gem-c-document-list__item-title, .gem-c-document-list a"):
            href = a.get("href", "")
            if href.startswith("/cma-cases/"):
                links.append({"title": a.get_text(strip=True), "link": href})
        # detect pagination: if no "Next" or the page repeats, stop
        if f"page={page+1}" not in resp.text:
            break
        page += 1
        time.sleep(0.5)
    # de-duplicate by link
    seen, unique = set(), []
    for it in links:
        link = it.get("link")
        if link and link not in seen:
            seen.add(link)
            unique.append(it)
    return unique

DATE_RX = re.compile(r"\((\d{1,2}\.\d{1,2}\.\d{2,4})\)$")  # e.g. (9.9.25) or (10.8.2021)

def ensure_absolute_asset_url(case_url: str, href: str) -> str:
    """Return an absolute URL for GOV.UK asset links."""
    if not href:
        return ""
    # Some links use protocol-relative form (//assets.publishing...).
    if href.startswith("//"):
        return f"https:{href}"
    if href.startswith("http://") or href.startswith("https://"):
        return href
    # Fallback to joining against case URL (covers /government/uploads/...)
    return urljoin(case_url, href)


def parse_case_for_docs(session, case: Dict[str, str]) -> List[Dict[str, str]]:
    """Parse a case page and return list of attachment dicts that look like IEO/Revocation/Derogation."""
    case_path = case.get("link", "")
    case_title = case.get("title", "")
    if not case_path:
        return []
    url = urljoin(BASE, case_path)
    resp = session.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, BS_PARSER)

    out = []
    for a in soup.select("a"):
        text = a.get_text(" ", strip=True)
        href = ensure_absolute_asset_url(url, a.get("href", ""))
        # We only care about GOV.UK assets (PDFs usually at assets.publishing.service.gov.uk)
        if not href or "assets.publishing.service.gov.uk" not in href:
            continue
        if not href.lower().endswith(".pdf"):
            # Ignore non-PDF attachments.
            continue
        doc_type = classify_type(text, href)
        if not doc_type:
            doc_type = "Other"
        # Try to pick up the trailing (dd.mm.yy) GOV.UK convention shown next to links
        # Often the date is in the same text; if not, we leave blank.
        m = DATE_RX.search(text)
        date_disp = m.group(1).replace(".", "/") if m else ""

        out.append({
            "case_url": url,
            "case_path": case_path,
            "case_title": case_title,
            "doc_title": text,
            "doc_type": doc_type,
            "doc_date_display": date_disp,
            "doc_url": href,
        })
    return out


def slugify(value: str) -> str:
    """Return a filesystem-safe slug."""
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = value.strip("-")
    return value or "document"


def safe_folder_name(value: str) -> str:
    value = value.strip()
    value = re.sub(r"[\\/]+", "-", value)
    value = re.sub(r"[^A-Za-z0-9 _.-]", "", value)
    value = value.strip()
    return value or "case"


def truncate_component(value: str, max_len: int) -> str:
    if len(value) <= max_len:
        return value
    return value[:max_len].rstrip("-_ ") or value[:max_len]


def build_case_dirs(case_title: str, case_path: str) -> Dict[str, str]:
    """Return local and zip directory names for a case."""
    case_title = case_title or case_path or "case"
    case_slug = slugify(case_path or case_title)
    case_slug = truncate_component(case_slug, MAX_CASE_DIR_LEN)

    pretty = safe_folder_name(case_title)
    pretty = truncate_component(pretty, MAX_CASE_DIR_LEN)

    local = pretty
    if case_slug and case_slug not in local:
        local = truncate_component(f"{local}__{case_slug}", MAX_CASE_DIR_LEN * 2)

    return {
        "local": local or case_slug or "case",
        "zip": case_slug or local or "case",
        "slug": case_slug or "case",
    }


def download_documents(
    session: requests.Session, docs: Iterable[Dict[str, str]], base_dir: Path
) -> List[Dict[str, str]]:
    base_dir.mkdir(parents=True, exist_ok=True)
    downloaded: List[Dict[str, str]] = []

    for idx, record in enumerate(docs, 1):
        url = record["doc_url"]
        try:
            case_title = record.get("case_title") or record.get("case_path", "case")
            dirs = build_case_dirs(case_title, record.get("case_path", "case"))
            case_dir = base_dir / dirs["local"]
            category = CATEGORY_TO_FOLDER.get(record.get("doc_type"), "Other")
            category_dir = case_dir / category
            category_dir.mkdir(parents=True, exist_ok=True)

            title_slug = slugify(record.get("doc_title", "document"))
            title_slug = truncate_component(title_slug, MAX_FILE_STEM_LEN)
            stem = truncate_component(f"{dirs['slug']}__{title_slug}", MAX_FILE_STEM_LEN * 2)
            filename = f"{stem}.pdf"
            local_path = category_dir / filename
            counter = 2
            while local_path.exists() and local_path.stat().st_size > 0:
                stem = truncate_component(f"{dirs['slug']}__{title_slug}-{counter}", MAX_FILE_STEM_LEN * 2)
                filename = f"{stem}.pdf"
                local_path = category_dir / filename
                counter += 1

            if not local_path.exists():
                with session.get(url, headers=HEADERS, stream=True, timeout=120) as resp:
                    resp.raise_for_status()
                    with open(local_path, "wb") as f:
                        for chunk in resp.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

            record["local_path"] = str(local_path)
            record["zip_case_dir"] = dirs["zip"]
            record["zip_filename"] = os.path.basename(local_path)
            downloaded.append(record)
        except Exception as exc:
            record["local_path"] = ""
            downloaded.append(record)
            print(f"[warn] download failed {url}: {exc}", file=sys.stderr)
        time.sleep(0.2)

    return downloaded

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="Output folder for index & zip")
    ap.add_argument(
        "--max-cases",
        type=int,
        default=0,
        help="Optional limit on number of merger cases to scrape (useful for testing)",
    )
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--query-ieo-only", action="store_true", help="Only cases that mention IEO/derogation/revocation (faster)")
    mode.add_argument("--all-merger-cases", action="store_true", help="Parse all CMA merger cases (slower, more complete)")
    mode.add_argument("--all-merger-cases-with-outcomes", action="store_true", help="Parse all CMA merger cases with specific outcome types (most complete)")
    ap.add_argument(
        "--only-derogations",
        action="store_true",
        help="Download only documents classified as derogations",
    )
    ap.add_argument(
        "--only-full-text-decisions",
        action="store_true",
        help="Download only documents classified as 'Decision' and whose title contains 'Full text decision' (case-insensitive)",
    )
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    docs_dir = Path(args.out) / "downloads"

    s = requests.Session()

    if args.all_merger_cases_with_outcomes:
        # Use the outcome types from the provided URL
        outcome_types = [
            "markets-phase-1-no-enforcement-action",
            "markets-phase-1-undertakings-in-lieu-of-reference",
            "markets-phase-1-referral",
            "mergers-phase-1-clearance",
            "mergers-phase-1-clearance-with-undertakings-in-lieu",
            "mergers-phase-1-referral",
            "mergers-phase-1-found-not-to-qualify",
            "mergers-phase-1-public-interest-interventions",
            "markets-phase-2-clearance-no-adverse-effect-on-competition",
            "markets-phase-2-adverse-effect-on-competition-leading-to-remedies",
            "markets-phase-2-decision-to-dispense-with-procedural-obligations",
            "mergers-phase-2-clearance",
            "mergers-phase-2-clearance-with-remedies",
            "mergers-phase-2-prohibition",
            "mergers-phase-2-cancellation",
        ]
        cases = search_all_merger_cases(s, outcome_types=outcome_types)
    elif args.all_merger_cases:
        cases = search_all_merger_cases(s)
    else:
        cases = search_cases_ieo_only(s)
        if not cases:
            # fallback to the finder with the IEO keyword
            fallback_url = f"{BASE}/cma-cases?case_type[]=mergers&keywords=initial+enforcement+order"
            r = s.get(fallback_url, headers=HEADERS, timeout=60)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, BS_PARSER)
            cases = [{"title": a.get_text(strip=True), "link": a["href"]}
                     for a in soup.select("a.gem-c-document-list__item-title, .gem-c-document-list a")
                     if a.get("href", "").startswith("/cma-cases/")]

    # Crawl case pages
    records = []
    if args.max_cases and args.max_cases > 0:
        cases = cases[: args.max_cases]

    for i, it in enumerate(cases, 1):
        case_path = it.get("link")
        if not case_path:
            continue
        try:
            case_record = {"link": case_path, "title": it.get("title", "")}
            recs = parse_case_for_docs(s, case_record)
            records.extend(recs)
        except Exception as e:
            print(f"[warn] case {case_path}: {e}", file=sys.stderr)
        time.sleep(0.25)


    # De-duplicate docs by URL and apply filters
    seen, unique = set(), []
    for r in records:
        u = r["doc_url"]
        if u in seen:
            continue
        seen.add(u)
        # Derogation filter
        if args.only_derogations and r.get("doc_type") != "Derogation":
            continue
        # Full text decision filter
        if args.only_full_text_decisions:
            if r.get("doc_type") != "Decision":
                continue
            title = r.get("doc_title", "").lower()
            if "full text decision" not in title:
                continue
        unique.append(r)

    # Download
    downloaded = download_documents(s, unique, docs_dir)

    # Track files not downloaded (failed downloads)
    not_downloaded = [r for r in unique if not r.get("local_path")]

    # Index: include all expected files, with not_downloaded column per row
    all_rows = []
    for r in unique:
        row = {
            "case_title": r.get("case_title", ""),
            "case_url": r.get("case_url", ""),
            "case_path": r.get("case_path", ""),
            "doc_type": r.get("doc_type", ""),
            "doc_title": r.get("doc_title", ""),
            "doc_date_display": r.get("doc_date_display", ""),
            "doc_url": r.get("doc_url", ""),
            "local_path": r.get("local_path", ""),
            "not_downloaded": "" if r.get("local_path") else "NOT DOWNLOADED",
        }
        all_rows.append(row)
    df = pd.DataFrame(all_rows)
    df.sort_values(["case_title", "doc_type", "doc_title"], inplace=True)
    csv_path = os.path.join(args.out, "cma_ieo_derogs_revocations_index.csv")
    xlsx_path = os.path.join(args.out, "cma_ieo_derogs_revocations_index.xlsx")
    df.to_csv(csv_path, index=False, encoding="utf-8")
    df.to_excel(xlsx_path, index=False, engine="openpyxl")

    # Zip bundle
    zip_path = os.path.join(args.out, "cma_initial_orders_derogs_revocations.zip")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        # Include the index files too
        z.write(csv_path, arcname=os.path.basename(csv_path))
        z.write(xlsx_path, arcname=os.path.basename(xlsx_path))
        for r in downloaded:
            p = r.get("local_path")
            if not p:
                continue
            if not os.path.exists(p):
                continue
            zip_case_dir = r.get("zip_case_dir") or slugify(r.get("case_path", "case")) or "case"
            zip_case_dir = truncate_component(zip_case_dir, MAX_CASE_DIR_LEN)
            category = CATEGORY_TO_FOLDER.get(r.get("doc_type"), "Other")
            filename = r.get("zip_filename") or os.path.basename(p)
            arcname = f"{zip_case_dir}/{category}/{filename}"
            z.write(p, arcname=arcname)

    print("Wrote:", csv_path)
    print("Wrote:", xlsx_path)
    print("Wrote:", zip_path)

if __name__ == "__main__":
    main()
