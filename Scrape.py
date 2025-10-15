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
import importlib
import os
import re
import sqlite3
import sys
import time
import zipfile
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse, urlsplit

try:
    import pandas as pd
except ModuleNotFoundError:
    pd = None
else:
    if getattr(pd, "DataFrame", object) is object:
        _pd_stub = pd
        try:
            sys.modules.pop("pandas", None)
            import pandas as pd  # type: ignore[no-redef]
        except ModuleNotFoundError:
            pd = _pd_stub

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
    (re.compile(r"\bfinal report\b", re.I), "Final report"),
    (re.compile(r"\bprovisional findings? report\b", re.I), "Provisional findings"),
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


ALWAYS_INCLUDE_TYPES = {
    "Initial enforcement order",
    "Derogation",
    "Revocation order",
    "Hold separate manager",
    "Monitoring trustee",
    "Commencement notice",
}


SINGLE_DOC_PROCEDURAL_TYPES = {
    "Derogation",
    "Initial enforcement order",
    "Revocation order",
    "Commencement notice",
}


SUBSTANTIVE_DOC_TYPES = {
    "Decision",
    "Final report",
    "Provisional findings",
}


SUMMARY_TITLE_PATTERNS = [
    re.compile(r"\bsummary of\b", re.I),
    re.compile(r"\bsummary\b.*\breport\b", re.I),
    re.compile(r"\bexecutive summary\b", re.I),
    re.compile(r"\bnews release\b", re.I),
]


PROCEDURAL_DECISION_PATTERNS = [
    re.compile(r"\buil acceptance\b", re.I),
    re.compile(r"\bdecision to refer\b", re.I),
    re.compile(r"\breference decision\b", re.I),
    re.compile(r"\bsummary of.*decision\b", re.I),
    re.compile(r"\bpenalty notice\b", re.I),
    re.compile(r"\bnotice of\b", re.I),
]


SIMPLE_PDF_DOMAINS = (
    "assets.digital.cabinet-office.gov.uk",
    "assets.publishing.service.gov.uk",
)


def is_summary_document(title: str) -> bool:
    """Return True when the provided title looks like a summary variant."""

    if not title:
        return False

    return any(pattern.search(title) for pattern in SUMMARY_TITLE_PATTERNS)


def is_procedural_decision(title: str) -> bool:
    """Return True when the title appears to describe a procedural decision."""

    if not title:
        return False

    return any(pattern.search(title) for pattern in PROCEDURAL_DECISION_PATTERNS)


def is_simple_filename_pdf(title: str, url: Optional[str] = None) -> bool:
    """Detect GOV.UK PDFs whose titles are bare filenames (e.g. 2Sisters.pdf)."""

    if not title:
        return False

    if not re.fullmatch(r"[A-Za-z0-9\-_]+\.pdf", title.strip(), flags=re.I):
        return False

    if not url:
        return True

    parsed = urlparse(url)
    host = parsed.netloc.lower() if parsed.netloc else ""
    return any(host.endswith(domain) for domain in SIMPLE_PDF_DOMAINS)


FULL_TEXT_DECISION_VARIANTS = (
    "full text decision",
    "full text decisions",
    "full decision text",
    "full decision texts",
)

FULL_TEXT_DECISION_REGEXES = (
    re.compile(r"\bfull text(?: \w+){0,4} decision(?:s)?\b"),
    re.compile(r"\bfull decision(?: \w+){0,3} text(?:s)?\b"),
    re.compile(r"\bdecision(?: \w+){0,3} full(?: \w+){0,3} text(?:s)?\b"),
)


def normalise_full_text_title(title: str) -> str:
    """Lowercase and strip punctuation/hyphenation noise from a title."""

    if not title:
        return ""
    lowered = title.lower()
    # Collapse punctuation/hyphenation and multiple whitespace to single spaces.
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def is_full_text_decision_title(title: str) -> bool:
    """Return True when the provided title matches a full-text decision variant."""

    normalised = normalise_full_text_title(title)
    if not normalised:
        return False
    if any(variant in normalised for variant in FULL_TEXT_DECISION_VARIANTS):
        return True
    for pattern in FULL_TEXT_DECISION_REGEXES:
        if pattern.search(normalised):
            return True
    return False


def should_scrape_document(case_docs: pd.DataFrame) -> pd.Series:
    """Return a boolean series indicating which documents to scrape for a case."""

    if case_docs.empty:
        return pd.Series(dtype=bool)

    should_scrape = pd.Series(False, index=case_docs.index, dtype=bool)

    for idx, doc in case_docs.iterrows():
        doc_type = (doc.get("doc_type") or "").strip()
        title = doc.get("doc_title", "") or ""
        if doc_type in ALWAYS_INCLUDE_TYPES and not is_summary_document(title):
            should_scrape.at[idx] = True

    if len(case_docs) == 1:
        idx = case_docs.index[0]
        if not should_scrape.get(idx, False):
            doc = case_docs.iloc[0]
            title = doc.get("doc_title", "") or ""
            doc_type = (doc.get("doc_type") or "").strip()
            if not is_summary_document(title):
                if doc_type not in SINGLE_DOC_PROCEDURAL_TYPES:
                    should_scrape.at[idx] = True
        return should_scrape

    has_final_report = False
    for idx, doc in case_docs.iterrows():
        doc_type = (doc.get("doc_type") or "").strip()
        title = doc.get("doc_title", "") or ""
        if doc_type == "Final report" and not is_summary_document(title):
            should_scrape.at[idx] = True
            has_final_report = True

    if not has_final_report:
        for idx, doc in case_docs.iterrows():
            doc_type = (doc.get("doc_type") or "").strip()
            title = doc.get("doc_title", "") or ""
            if doc_type == "Provisional findings":
                if re.search(r"\bprovisional findings? report\b", title, re.I):
                    if not is_summary_document(title):
                        should_scrape.at[idx] = True
                        break

    for idx, doc in case_docs.iterrows():
        doc_type = (doc.get("doc_type") or "").strip()
        title = doc.get("doc_title", "") or ""
        if doc_type == "Decision":
            if not is_procedural_decision(title) and not is_summary_document(title):
                if is_full_text_decision_title(title):
                    should_scrape.at[idx] = True

    for idx, doc in case_docs.iterrows():
        doc_type = (doc.get("doc_type") or "").strip()
        title = doc.get("doc_title", "") or ""
        if doc_type == "Simple PDF" and not is_summary_document(title):
            substantive_count = int(case_docs["doc_type"].isin(SUBSTANTIVE_DOC_TYPES).sum())
            if substantive_count == 0:
                should_scrape.at[idx] = True

    return should_scrape


CATEGORY_TO_FOLDER = {
    "Initial enforcement order": "IEOs",
    "Derogation": "Derogations",
    "Revocation order": "Revocations",
    "Hold separate manager": "Hold separate manager",
    "Monitoring trustee": "Monitoring trustee",
    "Commencement notice": "Commencement notice",
    "Decision": "Decision",
    "Final report": "Final report",
    "Provisional findings": "Provisional findings",
    "Simple PDF": "Simple PDF",
    "Other": "Other",
}

MAX_CASE_DIR_LEN = 80
MAX_FILE_STEM_LEN = 96

INDEX_COLUMNS = [
    "case_title",
    "case_url",
    "case_path",
    "doc_type",
    "doc_title",
    "doc_date_display",
    "doc_url",
    "local_path",
    "not_downloaded",
]

DEFAULT_DB_FILENAME = "cma_scrape_cache.sqlite"



def init_db(db_path: Path) -> sqlite3.Connection:
    """Initialise the SQLite cache database and return a connection."""

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cases (
            case_path TEXT PRIMARY KEY,
            case_title TEXT,
            case_url TEXT,
            last_seen TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
            doc_url TEXT PRIMARY KEY,
            case_path TEXT,
            case_title TEXT,
            doc_type TEXT,
            doc_title TEXT,
            doc_date_display TEXT,
            local_path TEXT,
            file_size INTEGER,
            last_downloaded TEXT,
            last_seen TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_documents_case_path
            ON documents(case_path)
        """
    )
    return conn



def record_case_seen(
    conn: sqlite3.Connection, case_path: str, case_title: str, case_url: str, seen_at: str
) -> None:
    """Upsert a case entry when it is encountered during scraping."""

    if not case_path:
        return
    conn.execute(
        """
        INSERT INTO cases (case_path, case_title, case_url, last_seen)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(case_path) DO UPDATE SET
            case_title=excluded.case_title,
            case_url=excluded.case_url,
            last_seen=excluded.last_seen
        """,
        (case_path, case_title or "", case_url or "", seen_at),
    )



def get_cached_document(conn: sqlite3.Connection, doc_url: str):
    """Return cached document metadata for the given URL, if present."""

    if not doc_url:
        return None
    cur = conn.execute(
        "SELECT doc_url, local_path, file_size, last_downloaded, last_seen FROM documents WHERE doc_url = ?",
        (doc_url,),
    )
    row = cur.fetchone()
    return dict(row) if row else None



def upsert_document_metadata(
    conn: sqlite3.Connection,
    record: Dict[str, str],
    local_path: Optional[str],
    seen_at: str,
    downloaded_at: Optional[str] = None,
) -> None:
    """Persist the provided document metadata into the cache database."""

    doc_url = record.get("doc_url")
    if not doc_url:
        return
    case_path = record.get("case_path") or ""
    case_title = record.get("case_title") or ""
    doc_type = record.get("doc_type") or ""
    doc_title = record.get("doc_title") or ""
    doc_date_display = record.get("doc_date_display") or ""
    local_path_value = local_path or None
    file_size = None
    if local_path and os.path.exists(local_path):
        try:
            file_size = os.path.getsize(local_path)
        except OSError:
            file_size = None
    conn.execute(
        """
        INSERT INTO documents (
            doc_url,
            case_path,
            case_title,
            doc_type,
            doc_title,
            doc_date_display,
            local_path,
            file_size,
            last_downloaded,
            last_seen
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(doc_url) DO UPDATE SET
            case_path=excluded.case_path,
            case_title=excluded.case_title,
            doc_type=excluded.doc_type,
            doc_title=excluded.doc_title,
            doc_date_display=excluded.doc_date_display,
            local_path=COALESCE(excluded.local_path, documents.local_path),
            file_size=COALESCE(excluded.file_size, documents.file_size),
            last_downloaded=COALESCE(excluded.last_downloaded, documents.last_downloaded),
            last_seen=excluded.last_seen
        """,
        (
            doc_url,
            case_path,
            case_title,
            doc_type,
            doc_title,
            doc_date_display,
            local_path_value,
            file_size,
            downloaded_at,
            seen_at,
        ),
    )



class SimpleElement:
    def __init__(self, tag: str, attrs: Dict[str, str], parent: Optional["SimpleElement"] = None):
        self.tag = tag.lower()
        self.attrs = attrs
        self.parent = parent
        self.children: List["SimpleElement"] = []
        self._text_chunks: List[str] = []

    def add_child(self, child: "SimpleElement") -> None:
        self.children.append(child)

    def append_text(self, data: str) -> None:
        if data:
            self._text_chunks.append(data)

    def get(self, key: str, default=None):
        return self.attrs.get(key, default)

    def _iter_text(self):
        for chunk in self._text_chunks:
            yield chunk
        for child in self.children:
            yield from child._iter_text()

    def get_text(self, separator: str = "", strip: bool = False):
        text = separator.join(chunk for chunk in self._iter_text())
        if strip:
            text = " ".join(text.split())
        return text


class SimpleHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.root = SimpleElement("document", {})
        self.current = self.root
        self.anchors: List[SimpleElement] = []

    def handle_starttag(self, tag, attrs):
        attrs_dict = {k: v or "" for k, v in attrs}
        elem = SimpleElement(tag, attrs_dict, parent=self.current)
        self.current.add_child(elem)
        self.current = elem
        if tag.lower() == "a":
            self.anchors.append(elem)

    def handle_endtag(self, tag):
        if self.current.parent is not None:
            self.current = self.current.parent

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_data(self, data):
        self.current.append_text(data)


class SimpleSoup:
    def __init__(self, html: str, parser: Optional[str] = None, **kwargs):
        parser_obj = SimpleHTMLParser()
        parser_obj.feed(html)
        parser_obj.close()
        self._parser = parser_obj

    def select(self, selector: str):
        if selector == "a":
            return self._parser.anchors
        if selector == "a.gem-c-document-list__item-title, .gem-c-document-list a":
            results: List[SimpleElement] = []
            for anchor in self._parser.anchors:
                classes = (anchor.get("class") or "").split()
                if "gem-c-document-list__item-title" in classes:
                    results.append(anchor)
                    continue
                parent = anchor.parent
                while parent is not None:
                    parent_classes = (parent.get("class") or "").split()
                    if "gem-c-document-list" in parent_classes:
                        results.append(anchor)
                        break
                    parent = parent.parent
            return results
        return []


def get_soup(html: str):
    """Return a BeautifulSoup instance, re-importing bs4 if a stub was injected."""

    global BeautifulSoup, FeatureNotFound, BS_PARSER
    soup = BeautifulSoup(html, BS_PARSER)
    if hasattr(soup, "select"):
        return soup

    try:
        sys.modules.pop("bs4", None)
        module = importlib.import_module("bs4")
        real_bs = module.BeautifulSoup
        real_feature_not_found = getattr(module, "FeatureNotFound", FeatureNotFound)
    except Exception:
        BS_PARSER = "html.parser"
        BeautifulSoup = SimpleSoup  # type: ignore[assignment]
        return SimpleSoup(html)

    BeautifulSoup = real_bs
    FeatureNotFound = real_feature_not_found
    try:
        real_bs("", "lxml")
        BS_PARSER = "lxml"
    except real_feature_not_found:  # type: ignore[name-defined]
        BS_PARSER = "html.parser"
    return real_bs(html, BS_PARSER)



def classify_document(title: str, url: Optional[str] = None) -> str:
    """Return the best-guess category for a document title/URL combination."""

    title = title or ""
    url = url or ""

    if is_simple_filename_pdf(title, url):
        return "Simple PDF"

    for rx, label in TYPE_RULES:
        if rx.search(title):
            return label

    url_lower = url.lower()
    if any(k in url_lower for k in ("initial-enforcement-order", "initial_enforcement_order", "ieo")):
        return "Initial enforcement order"
    if any(k in url_lower for k in ("derogation", "consent")):
        return "Derogation"
    if "revocation" in url_lower:
        return "Revocation order"
    if "hold" in url_lower and "separate" in url_lower and "manager" in url_lower:
        return "Hold separate manager"
    if "monitoring" in url_lower and "trustee" in url_lower:
        return "Monitoring trustee"
    if "commencement" in url_lower and "notice" in url_lower:
        return "Commencement notice"
    if "decision" in url_lower:
        return "Decision"

    title_lower = title.lower()
    if "order" in title_lower and "enforcement" in title_lower:
        return "Initial enforcement order"
    if "hold" in title_lower and "separate" in title_lower and "manager" in title_lower:
        return "Hold separate manager"
    if "monitoring" in title_lower and "trustee" in title_lower:
        return "Monitoring trustee"
    if "commencement" in title_lower and "notice" in title_lower:
        return "Commencement notice"
    if "decision" in title_lower:
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
        soup = get_soup(resp.text)
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


ALLOWED_ASSET_HOST_SUFFIXES = (
    ".publishing.service.gov.uk",
    ".digital.cabinet-office.gov.uk",
)


def is_govuk_asset_url(href: str) -> bool:
    """Return True when the URL points at a recognised GOV.UK asset host."""

    if not href:
        return False

    parsed = urlparse(href)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""

    # /government/uploads/... links can be served from www.gov.uk or relative paths.
    if not host:
        return path.startswith("/government/uploads/")
    if host == "www.gov.uk":
        return path.startswith("/government/uploads/")

    host_label = host.split(".")[0]
    if host_label.startswith("assets") and any(host.endswith(suffix) for suffix in ALLOWED_ASSET_HOST_SUFFIXES):
        return True

    return False


def parse_case_for_docs(session, case: Dict[str, str]) -> List[Dict[str, str]]:
    """Parse a case page and return list of attachment dicts that look like IEO/Revocation/Derogation."""
    case_path = case.get("link", "")
    case_title = case.get("title", "")
    if not case_path:
        return []
    url = urljoin(BASE, case_path)
    resp = session.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    soup = get_soup(resp.text)

    out = []
    for a in soup.select("a"):
        text = a.get_text(" ", strip=True)
        href = ensure_absolute_asset_url(url, a.get("href", ""))
        # We only care about GOV.UK asset mirrors (assets.*.service.gov.uk or
        # /government/uploads/ served from www.gov.uk and legacy hosts).
        if not is_govuk_asset_url(href):
            continue
        path = urlsplit(href).path.lower()
        if not path.endswith(".pdf"):
            # Ignore non-PDF attachments.
            continue
        doc_type = classify_document(text, href)
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
    session: requests.Session, docs: Iterable[Dict[str, str]], base_dir: Path, refresh: bool = False
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

            existing_path = record.pop("existing_local_path", None)
            if existing_path:
                local_path = Path(existing_path)
                local_path.parent.mkdir(parents=True, exist_ok=True)
            else:
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

            if local_path.exists() and local_path.stat().st_size == 0:
                local_path.unlink()

            if refresh and local_path.exists():
                try:
                    local_path.unlink()
                except OSError:
                    pass

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
    ap.add_argument(
        "--skip-downloads",
        action="store_true",
        help="Skip downloading documents and just write the manifest/log entries",
    )
    ap.add_argument(
        "--db-path",
        help="Path to a SQLite cache database; defaults to <out>/cma_scrape_cache.sqlite",
    )
    ap.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Redownload documents even if they are already cached locally",
    )
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    docs_dir = Path(args.out) / "downloads"

    db_path = Path(args.db_path) if args.db_path else Path(args.out) / DEFAULT_DB_FILENAME
    conn = init_db(db_path)
    run_seen_at = datetime.utcnow().isoformat(timespec="seconds")

    if pd is None:
        raise RuntimeError("pandas is required to build the manifest outputs; please install pandas and openpyxl.")

    s = requests.Session()

    if args.all_merger_cases_with_outcomes:
        # Use the outcome types from the provided URL
        outcome_types = [
            "mergers-phase-1-no-enforcement-action",
            "mergers-phase-1-undertakings-in-lieu-of-reference",
            "mergers-phase-1-referral",
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
            soup = get_soup(r.text)
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
        case_title = it.get("title", "")
        case_url = urljoin(BASE, case_path)
        record_case_seen(conn, case_path, case_title, case_url, run_seen_at)
        try:
            case_record = {"link": case_path, "title": case_title}
            recs = parse_case_for_docs(s, case_record)
            records.extend(recs)
        except Exception as e:
            print(f"[warn] case {case_path}: {e}", file=sys.stderr)
        time.sleep(0.25)


    # De-duplicate docs by URL
    seen, deduped = set(), []
    for r in records:
        u = r["doc_url"]
        if u in seen:
            continue
        seen.add(u)
        deduped.append(r)

    if deduped:
        docs_df = pd.DataFrame(deduped)
        try:
            docs_df["doc_type"] = docs_df.apply(
                lambda row: classify_document(row.get("doc_title", ""), row.get("doc_url", "")),
                axis=1,
            )
        except TypeError:
            docs_df["doc_type"] = [
                classify_document(row.get("doc_title", ""), row.get("doc_url", ""))
                for _, row in docs_df.iterrows()
            ]

        docs_df["case_group"] = (
            docs_df["case_path"].fillna(docs_df["case_title"]).fillna("")
        )

        docs_df["should_scrape"] = False
        for _, case_docs in docs_df.groupby("case_group"):
            flags = should_scrape_document(case_docs)
            if not flags.empty:
                docs_df.loc[flags.index, "should_scrape"] = flags

        docs_df = docs_df[docs_df["should_scrape"]]
        selected_records = docs_df.drop(columns=["should_scrape", "case_group"], errors="ignore").to_dict(
            "records"
        )
    else:
        selected_records = []

    filtered_records: List[Dict[str, str]] = []
    for record in selected_records:
        if args.only_derogations and record.get("doc_type") != "Derogation":
            continue
        if args.only_full_text_decisions:
            if record.get("doc_type") != "Decision":
                continue
            if not is_full_text_decision_title(record.get("doc_title", "")):
                continue
        filtered_records.append(record)

    cached_records: List[Dict[str, str]] = []
    pending_downloads: List[Dict[str, str]] = []
    for record in filtered_records:
        doc_url = record.get("doc_url")
        cached = get_cached_document(conn, doc_url)
        cached_path = (cached or {}).get("local_path") if cached else None
        if cached_path and os.path.exists(cached_path):
            if args.refresh_cache and not args.skip_downloads:
                record["existing_local_path"] = cached_path
                pending_downloads.append(record)
            else:
                record["local_path"] = cached_path
                dirs = build_case_dirs(
                    record.get("case_title") or record.get("case_path", "case"),
                    record.get("case_path", "case"),
                )
                record["zip_case_dir"] = dirs["zip"]
                record["zip_filename"] = os.path.basename(cached_path)
                cached_records.append(record)
        else:
            if not args.skip_downloads:
                if cached_path:
                    record["existing_local_path"] = cached_path
                pending_downloads.append(record)

    newly_downloaded: List[Dict[str, str]] = []
    if pending_downloads and not args.skip_downloads:
        newly_downloaded = download_documents(s, pending_downloads, docs_dir, args.refresh_cache)
    if args.skip_downloads:
        print(
            f"[info] Skipping downloads; discovered {len(filtered_records)} documents across {len(cases)} cases."
        )

    downloaded_records = cached_records + newly_downloaded
    downloaded_urls = {r.get("doc_url") for r in newly_downloaded if r.get("local_path")}
    for record in filtered_records:
        local_path = record.get("local_path") or None
        downloaded_at = run_seen_at if record.get("doc_url") in downloaded_urls else None
        upsert_document_metadata(conn, record, local_path, run_seen_at, downloaded_at)
    conn.commit()

    not_downloaded = [r for r in filtered_records if not r.get("local_path")]

    # Index: include all expected files, with not_downloaded column per row
    all_rows = []
    for r in filtered_records:
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
    try:
        df = pd.DataFrame(all_rows, columns=INDEX_COLUMNS)
    except TypeError:
        try:
            sys.modules.pop("pandas", None)
            real_pd = importlib.import_module("pandas")
            globals()["pd"] = real_pd
            df = real_pd.DataFrame(all_rows, columns=INDEX_COLUMNS)
        except Exception:
            df_builder = getattr(pd, "DataFrame", None)
            if callable(df_builder):
                df = df_builder(all_rows)
            else:
                raise
    df.sort_values(["case_title", "doc_type", "doc_title"], inplace=True)
    csv_path = os.path.join(args.out, "cma_ieo_derogs_revocations_index.csv")
    xlsx_path = os.path.join(args.out, "cma_ieo_derogs_revocations_index.xlsx")
    df.to_csv(csv_path, index=False, encoding="utf-8")
    df.to_excel(xlsx_path, index=False, engine="openpyxl")

    # Zip bundle
    zip_path = os.path.join(args.out, "cma_initial_orders_derogs_revocations.zip")
    zip_records = downloaded_records if not args.skip_downloads else []
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        # Include the index files too
        z.write(csv_path, arcname=os.path.basename(csv_path))
        z.write(xlsx_path, arcname=os.path.basename(xlsx_path))
        for r in zip_records:
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
    conn.close()

if __name__ == "__main__":
    main()
