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

import argparse, os, re, time, zipfile, sys
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE = "https://www.gov.uk"
SEARCH_API = f"{BASE}/api/search.json"
HEADERS = {"User-Agent": "CMA-IEO-scraper (legal research; contact: your-email@example.com)"}

CMA_ORG_SLUG = "competition-and-markets-authority"
CMA_CASE_FORMAT = "cma_case"
IEO_KEYWORDS = [
    "initial enforcement order",
    "revocation order",
    "derogation",
    "consent granted",  # sometimes derogations are phrased as consents
]

# Heuristics for classifying attachment type from link text
TYPE_RULES = [
    (re.compile(r"\binitial enforcement order\b", re.I), "Initial enforcement order"),
    (re.compile(r"\brevocation\b", re.I), "Revocation order"),
    (re.compile(r"\bderogation\b|\bconsent\b", re.I), "Derogation"),
]

def classify_type(text):
    for rx, label in TYPE_RULES:
        if rx.search(text or ""):
            return label
    return None

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

def search_all_merger_cases(session):
    """
    Broader: use the finder page HTML with the same filter the UI uses:
    /cma-cases?case_type[]=mergers
    Then paginate (?page=2,3,...) and collect case links.
    """
    links = []
    page = 1
    while True:
        url = f"{BASE}/cma-cases?case_type[]=mergers&page={page}"
        resp = session.get(url, headers=HEADERS, timeout=60)
        if resp.status_code != 200:
            break
        soup = BeautifulSoup(resp.text, "lxml")
        # result links are anchors under .gem-c-document-list__item or similar
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

def parse_case_for_docs(session, case_path):
    """Parse a case page and return list of attachment dicts that look like IEO/Revocation/Derogation."""
    url = urljoin(BASE, case_path)
    resp = session.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    out = []
    for a in soup.select("a"):
        text = a.get_text(" ", strip=True)
        href = a.get("href", "")
        # We only care about GOV.UK assets (PDFs usually at assets.publishing.service.gov.uk)
        if not href or "assets.publishing.service.gov.uk" not in href:
            continue
        doc_type = classify_type(text)
        if not doc_type:
            continue
        # Try to pick up the trailing (dd.mm.yy) GOV.UK convention shown next to links
        # Often the date is in the same text; if not, we leave blank.
        m = DATE_RX.search(text)
        date_disp = m.group(1).replace(".", "/") if m else ""

        out.append({
            "case_url": url,
            "case_path": case_path,
            "doc_title": text,
            "doc_type": doc_type,
            "doc_date_display": date_disp,
            "doc_url": href,
        })
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="Output folder for index & zip")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--query-ieo-only", action="store_true", help="Only cases that mention IEO/derogation/revocation (faster)")
    mode.add_argument("--all-merger-cases", action="store_true", help="Parse all CMA merger cases (slower, more complete)")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    docs_dir = os.path.join(args.out, "docs")
    os.makedirs(docs_dir, exist_ok=True)

    s = requests.Session()

    if args.all_merger_cases:
        cases = search_all_merger_cases(s)
    else:
        cases = search_cases_ieo_only(s)
        if not cases:
            # fallback to the finder with the IEO keyword
            fallback_url = f"{BASE}/cma-cases?case_type[]=mergers&keywords=initial+enforcement+order"
            r = s.get(fallback_url, headers=HEADERS, timeout=60)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            cases = [{"title": a.get_text(strip=True), "link": a["href"]}
                     for a in soup.select("a.gem-c-document-list__item-title, .gem-c-document-list a")
                     if a.get("href", "").startswith("/cma-cases/")]

    # Crawl case pages
    records = []
    for i, it in enumerate(cases, 1):
        case_path = it.get("link")
        if not case_path:
            continue
        try:
            recs = parse_case_for_docs(s, case_path)
            records.extend(recs)
        except Exception as e:
            print(f"[warn] case {case_path}: {e}", file=sys.stderr)
        time.sleep(0.25)

    # De-duplicate docs by URL
    seen, unique = set(), []
    for r in records:
        u = r["doc_url"]
        if u not in seen:
            seen.add(u)
            unique.append(r)

    # Download
    downloaded = []
    for r in unique:
        url = r["doc_url"]
        try:
            fn = url.split("/")[-1]
            local = os.path.join(docs_dir, fn)
            if not os.path.exists(local):
                with s.get(url, headers=HEADERS, timeout=120) as resp:
                    resp.raise_for_status()
                    with open(local, "wb") as f:
                        f.write(resp.content)
            r["local_path"] = local
            downloaded.append(r)
        except Exception as e:
            r["local_path"] = ""
            print(f"[warn] download failed {url}: {e}", file=sys.stderr)
        time.sleep(0.2)

    # Index
    df = pd.DataFrame(downloaded, columns=[
        "case_url", "case_path", "doc_type", "doc_title", "doc_date_display", "doc_url", "local_path"
    ])
    df.sort_values(["case_path", "doc_type", "doc_title"], inplace=True)
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
            if p and os.path.exists(p):
                arcname = f"docs/{os.path.basename(p)}"
                z.write(p, arcname=arcname)

    print("Wrote:", csv_path)
    print("Wrote:", xlsx_path)
    print("Wrote:", zip_path)

if __name__ == "__main__":
    main()
