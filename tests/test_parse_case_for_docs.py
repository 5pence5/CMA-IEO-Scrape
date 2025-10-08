import csv
from collections import Counter
from pathlib import Path
import sys
import types

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

dummy_pandas = types.ModuleType("pandas")


class _DummyDataFrame:
    def __init__(self, *args, **kwargs):
        self._data = args[0] if args else []

    def sort_values(self, by=None, inplace=False, *args, **kwargs):
        if by is None:
            return self
        if isinstance(by, str):
            by = [by]

        def sort_key(row):
            return tuple(row.get(field, "") for field in by)

        sorted_data = sorted(self._data, key=sort_key)
        if inplace:
            self._data = sorted_data
            return None
        return _DummyDataFrame(sorted_data)

    def to_csv(self, path, index=False, encoding="utf-8", *args, **kwargs):
        fieldnames = []
        for row in self._data:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(key)

        with open(path, "w", encoding=encoding, newline="") as handle:
            if not fieldnames:
                handle.write("")
                return None
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in self._data:
                writer.writerow(row)
        return None

    def to_excel(self, path, *args, **kwargs):
        with open(path, "wb") as handle:
            handle.write(b"dummy excel data")
        return None


dummy_pandas.DataFrame = _DummyDataFrame
sys.modules.setdefault("pandas", dummy_pandas)

dummy_requests = types.ModuleType("requests")


class _DummySession:
    def get(self, *args, **kwargs):
        raise RuntimeError("Network access is disabled in tests")


dummy_requests.Session = _DummySession
sys.modules.setdefault("requests", dummy_requests)

dummy_bs4 = types.ModuleType("bs4")


class FeatureNotFound(Exception):
    pass


class _SimpleTag:
    def __init__(self, attrs, text):
        self._attrs = dict(attrs)
        self._text = text or ""

    def get(self, key, default=None):
        return self._attrs.get(key, default)

    def get_text(self, separator="", strip=False):
        if strip or separator:
            parts = self._text.split()
            joiner = separator if separator is not None else ""
            return joiner.join(parts)
        return self._text


class _AnchorCollector:
    def __init__(self, html: str):
        from html.parser import HTMLParser

        class _Parser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.anchors = []
                self._current = None
                self._data = []

            def handle_starttag(self, tag, attrs):
                if tag == "a":
                    self._current = attrs
                    self._data = []

            def handle_data(self, data):
                if self._current is not None:
                    self._data.append(data)

            def handle_endtag(self, tag):
                if tag == "a" and self._current is not None:
                    text = "".join(self._data)
                    self.anchors.append(_SimpleTag(self._current, text))
                    self._current = None
                    self._data = []

        parser = _Parser()
        parser.feed(html)
        self.anchors = parser.anchors


class BeautifulSoup:
    def __init__(self, html: str, parser: str = "html.parser"):
        if parser not in {"html.parser", "lxml"}:
            raise FeatureNotFound(parser)
        self._collector = _AnchorCollector(html)

    def select(self, selector: str):
        selector = selector or ""
        selector = selector.strip()
        if selector == "a" or selector.startswith("a") or selector.endswith(" a"):
            return list(self._collector.anchors)
        if "a" in selector:
            return list(self._collector.anchors)
        return []


dummy_bs4.BeautifulSoup = BeautifulSoup
dummy_bs4.FeatureNotFound = FeatureNotFound
sys.modules.setdefault("bs4", dummy_bs4)

import pytest

import Scrape
from Scrape import BASE, is_govuk_asset_url, parse_case_for_docs


class FakeResponse:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise ValueError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, html: str):
        self.html = html
        self.requested_urls = []

    def get(self, url, headers=None, timeout=60):
        self.requested_urls.append(url)
        return FakeResponse(self.html)


@pytest.fixture
def legacy_case_html():
    return Path("tests/fixtures/legacy_asset_case.html").read_text()


@pytest.fixture
def multi_document_html():
    return Path("tests/fixtures/multi_document_case.html").read_text()


def test_parse_case_allows_legacy_asset_hosts(legacy_case_html):
    session = FakeSession(legacy_case_html)
    case = {"link": "/cma-cases/example-case", "title": "Example case"}

    docs = parse_case_for_docs(session, case)

    assert len(docs) == 2
    urls = {doc["doc_url"] for doc in docs}
    assert (
        "https://assets.digital.cabinet-office.gov.uk/government/uploads/system/uploads/attachment_data/file/123456/legacy-derogation.pdf"
        in urls
    )
    assert (
        f"{BASE}/government/uploads/system/uploads/attachment_data/file/654321/revocation-order.pdf"
        in urls
    )


def test_parse_case_counts_documents(multi_document_html):
    session = FakeSession(multi_document_html)
    case = {"link": "/cma-cases/multi-doc-case", "title": "Multi document case"}

    docs = parse_case_for_docs(session, case)

    url_counts = Counter(doc["doc_url"] for doc in docs)
    assert all(count == 1 for count in url_counts.values())
    assert sum(url_counts.values()) == 5

    type_counts = Counter(doc["doc_type"] for doc in docs)
    assert type_counts == Counter(
        {
            "Initial enforcement order": 1,
            "Derogation": 1,
            "Revocation order": 1,
            "Decision": 1,
            "Other": 1,
        }
    )


def test_is_govuk_asset_url_accepts_mirrors():
    assert is_govuk_asset_url(
        "https://assets.digital.cabinet-office.gov.uk/government/uploads/system/uploads/attachment_data/file/123456/legacy-derogation.pdf"
    )
    assert is_govuk_asset_url(
        "https://assets-origin.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/654321/file.pdf"
    )
    assert is_govuk_asset_url(
        "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/000001/file.pdf"
    )
    assert is_govuk_asset_url("/government/uploads/system/uploads/attachment_data/file/000001/file.pdf")
    assert is_govuk_asset_url("https://www.gov.uk/government/uploads/system/file.pdf")
    assert not is_govuk_asset_url("https://www.gov.uk/government/news/file.pdf")
    assert not is_govuk_asset_url("https://example.com/file.pdf")
    assert not is_govuk_asset_url("https://malicious.publishing.service.gov.uk/file.pdf")


def test_filters_trim_hits(monkeypatch, tmp_path):
    fake_case = {"link": "/cma-cases/example-case", "title": "Example Case"}
    fake_docs = [
        {
            "case_url": f"{BASE}{fake_case['link']}",
            "case_path": fake_case["link"],
            "case_title": fake_case["title"],
            "doc_title": "Consent derogation (02.02.2024)",
            "doc_type": "Derogation",
            "doc_date_display": "02/02/2024",
            "doc_url": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/200001/derogation.pdf",
        },
        {
            "case_url": f"{BASE}{fake_case['link']}",
            "case_path": fake_case["link"],
            "case_title": fake_case["title"],
            "doc_title": "Full text decision on merger (04.04.2024)",
            "doc_type": "Decision",
            "doc_date_display": "04/04/2024",
            "doc_url": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/200002/full-text-decision.pdf",
        },
        {
            "case_url": f"{BASE}{fake_case['link']}",
            "case_path": fake_case["link"],
            "case_title": fake_case["title"],
            "doc_title": "Decision summary (05.05.2024)",
            "doc_type": "Decision",
            "doc_date_display": "05/05/2024",
            "doc_url": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/200003/decision-summary.pdf",
        },
        {
            "case_url": f"{BASE}{fake_case['link']}",
            "case_path": fake_case["link"],
            "case_title": fake_case["title"],
            "doc_title": "Initial enforcement order (01.01.2024)",
            "doc_type": "Initial enforcement order",
            "doc_date_display": "01/01/2024",
            "doc_url": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/200004/ieo.pdf",
        },
    ]

    monkeypatch.setattr(Scrape, "search_cases_ieo_only", lambda session: [fake_case])

    def fake_parse(session, case):
        return [dict(doc) for doc in fake_docs]

    monkeypatch.setattr(Scrape, "parse_case_for_docs", fake_parse)

    def fake_download(session, docs, base_dir):
        base_dir.mkdir(parents=True, exist_ok=True)
        return [dict(doc) for doc in docs]

    monkeypatch.setattr(Scrape, "download_documents", fake_download)

    # Derogations only run
    derogs_dir = tmp_path / "derogations"
    monkeypatch.setattr(sys, "argv", ["Scrape.py", "--out", str(derogs_dir), "--only-derogations"])
    Scrape.main()

    derogs_csv = derogs_dir / "cma_ieo_derogs_revocations_index.csv"
    with open(derogs_csv, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert rows
    assert Counter(row["doc_type"] for row in rows) == Counter({"Derogation": 1})
    url_counts = Counter(row["doc_url"] for row in rows)
    assert all(count == 1 for count in url_counts.values())
    assert {row["doc_url"] for row in rows} == {fake_docs[0]["doc_url"]}

    # Full text decisions only run
    decisions_dir = tmp_path / "full_text_decisions"
    monkeypatch.setattr(
        sys,
        "argv",
        ["Scrape.py", "--out", str(decisions_dir), "--only-full-text-decisions"],
    )
    Scrape.main()

    decisions_csv = decisions_dir / "cma_ieo_derogs_revocations_index.csv"
    with open(decisions_csv, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert rows
    assert Counter(row["doc_type"] for row in rows) == Counter({"Decision": 1})
    url_counts = Counter(row["doc_url"] for row in rows)
    assert all(count == 1 for count in url_counts.values())
    assert {row["doc_url"] for row in rows} == {fake_docs[1]["doc_url"]}
