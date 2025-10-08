from html.parser import HTMLParser
from pathlib import Path
import sys
import types

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

dummy_pandas = types.ModuleType("pandas")

dummy_requests = types.ModuleType("requests")
dummy_requests.Session = object

dummy_bs4 = types.ModuleType("bs4")


class _DummySoup:
    def __init__(self, html, parser=None):
        parser = _AnchorCollector()
        parser.feed(html or "")
        self._anchors = parser.anchors

    def select(self, selector: str):
        if not selector:
            return []
        selectors = [part.strip() for part in selector.split(",") if part.strip()]
        if any("a" in sel for sel in selectors):
            return list(self._anchors)
        return []


class _DummyFeatureNotFound(Exception):
    pass


class _DummyAnchor:
    def __init__(self, href: str, text_chunks):
        self._href = href
        self._text_chunks = text_chunks or []

    def get(self, attr: str, default=None):
        if attr == "href":
            return self._href or default
        return default

    def get_text(self, separator=" ", strip=False):
        raw = "".join(self._text_chunks)
        parts = raw.split()
        if separator is None:
            joined = "".join(parts)
        else:
            joined = separator.join(parts)
        if strip:
            return joined.strip()
        return joined


class _AnchorCollector(HTMLParser):
    def __init__(self):
        super().__init__()
        self.anchors = []
        self._current = None
        self._depth = 0

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            if self._current is None:
                href = dict(attrs).get("href", "")
                self._current = {"href": href, "text": []}
                self._depth = 1
            else:
                self._depth += 1
        elif self._current is not None:
            self._depth += 1

    def handle_data(self, data):
        if self._current is not None:
            self._current["text"].append(data)

    def handle_endtag(self, tag):
        if self._current is None:
            return
        self._depth -= 1
        if self._depth <= 0:
            self.anchors.append(_DummyAnchor(self._current["href"], self._current["text"]))
            self._current = None
            self._depth = 0


dummy_bs4.BeautifulSoup = _DummySoup
dummy_bs4.FeatureNotFound = _DummyFeatureNotFound


class _DummyDataFrame:
    def __init__(self, *args, **kwargs):
        self._data = args[0] if args else []

    def sort_values(self, *args, **kwargs):
        return self

    def to_csv(self, *args, **kwargs):
        return None

    def to_excel(self, *args, **kwargs):
        return None


dummy_pandas.DataFrame = _DummyDataFrame
sys.modules.setdefault("pandas", dummy_pandas)
sys.modules.setdefault("requests", dummy_requests)
sys.modules.setdefault("bs4", dummy_bs4)

import pytest

from Scrape import BASE, download_documents, is_govuk_asset_url, parse_case_for_docs


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


class DummyStreamResponse:
    def __init__(self, chunks, status_code: int = 200):
        self._chunks = chunks
        self.status_code = status_code

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise ValueError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size=8192):
        for chunk in self._chunks:
            yield chunk


class DummyDownloadSession:
    def __init__(self, responses):
        self._responses = responses
        self.calls = 0

    def get(self, url, headers=None, stream=True, timeout=120):
        response_chunks = self._responses[self.calls]
        self.calls += 1
        return DummyStreamResponse(response_chunks)


@pytest.fixture
def legacy_case_html():
    return Path("tests/fixtures/legacy_asset_case.html").read_text()


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


def _example_doc():
    return {
        "doc_url": "https://example.com/doc.pdf",
        "doc_title": "Example document",
        "doc_type": "Initial enforcement order",
        "case_title": "Example Merger",
        "case_path": "example-merger",
    }


def test_download_documents_retries_zero_byte_file(tmp_path):
    base_dir = tmp_path / "downloads"
    base_dir.mkdir()
    doc = _example_doc()

    first_session = DummyDownloadSession([[b"first"]])
    result = download_documents(first_session, [doc.copy()], base_dir)
    first_path = Path(result[0]["local_path"])
    assert first_path.exists()
    assert first_path.read_bytes() == b"first"

    # Simulate a zero-byte file left over from a previous run.
    first_path.write_bytes(b"")
    assert first_path.exists() and first_path.stat().st_size == 0

    second_session = DummyDownloadSession([[b"second"]])
    result_second = download_documents(second_session, [doc.copy()], base_dir)
    second_path = Path(result_second[0]["local_path"])
    assert second_path.exists()
    assert second_path.stat().st_size > 0
    assert second_path.read_bytes() == b"second"
    assert second_session.calls == 1


def test_download_documents_marks_zero_byte_download(tmp_path):
    base_dir = tmp_path / "downloads"
    base_dir.mkdir()
    doc = _example_doc()

    empty_session = DummyDownloadSession([[]])
    result = download_documents(empty_session, [doc.copy()], base_dir)
    assert result[0]["local_path"] == ""
    assert result[0]["zip_filename"] == ""

    # Ensure the filesystem does not retain the zero-byte placeholder.
    assert list(base_dir.rglob("*.pdf")) == []
