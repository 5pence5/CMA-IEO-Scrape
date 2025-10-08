from pathlib import Path
import sys
import types

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

dummy_pandas = types.ModuleType("pandas")


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


class _DummySeries:
    def __init__(self, *args, **kwargs):
        self._data = args[0] if args else []
        self.index = kwargs.get("index")
        self.dtype = kwargs.get("dtype")

    def __bool__(self):
        return bool(self._data)


dummy_pandas.Series = _DummySeries
sys.modules.setdefault("pandas", dummy_pandas)


class _DummySession:
    def __init__(self, *args, **kwargs):
        raise RuntimeError("Dummy requests.Session should be monkeypatched in tests")


dummy_requests = types.ModuleType("requests")
dummy_requests.Session = _DummySession
sys.modules.setdefault("requests", dummy_requests)

import pytest

pytest.importorskip("bs4")

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


def test_parse_case_includes_pdf_with_query_and_fragment():
    html = """
    <html>
      <body>
        <main>
          <a href="https://assets.digital.cabinet-office.gov.uk/government/uploads/system/uploads/attachment_data/file/123456/legacy-derogation.pdf?download=1#section">
            Legacy derogation (12.10.2015)
          </a>
        </main>
      </body>
    </html>
    """
    session = FakeSession(html)
    case = {"link": "/cma-cases/example-case", "title": "Example case"}

    docs = parse_case_for_docs(session, case)

    assert docs, "Expected at least one document to be captured"
    assert any(
        doc["doc_url"].endswith("legacy-derogation.pdf?download=1#section") for doc in docs
    ), "Document with query/fragment should be captured"
