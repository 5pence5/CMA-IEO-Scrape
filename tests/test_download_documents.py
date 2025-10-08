from __future__ import annotations

import sys
import types
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

dummy_pandas = types.ModuleType("pandas")
dummy_pandas.DataFrame = object
dummy_pandas.Series = object
sys.modules.setdefault("pandas", dummy_pandas)

dummy_requests = types.ModuleType("requests")
dummy_requests.Session = object
sys.modules.setdefault("requests", dummy_requests)

dummy_bs4 = types.ModuleType("bs4")


class _DummySoup:  # pragma: no cover - not used in this test
    def __init__(self, *args, **kwargs):
        pass


class _DummyFeatureNotFound(Exception):
    pass


dummy_bs4.BeautifulSoup = _DummySoup
dummy_bs4.FeatureNotFound = _DummyFeatureNotFound
sys.modules.setdefault("bs4", dummy_bs4)

import Scrape


class _FakeResponse:
    def __init__(self, payload: bytes):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size: int = 8192):
        yield self._payload


class _FakeSession:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _FakeResponse(self.payload)


def test_download_documents_retries_zero_byte(tmp_path):
    base_dir = tmp_path / "downloads"
    record = {
        "case_title": "Example Merger",
        "case_path": "/cma-cases/example-merger",
        "doc_title": "Important Derogation",
        "doc_type": "Derogation",
        "doc_url": "https://example.test/derogation.pdf",
    }

    dirs = Scrape.build_case_dirs(record["case_title"], record["case_path"])
    category = Scrape.CATEGORY_TO_FOLDER.get(record["doc_type"], "Other")
    case_dir = base_dir / dirs["local"] / category
    case_dir.mkdir(parents=True, exist_ok=True)

    title_slug = Scrape.slugify(record["doc_title"])
    title_slug = Scrape.truncate_component(title_slug, Scrape.MAX_FILE_STEM_LEN)
    stem = Scrape.truncate_component(
        f"{dirs['slug']}__{title_slug}", Scrape.MAX_FILE_STEM_LEN * 2
    )
    expected_path = case_dir / f"{stem}.pdf"
    expected_path.write_bytes(b"")

    payload = b"fresh-pdf-content"
    session = _FakeSession(payload)

    results = Scrape.download_documents(session, [dict(record)], base_dir)

    assert session.calls == [
        (
            record["doc_url"],
            {"headers": Scrape.HEADERS, "stream": True, "timeout": 120},
        )
    ]
    assert results[0]["local_path"] == str(expected_path)
    assert expected_path.read_bytes() == payload
    assert expected_path.stat().st_size == len(payload)
