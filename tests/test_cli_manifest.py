from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys
import types
import zipfile

import pytest


pytest.importorskip("bs4")


class DummyDataFrame:
    def __init__(self, rows):
        self._rows = list(rows)

    def sort_values(self, columns, inplace=False):
        sorted_rows = sorted(
            self._rows,
            key=lambda row: tuple(row.get(col, "") for col in columns),
        )
        if inplace:
            self._rows = sorted_rows
            return None
        return DummyDataFrame(sorted_rows)

    def to_csv(self, path, index=False, encoding="utf-8"):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = list(self._rows[0].keys()) if self._rows else []
        with path.open("w", newline="", encoding=encoding) as fh:
            if not fieldnames:
                return
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self._rows)

    def to_excel(self, path, index=False, engine=None):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if not self._rows:
            path.write_text("", encoding="utf-8")
            return
        fieldnames = list(self._rows[0].keys())
        lines = [",".join(fieldnames)]
        for row in self._rows:
            lines.append(
                ",".join(str(row.get(col, "")) for col in fieldnames)
            )
        path.write_text("\n".join(lines), encoding="utf-8")


dummy_pandas = types.ModuleType("pandas")
dummy_pandas.DataFrame = DummyDataFrame
sys.modules.setdefault("pandas", dummy_pandas)


class _DummySession:
    def __init__(self, *args, **kwargs):
        raise RuntimeError("Dummy requests.Session should be monkeypatched in tests")


dummy_requests = types.ModuleType("requests")
dummy_requests.Session = _DummySession
sys.modules.setdefault("requests", dummy_requests)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import Scrape  # noqa: E402


@pytest.fixture(autouse=True)
def fake_pandas(monkeypatch):
    dummy_pd = types.SimpleNamespace(DataFrame=DummyDataFrame)
    monkeypatch.setattr(Scrape, "pd", dummy_pd)


def test_cli_manifest_marks_not_downloaded(tmp_path, monkeypatch):
    class FakeResponse:
        def __init__(self, *, json_data=None, status_code=200):
            self._json_data = json_data or {}
            self.status_code = status_code
            self.text = ""

        def json(self):
            return self._json_data

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, **kwargs):
            self.calls.append((url, kwargs))
            if url == Scrape.SEARCH_API:
                return FakeResponse(
                    json_data={
                        "results": [
                            {"title": "Example Merger", "link": "/cma-cases/example"}
                        ]
                    }
                )
            raise AssertionError(f"Unexpected GET {url}")

    fake_session = FakeSession()
    monkeypatch.setattr(Scrape.requests, "Session", lambda: fake_session)

    def fake_parse_case_for_docs(session, case):
        return [
            {
                "case_title": case["title"],
                "case_url": f"{Scrape.BASE}{case['link']}",
                "case_path": case["link"],
                "doc_title": "Derogation document",
                "doc_type": "Derogation",
                "doc_date_display": "01/01/2023",
                "doc_url": "https://assets.publishing.service.gov.uk/documents/derogation.pdf",
            },
            {
                "case_title": case["title"],
                "case_url": f"{Scrape.BASE}{case['link']}",
                "case_path": case["link"],
                "doc_title": "Revocation order",
                "doc_type": "Revocation order",
                "doc_date_display": "02/01/2023",
                "doc_url": "https://assets.publishing.service.gov.uk/documents/revocation.pdf",
            },
        ]

    monkeypatch.setattr(Scrape, "parse_case_for_docs", fake_parse_case_for_docs)

    def fake_download_documents(session, docs, base_dir):
        base_dir.mkdir(parents=True, exist_ok=True)
        result = []
        for doc in docs:
            record = dict(doc)
            record["local_path"] = ""
            record["zip_case_dir"] = Scrape.slugify(doc.get("case_path", "case"))
            record["zip_filename"] = ""
            result.append(record)
        return result

    monkeypatch.setattr(Scrape, "download_documents", fake_download_documents)

    real_parse_args = argparse.ArgumentParser.parse_args

    def fake_parse_args(self, args=None, namespace=None):
        return real_parse_args(self, ["--out", str(tmp_path), "--query-ieo-only"])

    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", fake_parse_args)

    Scrape.main()

    csv_path = tmp_path / "cma_ieo_derogs_revocations_index.csv"
    xlsx_path = tmp_path / "cma_ieo_derogs_revocations_index.xlsx"
    zip_path = tmp_path / "cma_initial_orders_derogs_revocations.zip"

    assert csv_path.exists()
    assert xlsx_path.exists()
    assert zip_path.exists()

    csv_text = csv_path.read_text(encoding="utf-8")
    assert csv_text.count("NOT DOWNLOADED") == 2

    with zipfile.ZipFile(zip_path) as zf:
        names = sorted(zf.namelist())
    assert names == [
        "cma_ieo_derogs_revocations_index.csv",
        "cma_ieo_derogs_revocations_index.xlsx",
    ]
