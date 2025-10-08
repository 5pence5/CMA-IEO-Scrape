from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
import zipfile

import pandas as real_pandas
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import Scrape


class _FakeResponse:
    def __init__(self, text: str = "<html></html>", status_code: int = 200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise ValueError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self) -> None:
        self.requested_urls: list[str] = []

    def get(self, url: str, headers: dict[str, Any] | None = None, timeout: int = 60) -> _FakeResponse:
        self.requested_urls.append(url)
        return _FakeResponse()


def test_main_handles_empty_discovery(tmp_path, monkeypatch):
    monkeypatch.setattr(Scrape, "pd", real_pandas)
    monkeypatch.setattr(Scrape.requests, "Session", lambda: _FakeSession())
    monkeypatch.setattr(Scrape, "search_cases_ieo_only", lambda session: [])
    monkeypatch.setattr(Scrape, "download_documents", lambda session, unique, docs_dir: [])

    out_dir = tmp_path / "bundle"
    argv = ["Scrape.py", "--out", str(out_dir), "--query-ieo-only"]
    monkeypatch.setattr(sys, "argv", argv)

    Scrape.main()

    csv_path = out_dir / "cma_ieo_derogs_revocations_index.csv"
    xlsx_path = out_dir / "cma_ieo_derogs_revocations_index.xlsx"
    zip_path = out_dir / "cma_initial_orders_derogs_revocations.zip"

    assert csv_path.exists()
    assert xlsx_path.exists()
    assert zip_path.exists()

    df = real_pandas.read_csv(csv_path)
    assert df.empty
    assert list(df.columns) == Scrape.INDEX_COLUMNS

    with zipfile.ZipFile(zip_path) as zf:
        names = set(zf.namelist())
        assert "cma_ieo_derogs_revocations_index.csv" in names
        assert "cma_ieo_derogs_revocations_index.xlsx" in names
