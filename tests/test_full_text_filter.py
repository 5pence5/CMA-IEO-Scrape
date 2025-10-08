from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Scrape import is_full_text_decision_title


@pytest.mark.parametrize(
    "title",
    [
        "Full text decision",
        "Full text of the decision",
        "Full-text decision",
        "Full decision text",
        "Full text decision (final)",
    ],
)
def test_full_text_decision_variants_pass(title):
    assert is_full_text_decision_title(title)


@pytest.mark.parametrize(
    "title",
    [
        "Summary decision",
        "Decision summary",
        "Provisional findings",
        "Full statement",
        "",
    ],
)
def test_non_full_text_titles_fail(title):
    assert not is_full_text_decision_title(title)
