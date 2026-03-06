"""
test_get_keywords.py

Unit tests for get_keywords.py using pytest.
All file I/O is mocked — no real CSVs or chart files are required.
"""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from get_keywords import (
    get_top_keywords,
    load_processed_csv,
    tokenise_titles,
)


# ---------------------------------------------------------------------------
# load_processed_csv
# ---------------------------------------------------------------------------


def test_load_processed_csv_raises_if_file_missing():
    with patch("get_keywords.Path.is_file", return_value=False):
        with pytest.raises(FileNotFoundError, match="Processed data not found"):
            load_processed_csv()


def test_load_processed_csv_returns_dataframe(tmp_path, monkeypatch):
    csv = tmp_path / "PROCESSED_DATA.csv"
    csv.write_text(
        "title,author_name,year,rating,ratings\nBook A,Author One,2020,4.5,1000\n")
    monkeypatch.chdir(tmp_path)
    df = load_processed_csv()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


# ---------------------------------------------------------------------------
# tokenise_titles
# ---------------------------------------------------------------------------


def test_tokenise_titles_lowercases_tokens():
    titles = pd.Series(["The GREAT Gatsby"])
    tokens = tokenise_titles(titles)
    assert all(t == t.lower() for t in tokens)


def test_tokenise_titles_removes_stopwords():
    titles = pd.Series(["The Court of Thorns"])
    tokens = tokenise_titles(titles)
    # "the", "of" are stopwords and should be absent
    assert "the" not in tokens
    assert "of" not in tokens


def test_tokenise_titles_removes_single_character_tokens():
    titles = pd.Series(["A Walk to Remember"])
    tokens = tokenise_titles(titles)
    assert all(len(t) > 1 for t in tokens)


def test_tokenise_titles_removes_numeric_tokens():
    titles = pd.Series(["Outlander 1", "Book 99"])
    tokens = tokenise_titles(titles)
    assert all(not t.isnumeric() for t in tokens)


def test_tokenise_titles_strips_punctuation():
    titles = pd.Series(["P.S. I Love You", "It's Not Summer"])
    tokens = tokenise_titles(titles)
    assert all(c not in t for t in tokens for c in [".", "'", ","])


def test_tokenise_titles_ignores_null_titles():
    titles = pd.Series(["Book One", None, "Book Two"])
    # Should not raise, null is skipped
    tokens = tokenise_titles(titles)
    assert len(tokens) > 0


# ---------------------------------------------------------------------------
# get_top_keywords
# ---------------------------------------------------------------------------


def test_get_top_keywords_returns_correct_n():
    tokens = ["love", "dark", "love", "night", "dark", "love", "fire", "heart", "kiss", "rose",
              "shadow", "blood", "moon", "star", "dream", "fate", "war", "ice", "wind", "sea", "sun"]
    result = get_top_keywords(tokens, n=5)
    assert len(result) == 5


def test_get_top_keywords_sorted_by_frequency():
    tokens = ["love", "love", "love", "dark", "dark", "night"]
    result = get_top_keywords(tokens)
    counts = result["count"].tolist()
    assert counts == sorted(counts, reverse=True)
