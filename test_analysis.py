"""
test_analyse_processed_data.py

Unit tests for analyse_processed_data.py using pytest.
All file I/O is mocked — no real CSVs or chart files are required.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from analyse_processed_data import (
    build_decade_counts,
    build_top_authors,
    load_processed_csv,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "title": ["Book A", "Book B", "Book C", "Book D", "Book E"],
        "author_name": ["Author One", "Author Two", "Author One", "Author Three", "Author Two"],
        "year": pd.array([1995, 2003, 2005, 2010, 2018], dtype="Int64"),
        "rating": [4.5, 3.8, 4.1, 3.6, 4.3],
        "ratings": pd.array([1000, 500, 750, 200, 300], dtype="Int64"),
    })


# ---------------------------------------------------------------------------
# load_processed_csv
# ---------------------------------------------------------------------------


def test_load_processed_csv_raises_if_file_missing():
    with patch("analyse_processed_data.Path.is_file", return_value=False):
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
# build_decade_counts
# ---------------------------------------------------------------------------


def test_build_decade_counts_buckets_correctly(sample_df):
    result = build_decade_counts(sample_df)
    assert set(result["decade"].tolist()) == {1990, 2000, 2010}


def test_build_decade_counts_adds_decade_label(sample_df):
    result = build_decade_counts(sample_df)
    assert "decade_label" in result.columns
    assert set(result["decade_label"].tolist()) == {"1990s", "2000s", "2010s"}


def test_build_decade_counts_sorted_ascending(sample_df):
    result = build_decade_counts(sample_df)
    decades = result["decade"].tolist()
    assert decades == sorted(decades)


def test_build_decade_counts_ignores_missing_years(sample_df):
    sample_df.loc[0, "year"] = None
    result = build_decade_counts(sample_df)
    # Row with null year should be excluded, not cause an error
    assert result["count"].sum() == len(sample_df) - 1


# ---------------------------------------------------------------------------
# build_top_authors
# ---------------------------------------------------------------------------


def test_build_top_authors_sums_ratings_per_author(sample_df):
    result = build_top_authors(sample_df)
    author_one_total = result.loc[result["author_name"]
                                  == "Author One", "total_ratings"].iloc[0]
    # Author One has books with 1000 and 750 ratings
    assert author_one_total == 1750


def test_build_top_authors_respects_n(sample_df):
    result = build_top_authors(sample_df, n=2)
    assert len(result) == 2


def test_build_top_authors_sorted_descending(sample_df):
    result = build_top_authors(sample_df)
    totals = result["total_ratings"].tolist()
    assert totals == sorted(totals, reverse=True)
