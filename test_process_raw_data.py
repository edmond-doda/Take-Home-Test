"""
test_process_raw_data.py

Unit tests for process_raw_data.py using pytest.
All file I/O is mocked — no real CSVs or databases are required.
"""

import re
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from process_raw_data import (
    clean_rating,
    clean_ratings_count,
    clean_year,
    drop_invalid_rows,
    drop_unnamed_index_columns,
    load_author_lookup,
    load_raw_csv,
    rename_columns,
    resolve_author_names,
    select_and_order_columns,
    sort_by_rating,
    strip_bracket_content_from_titles,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def minimal_df():
    """A small clean DataFrame in the shape of post-rename processed data."""
    return pd.DataFrame({
        "title": ["Book A", "Book B", "Book C"],
        "author_name": ["Author One", "Author Two", "Author Three"],
        "year": pd.array([2020, 2019, 2021], dtype="Int64"),
        "rating": [4.5, 3.8, 4.1],
        "ratings": pd.array([1000, 500, 750], dtype="Int64"),
    })


# ---------------------------------------------------------------------------
# load_raw_csv
# ---------------------------------------------------------------------------


def test_load_raw_csv_raises_if_file_missing():
    with pytest.raises(FileNotFoundError, match="Input CSV not found"):
        load_raw_csv(Path("nonexistent.csv"))


def test_load_raw_csv_returns_dataframe(tmp_path):
    csv = tmp_path / "test.csv"
    csv.write_text("book_title,author_id\nSome Book,1\n")
    df = load_raw_csv(csv)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


# ---------------------------------------------------------------------------
# load_author_lookup
# ---------------------------------------------------------------------------


def test_load_author_lookup_raises_if_db_missing(tmp_path):
    csv_path = tmp_path / "RAW_DATA.csv"
    csv_path.touch()
    with pytest.raises(FileNotFoundError, match="Authors database not found"):
        load_author_lookup(csv_path)


def test_load_author_lookup_returns_correct_mapping(tmp_path):
    db_path = tmp_path / "authors.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE author (id INT, name TEXT)")
        conn.executemany("INSERT INTO author VALUES (?, ?)", [
                         (1, "Jane Austen"), (2, "Colleen Hoover")])

    csv_path = tmp_path / "RAW_DATA.csv"
    csv_path.touch()

    lookup = load_author_lookup(csv_path)
    assert lookup == {1: "Jane Austen", 2: "Colleen Hoover"}


# ---------------------------------------------------------------------------
# drop_unnamed_index_columns
# ---------------------------------------------------------------------------


def test_drop_unnamed_index_columns_removes_junk():
    df = pd.DataFrame({"Unnamed: 0": [1], "Unnamed: 0.1": [
                      1], "index": [1], "book_title": ["A"]})
    result = drop_unnamed_index_columns(df)
    assert list(result.columns) == ["book_title"]


# ---------------------------------------------------------------------------
# rename_columns
# ---------------------------------------------------------------------------


def test_rename_columns():
    df = pd.DataFrame({"book_title": ["A"], "author_id": [1], "Year released": [
                      2020], "Rating": [4.5], "ratings": [100]})
    result = rename_columns(df)
    assert "title" in result.columns
    assert "year" in result.columns
    assert "rating" in result.columns
    assert "book_title" not in result.columns
    assert "Year released" not in result.columns


# ---------------------------------------------------------------------------
# strip_bracket_content_from_titles
# ---------------------------------------------------------------------------


def test_strip_bracket_content_removes_format_descriptors():
    df = pd.DataFrame({"title": ["Jane Eyre (Paperback)", "Dune [Hardcover]"]})
    result = strip_bracket_content_from_titles(df)
    assert result["title"].tolist() == ["Jane Eyre", "Dune"]


def test_strip_bracket_content_removes_series_info():
    df = pd.DataFrame(
        {"title": ["Outlander (Outlander, #1)", "New Moon (The Twilight Saga, #2)"]})
    result = strip_bracket_content_from_titles(df)
    assert result["title"].tolist() == ["Outlander", "New Moon"]


def test_strip_bracket_content_leaves_clean_titles_unchanged():
    df = pd.DataFrame({"title": ["Rebecca", "Normal People"]})
    result = strip_bracket_content_from_titles(df)
    assert result["title"].tolist() == ["Rebecca", "Normal People"]


# ---------------------------------------------------------------------------
# clean_ratings_count
# ---------------------------------------------------------------------------


def test_clean_ratings_count_strips_backticks():
    df = pd.DataFrame({"ratings": ["`3732237`", "`100`"]})
    result = clean_ratings_count(df)
    assert result["ratings"].tolist() == [3732237, 100]


# ---------------------------------------------------------------------------
# clean_rating
# ---------------------------------------------------------------------------


def test_clean_rating_fixes_comma_decimal_separator():
    df = pd.DataFrame({"rating": ["4,28", "3,99", "4,0"]})
    result = clean_rating(df)
    assert result["rating"].tolist() == [4.28, 3.99, 4.0]


# ---------------------------------------------------------------------------
# resolve_author_names
# ---------------------------------------------------------------------------


def test_resolve_author_names_maps_ids_to_names():
    df = pd.DataFrame({"author_id": [1.0, 2.0], "title": ["Book A", "Book B"]})
    lookup = {1: "Jane Austen", 2: "Colleen Hoover"}
    result = resolve_author_names(df, lookup)
    assert result["author_name"].tolist() == ["Jane Austen", "Colleen Hoover"]
    assert "author_id" not in result.columns


def test_resolve_author_names_produces_nan_for_missing_id():
    df = pd.DataFrame({"author_id": [None], "title": ["Unknown Book"]})
    result = resolve_author_names(df, {1: "Jane Austen"})
    assert pd.isna(result["author_name"].iloc[0])


# ---------------------------------------------------------------------------
# drop_invalid_rows
# ---------------------------------------------------------------------------


def test_drop_invalid_rows_removes_missing_title(minimal_df):
    minimal_df.loc[0, "title"] = None
    result = drop_invalid_rows(minimal_df)
    assert len(result) == 2
    assert "Book A" not in result["title"].values


def test_drop_invalid_rows_removes_missing_author(minimal_df):
    minimal_df.loc[1, "author_name"] = None
    result = drop_invalid_rows(minimal_df)
    assert len(result) == 2
    assert "Author Two" not in result["author_name"].values


def test_drop_invalid_rows_removes_exact_duplicates(minimal_df):
    df_with_dupe = pd.concat(
        [minimal_df, minimal_df.iloc[[0]]], ignore_index=True)
    result = drop_invalid_rows(df_with_dupe)
    assert len(result) == 3


# ---------------------------------------------------------------------------
# sort_by_rating
# ---------------------------------------------------------------------------


def test_sort_by_rating_sorts_descending(minimal_df):
    result = sort_by_rating(minimal_df)
    ratings = result["rating"].tolist()
    assert ratings == sorted(ratings, reverse=True)
