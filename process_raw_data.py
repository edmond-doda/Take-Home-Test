"""
process_raw_data.py

Transform stage of the Adora Enhance book data pipeline.

Takes a raw scraped CSV file as input, cleans and enriches it,
and writes a standardised PROCESSED_DATA.csv to the current
working directory.

Usage:
    python process_raw_data.py <path_to_raw_csv>

Example:
    python process_raw_data.py data/RAW_DATA_0.csv
"""

import argparse
import logging
import re
import sqlite3
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the data processing pipeline."""
    parser = argparse.ArgumentParser(
        description="Clean and transform a raw scraped romance book CSV."
    )
    parser.add_argument("csv_path", type=Path,
                        help="Path to the raw input CSV file.")
    return parser.parse_args()


def load_raw_csv(csv_path: Path) -> pd.DataFrame:
    """Load the raw CSV into a DataFrame, ensuring the file exists."""
    if not csv_path.is_file():
        raise FileNotFoundError(f"Input CSV not found: {csv_path}")
    logger.info("Loading raw data from: %s", csv_path)
    df = pd.read_csv(csv_path)
    logger.info("Loaded %d rows", len(df))
    return df


def load_author_lookup(csv_path: Path) -> dict[int, str]:
    """Load author ID -> name mapping from authors.db in the same directory as the CSV."""
    db_path = csv_path.parent / "authors.db"
    if not db_path.is_file():
        raise FileNotFoundError(f"Authors database not found: {db_path}")

    logger.info("Loading author lookup from: %s", db_path)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT id, name FROM author").fetchall()

    lookup = {int(author_id): name for author_id, name in rows}
    logger.info("Loaded %d authors", len(lookup))
    return lookup


def write_processed_csv(df: pd.DataFrame) -> None:
    """Write the cleaned DataFrame to PROCESSED_DATA.csv in the current working directory."""
    output_path = Path("PROCESSED_DATA.csv")
    df.to_csv(output_path, index=False)
    logger.info("Written %d rows to: %s", len(df), output_path)


# ---------------------------------------------------------------------------
# Transformation steps
# ---------------------------------------------------------------------------


def drop_unnamed_index_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the spurious index columns produced by the scraper."""
    index_cols = [col for col in df.columns if col ==
                  "index" or col.startswith("Unnamed")]
    logger.info("Dropping index columns: %s", index_cols)
    return df.drop(columns=index_cols)


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to standardised names."""
    return df.rename(columns={
        "book_title": "title",
        "Year released": "year",
        "Rating": "rating",
    })


def strip_bracket_content_from_titles(df: pd.DataFrame) -> pd.DataFrame:
    """Remove bracketed content from titles e.g. "(Paperback)", "(The Notebook, #1)"."""
    bracket_pattern = re.compile(r"\s*[\(\[\{][^)\]\}]*[\)\]\}]")
    df["title"] = df["title"].str.replace(
        bracket_pattern, "", regex=True).str.strip()
    return df


def clean_ratings_count(df: pd.DataFrame) -> pd.DataFrame:
    """Strip backtick wrappers from ratings count e.g. `3732237` -> 3732237."""
    df["ratings"] = (
        df["ratings"]
        .astype(str)
        .str.extract(re.compile(r"`(\d+)`"), expand=False)
        .astype("Int64")
    )
    return df


def clean_rating(df: pd.DataFrame) -> pd.DataFrame:
    """Fix comma decimal separator e.g. "4,28" -> 4.28 and cast to float."""
    df["rating"] = (
        df["rating"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )
    return df


def clean_year(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce year to numeric, setting invalid parsing to NaN, and convert to Int64."""
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df


def resolve_author_names(df: pd.DataFrame, author_lookup: dict[int, str]) -> pd.DataFrame:
    """
    Replace author_id with author_name via the lookup dict.
    IDs arrive as floats (e.g. 1.0) and are cast to int before lookup.
    """
    author_id_as_int = pd.to_numeric(
        df["author_id"], errors="coerce").astype("Int64")
    df["author_name"] = author_id_as_int.map(author_lookup)
    return df.drop(columns=["author_id"])


def select_and_order_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Select only the relevant columns and order them."""
    return df[["title", "author_name", "year", "rating", "ratings"]]


# ---------------------------------------------------------------------------
# Investigation and drop
# ---------------------------------------------------------------------------


def investigate_rows_to_drop(df: pd.DataFrame) -> None:
    """Log a breakdown of rows candidates for removal without dropping anything."""
    missing_title = df["title"].isna() | (df["title"] == "")
    missing_author = df["author_name"].isna()
    is_duplicate = df.duplicated(keep=False)

    logger.info("--- Drop Investigation ---")
    logger.info("Rows with missing title:  %d", missing_title.sum())
    logger.info("Rows with missing author: %d", missing_author.sum())
    logger.info("Rows with both missing:   %d",
                (missing_title & missing_author).sum())
    logger.info("Duplicate rows:           %d", is_duplicate.sum())

    if missing_title.any():
        logger.info("Missing title rows:\n%s", df[missing_title].to_string())
    if missing_author.any():
        logger.info("Missing author rows:\n%s", df[missing_author].to_string())
    if is_duplicate.any():
        dupes = df[is_duplicate].sort_values(by=["title", "author_name"])
        logger.info("Duplicate rows:\n%s", dupes.to_string())

    logger.info("--- End Investigation ---")


def drop_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with a missing title or author, and exact duplicates."""
    initial_count = len(df)

    missing_title = df["title"].isna() | (df["title"] == "")
    missing_author = df["author_name"].isna()
    df = df[~(missing_title | missing_author)].drop_duplicates()

    logger.info("Dropped %d rows (%d remaining)",
                initial_count - len(df), len(df))
    return df


def sort_by_rating(df: pd.DataFrame) -> pd.DataFrame:
    """Sort the DataFrame by rating in descending order."""
    return df.sort_values(by="rating", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def run_pipeline(csv_path: Path) -> None:
    """
    Execute the full transform pipeline:
      1. Load raw CSV and author lookup
      2. Drop junk index columns and rename columns
      3. Clean all values (types, formatting)
      4. Resolve author IDs to names
      5. Select and order output columns
      6. Investigate candidates for removal, then drop them
      7. Sort by rating descending and write output
    """
    df = load_raw_csv(csv_path)
    author_lookup = load_author_lookup(csv_path)

    df = drop_unnamed_index_columns(df)
    df = rename_columns(df)

    df = strip_bracket_content_from_titles(df)
    df = clean_ratings_count(df)
    df = clean_rating(df)
    df = clean_year(df)

    df = resolve_author_names(df, author_lookup)
    df = select_and_order_columns(df)

    investigate_rows_to_drop(df)
    df = drop_invalid_rows(df)

    df = sort_by_rating(df)
    write_processed_csv(df)


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args.csv_path)
