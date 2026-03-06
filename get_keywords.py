"""
Loads PROCESSED_DATA.csv from the current working directory, extracts
keywords from book titles, and produces a bar chart of the 20 most
common keywords.

- Stopwords are filtered using NLTK's English stopword corpus
- Single character tokens and purely numeric tokens are excluded
- Keywords are lowercased and stripped of punctuation

Output:
    top_keywords.png : sorted bar chart of the 20 most common keywords

Usage:
    python get_keywords.py
"""

from nltk.corpus import stopwords
import logging
import re
import string
from collections import Counter
from pathlib import Path

import altair as alt
import nltk
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Downloaded once on first run, no-op if already present
nltk.download("stopwords", quiet=True)


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------


def load_processed_csv() -> pd.DataFrame:
    input_path = Path("PROCESSED_DATA.csv")
    if not input_path.is_file():
        raise FileNotFoundError(f"Processed data not found: {input_path}")
    logger.info("Loading processed data from: %s", input_path)
    df = pd.read_csv(input_path)
    logger.info("Loaded %d rows", len(df))
    return df


def save_chart(chart: alt.Chart, filename: str) -> None:
    output_path = Path(filename)
    chart.save(str(output_path))
    logger.info("Saved chart to: %s", output_path)


# ---------------------------------------------------------------------------
# Keyword extraction
# ---------------------------------------------------------------------------


def tokenise_titles(titles: pd.Series) -> list[str]:
    """
    Lowercase, strip punctuation, and split all titles into individual tokens.
    Filters out single character tokens and purely numeric tokens.
    """
    english_stopwords = set(stopwords.words("english"))
    tokens = []

    for title in titles.dropna():
        # Remove punctuation and lowercase
        cleaned = title.lower().translate(str.maketrans("", "", string.punctuation))
        words = cleaned.split()
        tokens.extend([
            word for word in words
            if word not in english_stopwords
            and len(word) > 1
            and not word.isnumeric()
        ])

    return tokens


def get_top_keywords(tokens: list[str], n: int = 20) -> pd.DataFrame:
    """Return the top n most frequent keywords as a DataFrame."""
    counts = Counter(tokens).most_common(n)
    return pd.DataFrame(counts, columns=["keyword", "count"])


# ---------------------------------------------------------------------------
# Chart
# ---------------------------------------------------------------------------


def plot_top_keywords(top_keywords: pd.DataFrame) -> alt.Chart:
    """Horizontal bar chart of the 20 most common title keywords."""
    return (
        alt.Chart(top_keywords)
        .mark_bar()
        .encode(
            x=alt.X("count:Q", title="Occurrences"),
            y=alt.Y("keyword:N", sort="x", title="Keyword"),
            tooltip=["keyword:N", "count:Q"],
        )
        .properties(title="Top 20 Keywords in Book Titles", width=600, height=500)
    )


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def run_keyword_analysis() -> None:
    df = load_processed_csv()
    tokens = tokenise_titles(df["title"])
    top_keywords = get_top_keywords(tokens)
    logger.info("Top keywords:\n%s", top_keywords.to_string(index=False))
    save_chart(plot_top_keywords(top_keywords), "top_keywords.png")


if __name__ == "__main__":
    run_keyword_analysis()
