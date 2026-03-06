"""
analyse_processed_data.py

Analysis stage of the Adora Enhance book data pipeline.

Loads PROCESSED_DATA.csv from the current working directory and produces
two chart outputs:

- decade_releases.png : pie chart of books released per decade
- top_authors.png     : bar chart of total ratings for the top 10 authors

Usage:
    python analyse_processed_data.py
"""
import logging
from pathlib import Path
import pandas as pd
import altair as alt


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------


def load_processed_csv() -> pd.DataFrame:
    """Load the processed data CSV file into a DataFrame."""
    input_path = Path("PROCESSED_DATA.csv")
    if not input_path.is_file():
        raise FileNotFoundError(f"Processed data not found: {input_path}")
    logger.info("Loading processed data from: %s", input_path)
    df = pd.read_csv(input_path)
    logger.info("Loaded %d rows", len(df))
    return df


def save_chart(chart: alt.Chart, filename: str) -> None:
    """Save an Altair chart to a file."""
    output_path = Path(filename)
    chart.save(str(output_path))
    logger.info("Saved chart to: %s", output_path)


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------


def build_decade_counts(df: pd.DataFrame) -> pd.DataFrame:
    """Bucket books into decades and count releases per decade."""
    decade = (df["year"].dropna() // 10 * 10).astype(int)
    counts = decade.value_counts().reset_index()
    counts.columns = ["decade", "count"]
    counts["decade_label"] = counts["decade"].astype(str) + "s"
    return counts.sort_values("decade")


def build_top_authors(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Sum total ratings per author and return the top n."""
    return (
        df.groupby("author_name")["ratings"]
        .sum()
        .nlargest(n)
        .reset_index()
        .rename(columns={"ratings": "total_ratings"})
        .sort_values("total_ratings", ascending=False)
    )


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------


def plot_decade_releases(decade_counts: pd.DataFrame) -> alt.Chart:
    """Pie chart showing proportion of books released per decade."""
    return (
        alt.Chart(decade_counts)
        .mark_arc()
        .encode(
            theta=alt.Theta("count:Q"),
            color=alt.Color(
                "decade_label:N",
                legend=alt.Legend(title="Decade"),
                sort=decade_counts["decade_label"].tolist(),
            ),
            tooltip=["decade_label:N", "count:Q"],
        )
        .properties(title="Books Released by Decade", width=500, height=400)
    )


def plot_top_authors(top_authors: pd.DataFrame) -> alt.Chart:
    """Horizontal bar chart of total ratings for the top 10 authors."""
    return (
        alt.Chart(top_authors)
        .mark_bar()
        .encode(
            x=alt.X("total_ratings:Q", title="Total Ratings"),
            y=alt.Y(
                "author_name:N",
                sort="x",
                title="Author",
            ),
            tooltip=["author_name:N", "total_ratings:Q"],
        )
        .properties(title="Top 10 Most Rated Authors", width=600, height=400)
    )


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def run_analysis() -> None:
    """Run the analysis pipeline."""
    df = load_processed_csv()

    decade_counts = build_decade_counts(df)
    top_authors = build_top_authors(df)

    save_chart(plot_decade_releases(decade_counts), "decade_releases.png")
    save_chart(plot_top_authors(top_authors), "top_authors.png")


if __name__ == "__main__":
    run_analysis()
