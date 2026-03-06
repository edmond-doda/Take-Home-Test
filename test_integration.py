"""
test_integration.py

Integration test for process_raw_data.py.

Runs the full pipeline against RAW_DATA_4.csv and asserts the output
matches EXAMPLE_DATA_4.csv exactly. Both files, along with authors.db,
are expected to live in data/ relative to this test file.
"""

from pathlib import Path

import pandas as pd
import pytest

from process_raw_data import run_pipeline


DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(autouse=True)
def run_from_data_dir(monkeypatch):
    """Ensure PROCESSED_DATA.csv is written to the data directory."""
    monkeypatch.chdir(DATA_DIR)


def test_pipeline_output_matches_example_data():
    raw_data_path = DATA_DIR / "RAW_DATA_4.csv"
    expected_path = DATA_DIR / "EXAMPLE_DATA_4.csv"
    output_path = DATA_DIR / "PROCESSED_DATA.csv"

    if not raw_data_path.is_file():
        pytest.skip("RAW_DATA_4.csv not found — skipping integration test")

    if not expected_path.is_file():
        pytest.skip("EXAMPLE_DATA_4.csv not found — skipping integration test")

    run_pipeline(raw_data_path)

    result = pd.read_csv(output_path)
    expected = pd.read_csv(expected_path)

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
        check_like=False,  # column order must match
    )
