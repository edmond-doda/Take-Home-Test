# Adora Enhance — Book Data Pipeline

A data pipeline for cleaning, analysing, and visualising romance book sales data.

## Project Structure

```
├── data/
│   ├── authors.db          # SQLite author lookup database
│   ├── RAW_DATA_0.csv      # Raw scraped input files
│   ├── RAW_DATA_1.csv
│   ├── RAW_DATA_4.csv
│   └── EXAMPLE_DATA_4.csv  # Expected output for RAW_DATA_4
├── process_raw_data.py         # Task 1 — transform raw CSV to clean output
├── analyse_processed_data.py   # Task 2 — generate charts from processed data
├── get_keywords.py             # Task 3 — extract and chart top title keywords
├── test_process_raw_data.py    # Unit tests for Task 1
├── test_analyse_processed_data.py  # Unit tests for Task 2
├── test_get_keywords.py        # Unit tests for Task 3
├── test_integration.py         # Integration test — RAW_DATA_4 vs EXAMPLE_DATA_4
└── architecture.png            # Task 4 — AWS pipeline architecture diagram
```

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

## How to Run

### Task 1 — Process raw data

Takes a raw CSV and produces `PROCESSED_DATA.csv` in the current working directory.

```bash
python3 process_raw_data.py data/RAW_DATA_0.csv
```

### Task 2 — Analyse processed data

Reads `PROCESSED_DATA.csv` from the current working directory and produces
`decade_releases.png` and `top_authors.png`.

```bash
python3 analyse_processed_data.py
```

### Task 3 — Extract keywords

Reads `PROCESSED_DATA.csv` from the current working directory and produces
`top_keywords.png`.

```bash
python3 get_keywords.py
```

### Running tests

```bash
pytest test_process_raw_data.py -v
pytest test_analyse_processed_data.py -v
pytest test_get_keywords.py -v
pytest test_integration.py -v
```

## Tableau Dashboard

[Romance Reads: Ratings & Trends](https://public.tableau.com/views/EdmondTakeHomeTest/Dashboard1)