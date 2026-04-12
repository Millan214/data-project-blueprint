# Sleep Health Data Pipeline

A medallion-architecture data pipeline (Bronze -> Silver -> Gold) that processes sleep health survey data into analysis-ready datasets.

## Architecture

```
CSV (100k rows)
    |
    v
 [BRONZE] -- Ingest raw data, add metadata (batch_id, timestamp, source)
    |
    v
 [SILVER] -- Validate, deduplicate, enrich (BMI category, age group, etc.)
    |          Invalid rows --> quarantine (flagged, never dropped)
    v
  [GOLD]  -- Analytical datasets:
              - Correlation matrix (numeric pairs)
              - Lifestyle impact on sleep quality
              - Sleep disorder risk profiles
```

## Project Structure

```
sleep/
├── data/
│   ├── input/                Raw source data
│   └── output/               Pipeline outputs (bronze/silver/gold/quarantine)
├── logs/                     Log files + run summary JSON
├── tools/
│   └── log_viewer.py         Live log viewer (HTML dashboard)
├── pipeline/                 Production code
│   ├── config/
│   │   ├── paths.py          File system paths
│   │   ├── validation_rules.py  Domain thresholds + categorization
│   │   └── analysis_params.py   Gold layer analysis parameters
│   ├── schemas/              Column name constants
│   ├── io/                   Read/write CSV, Parquet, JSON
│   ├── transforms/           Pure transform functions (bronze, silver, gold)
│   └── main.py               Orchestrator + CLI
├── tests/                    pytest unit tests
├── notebooks/                Development notebooks (bronze/silver/gold/sandbox)
└── pyproject.toml            Poetry project config
```

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements-dev.txt
```

## Usage

### Run the full pipeline

```bash
python -m pipeline
```

### CLI options

```bash
python -m pipeline --source other_data.csv   # Different input file
python -m pipeline --layer gold               # Run only the gold layer
python -m pipeline --dry-run                  # Validate config without running
```

### Run tests

```bash
pytest tests/ -v
```

### Live log viewer

```bash
python tools/log_viewer.py
# Open http://localhost:8777
```

### Run summary

After each run, a JSON summary is written to `logs/run_summary.json` with run ID, row counts, timing, and status.

## Notebooks

Interactive development notebooks mirror the layer structure:

| Notebook | Purpose |
|----------|---------|
| `notebooks/bronze/` | Raw data profiling and EDA |
| `notebooks/silver/` | Validation and enrichment prototyping |
| `notebooks/gold/` | Analysis, visualization, reporting |
| `notebooks/sandbox/` | Free-form experiments |

Each notebook includes a setup cell that resolves project imports automatically.
