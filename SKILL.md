# SKILL: Medallion Data Pipeline

Blueprint for creating and managing medallion-architecture data pipelines (Bronze -> Silver -> Gold) for tabular datasets.

## When to Use

- Processing CSV/Excel survey, transactional, or operational data
- Building layered data transformations with validation and enrichment
- Creating analysis-ready datasets from raw source data
- Any project where data quality tracking (quarantine) matters

## Project Structure

```
<project>/
├── data/
│   ├── input/                    Raw source files (CSV, Excel, etc.)
│   └── output/                   Pipeline outputs
│       ├── bronze/               Ingested data with metadata (Parquet)
│       ├── silver/               Validated + enriched data (Parquet)
│       ├── gold/                 Analytical datasets (CSV)
│       └── quarantine/           Invalid records with failure reasons
├── logs/
│   ├── pipeline.log              Rotating log file (5MB, 3 backups)
│   ├── run_summary.json          Array of all run summaries
│   └── log_viewer.py             Live web dashboard (port 8777)
├── pipeline/
│   ├── __init__.py
│   ├── main.py                   Orchestrator + CLI entry point
│   ├── config/
│   │   ├── __init__.py
│   │   ├── config.py             Paths, thresholds, bins, analysis params
│   │   └── logging_config.py     Dual-output logging with run context
│   ├── entities/
│   │   ├── __init__.py
│   │   └── columns.py            Column name constants per layer
│   ├── controllers/
│   │   ├── __init__.py
│   │   └── io_adapter.py         Read/write CSV, Parquet, JSON
│   └── layer_transforms/
│       ├── __init__.py
│       ├── bronze_transforms.py  Ingestion + metadata
│       ├── silver_transforms.py  Validate, deduplicate, enrich
│       └── gold_transforms.py    Analytical aggregations
├── tests/
│   ├── conftest.py               Factory fixtures chained through layers
│   ├── test_bronze_transforms.py
│   ├── test_silver_transforms.py
│   ├── test_gold_transforms.py
│   └── test_io_adapter.py
├── notebooks/
│   ├── bronze/                   Raw data profiling and EDA
│   ├── silver/                   Validation prototyping
│   ├── gold/                     Analysis and visualization
│   └── sandbox/                  Free-form experiments
├── pyproject.toml                Poetry config
├── poetry.lock
└── .gitignore
```

## Setup Steps

### 1. Initialize project

```bash
mkdir <project> && cd <project>
poetry init
poetry config virtualenvs.in-project true
poetry add pandas numpy pyarrow
poetry add --group dev jupyter ipykernel matplotlib pytest
```

### 2. Create directory skeleton

```
pipeline/config/
pipeline/entities/
pipeline/controllers/
pipeline/layer_transforms/
data/input/
data/output/{bronze,silver,gold,quarantine}
logs/
tests/
notebooks/{bronze,silver,gold,sandbox}
```

Every Python package needs an empty `__init__.py`.

### 3. Configure .gitignore

```gitignore
.venv/
__pycache__/
*.pyc
logs/
!logs/log_viewer.py
.idea/
.claude/
.ipynb_checkpoints/
data/output/
```

Track `data/input/` (source files) but ignore `data/output/` (regenerated).

---

## Architecture Rules

### Core Principles

1. **Pure transforms** — all layer transform functions take a DataFrame in and return a DataFrame out. No side effects (logging is the one exception). Always `.copy()` input before mutating.

2. **Never drop data** — invalid records are flagged with `_is_quarantined = True` and `_quarantine_reason = "age out of range; bmi out of range;"`. They flow through the pipeline and get split out at write time.

3. **Separate I/O from logic** — controllers handle all file reads/writes. Transforms never touch the filesystem.

4. **Explicit column names** — no magic strings. All column names are constants in `entities/columns.py`.

5. **Composition via `.pipe()`** — each layer has an `apply_<layer>_transforms()` function that chains individual transforms:
   ```python
   def apply_silver_transforms(df):
       return (
           df
           .pipe(validate_records)
           .pipe(deduplicate)
           .pipe(add_bmi_category)
           .pipe(add_age_group)
       )
   ```

6. **Idempotent writes** — all outputs overwrite. The run summary JSON is the one exception: it appends to an array, deduplicating by `run_id`.

### Layer Responsibilities

| Layer | Input | Output | Purpose |
|-------|-------|--------|---------|
| **Bronze** | Raw CSV | Parquet + metadata | Add `_ingested_at`, `_source_file`, `_batch_id`. No cleaning. |
| **Silver** | Bronze Parquet | Parquet (valid) + Parquet (quarantine) | Validate ranges/categories, deduplicate, enrich with derived columns. |
| **Gold** | Silver Parquet | Multiple CSVs | Analytical aggregations. Each gold dataset is independent. |

### Data Flow in main.py

```
parse_args() → set_run_id()
  → Bronze: read_csv → apply_bronze_transforms → write_parquet
  → Silver: apply_silver_transforms → split_valid_and_quarantine → write_parquet (x2)
  → Gold:   apply_gold_transforms → dict of DataFrames → write_csv (each)
  → write_json(summary)
```

If running a single layer (`--layer silver`), it loads the previous layer's output from disk.

---

## Config Pattern

### `pipeline/config/config.py`

```python
from pathlib import Path

# --- Paths ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "input"
LAYERS_DIR = BASE_DIR / "data" / "output"
BRONZE_DIR = LAYERS_DIR / "bronze"
SILVER_DIR = LAYERS_DIR / "silver"
GOLD_DIR = LAYERS_DIR / "gold"
QUARANTINE_DIR = LAYERS_DIR / "quarantine"
LOGS_DIR = BASE_DIR / "logs"
SUMMARY_FILE = LOGS_DIR / "run_summary.json"

SOURCE_FILE = "your_dataset.csv"

# --- Validation thresholds (adapt per dataset) ---
VALID_AGE_RANGE = (0, 120)
VALID_SCORE_RANGE = (1.0, 10.0)
VALID_CATEGORIES = {"CategoryA", "CategoryB", "CategoryC"}

# --- Categorization bins (for pd.cut) ---
SCORE_BINS = [0, 3, 6, 10]
SCORE_LABELS = ["Low", "Medium", "High"]

# --- Analysis parameters ---
NUMERIC_COLUMNS_FOR_CORRELATION = ["col_a", "col_b", "col_c"]
CORRELATION_MIN_THRESHOLD = 0.05
```

All thresholds, bins, valid sets, and column lists live here. Transform functions import what they need.

---

## Column Constants Pattern

### `pipeline/entities/columns.py`

```python
class BronzeCols:
    """Raw source columns + ingestion metadata."""
    # Source columns
    ID = "person_id"
    AGE = "age"
    # ... all raw columns ...

    # Metadata (prefixed with _)
    INGESTED_AT = "_ingested_at"
    SOURCE_FILE = "_source_file"
    BATCH_ID = "_batch_id"


class SilverCols(BronzeCols):
    """Inherits Bronze + adds enrichment and quarantine columns."""
    BMI_CATEGORY = "bmi_category"
    AGE_GROUP = "age_group"
    IS_QUARANTINED = "_is_quarantined"
    QUARANTINE_REASON = "_quarantine_reason"


class GoldCols:
    """Analysis-specific columns (no inheritance needed)."""
    VARIABLE_1 = "variable_1"
    CORRELATION = "correlation"
    # ... per gold dataset ...
```

Usage: `from pipeline.entities.columns import BronzeCols as C` then `df[C.AGE]`.

---

## Transform Patterns

### Bronze — Metadata only

```python
def add_ingestion_metadata(df, source_file):
    result = df.copy()
    result[C.INGESTED_AT] = datetime.now(timezone.utc).isoformat()
    result[C.SOURCE_FILE] = source_file
    result[C.BATCH_ID] = str(uuid.uuid4())
    return result

def apply_bronze_transforms(df, source_file):
    return df.pipe(add_ingestion_metadata, source_file=source_file)
```

### Silver — Validate + Enrich

```python
def validate_records(df):
    result = df.copy()
    reasons = pd.Series("", index=result.index)

    # Range checks
    mask = ~result[C.AGE].between(*VALID_AGE_RANGE)
    reasons = reasons.where(~mask, reasons + "age out of range; ")

    # Category checks
    mask = ~result[C.GENDER].isin(VALID_GENDERS)
    reasons = reasons.where(~mask, reasons + "gender invalid; ")

    result[SC.IS_QUARANTINED] = reasons.str.len() > 0
    result[SC.QUARANTINE_REASON] = reasons.str.rstrip("; ")
    return result

def deduplicate(df):
    return df.sort_values(C.INGESTED_AT).drop_duplicates(
        subset=[C.ID], keep="last"
    )

def add_derived_column(df):
    result = df.copy()
    result[SC.NEW_COL] = pd.cut(result[C.SOURCE_COL], bins=BINS, labels=LABELS)
    return result

def apply_silver_transforms(df):
    return df.pipe(validate_records).pipe(deduplicate).pipe(add_derived_column)

def split_valid_and_quarantine(df):
    valid = df.loc[~df[SC.IS_QUARANTINED]].drop(
        columns=[SC.IS_QUARANTINED, SC.QUARANTINE_REASON]
    )
    quarantine = df.loc[df[SC.IS_QUARANTINED]].copy()
    return valid, quarantine
```

### Gold — Independent Analyses

```python
def build_analysis_a(df):
    # Returns a new DataFrame (aggregation, pivot, etc.)
    ...

def build_analysis_b(df):
    ...

def apply_gold_transforms(df):
    return {
        "gold_analysis_a": build_analysis_a(df),
        "gold_analysis_b": build_analysis_b(df),
    }
```

Gold returns a `dict[str, DataFrame]`. The orchestrator iterates and writes each as a separate CSV.

---

## I/O Adapter Pattern

### `pipeline/controllers/io_adapter.py`

```python
import logging
logger = logging.getLogger(__name__)

def read_csv(path):
    logger.info("Reading source: %s", path.name)
    df = pd.read_csv(path)
    logger.info("Read %d rows, %d columns", len(df), len(df.columns))
    return df

def write_parquet(df, directory, filename):
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / filename
    df.to_parquet(path, index=False)
    logger.info("Wrote %d rows -> %s", len(df), path)
    return path

def write_csv(df, directory, filename):
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / filename
    df.to_csv(path, index=False)
    logger.info("Wrote %d rows => %s", len(df), path)
    return path

def write_json(data, path):
    # Maintains JSON array of run summaries
    # Deduplicates by run_id (updates existing, appends new)
    ...
```

All write functions: create dirs, write file, log result, return Path.

---

## Logging Pattern

### `pipeline/config/logging_config.py`

- **Console handler**: INFO+, short timestamps (`%H:%M:%S`)
- **File handler**: DEBUG+, full timestamps, rotating (5MB x 3 backups)
- **Context filter**: injects `run_id` (8-char UUID) and `layer` (bronze/silver/gold/general) into every record

```
Console: 17:14:49 | INFO    | __main__ | Pipeline started
File:    2026-03-31 17:14:49 | INFO    | fc10c7c9 | bronze  | pipeline.controllers.io_adapter | read_csv | Reading source: data.csv
```

Functions: `setup_logging()`, `set_run_id()`, `clear_run_id()`, `set_layer()`, `clear_layer()`

---

## Testing Pattern

### `tests/conftest.py` — Factory Fixtures

```python
def _make_raw_row(**overrides):
    base = { ...sensible defaults for every column... }
    base.update(overrides)
    return base

@pytest.fixture
def raw_df():
    rows = [_make_raw_row(person_id=i, ...) for i in range(1, 6)]
    return pd.DataFrame(rows)

@pytest.fixture
def bronze_df(raw_df):
    return apply_bronze_transforms(raw_df, source_file="test.csv")

@pytest.fixture
def silver_df(bronze_df):
    return apply_silver_transforms(bronze_df)
```

Fixtures chain through layers: `raw_df -> bronze_df -> silver_df -> valid_and_quarantine`.

### Test Structure

```python
class TestValidateRecords:
    def test_valid_data_not_quarantined(self, bronze_df):
        result = validate_records(bronze_df)
        assert not result[SC.IS_QUARANTINED].any()

    def test_invalid_age_quarantined(self, make_raw_row):
        df = pd.DataFrame([make_raw_row(age=999)])
        df = apply_bronze_transforms(df, source_file="test.csv")
        result = validate_records(df)
        assert result[SC.IS_QUARANTINED].all()
        assert "age out of range" in result[SC.QUARANTINE_REASON].iloc[0]
```

Test what matters: column existence, row counts, quarantine flags, no-mutation guarantees.

---

## Notebook Pattern

Each notebook starts with a setup cell:

```python
import sys
from pathlib import Path
PROJECT_ROOT = Path.cwd().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.config.config import DATA_DIR, GOLD_DIR
from pipeline.controllers.io_adapter import read_csv
```

Notebooks mirror the pipeline layers and are for exploration/prototyping only. Production logic goes in `pipeline/layer_transforms/`.

---

## Log Viewer

A self-contained Python HTTP server (`logs/log_viewer.py`) that serves a dark-themed HTML dashboard:

- **SSE streaming** of `pipeline.log` for real-time tail
- **Execution groups** collapsed by `run_id` (extracted from log format)
- **Layer sub-groups** (bronze/silver/gold/general) with colored borders
- **Run summary sidebar** — click a status badge to see that run's stats
- **Run Pipeline button** — triggers pipeline via subprocess in background thread
- **ThreadingHTTPServer** — required to handle SSE + fetch concurrently
- **Level filters** (ALL/DEBUG/INFO/WARNING/ERROR)

---

## CLI

```bash
# Full pipeline
python -m pipeline.main

# Specific source file
python -m pipeline.main --source other_data.csv

# Single layer (loads previous layer from disk)
python -m pipeline.main --layer gold

# Dry run (validate config, don't run)
python -m pipeline.main --dry-run
```

---

## Run Summary

After each run, `logs/run_summary.json` accumulates an entry:

```json
{
  "run_id": "e878840b",
  "timestamp": "2026-03-31T11:53:45.520330+00:00",
  "source_file": "sleep_health_dataset.csv",
  "layers": {
    "bronze": { "rows_in": 100000, "rows_out": 100000 },
    "silver": { "rows_in": 100000, "rows_valid": 100000, "rows_quarantined": 0 },
    "gold": { "gold_correlation_matrix": 61, "gold_lifestyle_impact": 391 }
  },
  "status": "success",
  "elapsed_seconds": 1.48
}
```

---

## Adapting for a New Dataset

1. **Place source CSV** in `data/input/`
2. **Define columns** in `entities/columns.py` — `BronzeCols` for raw, `SilverCols` for enriched, `GoldCols` for analysis
3. **Set validation rules** in `config/config.py` — ranges, valid sets, categorization bins
4. **Write bronze transforms** — usually just `add_ingestion_metadata` (copy as-is)
5. **Write silver transforms** — `validate_records` (adapt checks), `deduplicate` (adapt key), add enrichment functions
6. **Write gold transforms** — one `build_*` function per analytical dataset
7. **Write tests** — update `_make_raw_row` defaults, add edge case tests per validation rule
8. **Create notebooks** — profile raw data in bronze, prototype validation in silver, build visualizations in gold
9. **Update `SOURCE_FILE`** and `NUMERIC_COLUMNS_FOR_CORRELATION` in config
10. **Run**: `python -m pipeline.main`

---

## Naming Conventions

| What | Convention | Example |
|------|-----------|---------|
| Files | `snake_case.py` | `bronze_transforms.py` |
| Directories | `lowercase` | `layer_transforms/` |
| Functions | `snake_case` | `apply_silver_transforms()` |
| Constants | `UPPER_SNAKE` | `VALID_AGE_RANGE` |
| Classes | `PascalCase` | `BronzeCols`, `SilverCols` |
| Column aliases | Single letter | `C`, `BC`, `SC`, `G` |
| Transform functions | `add_*` (enrich), `build_*` (gold), `apply_*` (compose) | `add_bmi_category()` |
| Test classes | `Test<FunctionName>` | `TestValidateRecords` |
| Output files | `<layer>_<name>.<ext>` | `gold_correlation_matrix.csv` |
| Metadata columns | Prefixed with `_` | `_ingested_at`, `_batch_id` |
