"""
I/O adapters for reading and writing data.

All file system interaction is isolated here.
Writes are idempotent: overwrite the target, never blind-append.
"""
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def read_csv(path: Path) -> pd.DataFrame:
    """Read a CSV source file into a DataFrame."""
    logger.info("Reading source: %s", path.name)
    logger.debug("Full path: %s", path)
    df = pd.read_csv(path)
    logger.info("Read %d rows, %d columns", len(df), len(df.columns))
    return df


def write_parquet(df: pd.DataFrame, directory: Path, filename: str) -> Path:
    """Write a DataFrame to parquet — idempotent (overwrites existing)."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / filename
    df.to_parquet(path, index=False)
    logger.info("Wrote %d rows → %s", len(df), path)
    return path


def write_csv(df: pd.DataFrame, directory: Path, filename: str) -> Path:
    """Write a DataFrame to CSV — idempotent (overwrites existing)."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / filename
    df.to_csv(path, index=False)
    logger.info("Wrote %d rows => %s", len(df), path)
    return path


def write_json(data: dict[str, Any], path: Path) -> Path:
    """Append a run summary to the JSON array file."""
    path.parent.mkdir(parents=True, exist_ok=True)

    # Load existing array or start fresh
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(existing, list):
                # Migrate old single-object format to array
                existing = [existing]
        except (json.JSONDecodeError, ValueError):
            existing = []
    else:
        existing = []

    # Update in-place if same run_id, otherwise append
    updated = False
    for i, entry in enumerate(existing):
        if entry.get("run_id") == data.get("run_id"):
            existing[i] = data
            updated = True
            break
    if not updated:
        existing.append(data)

    path.write_text(json.dumps(existing, indent=2, default=str), encoding="utf-8")
    logger.info("Wrote summary => %s", path)
    return path
