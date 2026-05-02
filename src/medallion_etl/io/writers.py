from __future__ import annotations

import shutil
from pathlib import Path
from typing import Final

import pandas as pd

from medallion_etl.io.catalog import uri_for

_STAGING: Final = "_staging"
_QUARANTINE: Final = "_quarantine"


def _to_local(uri: str) -> Path:
    return Path(uri.removeprefix("file://"))


def write_staging(layer: str, table: str, df: pd.DataFrame, run_id: str) -> str:
    target = _to_local(uri_for(layer, table, _STAGING)) / f"run_id={run_id}"
    target.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target / "part-0.parquet", index=False)
    return str(target)


def write_quarantine(layer: str, table: str, df: pd.DataFrame, run_id: str) -> str:
    target = _to_local(uri_for(layer, table, _QUARANTINE)) / f"run_id={run_id}"
    target.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target / "violations.parquet", index=False)
    return str(target)


def promote(layer: str, table: str, run_id: str) -> str:
    """Atomic rename of staging dir into the published table location."""
    staging = _to_local(uri_for(layer, table, _STAGING)) / f"run_id={run_id}"
    final = _to_local(uri_for(layer, table)) / f"run_id={run_id}"
    final.parent.mkdir(parents=True, exist_ok=True)
    if final.exists():
        shutil.rmtree(final)
    shutil.move(str(staging), str(final))
    return str(final)
