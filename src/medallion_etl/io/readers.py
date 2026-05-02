from pathlib import Path

import pandas as pd

from medallion_etl.io.catalog import uri_for
from medallion_etl.settings import settings


def read_bronze(table: str) -> pd.DataFrame:
    root = Path(uri_for("bronze", table).removeprefix("file://"))
    files = sorted(root.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def read_silver(table: str) -> pd.DataFrame:
    root = Path(uri_for("silver", table).removeprefix("file://"))
    files = sorted(root.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def read_csv_landing(filename: str) -> pd.DataFrame:
    root = Path(settings.storage_root.removeprefix("file://")) / settings.landing_dir
    return pd.read_csv(root / filename)
