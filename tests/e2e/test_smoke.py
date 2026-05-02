import shutil
from pathlib import Path

import pandas as pd
import pytest

from medallion_etl.pipelines import bronze, silver
from medallion_etl.settings import settings


@pytest.fixture(autouse=True)
def _clean_data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(settings, "storage_root", f"file://{tmp_path.as_posix()}")
    landing = tmp_path / "00_landing"
    landing.mkdir(parents=True)
    fixture = Path(__file__).resolve().parents[1] / "fixtures" / "orders_raw.csv"
    shutil.copy(fixture, landing / "orders_raw.csv")
    yield


def test_bad_row_lands_in_quarantine() -> None:
    bronze.ingest_orders(run_id="rtest")
    paths = silver.build_orders(run_id="rtest")

    silver_files = sorted(Path(paths["silver"]).rglob("*.parquet"))
    quarantine_files = sorted(Path(paths["quarantine"]).rglob("*.parquet"))
    silver_df = pd.concat([pd.read_parquet(f) for f in silver_files], ignore_index=True)
    quarantine_df = pd.concat([pd.read_parquet(f) for f in quarantine_files], ignore_index=True)

    assert len(silver_df) == 4
    assert len(quarantine_df) == 1
    assert quarantine_df["order_id"].iloc[0] == 1003
    assert quarantine_df["total_amount"].iloc[0] == -15.00
    assert "_violations" in quarantine_df.columns
