import uuid
from pathlib import Path

import pandas as pd

from medallion_etl.gates.gold_gate import gate_gold_orders_daily
from medallion_etl.io.catalog import uri_for
from medallion_etl.io.writers import promote, write_quarantine, write_staging
from medallion_etl.transformations.gold.orders_daily import build_orders_daily


def _read_silver_orders() -> pd.DataFrame:
    root = Path(uri_for("silver", "orders").removeprefix("file://"))
    files = sorted(root.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def build_orders_daily_pipeline(run_id: str | None = None) -> dict[str, str]:
    run_id = run_id or uuid.uuid4().hex[:8]
    silver_df = _read_silver_orders()
    candidate = build_orders_daily(silver_df)
    clean, quarantine = gate_gold_orders_daily(candidate)

    paths: dict[str, str] = {}
    if not quarantine.empty:
        paths["quarantine"] = write_quarantine("gold", "orders_daily", quarantine, run_id)
    write_staging("gold", "orders_daily", clean, run_id)
    paths["gold"] = promote("gold", "orders_daily", run_id)
    return paths
