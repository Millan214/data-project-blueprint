import uuid

import pandas as pd

from medallion_etl.gates.gold_gate import gate_gold_orders_daily
from medallion_etl.io.readers import read_silver
from medallion_etl.io.writers import promote, write_quarantine, write_staging
from medallion_etl.observability import build_loginfo, logger
from medallion_etl.settings import settings
from medallion_etl.transformations.gold.orders_daily import build_orders_daily


loginfo = build_loginfo({
    "gold.read_silver": {
        "layer": "gold", "layer_order": 3,
        "step_name": "Reading silver", "step_order": 0,
        "description": "Reads every parquet file under the silver orders table into a single DataFrame.",
        "rows_out_fn": len,
    },
    "gold.aggregate": {
        "layer": "gold", "layer_order": 3,
        "step_name": "Aggregating daily", "step_order": 1,
        "description": "Groups orders by (order_date, currency) and sums totals into a daily fact table.",
        "rows_in_fn": lambda df, **_: len(df),
        "rows_out_fn": len,
    },
    "gold.gate": {
        "layer": "gold", "layer_order": 3,
        "step_name": "Validating (Pandera gate)", "step_order": 2,
        "description": "Validates aggregated rows against GoldOrdersDaily; failures route to quarantine.",
        "rows_in_fn": lambda df, **_: len(df),
        "rows_out_fn": lambda result: len(result[0]),
        "extra_out_fn": lambda result: {"quarantined": int(len(result[1]))},
    },
    "gold.write": {
        "layer": "gold", "layer_order": 3,
        "step_name": "Writing parquet", "step_order": 3,
        "description": "Writes the daily aggregate to gold _staging then atomically promotes it.",
        "rows_in_fn": lambda clean, **_: len(clean),
        "extra_out_fn": lambda paths: {**paths},
    },
})


@logger.step(loginfo["gold.read_silver"])
def read_silver_orders(orders_table: str) -> pd.DataFrame:
    return read_silver(orders_table)


@logger.step(loginfo["gold.aggregate"])
def aggregate_daily(df: pd.DataFrame) -> pd.DataFrame:
    return build_orders_daily(df)


@logger.step(loginfo["gold.gate"])
def gate_gold(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    return gate_gold_orders_daily(df)


@logger.step(loginfo["gold.write"])
def publish_to_gold(
    clean: pd.DataFrame, quarantine: pd.DataFrame, table: str, run_id: str,
) -> dict[str, str]:
    paths: dict[str, str] = {}
    if not quarantine.empty:
        paths["quarantine"] = write_quarantine("gold", table, quarantine, run_id)
    write_staging("gold", table, clean, run_id)
    paths["gold"] = promote("gold", table, run_id)
    return paths


def build_orders_daily_pipeline(run_id: str | None = None) -> dict[str, str]:
    run_id = run_id or uuid.uuid4().hex[:8]
    table = settings.orders_daily_table
    silver_df = read_silver_orders(settings.orders_table)
    candidate = aggregate_daily(silver_df)
    clean, quarantine = gate_gold(candidate)
    return publish_to_gold(clean, quarantine, table, run_id)
