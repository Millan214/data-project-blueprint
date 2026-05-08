import uuid

import pandas as pd

from medallion_etl.gates.silver_gate import gate_silver_orders
from medallion_etl.io.readers import read_bronze
from medallion_etl.io.writers import promote, write_quarantine, write_staging
from medallion_etl.observability import build_loginfo, logger
from medallion_etl.settings import settings
from medallion_etl.transformations.silver.orders import build_silver_orders


loginfo = build_loginfo({
    "silver.read_bronze": {
        "layer": "silver", "layer_order": 2,
        "step_name": "Reading bronze", "step_order": 0,
        "description": "Reads every parquet file under the bronze orders table into a single DataFrame.",
        "rows_out_fn": len,
    },
    "silver.transform": {
        "layer": "silver", "layer_order": 2,
        "step_name": "Cleansing & deduping", "step_order": 1,
        "description": "Casts types to UTC, deduplicates by primary key, and normalizes currency casing.",
        "rows_in_fn": lambda df, **_: len(df),
        "rows_out_fn": len,
    },
    "silver.gate": {
        "layer": "silver", "layer_order": 2,
        "step_name": "Validating (Pandera gate)", "step_order": 2,
        "description": "Validates rows against SilverOrders; rows that fail are routed to quarantine.",
        "rows_in_fn": lambda df, **_: len(df),
        "rows_out_fn": lambda result: len(result[0]),
        "extra_out_fn": lambda result: {"quarantined": int(len(result[1]))},
    },
    "silver.write": {
        "layer": "silver", "layer_order": 2,
        "step_name": "Writing parquet", "step_order": 3,
        "description": "Writes clean rows to silver and any quarantined rows alongside, then promotes.",
        "rows_in_fn": lambda clean, **_: len(clean),
        "extra_out_fn": lambda paths: {**paths},
    },
})


@logger.step(loginfo["silver.read_bronze"])
def read_bronze_orders(table: str) -> pd.DataFrame:
    return read_bronze(table)


@logger.step(loginfo["silver.transform"])
def cleanse_and_dedupe(df: pd.DataFrame) -> pd.DataFrame:
    return build_silver_orders(df)


@logger.step(loginfo["silver.gate"])
def gate_silver(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    return gate_silver_orders(df)


@logger.step(loginfo["silver.write"])
def publish_to_silver(
    clean: pd.DataFrame, quarantine: pd.DataFrame, table: str, run_id: str,
) -> dict[str, str]:
    paths: dict[str, str] = {}
    if not quarantine.empty:
        paths["quarantine"] = write_quarantine("silver", table, quarantine, run_id)
    write_staging("silver", table, clean, run_id)
    paths["silver"] = promote("silver", table, run_id)
    return paths


def build_orders(run_id: str | None = None) -> dict[str, str]:
    run_id = run_id or uuid.uuid4().hex[:8]
    table = settings.orders_table
    bronze_df = read_bronze_orders(table)
    candidate = cleanse_and_dedupe(bronze_df)
    clean, quarantine = gate_silver(candidate)
    return publish_to_silver(clean, quarantine, table, run_id)
