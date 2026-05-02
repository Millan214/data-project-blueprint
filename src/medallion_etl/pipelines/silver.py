import uuid

from medallion_etl.gates.silver_gate import gate_silver_orders
from medallion_etl.io.readers import read_bronze
from medallion_etl.io.writers import promote, write_quarantine, write_staging
from medallion_etl.observability import logger
from medallion_etl.settings import settings
from medallion_etl.transformations.silver.orders import build_silver_orders


def build_orders(run_id: str | None = None) -> dict[str, str]:
    run_id = run_id or uuid.uuid4().hex[:8]
    table = settings.orders_table

    with logger.step(
        layer="silver", layer_order=2,
        step_id="silver.read_bronze", step_name="Reading bronze", step_order=0,
        description="Reads every parquet file under the bronze orders table into a single DataFrame.",
    ) as step:
        bronze_df = read_bronze(table)
        step.rows_out(len(bronze_df))

    with logger.step(
        layer="silver", layer_order=2,
        step_id="silver.transform", step_name="Cleansing & deduping", step_order=1,
        description="Casts types to UTC, deduplicates by primary key, and normalizes currency casing.",
        rows_in=len(bronze_df),
    ) as step:
        candidate = build_silver_orders(bronze_df)
        step.rows_out(len(candidate))

    with logger.step(
        layer="silver", layer_order=2,
        step_id="silver.gate", step_name="Validating (Pandera gate)", step_order=2,
        description="Validates rows against SilverOrders; rows that fail are routed to quarantine.",
        rows_in=len(candidate),
    ) as step:
        clean, quarantine = gate_silver_orders(candidate)
        step.rows_out(len(clean))
        step.add_extra(quarantined=int(len(quarantine)))

    paths: dict[str, str] = {}
    with logger.step(
        layer="silver", layer_order=2,
        step_id="silver.write", step_name="Writing parquet", step_order=3,
        description="Writes clean rows to silver and any quarantined rows alongside, then promotes.",
        rows_in=len(clean),
    ) as step:
        if not quarantine.empty:
            paths["quarantine"] = write_quarantine("silver", table, quarantine, run_id)
        write_staging("silver", table, clean, run_id)
        paths["silver"] = promote("silver", table, run_id)
        step.add_extra(**paths, run_id=run_id)

    return paths
