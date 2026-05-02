import uuid

from medallion_etl.gates.gold_gate import gate_gold_orders_daily
from medallion_etl.io.readers import read_silver
from medallion_etl.io.writers import promote, write_quarantine, write_staging
from medallion_etl.observability import logger
from medallion_etl.settings import settings
from medallion_etl.transformations.gold.orders_daily import build_orders_daily


def build_orders_daily_pipeline(run_id: str | None = None) -> dict[str, str]:
    run_id = run_id or uuid.uuid4().hex[:8]
    table = settings.orders_daily_table

    with logger.step(
        layer="gold", layer_order=3,
        step_id="gold.read_silver", step_name="Reading silver", step_order=0,
        description="Reads every parquet file under the silver orders table into a single DataFrame.",
    ) as step:
        silver_df = read_silver(settings.orders_table)
        step.rows_out(len(silver_df))

    with logger.step(
        layer="gold", layer_order=3,
        step_id="gold.aggregate", step_name="Aggregating daily", step_order=1,
        description="Groups orders by (order_date, currency) and sums totals into a daily fact table.",
        rows_in=len(silver_df),
    ) as step:
        candidate = build_orders_daily(silver_df)
        step.rows_out(len(candidate))

    with logger.step(
        layer="gold", layer_order=3,
        step_id="gold.gate", step_name="Validating (Pandera gate)", step_order=2,
        description="Validates aggregated rows against GoldOrdersDaily; failures route to quarantine.",
        rows_in=len(candidate),
    ) as step:
        clean, quarantine = gate_gold_orders_daily(candidate)
        step.rows_out(len(clean))
        step.add_extra(quarantined=int(len(quarantine)))

    paths: dict[str, str] = {}
    with logger.step(
        layer="gold", layer_order=3,
        step_id="gold.write", step_name="Writing parquet", step_order=3,
        description="Writes the daily aggregate to gold _staging then atomically promotes it.",
        rows_in=len(clean),
    ) as step:
        if not quarantine.empty:
            paths["quarantine"] = write_quarantine("gold", table, quarantine, run_id)
        write_staging("gold", table, clean, run_id)
        paths["gold"] = promote("gold", table, run_id)
        step.add_extra(**paths, run_id=run_id)

    return paths
