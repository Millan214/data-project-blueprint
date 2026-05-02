import uuid

from medallion_etl.gates.gold_gate import gate_gold_orders_daily
from medallion_etl.io.readers import read_silver
from medallion_etl.io.writers import promote, write_quarantine, write_staging
from medallion_etl.settings import settings
from medallion_etl.transformations.gold.orders_daily import build_orders_daily


def build_orders_daily_pipeline(run_id: str | None = None) -> dict[str, str]:
    run_id = run_id or uuid.uuid4().hex[:8]
    table = settings.orders_daily_table
    silver_df = read_silver(settings.orders_table)
    candidate = build_orders_daily(silver_df)
    clean, quarantine = gate_gold_orders_daily(candidate)

    paths: dict[str, str] = {}
    if not quarantine.empty:
        paths["quarantine"] = write_quarantine("gold", table, quarantine, run_id)
    write_staging("gold", table, clean, run_id)
    paths["gold"] = promote("gold", table, run_id)
    return paths
