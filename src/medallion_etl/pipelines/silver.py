import uuid

from medallion_etl.gates.silver_gate import gate_silver_orders
from medallion_etl.io.readers import read_bronze
from medallion_etl.io.writers import promote, write_quarantine, write_staging
from medallion_etl.transformations.silver.orders import build_silver_orders


def build_orders(run_id: str | None = None) -> dict[str, str]:
    run_id = run_id or uuid.uuid4().hex[:8]
    bronze_df = read_bronze("orders")
    candidate = build_silver_orders(bronze_df)
    clean, quarantine = gate_silver_orders(candidate)

    paths: dict[str, str] = {}
    if not quarantine.empty:
        paths["quarantine"] = write_quarantine("silver", "orders", quarantine, run_id)
    write_staging("silver", "orders", clean, run_id)
    paths["silver"] = promote("silver", "orders", run_id)
    return paths
