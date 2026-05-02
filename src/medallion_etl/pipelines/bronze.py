import uuid

from medallion_etl.io.readers import read_csv_landing
from medallion_etl.io.writers import promote, write_staging
from medallion_etl.settings import settings
from medallion_etl.transformations.bronze.orders import stamp_ingest_metadata


def ingest_orders(run_id: str | None = None) -> str:
    run_id = run_id or uuid.uuid4().hex[:8]
    raw = read_csv_landing(settings.orders_landing_file)
    raw = stamp_ingest_metadata(raw, source_file=settings.orders_landing_file)
    write_staging("bronze", settings.orders_table, raw, run_id)
    return promote("bronze", settings.orders_table, run_id)
