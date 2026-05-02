import uuid

from medallion_etl.io.readers import read_csv_landing
from medallion_etl.io.writers import promote, write_staging
from medallion_etl.observability import logger
from medallion_etl.settings import settings
from medallion_etl.transformations.bronze.orders import stamp_ingest_metadata


def ingest_orders(run_id: str | None = None) -> str:
    run_id = run_id or uuid.uuid4().hex[:8]

    with logger.step(
        layer="landing", layer_order=0,
        step_id="landing.read_csv", step_name="Reading CSV", step_order=0,
        description="Loads the configured CSV from the landing zone into an in-memory DataFrame.",
    ) as step:
        raw = read_csv_landing(settings.orders_landing_file)
        step.rows_out(len(raw))
        step.add_extra(source_file=settings.orders_landing_file)

    with logger.step(
        layer="bronze", layer_order=1,
        step_id="bronze.stamp_metadata", step_name="Stamping ingest metadata", step_order=0,
        description="Adds _ingested_at and _source_file columns so bronze rows carry provenance.",
        rows_in=len(raw),
    ) as step:
        raw = stamp_ingest_metadata(raw, source_file=settings.orders_landing_file)
        step.rows_out(len(raw))

    with logger.step(
        layer="bronze", layer_order=1,
        step_id="bronze.write", step_name="Writing parquet", step_order=1,
        description="Writes raw rows to bronze _staging then atomically promotes the run partition.",
        rows_in=len(raw),
    ) as step:
        write_staging("bronze", settings.orders_table, raw, run_id)
        path = promote("bronze", settings.orders_table, run_id)
        step.add_extra(output_path=path, run_id=run_id)

    return path
