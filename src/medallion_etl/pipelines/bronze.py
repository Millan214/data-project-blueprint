import uuid

import pandas as pd

from medallion_etl.io.readers import read_csv_landing
from medallion_etl.io.writers import promote, write_staging
from medallion_etl.observability import build_loginfo, logger
from medallion_etl.settings import settings
from medallion_etl.transformations.bronze.orders import stamp_ingest_metadata


loginfo = build_loginfo({
    "landing.read_csv": {
        "layer": "landing", "layer_order": 0,
        "step_name": "Reading CSV", "step_order": 0,
        "description": "Loads the CSV from the landing zone and stamps ingest metadata onto every row.",
        "rows_out_fn": len,
        "extra_in_fn": lambda src, **_: {"source_file": src},
    },
    "bronze.write": {
        "layer": "bronze", "layer_order": 1,
        "step_name": "Writing parquet", "step_order": 0,
        "description": "Writes stamped rows to bronze _staging then atomically promotes the run partition.",
        "rows_in_fn": lambda df, **_: len(df),
        "extra_out_fn": lambda path: {"output_path": path},
    },
})


@logger.step(loginfo["landing.read_csv"])
def read_and_stamp(src: str) -> pd.DataFrame:
    return stamp_ingest_metadata(read_csv_landing(src), source_file=src)


@logger.step(loginfo["bronze.write"])
def publish_to_bronze(table: str, df: pd.DataFrame, run_id: str) -> str:
    write_staging("bronze", table, df, run_id)
    return promote("bronze", table, run_id)


def ingest_orders(run_id: str | None = None) -> str:

    run_id = run_id or uuid.uuid4().hex[:8]
    src = settings.orders_landing_file
    table = settings.orders_table

    raw = read_and_stamp(src)
    
    return publish_to_bronze(table, raw, run_id)
