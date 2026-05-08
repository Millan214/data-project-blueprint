import uuid

import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame

from medallion_etl.io.readers import read_csv_landing
from medallion_etl.io.writers import promote, write_staging
from medallion_etl.observability import build_loginfo, logger
from medallion_etl.schemas.bronze.orders import BronzeOrders
from medallion_etl.settings import settings
from medallion_etl.transformations.bronze.orders import stamp_ingest_metadata


# Metadata estática + shapers para cada step registrado en el log.
# Las claves del diccionario corresponden al `step_id` de cada paso.
loginfo = build_loginfo({
    "landing.read_csv": {
        "layer": "landing", "layer_order": 0,
        "step_name": "Leyendo CSV", "step_order": 0,
        "description": "Carga el CSV desde la zona de landing y estampa metadata de ingesta en cada fila.",
        "rows_out_fn": len,
        "extra_in_fn": lambda src, **_: {"source_file": src},
    },
    "bronze.write": {
        "layer": "bronze", "layer_order": 1,
        "step_name": "Escribiendo parquet", "step_order": 0,
        "description": "Escribe filas estampadas en bronze _staging y promueve atómicamente la partición del run.",
        "rows_in_fn": lambda df, **_: len(df),
        "extra_out_fn": lambda path: {"output_path": path},
    },
})


# `lazy=True` hace que pandera recopile TODOS los errores de schema en lugar de
# detenerse en el primero. Mejora el diagnóstico cuando varios checks fallan a
# la vez. El costo de validación es el mismo.
@logger.step(loginfo["landing.read_csv"])
@pa.check_types(lazy=True)
def read_and_stamp(src: str) -> DataFrame[BronzeOrders]:
    return stamp_ingest_metadata(read_csv_landing(src), source_file=src)


@logger.step(loginfo["bronze.write"])
def publish_to_bronze(table: str, df: pd.DataFrame, run_id: str) -> str:
    write_staging("bronze", table, df, run_id)
    return promote("bronze", table, run_id)


def ingest_orders(run_id: str | None = None) -> str:

    run_id = run_id or uuid.uuid4().hex[:8]
    src = settings.orders_landing_file
    table_name = settings.orders_table

    # Lectura
    raw = read_and_stamp(src)

    # Escritura
    return publish_to_bronze(table_name, raw, run_id)
