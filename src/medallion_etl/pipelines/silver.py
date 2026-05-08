import uuid

import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame

from medallion_etl.gates.silver_gate import gate_silver_orders
from medallion_etl.io.readers import read_bronze
from medallion_etl.io.writers import promote, write_quarantine, write_staging
from medallion_etl.observability import build_loginfo, logger
from medallion_etl.schemas.bronze.orders import BronzeOrders
from medallion_etl.settings import settings
from medallion_etl.transformations.silver.orders import build_silver_orders


# Metadata estática + shapers para cada step registrado en el log.
# Las claves del diccionario corresponden al `step_id` de cada paso.
loginfo = build_loginfo({
    "silver.read_bronze": {
        "layer": "silver", "layer_order": 2,
        "step_name": "Leyendo bronze", "step_order": 0,
        "description": "Lee todos los archivos parquet de la tabla bronze de orders en un único DataFrame.",
        "rows_out_fn": len,
    },
    "silver.transform": {
        "layer": "silver", "layer_order": 2,
        "step_name": "Limpieza y deduplicación", "step_order": 1,
        "description": "Convierte tipos a UTC, deduplica por clave primaria y normaliza el casing de currency.",
        "rows_in_fn": lambda df, **_: len(df),
        "rows_out_fn": len,
    },
    "silver.gate": {
        "layer": "silver", "layer_order": 2,
        "step_name": "Validación (gate Pandera)", "step_order": 2,
        "description": "Valida filas contra SilverOrders; las que fallan se enrutan a cuarentena.",
        "rows_in_fn": lambda df, **_: len(df),
        "rows_out_fn": lambda result: len(result[0]),
        "extra_out_fn": lambda result: {"quarantined": int(len(result[1]))},
    },
    "silver.write": {
        "layer": "silver", "layer_order": 2,
        "step_name": "Escribiendo parquet", "step_order": 3,
        "description": "Escribe filas limpias en silver y, si hay, las filas en cuarentena, luego promueve.",
        "rows_in_fn": lambda clean, **_: len(clean),
        "extra_out_fn": lambda paths: {**paths},
    },
})


# `lazy=True` hace que pandera recopile TODOS los errores de schema en lugar de
# detenerse en el primero. Mejora el diagnóstico cuando varios checks fallan a
# la vez. El costo de validación es el mismo.
@logger.step(loginfo["silver.read_bronze"])
@pa.check_types(lazy=True)
def read_bronze_orders(table: str) -> DataFrame[BronzeOrders]:
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
    table_name = settings.orders_table

    # Lectura
    bronze_df = read_bronze_orders(table_name)

    # Transformación
    candidate = cleanse_and_dedupe(bronze_df)

    # Validación
    clean, quarantine = gate_silver(candidate)

    # Escritura
    return publish_to_silver(clean, quarantine, table_name, run_id)
