import uuid

import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame

from medallion_etl.gates.gold_gate import gate_gold_orders_daily
from medallion_etl.io.readers import read_silver
from medallion_etl.io.writers import promote, write_quarantine, write_staging
from medallion_etl.observability import build_loginfo, logger
from medallion_etl.schemas.silver.orders import SilverOrders
from medallion_etl.settings import settings
from medallion_etl.transformations.gold.orders_daily import build_orders_daily


# Metadata estática + shapers para cada step registrado en el log.
# Las claves del diccionario corresponden al `step_id` de cada paso.
loginfo = build_loginfo({
    "gold.read_silver": {
        "layer": "gold", "layer_order": 3,
        "step_name": "Leyendo silver", "step_order": 0,
        "description": "Lee todos los archivos parquet de la tabla silver de orders en un único DataFrame.",
        "rows_out_fn": len,
    },
    "gold.aggregate": {
        "layer": "gold", "layer_order": 3,
        "step_name": "Agregando diario", "step_order": 1,
        "description": "Agrupa orders por (order_date, currency) y suma totales en una tabla de hechos diaria.",
        "rows_in_fn": lambda df, **_: len(df),
        "rows_out_fn": len,
    },
    "gold.gate": {
        "layer": "gold", "layer_order": 3,
        "step_name": "Validación (gate Pandera)", "step_order": 2,
        "description": "Valida filas agregadas contra GoldOrdersDaily; las que fallan se enrutan a cuarentena.",
        "rows_in_fn": lambda df, **_: len(df),
        "rows_out_fn": lambda result: len(result[0]),
        "extra_out_fn": lambda result: {"quarantined": int(len(result[1]))},
    },
    "gold.write": {
        "layer": "gold", "layer_order": 3,
        "step_name": "Escribiendo parquet", "step_order": 3,
        "description": "Escribe el agregado diario en gold _staging y luego lo promueve atómicamente.",
        "rows_in_fn": lambda clean, **_: len(clean),
        "extra_out_fn": lambda paths: {**paths},
    },
})


# `lazy=True` hace que pandera recopile TODOS los errores de schema en lugar de
# detenerse en el primero. Mejora el diagnóstico cuando varios checks fallan a
# la vez. El costo de validación es el mismo.
@logger.step(loginfo["gold.read_silver"])
@pa.check_types(lazy=True)
def read_silver_orders(orders_table: str) -> DataFrame[SilverOrders]:
    return read_silver(orders_table)


@logger.step(loginfo["gold.aggregate"])
def aggregate_daily(df: pd.DataFrame) -> pd.DataFrame:
    return build_orders_daily(df)


@logger.step(loginfo["gold.gate"])
def gate_gold(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    return gate_gold_orders_daily(df)


@logger.step(loginfo["gold.write"])
def publish_to_gold(
    clean: pd.DataFrame, quarantine: pd.DataFrame, table: str, run_id: str,
) -> dict[str, str]:
    paths: dict[str, str] = {}
    if not quarantine.empty:
        paths["quarantine"] = write_quarantine("gold", table, quarantine, run_id)
    write_staging("gold", table, clean, run_id)
    paths["gold"] = promote("gold", table, run_id)
    return paths


def build_orders_daily_pipeline(run_id: str | None = None) -> dict[str, str]:
    run_id = run_id or uuid.uuid4().hex[:8]
    table_name = settings.orders_daily_table

    # Lectura
    silver_df = read_silver_orders(settings.orders_table)

    # Transformación
    candidate = aggregate_daily(silver_df)

    # Validación
    clean, quarantine = gate_gold(candidate)
    logger.snapshot_schema("gold", clean)

    # Escritura
    return publish_to_gold(clean, quarantine, table_name, run_id)
