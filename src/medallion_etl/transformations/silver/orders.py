from __future__ import annotations

import pandas as pd

from medallion_etl.observability import logger
from medallion_etl.settings import settings


@logger.pipe(name="Cast types", description="Convierte `order_ts` a datetime UTC.")
def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["order_ts"] = pd.to_datetime(out["order_ts"], utc=True)
    return out


@logger.pipe(
    name="Dedupe",
    description="Elimina duplicados por clave primaria, conservando la última fila.",
)
def dedupe(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset=settings.orders_dedupe_keys, keep="last").reset_index(drop=True)


@logger.pipe(
    name="Normalize currency",
    description="Normaliza el casing y los espacios del código de moneda.",
)
def normalize_currency(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(currency=df["currency"].str.upper().str.strip())


def build_silver_orders(bronze: pd.DataFrame) -> pd.DataFrame:
    """Functional core: a single chain of pure transformations."""
    return bronze.pipe(cast_types).pipe(dedupe).pipe(normalize_currency)
