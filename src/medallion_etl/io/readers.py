from pathlib import Path

import pandas as pd

from medallion_etl.io.catalog import uri_for
from medallion_etl.settings import settings


def read_bronze(table: str) -> pd.DataFrame:
    """Lee TODAS las particiones de bronze acumuladas como un único DataFrame.

    Bronze es un archivo append-only (preserva todas las ingestas históricas);
    silver se encarga de deduplicar por clave primaria al consumirlo.
    """
    root = Path(uri_for("bronze", table).removeprefix("file://"))
    files = sorted(root.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def read_silver(table: str) -> pd.DataFrame:
    """Lee solamente la última partición run_id.

    Silver es el "estado actual" canónico de la tabla — cada run escribe un
    snapshot fresco. Leer todas las particiones históricas devolvería claves
    primarias duplicadas y rompería invariantes downstream (uniqueness, etc).
    """
    root = Path(uri_for("silver", table).removeprefix("file://"))
    if not root.exists():
        return pd.DataFrame()
    partitions = [p for p in root.iterdir() if p.is_dir() and p.name.startswith("run_id=")]
    if not partitions:
        return pd.DataFrame()
    # Selecciona la partición más recientemente modificada según el filesystem.
    latest = max(partitions, key=lambda p: p.stat().st_mtime)
    files = sorted(latest.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def read_csv_landing(filename: str) -> pd.DataFrame:
    """Lee un CSV desde la zona de landing configurada en settings."""
    root = Path(settings.storage_root.removeprefix("file://")) / settings.landing_dir
    return pd.read_csv(root / filename)
