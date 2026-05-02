from __future__ import annotations

import pandas as pd


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["order_ts"] = pd.to_datetime(out["order_ts"], utc=True)
    return out


def dedupe(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset=["order_id"], keep="last").reset_index(drop=True)


def normalize_currency(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(currency=df["currency"].str.upper().str.strip())


def build_silver_orders(bronze: pd.DataFrame) -> pd.DataFrame:
    """Functional core: a single chain of pure transformations."""
    return bronze.pipe(cast_types).pipe(dedupe).pipe(normalize_currency)
