import pandas as pd

from medallion_etl.gates.base import split_clean_quarantine
from medallion_etl.schemas.gold.orders_daily import GoldOrdersDaily


def gate_gold_orders_daily(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    return split_clean_quarantine(df, GoldOrdersDaily)
