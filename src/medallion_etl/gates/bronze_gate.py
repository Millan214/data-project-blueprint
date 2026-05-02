import pandas as pd

from medallion_etl.gates.base import split_clean_quarantine
from medallion_etl.schemas.bronze.orders import BronzeOrders


def gate_bronze_orders(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    return split_clean_quarantine(df, BronzeOrders)
