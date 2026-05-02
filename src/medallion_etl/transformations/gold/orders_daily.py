from __future__ import annotations

import pandas as pd


def aggregate_daily(silver: pd.DataFrame) -> pd.DataFrame:
    out = (
        silver.assign(order_date=silver["order_ts"].dt.floor("D"))
        .groupby(["order_date", "currency"], as_index=False)
        .agg(n_orders=("order_id", "count"), total_revenue=("total_amount", "sum"))
    )
    return out


def build_orders_daily(silver: pd.DataFrame) -> pd.DataFrame:
    return silver.pipe(aggregate_daily)
