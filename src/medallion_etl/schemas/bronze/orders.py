import pandera.pandas as pa
from pandera.typing import Series


class BronzeOrders(pa.DataFrameModel):
    """Raw orders as ingested — minimal constraints, preserve everything."""

    order_id: Series[int]
    customer_id: Series[str]
    order_ts: Series[str]
    total_amount: Series[float]
    currency: Series[str]

    class Config:
        strict = False
        coerce = True
