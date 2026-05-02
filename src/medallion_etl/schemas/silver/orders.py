import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series

from medallion_etl.settings import settings


class SilverOrders(pa.DataFrameModel):
    """Cleansed, typed, business-rule-validated orders.

    Validation rules pull from `settings` so they can be tuned via env vars
    without touching this file.
    """

    order_id: Series[int] = pa.Field(unique=True, ge=1)
    customer_id: Series[str] = pa.Field(str_matches=settings.customer_id_pattern)
    order_ts: Series[pd.DatetimeTZDtype] = pa.Field(dtype_kwargs={"tz": "UTC"})
    total_amount: Series[float] = pa.Field(ge=0)
    currency: Series[str] = pa.Field(isin=list(settings.allowed_currencies))

    class Config:
        strict = False
        coerce = True
