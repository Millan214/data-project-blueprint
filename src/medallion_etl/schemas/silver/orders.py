import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series


class SilverOrders(pa.DataFrameModel):
    """Cleansed, typed, business-rule-validated orders."""

    order_id: Series[int] = pa.Field(unique=True, ge=1)
    customer_id: Series[str] = pa.Field(str_matches=r"^C\d{3,}$")
    order_ts: Series[pd.DatetimeTZDtype] = pa.Field(dtype_kwargs={"tz": "UTC"})
    total_amount: Series[float] = pa.Field(ge=0)
    currency: Series[str] = pa.Field(isin=["EUR", "USD", "GBP"])

    class Config:
        strict = False
        coerce = True
