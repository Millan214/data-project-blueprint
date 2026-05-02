import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series


class GoldOrdersDaily(pa.DataFrameModel):
    order_date: Series[pd.DatetimeTZDtype] = pa.Field(dtype_kwargs={"tz": "UTC"})
    currency: Series[str]
    n_orders: Series[int] = pa.Field(ge=0)
    total_revenue: Series[float] = pa.Field(ge=0)
