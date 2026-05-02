from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import TypeVar

import pandas as pd

from medallion_etl.observability.logging import get_logger

F = TypeVar("F", bound=Callable[..., pd.DataFrame])


def log_shape(name: str | None = None) -> Callable[[F], F]:
    """Log dataframe shape before and after a transform."""

    def decorator(func: F) -> F:
        log = get_logger(name or func.__module__)

        @wraps(func)
        def wrapper(df: pd.DataFrame, *args: object, **kwargs: object) -> pd.DataFrame:
            log.info("transform_start", func=func.__name__, rows_in=len(df), cols_in=df.shape[1])
            out = func(df, *args, **kwargs)
            log.info("transform_end", func=func.__name__, rows_out=len(out), cols_out=out.shape[1])
            return out

        return wrapper  # type: ignore[return-value]

    return decorator
