import pandas as pd

from medallion_etl.transformations.silver.orders import (
    build_silver_orders,
    cast_types,
    dedupe,
    normalize_currency,
)


def _bronze_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "order_id": 1,
                "customer_id": "C001",
                "order_ts": "2026-01-01T10:00:00Z",
                "total_amount": 10.0,
                "currency": " eur ",
            },
            {
                "order_id": 1,
                "customer_id": "C001",
                "order_ts": "2026-01-01T10:00:00Z",
                "total_amount": 10.0,
                "currency": "eur",
            },
            {
                "order_id": 2,
                "customer_id": "C002",
                "order_ts": "2026-01-02T11:00:00Z",
                "total_amount": 25.0,
                "currency": "USD",
            },
        ]
    )


def test_cast_types_makes_order_ts_utc():
    out = cast_types(_bronze_frame())
    assert str(out["order_ts"].dtype).startswith("datetime64[") and "UTC" in str(out["order_ts"].dtype)


def test_dedupe_keeps_last():
    out = dedupe(_bronze_frame())
    assert len(out) == 2
    assert sorted(out["order_id"].tolist()) == [1, 2]


def test_normalize_currency_uppercases_and_strips():
    out = normalize_currency(_bronze_frame())
    assert set(out["currency"]) == {"EUR", "USD"}


def test_build_silver_is_pure_chain():
    out = build_silver_orders(_bronze_frame())
    assert len(out) == 2
    assert str(out["order_ts"].dtype).startswith("datetime64[") and "UTC" in str(out["order_ts"].dtype)
    assert set(out["currency"]) == {"EUR", "USD"}
