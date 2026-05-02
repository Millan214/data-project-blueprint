import pandas as pd

from medallion_etl.gates.silver_gate import gate_silver_orders


def _candidate_silver() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "order_id": 1,
                "customer_id": "C001",
                "order_ts": pd.Timestamp("2026-01-01T10:00:00Z"),
                "total_amount": 10.0,
                "currency": "EUR",
            },
            {
                "order_id": 2,
                "customer_id": "C002",
                "order_ts": pd.Timestamp("2026-01-02T11:00:00Z"),
                "total_amount": -5.0,
                "currency": "EUR",
            },
        ]
    )


def test_gate_routes_negative_amount_to_quarantine():
    clean, quarantine = gate_silver_orders(_candidate_silver())
    assert len(clean) == 1
    assert clean["order_id"].iloc[0] == 1
    assert len(quarantine) == 1
    assert quarantine["order_id"].iloc[0] == 2
    assert "_violations" in quarantine.columns
    assert "_quarantined_at" in quarantine.columns


def test_gate_passes_through_clean_data():
    df = _candidate_silver().iloc[[0]]
    clean, quarantine = gate_silver_orders(df)
    assert len(clean) == 1
    assert quarantine.empty
