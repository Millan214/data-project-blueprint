from __future__ import annotations

import pandas as pd
import pandera.pandas as pa


def split_clean_quarantine(
    df: pd.DataFrame,
    schema: type[pa.DataFrameModel],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate `df` against `schema`. Return (clean, quarantine).

    Clean rows pass schema. Quarantined rows carry forward original data
    plus a ``_violations`` column describing why they failed and a
    ``_quarantined_at`` timestamp.
    """
    try:
        clean = schema.validate(df, lazy=True)
        empty_quarantine = df.iloc[0:0].assign(
            _violations=pd.Series(dtype="object"),
            _quarantined_at=pd.Series(dtype="datetime64[ns, UTC]"),
        )
        return clean, empty_quarantine
    except pa.errors.SchemaErrors as exc:
        failure_cases = exc.failure_cases.dropna(subset=["index"])
        bad_idx = failure_cases["index"].astype(int).unique().tolist()

        violations_by_idx: dict[int, list[dict[str, object]]] = {}
        for _, row in failure_cases.iterrows():
            i = int(row["index"])
            violations_by_idx.setdefault(i, []).append(
                {
                    "check": row.get("check"),
                    "column": row.get("column"),
                    "failure_case": row.get("failure_case"),
                }
            )

        quarantine = df.loc[bad_idx].copy()
        quarantine["_violations"] = [violations_by_idx[i] for i in bad_idx]
        quarantine["_quarantined_at"] = pd.Timestamp.now("UTC")

        clean_df = df.drop(index=bad_idx)
        return schema.validate(clean_df, lazy=False), quarantine
