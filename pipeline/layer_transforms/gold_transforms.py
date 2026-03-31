"""
Gold layer transforms — analysis-ready aggregations.

Use Cases layer: pure functions, DataFrame in → DataFrame out.
Each function produces a single analytical dataset.
"""
import pandas as pd
import numpy as np

from pipeline.entities.columns import GoldCols as G
from pipeline.config.config import (
    NUMERIC_COLUMNS_FOR_CORRELATION,
    CORRELATION_MIN_THRESHOLD,
    LIFESTYLE_FACTORS,
    RISK_PROFILE_FEATURES,
)


# ---------------------------------------------------------------------------
# Gold 1: Correlation matrix (all numeric pairs)
# ---------------------------------------------------------------------------

def build_correlation_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Compute pairwise Pearson correlations for all numeric sleep/health columns."""
    # Input contract — select only the columns we need
    numeric_df = df[NUMERIC_COLUMNS_FOR_CORRELATION]

    corr = numeric_df.corr()

    # Melt into long format for easy filtering and visualization
    rows = []
    for i, col1 in enumerate(corr.columns):
        for j, col2 in enumerate(corr.columns):
            if i < j:  # upper triangle only, skip self-correlation
                r = corr.loc[col1, col2]
                rows.append({
                    G.VARIABLE_1: col1,
                    G.VARIABLE_2: col2,
                    G.CORRELATION: round(r, 4),
                    G.ABS_CORRELATION: round(abs(r), 4),
                })

    result = pd.DataFrame(rows)

    # Filter out noise — keep only correlations above threshold
    result = result.loc[result[G.ABS_CORRELATION] >= CORRELATION_MIN_THRESHOLD]

    # Output contract
    return result[
        [G.VARIABLE_1, G.VARIABLE_2, G.CORRELATION, G.ABS_CORRELATION]
    ].sort_values(G.ABS_CORRELATION, ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Gold 2: Lifestyle impact on sleep quality
# ---------------------------------------------------------------------------

def build_lifestyle_impact(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate sleep outcomes by lifestyle factor levels."""
    all_results = []

    for factor in LIFESTYLE_FACTORS:
        if factor not in df.columns:
            continue

        grouped = (
            df
            .groupby(factor, observed=True)
            .agg(
                avg_sleep_quality=("sleep_quality_score", "mean"),
                avg_sleep_duration=("sleep_duration_hrs", "mean"),
                avg_cognitive_score=("cognitive_performance_score", "mean"),
                sample_count=("person_id", "count"),
            )
            .reset_index()
            .rename(columns={factor: G.FACTOR_LEVEL})
        )
        grouped[G.LIFESTYLE_FACTOR] = factor
        grouped[G.FACTOR_LEVEL] = grouped[G.FACTOR_LEVEL].astype(str)
        all_results.append(grouped)

    result = pd.concat(all_results, ignore_index=True)

    # Round for readability
    for col in [G.AVG_SLEEP_QUALITY, G.AVG_SLEEP_DURATION, G.AVG_COGNITIVE_SCORE]:
        result[col] = result[col].round(2)

    # Output contract
    return result[
        [G.LIFESTYLE_FACTOR, G.FACTOR_LEVEL, G.AVG_SLEEP_QUALITY,
         G.AVG_SLEEP_DURATION, G.AVG_COGNITIVE_SCORE, G.SAMPLE_COUNT]
    ]


# ---------------------------------------------------------------------------
# Gold 3: Sleep disorder risk profiling
# ---------------------------------------------------------------------------

def build_risk_profiles(df: pd.DataFrame) -> pd.DataFrame:
    """Compare feature distributions across sleep disorder risk levels."""
    rows = []
    for risk_level in ["Healthy", "Mild", "Moderate", "Severe"]:
        subset = df.loc[df["sleep_disorder_risk"] == risk_level]
        for feature in RISK_PROFILE_FEATURES:
            rows.append({
                G.RISK_LEVEL: risk_level,
                G.FEATURE: feature,
                G.MEAN_VALUE: round(subset[feature].mean(), 2),
                G.STD_VALUE: round(subset[feature].std(), 2),
                G.SAMPLE_COUNT: len(subset),
            })

    result = pd.DataFrame(rows)

    # Output contract
    return result[
        [G.RISK_LEVEL, G.FEATURE, G.MEAN_VALUE, G.STD_VALUE, G.SAMPLE_COUNT]
    ]


# ---------------------------------------------------------------------------
# Composition — chain all Gold transforms
# ---------------------------------------------------------------------------

def apply_gold_transforms(
    df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Chain all Gold-layer transforms and return named analytical datasets."""
    return {
        "gold_correlation_matrix": build_correlation_matrix(df),
        "gold_lifestyle_impact": build_lifestyle_impact(df),
        "gold_risk_profiles": build_risk_profiles(df),
    }
