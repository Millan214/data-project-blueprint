"""Tests for Gold layer transforms."""
import pandas as pd

from pipeline.entities.columns import GoldCols as G
from pipeline.layer_transforms.gold_transforms import (
    build_correlation_matrix,
    build_lifestyle_impact,
    build_risk_profiles,
    apply_gold_transforms,
)


class TestBuildCorrelationMatrix:
    def test_output_columns(self, valid_and_quarantine):
        valid_df, _ = valid_and_quarantine
        result = build_correlation_matrix(valid_df)
        assert list(result.columns) == [
            G.VARIABLE_1, G.VARIABLE_2, G.CORRELATION, G.ABS_CORRELATION,
        ]

    def test_no_self_correlations(self, valid_and_quarantine):
        valid_df, _ = valid_and_quarantine
        result = build_correlation_matrix(valid_df)
        same = result[result[G.VARIABLE_1] == result[G.VARIABLE_2]]
        assert len(same) == 0

    def test_correlations_bounded(self, valid_and_quarantine):
        valid_df, _ = valid_and_quarantine
        result = build_correlation_matrix(valid_df)
        assert (result[G.ABS_CORRELATION] <= 1.0).all()
        assert (result[G.ABS_CORRELATION] >= 0.0).all()


class TestBuildLifestyleImpact:
    def test_output_columns(self, valid_and_quarantine):
        valid_df, _ = valid_and_quarantine
        result = build_lifestyle_impact(valid_df)
        expected = [
            G.LIFESTYLE_FACTOR, G.FACTOR_LEVEL, G.AVG_SLEEP_QUALITY,
            G.AVG_SLEEP_DURATION, G.AVG_COGNITIVE_SCORE, G.SAMPLE_COUNT,
        ]
        assert list(result.columns) == expected

    def test_sample_counts_positive(self, valid_and_quarantine):
        valid_df, _ = valid_and_quarantine
        result = build_lifestyle_impact(valid_df)
        assert (result[G.SAMPLE_COUNT] > 0).all()


class TestBuildRiskProfiles:
    def test_output_columns(self, valid_and_quarantine):
        valid_df, _ = valid_and_quarantine
        result = build_risk_profiles(valid_df)
        expected = [G.RISK_LEVEL, G.FEATURE, G.MEAN_VALUE, G.STD_VALUE, G.SAMPLE_COUNT]
        assert list(result.columns) == expected

    def test_all_risk_levels_present(self, valid_and_quarantine):
        valid_df, _ = valid_and_quarantine
        result = build_risk_profiles(valid_df)
        levels = set(result[G.RISK_LEVEL].unique())
        assert levels == {"Healthy", "Mild", "Moderate", "Severe"}


class TestApplyGoldTransforms:
    def test_returns_three_datasets(self, valid_and_quarantine):
        valid_df, _ = valid_and_quarantine
        result = apply_gold_transforms(valid_df)
        assert set(result.keys()) == {
            "gold_correlation_matrix",
            "gold_lifestyle_impact",
            "gold_risk_profiles",
        }

    def test_all_values_are_dataframes(self, valid_and_quarantine):
        valid_df, _ = valid_and_quarantine
        result = apply_gold_transforms(valid_df)
        for name, df in result.items():
            assert isinstance(df, pd.DataFrame), f"{name} is not a DataFrame"
