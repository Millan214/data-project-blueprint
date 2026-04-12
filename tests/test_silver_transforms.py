"""Tests for Silver layer transforms."""
import pandas as pd

from pipeline.schemas.columns import BronzeCols as BC, SilverCols as SC
from pipeline.transforms.silver import (
    validate_records,
    deduplicate,
    add_bmi_category,
    add_age_group,
    add_sleep_efficiency,
    add_caffeine_category,
    apply_silver_transforms,
    split_valid_and_quarantine,
)


class TestValidateRecords:
    def test_valid_data_not_quarantined(self, bronze_df):
        result = validate_records(bronze_df)
        assert not result[SC.IS_QUARANTINED].any()

    def test_out_of_range_age_flagged(self, make_raw_row, bronze_df):
        row = make_raw_row(person_id=99, age=200)
        df = pd.concat([bronze_df, pd.DataFrame([row])], ignore_index=True)
        # Need metadata columns for validate_records
        from pipeline.transforms.bronze import apply_bronze_transforms
        df = apply_bronze_transforms(df, source_file="test.csv")
        result = validate_records(df)
        flagged = result[result[SC.IS_QUARANTINED]]
        assert len(flagged) == 1
        assert "age out of range" in flagged.iloc[0][SC.QUARANTINE_REASON]

    def test_invalid_gender_flagged(self, make_raw_row, bronze_df):
        row = make_raw_row(person_id=99, gender="InvalidGender")
        df = pd.concat([bronze_df, pd.DataFrame([row])], ignore_index=True)
        from pipeline.transforms.bronze import apply_bronze_transforms
        df = apply_bronze_transforms(df, source_file="test.csv")
        result = validate_records(df)
        flagged = result[result[SC.IS_QUARANTINED]]
        assert len(flagged) == 1
        assert "invalid value" in flagged.iloc[0][SC.QUARANTINE_REASON]

    def test_multiple_failures_accumulate_reasons(self, make_raw_row):
        row = make_raw_row(person_id=99, age=200, gender="Bad")
        df = pd.DataFrame([row])
        from pipeline.transforms.bronze import apply_bronze_transforms
        df = apply_bronze_transforms(df, source_file="test.csv")
        result = validate_records(df)
        reason = result.iloc[0][SC.QUARANTINE_REASON]
        assert "age out of range" in reason
        assert "invalid value" in reason

    def test_does_not_drop_rows(self, make_raw_row):
        row = make_raw_row(person_id=99, age=200)
        df = pd.DataFrame([row])
        from pipeline.transforms.bronze import apply_bronze_transforms
        df = apply_bronze_transforms(df, source_file="test.csv")
        result = validate_records(df)
        assert len(result) == 1


class TestDeduplicate:
    def test_removes_duplicates_keeps_last(self, make_raw_row):
        rows = [
            make_raw_row(person_id=1, age=25),
            make_raw_row(person_id=1, age=30),
        ]
        df = pd.DataFrame(rows)
        from pipeline.transforms.bronze import apply_bronze_transforms
        df = apply_bronze_transforms(df, source_file="test.csv")
        result = deduplicate(df)
        assert len(result) == 1
        assert result.iloc[0][BC.AGE] == 30

    def test_unique_ids_unchanged(self, bronze_df):
        result = deduplicate(bronze_df)
        assert len(result) == len(bronze_df)


class TestEnrichment:
    def test_bmi_normal(self, bronze_df):
        result = add_bmi_category(bronze_df)
        row = result[result[BC.BMI] == 21.0].iloc[0]
        assert row[SC.BMI_CATEGORY] == "Normal"

    def test_bmi_obese(self, make_raw_row):
        df = pd.DataFrame([make_raw_row(bmi=31.0)])
        from pipeline.transforms.bronze import apply_bronze_transforms
        df = apply_bronze_transforms(df, source_file="test.csv")
        result = add_bmi_category(df)
        assert result.iloc[0][SC.BMI_CATEGORY] == "Obese"

    def test_age_group_25_34(self, bronze_df):
        result = add_age_group(bronze_df)
        row = result[result[BC.AGE] == 35].iloc[0]
        assert row[SC.AGE_GROUP] == "35-44"

    def test_sleep_efficiency_sum(self, bronze_df):
        result = add_sleep_efficiency(bronze_df)
        row = result.iloc[0]
        expected = row[BC.REM_PERCENTAGE] + row[BC.DEEP_SLEEP_PERCENTAGE]
        assert row[SC.SLEEP_EFFICIENCY] == expected

    def test_caffeine_category(self, bronze_df):
        result = add_caffeine_category(bronze_df)
        # 50mg caffeine → "Low" (0-100 range)
        row = result[result[BC.CAFFEINE_MG] == 50].iloc[0]
        assert row[SC.CAFFEINE_CATEGORY] == "Low"


class TestSplitValidAndQuarantine:
    def test_all_valid_goes_to_valid(self, silver_df):
        valid, quarantine = split_valid_and_quarantine(silver_df)
        assert len(valid) == len(silver_df)
        assert len(quarantine) == 0

    def test_quarantine_columns_removed_from_valid(self, silver_df):
        valid, _ = split_valid_and_quarantine(silver_df)
        assert SC.IS_QUARANTINED not in valid.columns
        assert SC.QUARANTINE_REASON not in valid.columns

    def test_quarantine_keeps_reason(self, make_raw_row):
        rows = [
            make_raw_row(person_id=1),
            make_raw_row(person_id=2, age=200),  # invalid
        ]
        df = pd.DataFrame(rows)
        from pipeline.transforms.bronze import apply_bronze_transforms
        df = apply_bronze_transforms(df, source_file="test.csv")
        silver = apply_silver_transforms(df)
        valid, quarantine = split_valid_and_quarantine(silver)
        assert len(valid) == 1
        assert len(quarantine) == 1
        assert quarantine.iloc[0][SC.QUARANTINE_REASON] != ""
