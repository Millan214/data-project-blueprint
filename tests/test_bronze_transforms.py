"""Tests for Bronze layer transforms."""
import pandas as pd

from pipeline.schemas.columns import BronzeCols as C
from pipeline.transforms.bronze import (
    add_ingestion_metadata,
    apply_bronze_transforms,
)


class TestAddIngestionMetadata:
    def test_adds_metadata_columns(self, raw_df):
        result = add_ingestion_metadata(raw_df, source_file="test.csv")
        assert C.INGESTED_AT in result.columns
        assert C.SOURCE_FILE in result.columns
        assert C.BATCH_ID in result.columns

    def test_source_file_value(self, raw_df):
        result = add_ingestion_metadata(raw_df, source_file="my_data.csv")
        assert (result[C.SOURCE_FILE] == "my_data.csv").all()

    def test_batch_id_is_same_for_all_rows(self, raw_df):
        result = add_ingestion_metadata(raw_df, source_file="test.csv")
        assert result[C.BATCH_ID].nunique() == 1

    def test_row_count_unchanged(self, raw_df):
        result = add_ingestion_metadata(raw_df, source_file="test.csv")
        assert len(result) == len(raw_df)

    def test_original_columns_preserved(self, raw_df):
        original_cols = set(raw_df.columns)
        result = add_ingestion_metadata(raw_df, source_file="test.csv")
        assert original_cols.issubset(set(result.columns))

    def test_does_not_mutate_input(self, raw_df):
        original = raw_df.copy()
        add_ingestion_metadata(raw_df, source_file="test.csv")
        pd.testing.assert_frame_equal(raw_df, original)


class TestApplyBronzeTransforms:
    def test_composition_adds_metadata(self, raw_df):
        result = apply_bronze_transforms(raw_df, source_file="test.csv")
        assert C.INGESTED_AT in result.columns
        assert C.BATCH_ID in result.columns
