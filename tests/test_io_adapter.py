"""Tests for I/O adapters."""
import pandas as pd

from pipeline.io.adapters import read_csv, write_parquet, write_csv


class TestWriteParquet:
    def test_creates_file(self, tmp_path, raw_df):
        path = write_parquet(raw_df, tmp_path, "test.parquet")
        assert path.exists()

    def test_returns_correct_path(self, tmp_path, raw_df):
        path = write_parquet(raw_df, tmp_path, "test.parquet")
        assert path == tmp_path / "test.parquet"

    def test_roundtrip_preserves_data(self, tmp_path, raw_df):
        write_parquet(raw_df, tmp_path, "test.parquet")
        loaded = pd.read_parquet(tmp_path / "test.parquet")
        pd.testing.assert_frame_equal(loaded, raw_df)

    def test_creates_directory(self, tmp_path, raw_df):
        nested = tmp_path / "sub" / "dir"
        write_parquet(raw_df, nested, "test.parquet")
        assert (nested / "test.parquet").exists()


class TestWriteCsv:
    def test_creates_file(self, tmp_path, raw_df):
        path = write_csv(raw_df, tmp_path, "test.csv")
        assert path.exists()

    def test_roundtrip_preserves_data(self, tmp_path, raw_df):
        write_csv(raw_df, tmp_path, "test.csv")
        loaded = pd.read_csv(tmp_path / "test.csv")
        pd.testing.assert_frame_equal(loaded, raw_df)


class TestReadCsv:
    def test_reads_csv(self, tmp_path, raw_df):
        csv_path = tmp_path / "input.csv"
        raw_df.to_csv(csv_path, index=False)
        result = read_csv(csv_path)
        pd.testing.assert_frame_equal(result, raw_df)
