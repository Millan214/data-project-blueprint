import pandas as pd


def stamp_ingest_metadata(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    return df.assign(
        _ingested_at=pd.Timestamp.now("UTC"),
        _source_file=source_file,
    )
