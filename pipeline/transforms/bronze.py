"""
Bronze layer transforms — ingest raw data as-is with metadata.

Pure functions, DataFrame in → DataFrame out.
No I/O, no side effects.
"""
import uuid
from datetime import datetime, timezone

import pandas as pd

from pipeline.schemas.columns import BronzeCols as C


def add_ingestion_metadata(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Stamp each row with ingestion metadata for auditability."""
    # Input contract
    result = df.copy()

    result[C.INGESTED_AT] = datetime.now(timezone.utc).isoformat()
    result[C.SOURCE_FILE] = source_file
    result[C.BATCH_ID] = str(uuid.uuid4())

    # Output contract — all original columns + metadata
    return result


# ---------------------------------------------------------------------------
# Composition — chain all Bronze transforms
# ---------------------------------------------------------------------------

def apply_bronze_transforms(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Chain all Bronze-layer transforms: ingest raw data with metadata."""
    return df.pipe(add_ingestion_metadata, source_file=source_file)
