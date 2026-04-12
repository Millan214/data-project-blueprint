# %% Setup — project imports
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from pipeline.config.paths import DATA_DIR, SOURCE_FILE, BRONZE_DIR
from pipeline.schemas.columns import BronzeCols as BC, SilverCols as SC
from pipeline.config.validation_rules import (
    VALID_AGE_RANGE, VALID_BMI_RANGE,
    VALID_SLEEP_DURATION_RANGE, VALID_SLEEP_QUALITY_RANGE,
    VALID_HEART_RATE_RANGE, VALID_STRESS_RANGE,
    VALID_GENDERS, VALID_CHRONOTYPES, VALID_MENTAL_HEALTH,
    VALID_RISK_LEVELS, VALID_SEASONS, VALID_DAY_TYPES,
)

# %% Load bronze data
bronze_df = pd.read_parquet(BRONZE_DIR / "bronze_sleep_health.parquet")
bronze_df.head()

# %% Run validation step-by-step to inspect `reasons`
result = bronze_df.copy()
reasons = pd.Series("", index=result.index)

# Numeric range checks
range_checks = [
    (BC.AGE, *VALID_AGE_RANGE),
    (BC.BMI, *VALID_BMI_RANGE),
    (BC.SLEEP_DURATION_HRS, *VALID_SLEEP_DURATION_RANGE),
    (BC.SLEEP_QUALITY_SCORE, *VALID_SLEEP_QUALITY_RANGE),
    (BC.HEART_RATE_BPM, *VALID_HEART_RATE_RANGE),
    (BC.STRESS_SCORE, *VALID_STRESS_RANGE),
]
for col, lo, hi in range_checks:
    out_of_range = ~result[col].between(lo, hi)
    reasons = reasons.where(~out_of_range, reasons + f"{col} out of range; ")

# Categorical checks
cat_checks = [
    (BC.GENDER, VALID_GENDERS),
    (BC.CHRONOTYPE, VALID_CHRONOTYPES),
    (BC.MENTAL_HEALTH, VALID_MENTAL_HEALTH),
    (BC.SLEEP_DISORDER_RISK, VALID_RISK_LEVELS),
    (BC.SEASON, VALID_SEASONS),
    (BC.DAY_TYPE, VALID_DAY_TYPES),
]
for col, valid_set in cat_checks:
    invalid = ~result[col].isin(valid_set)
    reasons = reasons.where(~invalid, reasons + f"{col} invalid value; ")

# Null check
null_id = result[BC.PERSON_ID].isna()
reasons = reasons.where(~null_id, reasons + "person_id is null; ")

# %% Inspect reasons — this is what you want to see
reasons.value_counts()

# %% Show only rows that have a reason (failed validation)
flagged = reasons[reasons.str.len() > 0]
print(f"Flagged rows: {len(flagged)} / {len(reasons)}")
flagged.head(20)

# %% See the full picture: data + reasons side by side
debug_df = result.copy()
debug_df["_reason"] = reasons
debug_df[debug_df["_reason"].str.len() > 0][["person_id", "age", "bmi", "_reason"]].head(20)
