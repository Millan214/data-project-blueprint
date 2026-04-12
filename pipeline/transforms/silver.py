"""
Silver layer transforms — clean, validate, enrich.

Pure functions, DataFrame in → DataFrame out.
Invalid records are flagged for quarantine, never silently dropped.
"""
import pandas as pd
import numpy as np

from pipeline.schemas.columns import BronzeCols as BC, SilverCols as SC
from pipeline.config.validation_rules import (
    VALID_AGE_RANGE,
    VALID_BMI_RANGE,
    VALID_SLEEP_DURATION_RANGE,
    VALID_SLEEP_QUALITY_RANGE,
    VALID_HEART_RATE_RANGE,
    VALID_STRESS_RANGE,
    VALID_GENDERS,
    VALID_CHRONOTYPES,
    VALID_MENTAL_HEALTH,
    VALID_RISK_LEVELS,
    VALID_SEASONS,
    VALID_DAY_TYPES,
    BMI_BINS,
    BMI_LABELS,
    AGE_BINS,
    AGE_LABELS,
    CAFFEINE_BINS,
    CAFFEINE_LABELS,
)


# ---------------------------------------------------------------------------
# Validation — quarantine pattern: flag, don't drop
# ---------------------------------------------------------------------------

def validate_records(df: pd.DataFrame) -> pd.DataFrame:
    """Flag rows that fail validation with a reason. Never drops rows."""
    result = df.copy()
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

    # Null check on primary key
    null_id = result[BC.PERSON_ID].isna()
    reasons = reasons.where(~null_id, reasons + "person_id is null; ")

    result[SC.IS_QUARANTINED] = reasons.str.len() > 0
    result[SC.QUARANTINE_REASON] = reasons.str.rstrip("; ")

    return result


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate on person_id, keeping the last ingested record."""
    return df.sort_values(BC.INGESTED_AT).drop_duplicates(
        subset=[BC.PERSON_ID], keep="last"
    )


# ---------------------------------------------------------------------------
# Enrichment — derived columns
# ---------------------------------------------------------------------------

def add_bmi_category(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result[SC.BMI_CATEGORY] = pd.cut(
        result[BC.BMI], bins=BMI_BINS, labels=BMI_LABELS, right=False
    )
    return result


def add_age_group(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result[SC.AGE_GROUP] = pd.cut(
        result[BC.AGE], bins=AGE_BINS, labels=AGE_LABELS, right=False
    )
    return result


def add_sleep_efficiency(df: pd.DataFrame) -> pd.DataFrame:
    """Sleep efficiency = (REM% + Deep%) as a ratio of total sleep composition."""
    result = df.copy()
    result[SC.SLEEP_EFFICIENCY] = (
        result[BC.REM_PERCENTAGE] + result[BC.DEEP_SLEEP_PERCENTAGE]
    )
    return result


def add_caffeine_category(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result[SC.CAFFEINE_CATEGORY] = pd.cut(
        result[BC.CAFFEINE_MG], bins=CAFFEINE_BINS, labels=CAFFEINE_LABELS
    )
    return result


# ---------------------------------------------------------------------------
# Composition — chain all Silver transforms
# ---------------------------------------------------------------------------

def apply_silver_transforms(df: pd.DataFrame) -> pd.DataFrame:
    """Chain all Silver-layer transforms in order: validate → dedup → enrich."""
    return (
        df
        .pipe(validate_records)
        .pipe(deduplicate)
        .pipe(add_bmi_category)
        .pipe(add_age_group)
        .pipe(add_sleep_efficiency)
        .pipe(add_caffeine_category)
    )


def split_valid_and_quarantine(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Separate valid records from quarantined ones."""
    valid = df.loc[~df[SC.IS_QUARANTINED]].drop(
        columns=[SC.IS_QUARANTINED, SC.QUARANTINE_REASON]
    )
    quarantine = df.loc[df[SC.IS_QUARANTINED]].copy()
    return valid, quarantine
