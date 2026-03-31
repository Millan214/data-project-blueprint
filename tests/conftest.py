"""Shared test fixtures — small DataFrames that mirror the real schema."""
import sys
from pathlib import Path

import pandas as pd
import pytest

# Ensure pipeline imports resolve
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.layer_transforms.bronze_transforms import apply_bronze_transforms
from pipeline.layer_transforms.silver_transforms import (
    apply_silver_transforms,
    split_valid_and_quarantine,
)


def _make_raw_row(**overrides) -> dict:
    """Build a single valid raw row with sensible defaults."""
    base = {
        "person_id": 1,
        "age": 30,
        "gender": "Male",
        "occupation": "Engineer",
        "bmi": 22.5,
        "country": "US",
        "sleep_duration_hrs": 7.5,
        "sleep_quality_score": 7.0,
        "rem_percentage": 25.0,
        "deep_sleep_percentage": 20.0,
        "sleep_latency_mins": 15,
        "wake_episodes_per_night": 1,
        "caffeine_mg_before_bed": 50,
        "alcohol_units_before_bed": 0.5,
        "screen_time_before_bed_mins": 30,
        "exercise_day": 1,
        "steps_that_day": 8000,
        "nap_duration_mins": 0,
        "stress_score": 4.0,
        "work_hours_that_day": 8.0,
        "chronotype": "Morning",
        "mental_health_condition": "Healthy",
        "heart_rate_resting_bpm": 70,
        "sleep_aid_used": 0,
        "shift_work": 0,
        "room_temperature_celsius": 21.0,
        "weekend_sleep_diff_hrs": 1.0,
        "season": "Spring",
        "day_type": "Weekday",
        "cognitive_performance_score": 75.0,
        "sleep_disorder_risk": "Healthy",
        "felt_rested": 1,
    }
    base.update(overrides)
    return base


@pytest.fixture
def make_raw_row():
    """Factory fixture — call with overrides to build custom rows."""
    return _make_raw_row


@pytest.fixture
def raw_df():
    """5-row DataFrame mimicking the raw CSV."""
    rows = [
        _make_raw_row(person_id=1, age=25, bmi=21.0, sleep_disorder_risk="Healthy"),
        _make_raw_row(person_id=2, age=35, bmi=27.0, sleep_disorder_risk="Mild", gender="Female"),
        _make_raw_row(person_id=3, age=45, bmi=32.0, sleep_disorder_risk="Moderate"),
        _make_raw_row(person_id=4, age=55, bmi=18.0, sleep_disorder_risk="Severe"),
        _make_raw_row(person_id=5, age=20, bmi=23.5, sleep_disorder_risk="Healthy", gender="Female"),
    ]
    return pd.DataFrame(rows)


@pytest.fixture
def bronze_df(raw_df):
    """Raw data after bronze transforms (with metadata columns)."""
    return apply_bronze_transforms(raw_df, source_file="test_data.csv")


@pytest.fixture
def silver_df(bronze_df):
    """Bronze data after silver transforms (validated + enriched)."""
    return apply_silver_transforms(bronze_df)


@pytest.fixture
def valid_and_quarantine(silver_df):
    """Tuple of (valid_df, quarantine_df) after split."""
    return split_valid_and_quarantine(silver_df)
