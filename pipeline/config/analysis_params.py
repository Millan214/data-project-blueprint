"""
Analysis parameters for Gold layer transforms.

Use-case layer — parameters that control which columns
and thresholds are used in analytical aggregations.
"""

# --- Correlation analysis ---
CORRELATION_MIN_THRESHOLD = 0.05
NUMERIC_COLUMNS_FOR_CORRELATION = [
    "age", "bmi", "sleep_duration_hrs", "sleep_quality_score",
    "rem_percentage", "deep_sleep_percentage", "sleep_latency_mins",
    "wake_episodes_per_night", "caffeine_mg_before_bed",
    "alcohol_units_before_bed", "screen_time_before_bed_mins",
    "exercise_day", "steps_that_day", "nap_duration_mins",
    "stress_score", "work_hours_that_day", "heart_rate_resting_bpm",
    "room_temperature_celsius", "weekend_sleep_diff_hrs",
    "cognitive_performance_score",
]

# --- Lifestyle impact ---
LIFESTYLE_FACTORS = [
    "caffeine_category", "alcohol_units_before_bed",
    "screen_time_before_bed_mins", "exercise_day",
    "nap_duration_mins", "stress_score",
]

# --- Risk profiling ---
RISK_PROFILE_FEATURES = [
    "age", "bmi", "sleep_duration_hrs", "sleep_quality_score",
    "rem_percentage", "deep_sleep_percentage", "sleep_latency_mins",
    "wake_episodes_per_night", "caffeine_mg_before_bed",
    "alcohol_units_before_bed", "screen_time_before_bed_mins",
    "stress_score", "work_hours_that_day", "heart_rate_resting_bpm",
    "cognitive_performance_score",
]
