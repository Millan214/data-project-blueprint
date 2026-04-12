"""
Column name constants for the sleep health pipeline.

Single source of truth for column names across Bronze, Silver,
and Gold layers. Pure Python, no framework dependency.
"""


# --- Bronze: raw columns as-is from source ---
class BronzeCols:
    PERSON_ID = "person_id"
    AGE = "age"
    GENDER = "gender"
    OCCUPATION = "occupation"
    BMI = "bmi"
    COUNTRY = "country"
    SLEEP_DURATION_HRS = "sleep_duration_hrs"
    SLEEP_QUALITY_SCORE = "sleep_quality_score"
    REM_PERCENTAGE = "rem_percentage"
    DEEP_SLEEP_PERCENTAGE = "deep_sleep_percentage"
    SLEEP_LATENCY_MINS = "sleep_latency_mins"
    WAKE_EPISODES = "wake_episodes_per_night"
    CAFFEINE_MG = "caffeine_mg_before_bed"
    ALCOHOL_UNITS = "alcohol_units_before_bed"
    SCREEN_TIME_MINS = "screen_time_before_bed_mins"
    EXERCISE_DAY = "exercise_day"
    STEPS = "steps_that_day"
    NAP_DURATION_MINS = "nap_duration_mins"
    STRESS_SCORE = "stress_score"
    WORK_HOURS = "work_hours_that_day"
    CHRONOTYPE = "chronotype"
    MENTAL_HEALTH = "mental_health_condition"
    HEART_RATE_BPM = "heart_rate_resting_bpm"
    SLEEP_AID = "sleep_aid_used"
    SHIFT_WORK = "shift_work"
    ROOM_TEMP_C = "room_temperature_celsius"
    WEEKEND_SLEEP_DIFF = "weekend_sleep_diff_hrs"
    SEASON = "season"
    DAY_TYPE = "day_type"
    COGNITIVE_SCORE = "cognitive_performance_score"
    SLEEP_DISORDER_RISK = "sleep_disorder_risk"
    FELT_RESTED = "felt_rested"

    # Ingestion metadata added at Bronze
    INGESTED_AT = "_ingested_at"
    SOURCE_FILE = "_source_file"
    BATCH_ID = "_batch_id"


# --- Silver: cleaned + derived columns ---
class SilverCols(BronzeCols):
    BMI_CATEGORY = "bmi_category"
    AGE_GROUP = "age_group"
    SLEEP_EFFICIENCY = "sleep_efficiency"
    CAFFEINE_CATEGORY = "caffeine_category"
    IS_QUARANTINED = "_is_quarantined"
    QUARANTINE_REASON = "_quarantine_reason"


# --- Gold: analysis-specific columns ---
class GoldCols:
    # Correlation analysis
    VARIABLE_1 = "variable_1"
    VARIABLE_2 = "variable_2"
    CORRELATION = "correlation"
    ABS_CORRELATION = "abs_correlation"

    # Lifestyle impact
    LIFESTYLE_FACTOR = "lifestyle_factor"
    FACTOR_LEVEL = "factor_level"
    AVG_SLEEP_QUALITY = "avg_sleep_quality"
    AVG_SLEEP_DURATION = "avg_sleep_duration"
    AVG_COGNITIVE_SCORE = "avg_cognitive_score"
    SAMPLE_COUNT = "sample_count"

    # Sleep disorder profiling
    RISK_LEVEL = "risk_level"
    FEATURE = "feature"
    MEAN_VALUE = "mean_value"
    STD_VALUE = "std_value"
