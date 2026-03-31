"""
Pipeline configuration constants.

Entities layer — domain rules and thresholds that govern
validation, categorization, and analysis parameters.
"""
from pathlib import Path


# --- Paths ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "input"
LOGS_DIR = BASE_DIR / "logs"
LAYERS_DIR = BASE_DIR / "data" / "output"
BRONZE_DIR = LAYERS_DIR / "bronze"
SILVER_DIR = LAYERS_DIR / "silver"
GOLD_DIR = LAYERS_DIR / "gold"
QUARANTINE_DIR = LAYERS_DIR / "quarantine"
SUMMARY_FILE = LOGS_DIR / "run_summary.json"

SOURCE_FILE = "sleep_health_dataset.csv"

# --- Validation thresholds ---
VALID_AGE_RANGE = (0, 120)
VALID_BMI_RANGE = (10.0, 60.0)
VALID_SLEEP_DURATION_RANGE = (0.0, 24.0)
VALID_SLEEP_QUALITY_RANGE = (1.0, 10.0)
VALID_HEART_RATE_RANGE = (30, 200)
VALID_STRESS_RANGE = (0.0, 10.0)

VALID_GENDERS = {"Male", "Female", "Other"}
VALID_CHRONOTYPES = {"Morning", "Evening", "Neutral"}
VALID_MENTAL_HEALTH = {"Healthy", "Anxiety", "Depression", "Both"}
VALID_RISK_LEVELS = {"Healthy", "Mild", "Moderate", "Severe"}
VALID_SEASONS = {"Spring", "Summer", "Autumn", "Winter"}
VALID_DAY_TYPES = {"Weekday", "Weekend"}

# --- Categorization rules ---
BMI_BINS = [0, 18.5, 25.0, 30.0, 100.0]
BMI_LABELS = ["Underweight", "Normal", "Overweight", "Obese"]

AGE_BINS = [0, 25, 35, 45, 55, 120]
AGE_LABELS = ["18-24", "25-34", "35-44", "45-54", "55+"]

CAFFEINE_BINS = [-1, 0, 100, 200, 500]
CAFFEINE_LABELS = ["None", "Low", "Moderate", "High"]

# --- Analysis parameters ---
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

LIFESTYLE_FACTORS = [
    "caffeine_category", "alcohol_units_before_bed",
    "screen_time_before_bed_mins", "exercise_day",
    "nap_duration_mins", "stress_score",
]

RISK_PROFILE_FEATURES = [
    "age", "bmi", "sleep_duration_hrs", "sleep_quality_score",
    "rem_percentage", "deep_sleep_percentage", "sleep_latency_mins",
    "wake_episodes_per_night", "caffeine_mg_before_bed",
    "alcohol_units_before_bed", "screen_time_before_bed_mins",
    "stress_score", "work_hours_that_day", "heart_rate_resting_bpm",
    "cognitive_performance_score",
]
