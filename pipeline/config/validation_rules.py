"""
Validation thresholds and categorization rules.

Domain layer — business rules that govern data quality
and enrichment logic.
"""

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
