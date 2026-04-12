"""
Path configuration for the sleep health pipeline.

Infrastructure layer — file system paths that may change by environment.
"""
from pathlib import Path


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
