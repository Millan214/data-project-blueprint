"""
Sleep Health Data Pipeline — Orchestrator

Runs the full Bronze -> Silver -> Gold pipeline:
  1. Bronze: ingest raw CSV with metadata
  2. Silver: validate, deduplicate, enrich (quarantine invalid records)
  3. Gold: produce analysis-ready datasets
     - Correlation matrix
     - Lifestyle impact on sleep
     - Sleep disorder risk profiles
"""
import argparse
import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

# Ensure the project root is on sys.path so `pipeline.*` imports resolve
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.config.paths import (
    DATA_DIR,
    SOURCE_FILE,
    BRONZE_DIR,
    SILVER_DIR,
    GOLD_DIR,
    QUARANTINE_DIR,
    SUMMARY_FILE,
)
from pipeline.io.adapters import (
    read_csv, write_parquet, write_csv, write_json,
)
from pipeline.transforms.bronze import apply_bronze_transforms
from pipeline.transforms.silver import (
    apply_silver_transforms,
    split_valid_and_quarantine,
)
from pipeline.transforms.gold import apply_gold_transforms
from pipeline.config.logging_config import setup_logging, set_run_id, clear_run_id, set_layer, clear_layer

setup_logging()
logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sleep Health Data Pipeline")
    parser.add_argument(
        "--source", default=SOURCE_FILE,
        help=f"Source CSV filename inside data/input/ (default: {SOURCE_FILE})",
    )
    parser.add_argument(
        "--layer", choices=["all", "bronze", "silver", "gold"], default="all",
        help="Run a specific layer or all (default: all)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Validate config and source file, but don't run transforms",
    )
    return parser.parse_args(argv)


def run(args: argparse.Namespace | None = None) -> None:
    if args is None:
        args = parse_args([])

    run_id = str(uuid.uuid4())[:8]
    set_run_id(run_id)
    start = time.perf_counter()
    source = args.source
    summary = {
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source_file": source,
        "layers": {},
        "status": "started",
    }

    logger.info("Pipeline started")

    # Dry-run: validate config and exit
    source_path = DATA_DIR / source
    if args.dry_run:
        exists = source_path.exists()
        logger.info("[Dry-run] Source: %s (exists=%s)", source_path, exists)
        logger.info("[Dry-run] Bronze: %s | Silver: %s | Gold: %s", BRONZE_DIR, SILVER_DIR, GOLD_DIR)
        summary["status"] = "dry_run"
        summary["elapsed_seconds"] = round(time.perf_counter() - start, 2)
        write_json(summary, SUMMARY_FILE)
        return

    try:
        # Bronze — ingest raw data with metadata
        if args.layer in ("all", "bronze"):
            set_layer("bronze")
            logger.info("Ingesting raw data")

            # Extract data
            raw_df = read_csv(source_path)

            # Transform data
            bronze_df = apply_bronze_transforms(raw_df, source_file=source)

            # Load data
            write_parquet(bronze_df, BRONZE_DIR, "bronze_sleep_health.parquet")

            # Report
            summary["layers"]["bronze"] = {
                "rows_in": len(raw_df),
                "rows_out": len(bronze_df),
            }

        # Silver — validate, deduplicate, enrich
        if args.layer in ("all", "silver"):
            set_layer("silver")
            logger.info("Validating and enriching")

            # Load only if not already loaded in Bronze step of this run
            if args.layer == "silver":
                bronze_df = pd.read_parquet(BRONZE_DIR / "bronze_sleep_health.parquet")

            # Transform data
            silver_all = apply_silver_transforms(bronze_df)

            # Split valid vs quarantined records
            valid_df, quarantine_df = split_valid_and_quarantine(silver_all)

            # Load valid records to Silver layer, quarantined records to Quarantine layer
            write_parquet(valid_df, SILVER_DIR, "silver_sleep_health.parquet")
            if len(quarantine_df) > 0:
                logger.info("Quarantined %d invalid rows", len(quarantine_df))

            # Report
            summary["layers"]["silver"] = {
                "rows_in": len(bronze_df),
                "rows_valid": len(valid_df),
                "rows_quarantined": len(quarantine_df),
            }

        # Gold — analytical datasets
        if args.layer in ("all", "gold"):
            set_layer("gold")
            logger.info("Building analytical datasets")

            # Load only if not already loaded in Silver step of this run
            if args.layer == "gold":
                valid_df = pd.read_parquet(SILVER_DIR / "silver_sleep_health.parquet")

            # Transform data into multiple Gold datasets
            gold_datasets = apply_gold_transforms(valid_df)

            # Load each Gold dataset as a separate CSV file
            gold_counts = {}
            for name, gold_df in gold_datasets.items():
                write_csv(gold_df, GOLD_DIR, f"{name}.csv")
                gold_counts[name] = len(gold_df)

            # Report
            summary["layers"]["gold"] = gold_counts

        # Summary
        clear_layer()
        elapsed = time.perf_counter() - start
        summary["elapsed_seconds"] = round(elapsed, 2)
        summary["status"] = "success"
        write_json(summary, SUMMARY_FILE)
        logger.info("Pipeline complete in %.1fs", elapsed)
        clear_run_id()

    except Exception:
        clear_layer()
        elapsed = time.perf_counter() - start
        summary["elapsed_seconds"] = round(elapsed, 2)
        summary["status"] = "failed"
        write_json(summary, SUMMARY_FILE)
        logger.exception("Pipeline failed after %.1fs", elapsed)
        clear_run_id()
        raise


if __name__ == "__main__":
    run(parse_args())
