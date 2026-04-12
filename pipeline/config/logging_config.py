"""
Logging configuration for the sleep health pipeline.

Call setup_logging() once at startup before any other imports that log.
Dual output: concise console (INFO) + detailed rotating file (DEBUG).
"""
import logging
from logging.handlers import RotatingFileHandler

from pipeline.config.paths import LOGS_DIR

_CONSOLE_FMT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
_FILE_FMT = "%(asctime)s | %(levelname)-7s | %(run_id)s | %(layer)-7s | %(name)s | %(funcName)s | %(message)s"
_CONSOLE_DATEFMT = "%H:%M:%S"
_FILE_DATEFMT = "%Y-%m-%d %H:%M:%S"

LOG_FILE = LOGS_DIR / "pipeline.log"

_current_run_id = "--------"
_current_layer = "general"


class _ContextFilter(logging.Filter):
    """Injects run_id and layer into every log record."""

    def filter(self, record):
        record.run_id = _current_run_id
        record.layer = _current_layer
        return True


def set_run_id(run_id: str) -> None:
    """Set the run_id that will appear in every subsequent log line."""
    global _current_run_id
    _current_run_id = run_id


def clear_run_id() -> None:
    """Reset run_id to the default placeholder."""
    global _current_run_id
    _current_run_id = "--------"


def set_layer(layer: str) -> None:
    """Set the current pipeline layer (bronze, silver, gold, general)."""
    global _current_layer
    _current_layer = layer


def clear_layer() -> None:
    """Reset layer to general."""
    global _current_layer
    _current_layer = "general"


def setup_logging() -> None:
    """Configure root logger with console + rotating file handlers."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()

    if root.handlers:
        return  # already configured

    root.setLevel(logging.DEBUG)

    context_filter = _ContextFilter()

    # Console: INFO and above, concise timestamps
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(_CONSOLE_FMT, datefmt=_CONSOLE_DATEFMT))

    # File: DEBUG and above, full timestamps, rotating
    file_h = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    file_h.setLevel(logging.DEBUG)
    file_h.setFormatter(logging.Formatter(_FILE_FMT, datefmt=_FILE_DATEFMT))
    file_h.addFilter(context_filter)

    root.addHandler(console)
    root.addHandler(file_h)
