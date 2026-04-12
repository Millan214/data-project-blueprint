"""
Backward-compatibility shim — imports split into:
  - paths.py            (file system paths)
  - validation_rules.py (domain thresholds and categorization)
  - analysis_params.py  (Gold layer analysis parameters)
"""
from pipeline.config.paths import *            # noqa: F401,F403
from pipeline.config.validation_rules import * # noqa: F401,F403
from pipeline.config.analysis_params import *  # noqa: F401,F403
