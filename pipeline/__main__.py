"""Allow running the pipeline with `python -m pipeline`."""
from pipeline.main import parse_args, run

run(parse_args())
