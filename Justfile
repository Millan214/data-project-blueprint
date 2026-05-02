default:
    @just --list

install:
    uv sync --all-groups

lint:
    uv run ruff check .
    uv run ruff format --check .

format:
    uv run ruff format .
    uv run ruff check --fix .

typecheck:
    uv run mypy src tests

test:
    uv run pytest

# End-to-end smoke that proves the quarantine path works
smoke:
    uv run pytest tests/e2e -v

# Layer entrypoints — overridden per branch if needed
bronze:
    uv run medallion-etl bronze

silver:
    uv run medallion-etl silver

gold:
    uv run medallion-etl gold

clean:
    rm -rf data/01_bronze data/02_silver data/03_gold data/_staging
