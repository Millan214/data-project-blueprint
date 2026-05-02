# medallion-etl

A Python ETL / analytics scaffold using the **medallion architecture** (Bronze → Silver → Gold) with explicit stage gates, functional `df.pipe()` chains, Pandera + Pydantic validation, a quarantine pattern for bad rows, and an `fsspec`-based storage layer (Parquet locally, swappable to S3/GCS/Azure).

## Two scaffolds, two branches

This repo demonstrates two folder-structure philosophies side-by-side. Pick one and delete the other.

```sh
git checkout proposal-a-layer-first   # Kedro-style, organized by medallion layer
git checkout proposal-b-domain-first  # dbt-style, organized by business domain
```

Both branches:

- share the same dependencies, tooling, and core helpers (settings, catalog, WAP writer, quarantine splitter)
- implement the same seven requirements
- ship a passing `just smoke` test that proves the **quarantine path** works (one deliberately bad row in the fixture must end up in `_quarantine/`, the rest promoted to `silver/`)

They differ only in *where the schemas, transforms, gates, and pipeline shells live*.

| | Branch A — layer-first | Branch B — domain-first |
|---|---|---|
| Top-level axis | medallion layer | business domain |
| Inspired by | Kedro | dbt |
| CLI | `medallion-etl bronze\|silver\|gold` | `medallion-etl <domain> bronze\|silver\|gold` |
| Schemas live in | `src/medallion_etl/schemas/<layer>/<table>.py` | `src/medallion_etl/pipelines/<domain>/schemas.py` |
| Best when | layer-level concerns dominate (governance, layer-specific tooling) | domain ownership boundaries dominate (per-team folders, READMEs, CODEOWNERS) |

To compare:

```sh
git diff proposal-a-layer-first proposal-b-domain-first -- src/
```

## Quickstart

```sh
just install      # uv sync --all-groups
just smoke        # end-to-end proves quarantine works
just lint
just typecheck
```

## Stack

Python 3.12 · uv · pandas · pyarrow · pandera · pydantic v2 · deltalake (delta-rs) · fsspec · structlog · typer · pytest · ruff · mypy --strict.

See [`docs/adr/0001-medallion-with-stage-gates.md`](docs/adr/0001-medallion-with-stage-gates.md) for the architecture decisions.
