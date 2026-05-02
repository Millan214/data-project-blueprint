# ADR-0001 — Medallion architecture with stage gates, WAP, and quarantine

**Status**: accepted
**Date**: 2026-05-02

## Context

We need a Python ETL pipeline that:

- ingests semi-trusted CSV/JSON sources
- enforces increasingly strict business invariants as data flows downstream
- never lets a bad row silently corrupt curated tables
- runs locally on Parquet today and on S3/GCS/Azure tomorrow without rewriting transforms
- keeps transformations testable in isolation, with no I/O coupling

## Decision

### 1. Medallion architecture (Bronze → Silver → Gold)

- **Bronze** preserves raw inputs verbatim (plus ingestion metadata). Schema is permissive.
- **Silver** is cleansed, typed, deduplicated, business-rule-validated.
- **Gold** is aggregated / denormalised for consumption.

### 2. Explicit stage gates between layers

A **gate** is a Pandera schema validation that splits a candidate dataframe into `(clean, quarantine)`. Clean rows continue downstream; quarantined rows are written to a sibling `_quarantine/` location with a `_violations` column describing why they failed and a `_quarantined_at` timestamp.

### 3. Write-Audit-Publish (WAP)

Every layer writes to `<layer>/_staging/run_id=<id>/` first. Only after the data is fully written and (optionally) audited does an atomic directory rename promote it into the published table location. This makes ingestion idempotent and crash-safe.

### 4. Pandera for dataframes, Pydantic for config

- **Pandera** validates the *shape* of dataframes at layer boundaries.
- **Pydantic v2** + **pydantic-settings** validates and types runtime configuration (env vars, `.env`).

The two libraries do not overlap; using both keeps each focused.

### 5. fsspec-backed storage

All paths are URIs (`file://`, `s3://`, `gs://`, `abfs://`). Readers/writers go through fsspec so the same code runs locally and in cloud.

### 6. Functional core, imperative shell

Transformations are pure functions composed via `df.pipe(...)`. Side effects (read, write, log) live only in the `pipelines/` modules. This makes the core trivially unit-testable.

## Consequences

**Pro:**
- Bad rows are observable and recoverable, never silently dropped.
- Pure transforms have no test fixtures larger than a small DataFrame.
- Storage backend changes touch one module (`io/writers.py`).
- Crash mid-write leaves staging dirty but published unchanged.

**Con:**
- Two locations to read for every layer (schema + transform). Mitigated in branch B by colocating per domain.
- WAP costs a directory rename per run; negligible on local FS, acceptable on object stores via prefix copy or atomic-commit protocols.
