"""Append-only event log for pipeline runs.

One execution = one JSONL file at `<storage_root>/<logs_dir>/run-NNNNNN.jsonl`.

Usage:

    from medallion_etl.observability.pipeline_logger import logger

    with logger.execution(pipeline="medallion"):
        with logger.step(
            layer="bronze", layer_order=1,
            step_id="bronze.transform_types",
            step_name="Transforming Types",
            step_order=1,
            rows_in=50_214,
        ) as step:
            df = transform(df)
            step.rows_out(len(df))

If no execution is active, `step()` is a no-op (so unit tests of pure
transforms keep working without setup).
"""

from __future__ import annotations

import json
import re
import threading
import time
import traceback
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any, Iterator, Self

from medallion_etl.settings import settings

_RUN_FILE_RE = re.compile(r"^run-(\d{6})\.jsonl$")


def _logs_root() -> Path:
    storage = settings.storage_root.removeprefix("file://")
    return Path(storage) / settings.logs_dir


def _index_path(root: Path) -> Path:
    return root / "index.json"


def _index_upsert(root: Path, entry: dict[str, Any]) -> None:
    """Add or update an entry in `_logs/index.json` keyed by execution id."""
    path = _index_path(root)
    data: dict[str, Any] = {"executions": []}
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            data = {"executions": []}
    executions = data.setdefault("executions", [])
    for i, existing in enumerate(executions):
        if existing.get("id") == entry["id"]:
            executions[i] = {**existing, **entry}
            break
    else:
        executions.append(entry)
    executions.sort(key=lambda e: e.get("id", 0))
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _next_execution_id(root: Path) -> int:
    if not root.exists():
        return 1
    used = [
        int(m.group(1))
        for p in root.iterdir()
        if (m := _RUN_FILE_RE.match(p.name))
    ]
    return max(used, default=0) + 1


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class StepHandle:
    """Returned by `logger.step(...)`. Lets the caller attach extra context."""

    def __init__(self, *, active: bool) -> None:
        self._active = active
        self._rows_out: int | None = None
        self._error_context: dict[str, Any] = {}
        self._extra: dict[str, Any] = {}

    def rows_out(self, n: int) -> None:
        if self._active:
            self._rows_out = int(n)

    def add_context(self, **kwargs: Any) -> None:
        """Attach domain-specific fields to a future error event."""
        if self._active:
            self._error_context.update(kwargs)

    def add_extra(self, **kwargs: Any) -> None:
        """Attach free-form fields to the step_finished event."""
        if self._active:
            self._extra.update(kwargs)


class PipelineLogger:
    """Module-level singleton. One open execution at a time, per process."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._fh: IO[str] | None = None
        self._execution_id: int | None = None
        self._pipeline: str | None = None
        self._t_started: float | None = None
        self._layers_seen: set[int] = set()
        self._layers_completed: set[int] = set()

    # ---- public API ----------------------------------------------------

    @contextmanager
    def execution(self, *, pipeline: str) -> Iterator[Self]:
        self._open(pipeline=pipeline)
        try:
            yield self
        except BaseException:
            self._close(status="error")
            raise
        else:
            self._close(status="success")

    @contextmanager
    def step(
        self,
        *,
        layer: str,
        layer_order: int,
        step_id: str,
        step_name: str,
        step_order: int,
        description: str,
        rows_in: int | None = None,
    ) -> Iterator[StepHandle]:
        """Wrap a unit of work and emit start/finish events.

        ``description`` is required: a one-line human-readable explanation of
        what the step does, surfaced by the viewer's detail pane.
        """
        if not description or not description.strip():
            raise ValueError(
                f"step {step_id!r} is missing a description; every pipe log must have one"
            )
        if self._fh is None:
            yield StepHandle(active=False)
            return

        self._layers_seen.add(layer_order)
        handle = StepHandle(active=True)
        t0 = time.monotonic()

        self._emit(
            event="step_started",
            level="info",
            status="running",
            layer=layer,
            layer_order=layer_order,
            step_id=step_id,
            step_name=step_name,
            step_order=step_order,
            description=description,
            rows_in=rows_in,
        )

        try:
            yield handle
        except BaseException as exc:
            self._emit(
                event="step_finished",
                level="error",
                status="error",
                layer=layer,
                layer_order=layer_order,
                step_id=step_id,
                step_name=step_name,
                step_order=step_order,
                description=description,
                duration_ms=int((time.monotonic() - t0) * 1000),
                rows_in=rows_in,
                rows_out=handle._rows_out,
                error={
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "traceback": "".join(traceback.format_exception(exc)),
                    "context": handle._error_context or None,
                },
                **handle._extra,
            )
            raise
        else:
            self._layers_completed.add(layer_order)
            self._emit(
                event="step_finished",
                level="info",
                status="success",
                layer=layer,
                layer_order=layer_order,
                step_id=step_id,
                step_name=step_name,
                step_order=step_order,
                description=description,
                duration_ms=int((time.monotonic() - t0) * 1000),
                rows_in=rows_in,
                rows_out=handle._rows_out,
                **handle._extra,
            )

    # ---- internals -----------------------------------------------------

    def _open(self, *, pipeline: str) -> None:
        with self._lock:
            if self._fh is not None:
                raise RuntimeError("an execution is already active")
            root = _logs_root()
            root.mkdir(parents=True, exist_ok=True)
            self._execution_id = _next_execution_id(root)
            self._pipeline = pipeline
            path = root / f"run-{self._execution_id:06d}.jsonl"
            self._fh = path.open("a", encoding="utf-8")
            self._t_started = time.monotonic()
            self._t_started_iso = _now_iso()
            self._layers_seen.clear()
            self._layers_completed.clear()
            self._emit(event="execution_started", level="info")
            _index_upsert(
                root,
                {
                    "id": self._execution_id,
                    "pipeline": pipeline,
                    "file": path.name,
                    "started_at": self._t_started_iso,
                    "finished_at": None,
                    "status": "running",
                    "layers_completed": 0,
                    "layers_seen": 0,
                },
            )

    def _close(self, *, status: str) -> None:
        with self._lock:
            if self._fh is None:
                return
            duration_ms = int((time.monotonic() - (self._t_started or 0)) * 1000)
            finished_at = _now_iso()
            self._emit(
                event="execution_finished",
                level="info" if status == "success" else "error",
                status=status,
                duration_ms=duration_ms,
                layers_completed=len(self._layers_completed),
                layers_seen=len(self._layers_seen),
            )
            _index_upsert(
                _logs_root(),
                {
                    "id": self._execution_id,
                    "pipeline": self._pipeline,
                    "file": f"run-{self._execution_id:06d}.jsonl",
                    "started_at": self._t_started_iso,
                    "finished_at": finished_at,
                    "status": status,
                    "duration_ms": duration_ms,
                    "layers_completed": len(self._layers_completed),
                    "layers_seen": len(self._layers_seen),
                },
            )
            self._fh.close()
            self._fh = None
            self._execution_id = None
            self._pipeline = None
            self._t_started = None

    def _emit(self, **fields: Any) -> None:
        if self._fh is None:
            return
        line = {
            "ts": _now_iso(),
            "execution_id": self._execution_id,
            "pipeline": self._pipeline,
            **{k: v for k, v in fields.items() if v is not None},
        }
        self._fh.write(json.dumps(line, default=str) + "\n")
        self._fh.flush()


logger = PipelineLogger()
