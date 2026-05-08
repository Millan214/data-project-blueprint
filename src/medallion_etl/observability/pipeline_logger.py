"""Registro de eventos append-only para ejecuciones de pipeline.

Una ejecución = un archivo JSONL en `<storage_root>/<logs_dir>/run-NNNNNN.jsonl`.

Uso:

    loginfo = build_loginfo({
        "bronze.transform": {
            "layer": "bronze", "layer_order": 1,
            "step_name": "Transformando", "step_order": 1,
            "description": "Convierte tipos y registra procedencia.",
            "rows_in_fn": lambda df, **_: len(df),
            "rows_out_fn": len,
        },
    })

    @logger.step(loginfo["bronze.transform"])
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(...)

Los cuerpos de los steps permanecen puros — sin imports del logger en su
interior. Toda la conformación del evento vive en los callables opcionales
de cada entrada de loginfo (rows_in_fn, rows_out_fn, extra_in_fn, extra_out_fn).

Si no hay una ejecución activa, el decorator es no-op (los tests unitarios
de transformaciones puras siguen funcionando sin necesidad de setup).
"""

from __future__ import annotations

import inspect
import json
import re
import threading
import time
import traceback
from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime
from functools import wraps
from pathlib import Path
from typing import IO, Any, Iterator, Self, TypedDict, TypeVar

from medallion_etl.settings import settings

F = TypeVar("F", bound=Callable[..., Any])

_RUN_FILE_RE = re.compile(r"^run-(\d{6})\.jsonl$")


class StepInfo(TypedDict, total=False):
    """Metadata estática + shapers para un step registrado.

    Claves requeridas: layer, layer_order, step_id, step_name, step_order, description.
    `step_id` se inyecta automáticamente desde la clave del diccionario en `build_loginfo`.

    Shapers opcionales:
        rows_in_fn(**bound_args)  -> int
        rows_out_fn(result)       -> int
        extra_in_fn(**bound_args) -> dict
        extra_out_fn(result)      -> dict
    """

    layer: str
    layer_order: int
    step_id: str
    step_name: str
    step_order: int
    description: str
    rows_in_fn: Callable[..., int]
    rows_out_fn: Callable[[Any], int]
    extra_in_fn: Callable[..., dict[str, Any]]
    extra_out_fn: Callable[[Any], dict[str, Any]]


def build_loginfo(entries: dict[str, dict[str, Any]]) -> dict[str, StepInfo]:
    """Inyecta el `step_id` de cada entrada desde su clave, dejando el resto intacto."""
    return {sid: {"step_id": sid, **spec} for sid, spec in entries.items()}  # type: ignore[misc]


def _logs_root() -> Path:
    storage = settings.storage_root.removeprefix("file://")
    return Path(storage) / settings.logs_dir


def _index_path(root: Path) -> Path:
    return root / "index.json"


def _index_upsert(root: Path, entry: dict[str, Any]) -> None:
    """Agrega o actualiza una entrada en `_logs/index.json` indexada por execution id."""
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


class PipelineLogger:
    """Singleton a nivel de módulo. Una ejecución abierta a la vez, por proceso."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._fh: IO[str] | None = None
        self._execution_id: int | None = None
        self._pipeline: str | None = None
        self._t_started: float | None = None
        self._layers_seen: set[int] = set()
        self._layers_completed: set[int] = set()

    # ---- API pública ---------------------------------------------------

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

    def step(self, info: StepInfo) -> Callable[[F], F]:
        """Decorator: envuelve una función para que cada llamada emita eventos de inicio/fin.

        Los cuerpos del step permanecen puros — sin imports del logger en su interior.
        Toda la conformación del evento vive en `info` (una entrada `StepInfo`,
        construida típicamente con `build_loginfo`).
        """
        self._validate_info(info)

        def decorator(func: F) -> F:
            sig = inspect.signature(func)

            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                # Si no hay ejecución activa, ejecutar la función sin instrumentación
                # (permite que tests unitarios llamen los steps directamente).
                if self._fh is None:
                    return func(*args, **kwargs)

                bound = sig.bind_partial(*args, **kwargs).arguments
                rows_in = self._call_shaper(info.get("rows_in_fn"), kwargs=bound)
                extra_in = self._call_shaper(info.get("extra_in_fn"), kwargs=bound) or {}

                self._layers_seen.add(info["layer_order"])
                t0 = time.monotonic()
                self._emit(
                    event="step_started",
                    level="info",
                    status="running",
                    layer=info["layer"],
                    layer_order=info["layer_order"],
                    step_id=info["step_id"],
                    step_name=info["step_name"],
                    step_order=info["step_order"],
                    description=info["description"],
                    rows_in=rows_in,
                )

                try:
                    result = func(*args, **kwargs)
                except BaseException as exc:
                    # Cualquier excepción en el step queda etiquetada como
                    # `status="error"` con el traceback completo y luego se re-lanza.
                    self._emit(
                        event="step_finished",
                        level="error",
                        status="error",
                        layer=info["layer"],
                        layer_order=info["layer_order"],
                        step_id=info["step_id"],
                        step_name=info["step_name"],
                        step_order=info["step_order"],
                        description=info["description"],
                        duration_ms=int((time.monotonic() - t0) * 1000),
                        rows_in=rows_in,
                        error={
                            "type": type(exc).__name__,
                            "message": str(exc),
                            "traceback": "".join(traceback.format_exception(exc)),
                            "context": extra_in or None,
                        },
                        **extra_in,
                    )
                    raise

                rows_out = self._call_shaper(info.get("rows_out_fn"), positional=(result,))
                extra_out = self._call_shaper(info.get("extra_out_fn"), positional=(result,)) or {}

                self._layers_completed.add(info["layer_order"])
                self._emit(
                    event="step_finished",
                    level="info",
                    status="success",
                    layer=info["layer"],
                    layer_order=info["layer_order"],
                    step_id=info["step_id"],
                    step_name=info["step_name"],
                    step_order=info["step_order"],
                    description=info["description"],
                    duration_ms=int((time.monotonic() - t0) * 1000),
                    rows_in=rows_in,
                    rows_out=rows_out,
                    **{**extra_in, **extra_out},
                )
                return result

            return wrapper  # type: ignore[return-value]

        return decorator

    # ---- internos ------------------------------------------------------

    @staticmethod
    def _validate_info(info: StepInfo) -> None:
        """Verifica que el StepInfo tenga las claves obligatorias y una description no vacía."""
        for required in ("layer", "layer_order", "step_id", "step_name", "step_order", "description"):
            if required not in info:
                raise ValueError(f"StepInfo missing required key {required!r}: {info!r}")
        if not str(info["description"]).strip():
            raise ValueError(
                f"step {info['step_id']!r} is missing a description; every pipe log must have one"
            )

    @staticmethod
    def _call_shaper(
        fn: Callable[..., Any] | None,
        *,
        positional: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> Any:
        """Invoca un shaper opcional de loginfo; devuelve None si no está definido."""
        if fn is None:
            return None
        return fn(*positional, **(kwargs or {}))

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
        """Escribe una línea JSONL en el archivo de la ejecución activa.

        Hace flush inmediato para que los logs sean visibles incluso si el
        proceso se detiene abruptamente.
        """
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
