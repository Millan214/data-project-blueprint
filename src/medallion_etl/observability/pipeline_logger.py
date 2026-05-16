"""Registro de eventos append-only para ejecuciones de pipeline.

Una ejecución = un directorio en `<storage_root>/<logs_dir>/run-NNNNNN/`,
con dos archivos JSONL hermanos:

  - `run-NNNNNN-info.jsonl`   eventos del pipeline (execution / step / pipe)
  - `run-NNNNNN-usage.jsonl`  muestras periódicas de CPU / memoria / disco

El índice (`<logs_dir>/index.json`) apunta al archivo info; el viewer
descubre el archivo de usage por convención (mismo nombre, sufijo `-usage`).

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

import psutil

from medallion_etl.settings import settings

F = TypeVar("F", bound=Callable[..., Any])

# Each run lives in its own directory like `run-000017/`. Inside it:
#   - run-000017-info.jsonl   (pipeline events)
#   - run-000017-usage.jsonl  (periodic CPU/memory/disk samples)
_RUN_DIR_RE = re.compile(r"^run-(\d{6})$")
_USAGE_INTERVAL_S = 1.0


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


def _safe_len(obj: Any) -> int | None:
    """Devuelve `len(obj)` si está definido; si no, None.

    Permite inferir `rows_in`/`rows_out` para DataFrames, listas, tuplas, etc.,
    sin romper para tipos sin `__len__` (escalares, None, etc.).
    """
    try:
        return len(obj)
    except (TypeError, AttributeError):
        return None


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
        if p.is_dir() and (m := _RUN_DIR_RE.match(p.name))
    ]
    return max(used, default=0) + 1


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class _UsageSampler:
    """Background thread that periodically writes CPU / memory / disk samples.

    Sampling stops as soon as `.stop()` is called (or the process exits, since
    the thread is a daemon). Each sample is one JSONL line keyed by execution id.
    """

    def __init__(self, fh: IO[str], execution_id: int, disk_path: Path, interval_s: float = _USAGE_INTERVAL_S):
        self._fh = fh
        self._execution_id = execution_id
        self._disk_path = str(disk_path)
        self._interval = interval_s
        self._stop = threading.Event()
        self._proc = psutil.Process()
        # Prime the per-call CPU counters so the first real sample is meaningful.
        try:
            self._proc.cpu_percent(interval=None)
            psutil.cpu_percent(interval=None)
        except Exception:
            pass
        self._thread = threading.Thread(target=self._run, name="usage-sampler", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2.0)

    def _run(self) -> None:
        # Wait one tick before the first sample so cpu_percent has data to diff.
        if self._stop.wait(self._interval):
            return
        while not self._stop.is_set():
            try:
                self._emit_sample()
            except Exception:
                # Sampler must never crash the pipeline — swallow and keep going.
                pass
            if self._stop.wait(self._interval):
                break

    def _emit_sample(self) -> None:
        cpu_proc = self._proc.cpu_percent(interval=None)
        cpu_sys = psutil.cpu_percent(interval=None)
        mem_sys = psutil.virtual_memory()
        mem_proc_rss = self._proc.memory_info().rss
        sample: dict[str, Any] = {
            "ts": _now_iso(),
            "execution_id": self._execution_id,
            "cpu_proc_pct": round(cpu_proc, 2),
            "cpu_system_pct": round(cpu_sys, 2),
            "mem_proc_rss_mb": round(mem_proc_rss / (1024 * 1024), 2),
            "mem_system_pct": round(mem_sys.percent, 2),
        }
        try:
            disk = psutil.disk_usage(self._disk_path)
            sample["disk_free_gb"] = round(disk.free / (1024 ** 3), 2)
            sample["disk_total_gb"] = round(disk.total / (1024 ** 3), 2)
            sample["disk_used_pct"] = round(disk.percent, 2)
        except OSError:
            pass
        self._fh.write(json.dumps(sample) + "\n")
        self._fh.flush()


class PipelineLogger:
    """Singleton a nivel de módulo. Una ejecución abierta a la vez, por proceso."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._fh: IO[str] | None = None
        self._usage_fh: IO[str] | None = None
        self._sampler: _UsageSampler | None = None
        self._execution_id: int | None = None
        self._pipeline: str | None = None
        self._t_started: float | None = None
        self._layers_seen: set[int] = set()
        self._layers_completed: set[int] = set()
        # Step actualmente en ejecución — lo lee `@pipe` para colgar sus
        # eventos del step padre. Se guarda/restaura en el wrapper de `step`
        # para soportar tests u otros casos de anidamiento.
        self._current_step: dict[str, Any] | None = None
        # Contador monotónico de pipes dentro del step actual; se reinicia
        # a 0 al entrar en cada step.
        self._pipe_counter: int = 0

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

                # Publica el step actual para que @pipe pueda colgar sus eventos.
                # Se guarda el contexto previo para restaurarlo al salir (soporta
                # cualquier nivel de anidamiento aunque normalmente solo haya uno).
                prev_step = self._current_step
                prev_counter = self._pipe_counter
                self._current_step = {
                    "layer": info["layer"],
                    "layer_order": info["layer_order"],
                    "step_id": info["step_id"],
                    "step_name": info["step_name"],
                    "step_order": info["step_order"],
                }
                self._pipe_counter = 0

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
                    self._current_step = prev_step
                    self._pipe_counter = prev_counter
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
                self._current_step = prev_step
                self._pipe_counter = prev_counter
                return result

            return wrapper  # type: ignore[return-value]

        return decorator

    def pipe(self, name: str | None = None, description: str = "") -> Callable[[F], F]:
        """Decorator: instrumenta una función usada en `.pipe()` como sub-paso del step activo.

        Emite eventos `pipe_started`/`pipe_finished` cuyo `step_id` apunta al
        step actualmente en ejecución (el que decoró la función llamadora con
        `@logger.step(...)`). Fuera de un step activo, el decorator es no-op,
        así los tests unitarios siguen llamando los pipes sin instrumentación.

        El nombre por defecto es `func.__name__`. La descripción es opcional
        pero recomendada — aparece en el viewer al hacer hover sobre el pipe.
        Convención: el primer argumento posicional es el DataFrame de entrada;
        de ahí se infieren `rows_in` / `rows_out`.
        """
        def decorator(func: F) -> F:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                # No-op si no hay ejecución activa o no hay step padre:
                # los pipes se llaman tal cual en los tests de transformaciones puras.
                if self._fh is None or self._current_step is None:
                    return func(*args, **kwargs)

                parent = self._current_step
                self._pipe_counter += 1
                pipe_order = self._pipe_counter
                pipe_id = f"{parent['step_id']}.{func.__name__}"
                pipe_name = name or func.__name__
                rows_in = _safe_len(args[0]) if args else None

                t0 = time.monotonic()
                self._emit(
                    event="pipe_started",
                    level="info",
                    status="running",
                    layer=parent["layer"],
                    layer_order=parent["layer_order"],
                    step_id=parent["step_id"],
                    pipe_id=pipe_id,
                    pipe_name=pipe_name,
                    pipe_order=pipe_order,
                    description=description,
                    rows_in=rows_in,
                )

                try:
                    result = func(*args, **kwargs)
                except BaseException as exc:
                    self._emit(
                        event="pipe_finished",
                        level="error",
                        status="error",
                        layer=parent["layer"],
                        layer_order=parent["layer_order"],
                        step_id=parent["step_id"],
                        pipe_id=pipe_id,
                        pipe_name=pipe_name,
                        pipe_order=pipe_order,
                        description=description,
                        duration_ms=int((time.monotonic() - t0) * 1000),
                        rows_in=rows_in,
                        error={
                            "type": type(exc).__name__,
                            "message": str(exc),
                            "traceback": "".join(traceback.format_exception(exc)),
                        },
                    )
                    raise

                rows_out = _safe_len(result)
                self._emit(
                    event="pipe_finished",
                    level="info",
                    status="success",
                    layer=parent["layer"],
                    layer_order=parent["layer_order"],
                    step_id=parent["step_id"],
                    pipe_id=pipe_id,
                    pipe_name=pipe_name,
                    pipe_order=pipe_order,
                    description=description,
                    duration_ms=int((time.monotonic() - t0) * 1000),
                    rows_in=rows_in,
                    rows_out=rows_out,
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
            run_dir = root / f"run-{self._execution_id:06d}"
            run_dir.mkdir(parents=True, exist_ok=True)
            info_path = run_dir / f"run-{self._execution_id:06d}-info.jsonl"
            usage_path = run_dir / f"run-{self._execution_id:06d}-usage.jsonl"
            self._fh = info_path.open("a", encoding="utf-8")
            self._usage_fh = usage_path.open("a", encoding="utf-8")
            self._t_started = time.monotonic()
            self._t_started_iso = _now_iso()
            self._layers_seen.clear()
            self._layers_completed.clear()
            self._emit(event="execution_started", level="info")
            # Index entry points at the info file (the viewer fetches this path
            # to replay events). Path is relative to _logs/ for portability.
            rel_info = f"{run_dir.name}/{info_path.name}"
            _index_upsert(
                root,
                {
                    "id": self._execution_id,
                    "pipeline": pipeline,
                    "file": rel_info,
                    "started_at": self._t_started_iso,
                    "finished_at": None,
                    "status": "running",
                    "layers_completed": 0,
                    "layers_seen": 0,
                },
            )
            # Start CPU/memory/disk sampler — runs until _close() stops it.
            self._sampler = _UsageSampler(
                self._usage_fh,
                self._execution_id,
                disk_path=root,
            )
            self._sampler.start()

    def _close(self, *, status: str) -> None:
        with self._lock:
            if self._fh is None:
                return
            # Stop sampling before we close the usage file handle.
            if self._sampler is not None:
                self._sampler.stop()
                self._sampler = None
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
            rel_info = f"run-{self._execution_id:06d}/run-{self._execution_id:06d}-info.jsonl"
            _index_upsert(
                _logs_root(),
                {
                    "id": self._execution_id,
                    "pipeline": self._pipeline,
                    "file": rel_info,
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
            if self._usage_fh is not None:
                self._usage_fh.close()
                self._usage_fh = None
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
