"""Local server for the pipeline viewer.

Serves project files (so `viewer.html` can fetch `data/_logs/*`), plus:
- `/open?path=<rel>` — opens the given relative path in the OS file explorer.
- `/run?layer=<bronze|silver|gold|all>` — spawns the corresponding pipeline
  command in a new terminal window so the user can watch its output live.
- `/parquet?path=<rel>&limit=N` — reads a parquet file/directory inside the
  project and returns up to N rows as JSON. Used by the viewer to render
  inline sample tables for quarantine / silver / gold outputs.

Run:
    python scripts/viewer_server.py [port]
"""

from __future__ import annotations

import http.server
import json
import os
import socketserver
import subprocess
import sys
import urllib.parse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

ALLOWED_LAYERS = {"bronze", "silver", "gold", "all"}


def _open_native(target: Path) -> None:
    if sys.platform == "win32":
        os.startfile(str(target))  # noqa: S606  (windows-specific)
    elif sys.platform == "darwin":
        subprocess.run(["open", str(target)], check=False)
    else:
        subprocess.run(["xdg-open", str(target)], check=False)


def _spawn_pipeline(layer: str) -> None:
    """Spawn `uv run medallion-etl <layer>` in a new terminal window.

    `layer` is validated against ALLOWED_LAYERS by the caller, so it is
    safe to embed in the platform-specific command string.
    """
    cmd_args = ["uv", "run", "medallion-etl", layer]
    if sys.platform == "win32":
        # `start` with a window title; `cmd /c` runs the command and CLOSES the
        # window when it finishes. We append a short timeout so the user can
        # glance at the final lines before the window disappears.
        subprocess.Popen(
            f'start "Pipeline · {layer}" cmd /c "uv run medallion-etl {layer} & timeout /t 3 /nobreak >nul"',
            shell=True,
            cwd=str(PROJECT_ROOT),
        )
    elif sys.platform == "darwin":
        script = f'tell application "Terminal" to do script "cd {PROJECT_ROOT} && uv run medallion-etl {layer}"'
        subprocess.Popen(["osascript", "-e", script])
    else:
        # Try a few common Linux terminal emulators; fall back to backgrounding
        # the command if none is found.
        for term in ("x-terminal-emulator", "gnome-terminal", "konsole", "xterm"):
            if subprocess.run(["which", term], capture_output=True).returncode == 0:
                subprocess.Popen([term, "-e", *cmd_args], cwd=str(PROJECT_ROOT))
                return
        subprocess.Popen(cmd_args, cwd=str(PROJECT_ROOT))


def _resolve_safe(rel: str) -> Path | None:
    """Resolve `rel` against PROJECT_ROOT and ensure it stays inside.

    Returns None if the path escapes the project root. Tolerates both
    forward- and back-slashed inputs (paths captured on Windows often
    arrive with `\\` from the JSONL event field).
    """
    if not rel:
        return None
    target = (PROJECT_ROOT / rel.replace("\\", "/")).resolve()
    try:
        target.relative_to(PROJECT_ROOT)
    except ValueError:
        return None
    return target


def _read_parquet_sample(target: Path, limit: int) -> dict:
    """Read a parquet file or partitioned directory, return up to `limit` rows
    plus the total row count and column names. Values are JSON-coerced."""
    import pandas as pd
    df = pd.read_parquet(target)
    total = len(df)
    sample = df.head(limit)
    rows: list[dict] = []
    for _, row in sample.iterrows():
        out: dict = {}
        for col, val in row.items():
            if pd.isna(val):
                out[col] = None
            elif hasattr(val, "isoformat"):
                out[col] = val.isoformat()
            elif isinstance(val, (int, float, bool, str)):
                out[col] = val
            elif isinstance(val, (list, dict)):
                out[col] = val
            else:
                out[col] = str(val)
        rows.append(out)
    return {
        "columns": [str(c) for c in df.columns],
        "dtypes": {str(c): str(df[c].dtype) for c in df.columns},
        "total": int(total),
        "rows": rows,
    }


class ViewerHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/open"):
            self._handle_open()
            return
        if self.path.startswith("/run"):
            self._handle_run()
            return
        if self.path.startswith("/parquet"):
            self._handle_parquet()
            return
        super().do_GET()

    def _handle_parquet(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        rel = params.get("path", [""])[0]
        try:
            limit = max(1, min(int(params.get("limit", ["20"])[0]), 500))
        except ValueError:
            limit = 20
        target = _resolve_safe(rel)
        if target is None:
            self.send_error(400, "missing or invalid path")
            return
        if not target.exists():
            self.send_error(404, f"path not found: {rel}")
            return
        try:
            data = _read_parquet_sample(target, limit)
        except ImportError:
            self.send_error(500, "pandas/pyarrow not installed in server env")
            return
        except Exception as exc:
            self.send_error(500, f"read failed: {exc}")
            return
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def _handle_run(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        layer = params.get("layer", [""])[0]
        if layer not in ALLOWED_LAYERS:
            self.send_error(400, f"layer must be one of {sorted(ALLOWED_LAYERS)}")
            return
        try:
            _spawn_pipeline(layer)
        except Exception as exc:  # pragma: no cover
            self.send_error(500, f"failed to spawn: {exc}")
            return
        self.send_response(204)
        self.end_headers()

    def _handle_open(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        rel = params.get("path", [""])[0]
        if not rel:
            self.send_error(400, "missing path")
            return

        target = (PROJECT_ROOT / rel).resolve()
        try:
            target.relative_to(PROJECT_ROOT)
        except ValueError:
            self.send_error(403, "path is outside project root")
            return

        if not target.exists():
            self.send_error(404, f"path not found: {rel}")
            return

        try:
            _open_native(target)
        except Exception as exc:  # pragma: no cover
            self.send_error(500, f"failed to open: {exc}")
            return

        self.send_response(204)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        # Quieter than the default — keep the "Serving HTTP on …" header line.
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), format % args))


def main() -> int:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    os.chdir(PROJECT_ROOT)
    with socketserver.TCPServer(("", port), ViewerHandler) as srv:
        srv.allow_reuse_address = True
        print(
            f"Serving HTTP on 0.0.0.0 port {port} (http://localhost:{port}/viewer.html) ...",
            flush=True,
        )
        try:
            srv.serve_forever()
        except KeyboardInterrupt:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
