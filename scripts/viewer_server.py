"""Local server for the pipeline viewer.

Serves project files (so `viewer.html` can fetch `data/_logs/*`), plus:
- `/open?path=<rel>` — opens the given relative path in the OS file explorer.
- `/run?layer=<bronze|silver|gold|all>` — spawns the corresponding pipeline
  command in a new terminal window so the user can watch its output live.

Run:
    python scripts/viewer_server.py [port]
"""

from __future__ import annotations

import http.server
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


class ViewerHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/open"):
            self._handle_open()
            return
        if self.path.startswith("/run"):
            self._handle_run()
            return
        super().do_GET()

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
