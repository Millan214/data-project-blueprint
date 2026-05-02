"""Local server for the pipeline viewer.

Serves project files (so `viewer.html` can fetch `data/_logs/*`) AND adds a
small `/open?path=<rel>` endpoint that opens the given path in the OS file
explorer. The viewer's path links call this endpoint instead of navigating.

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


def _open_native(target: Path) -> None:
    if sys.platform == "win32":
        os.startfile(str(target))  # noqa: S606  (windows-specific)
    elif sys.platform == "darwin":
        subprocess.run(["open", str(target)], check=False)
    else:
        subprocess.run(["xdg-open", str(target)], check=False)


class ViewerHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/open"):
            self._handle_open()
            return
        super().do_GET()

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
