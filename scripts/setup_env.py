"""Bootstrap the development environment.

1. Check whether `uv` is importable. If not, install it via pip.
2. Run `python -m uv sync --all-groups` to create .venv and install all deps.

Run from VS Code via the "Install dependencies (uv sync)" launch config,
or directly: `python scripts/setup_env.py`.
"""

from __future__ import annotations

import subprocess
import sys
from importlib.util import find_spec


def _have_uv() -> bool:
    return find_spec("uv") is not None


def _run(*args: str) -> int:
    print(f"\n$ {' '.join(args)}", flush=True)
    return subprocess.call(list(args))


def main() -> int:
    if _have_uv():
        print("uv is already installed.")
    else:
        print("uv not found — installing via pip...")
        rc = _run(sys.executable, "-m", "pip", "install", "--upgrade", "uv")
        if rc != 0:
            print("Failed to install uv.", file=sys.stderr)
            return rc

    rc = _run(sys.executable, "-m", "uv", "sync", "--all-groups")
    if rc != 0:
        print("uv sync failed.", file=sys.stderr)
        return rc

    print("\nEnvironment ready. Activate with:")
    if sys.platform == "win32":
        print("  PowerShell:  .venv\\Scripts\\Activate.ps1")
        print("  Git Bash:    source .venv/Scripts/activate")
    else:
        print("  source .venv/bin/activate")
    print("Or just prefix commands with `uv run ...`.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
