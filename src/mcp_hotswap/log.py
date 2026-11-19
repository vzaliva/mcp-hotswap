"""Optional debug log: spawns, stops, cache hits and misses, and (with
``--trace``) every frame, so it is easy to see what a client sends at
connect time and whether the cache covers it."""

from __future__ import annotations

import os
import sys
import time


class Log:
    def __init__(self, path: str | None, trace: bool = False):
        self.trace_enabled = bool(path) and trace
        self.file = None
        if path:
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            self.file = open(path, "a", encoding="utf-8", buffering=1)
        self.pid = os.getpid()

    def __call__(self, event: str, **fields) -> None:
        if self.file is None:
            return
        now = time.time()
        stamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(now)) + f".{int(now * 1000) % 1000:03d}"
        parts = [stamp, str(self.pid), event]
        parts += [f"{k}={v}" for k, v in fields.items()]
        try:
            self.file.write(" ".join(parts) + "\n")
        except (OSError, ValueError):
            pass

    def frame(self, direction: str, raw: bytes) -> None:
        if self.trace_enabled:
            self(direction, frame=raw.decode("utf-8", "replace").rstrip("\n"))

    @staticmethod
    def warn(message: str) -> None:
        """Visible in the client's MCP log (the wrapper's stderr)."""
        try:
            sys.stderr.write(f"mcp-hotswap: {message}\n")
            sys.stderr.flush()
        except (OSError, ValueError):
            pass
