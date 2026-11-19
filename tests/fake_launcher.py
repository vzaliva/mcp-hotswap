"""Stands in for uvx / npx / uv run: starts the fake server as its own child
(same stdin/stdout, same process group) and forwards SIGTERM/SIGINT to it."""

import os
import signal
import subprocess
import sys

child = subprocess.Popen([sys.executable, os.path.join(os.path.dirname(__file__), "fake_server.py")])


def forward(sig, _frame):
    try:
        child.send_signal(sig)
    except ProcessLookupError:
        pass


signal.signal(signal.SIGTERM, forward)
signal.signal(signal.SIGINT, forward)
sys.exit(child.wait())
