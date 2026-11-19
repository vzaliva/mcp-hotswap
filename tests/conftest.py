import json
import os
import queue
import signal
import subprocess
import sys
import threading
import time

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
FAKE = os.path.join(HERE, "fake_server.py")
LAUNCHER = os.path.join(HERE, "fake_launcher.py")

LEGACY_INIT = {
    "protocolVersion": "2025-11-25",
    "capabilities": {"roots": {"listChanged": True}},
    "clientInfo": {"name": "test-client", "version": "1.0"},
}
MODERN_META = {
    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
    "io.modelcontextprotocol/clientInfo": {"name": "test-client", "version": "1.0"},
    "io.modelcontextprotocol/clientCapabilities": {"roots": {"listChanged": True}},
}


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    # A zombie still answers kill(0); check its state.
    out = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return bool(out) and not out.startswith("Z")


def wait_until(pred, timeout=10.0, step=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(step)
    return pred()


class Env:
    """Paths and env for one test: a private cache dir and fake-server log."""

    def __init__(self, tmp_path):
        self.tmp = tmp_path
        self.cache = tmp_path / "cache"
        self.fake_log = tmp_path / "fake.jsonl"
        self.debug_log = tmp_path / "debug.log"
        self.vars = {"FAKE_LOG": str(self.fake_log)}

    def events(self, ev=None):
        if not self.fake_log.exists():
            return []
        out = [json.loads(line) for line in self.fake_log.read_text().splitlines() if line.strip()]
        return [e for e in out if ev is None or e["ev"] == ev]

    def starts(self):
        return [e["pid"] for e in self.events("start")]

    def received(self, pid=None):
        return [e["msg"] for e in self.events("recv") if pid is None or e["pid"] == pid]

    def signals(self, pid=None):
        return [e["sig"] for e in self.events("signal") if pid is None or e["pid"] == pid]

    def wrapper(self, *opts, server=None, extra_env=None):
        return Wrapper(self, list(opts), server or [sys.executable, FAKE], extra_env or {})

    def debug(self):
        return self.debug_log.read_text() if self.debug_log.exists() else ""


class Wrapper:
    def __init__(self, env: Env, opts, server, extra_env):
        self.env = env
        e = dict(os.environ)
        e.update(env.vars)
        e.update(extra_env)
        cmd = [sys.executable, "-m", "mcp_hotswap", "--cache-dir", str(env.cache),
               "--debug-log", str(env.debug_log), "--trace", *opts, "--", *server]
        self.proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=e)
        self.inbox: queue.Queue = queue.Queue()
        self.seen: list = []
        self.next_id = 100
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.proc.stdout:
            self.inbox.put(json.loads(line))
        self.inbox.put(None)

    def send(self, msg):
        self.proc.stdin.write((json.dumps(msg) + "\n").encode())
        self.proc.stdin.flush()

    def notify(self, method, params=None):
        msg = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        self.send(msg)

    def request_async(self, method, params=None, id_=None):
        if id_ is None:
            self.next_id += 1
            id_ = self.next_id
        msg = {"jsonrpc": "2.0", "id": id_, "method": method}
        if params is not None:
            msg["params"] = params
        self.send(msg)
        return id_

    def request(self, method, params=None, id_=None, timeout=10.0):
        id_ = self.request_async(method, params, id_)
        return self.wait_response(id_, timeout)

    def wait_for(self, pred, timeout=10.0):
        for msg in self.seen:
            if pred(msg):
                self.seen.remove(msg)
                return msg
        deadline = time.time() + timeout
        while True:
            left = deadline - time.time()
            if left <= 0:
                raise AssertionError(f"timed out; seen={self.seen}\n--- debug log ---\n{self.env.debug()}")
            try:
                msg = self.inbox.get(timeout=left)
            except queue.Empty:
                continue
            if msg is None:
                raise AssertionError(f"wrapper closed stdout; seen={self.seen}\n{self.env.debug()}")
            if pred(msg):
                return msg
            self.seen.append(msg)

    def wait_response(self, id_, timeout=10.0):
        return self.wait_for(lambda m: "method" not in m and m.get("id") == id_, timeout)

    def drain(self, wait=0.3):
        time.sleep(wait)
        while True:
            try:
                msg = self.inbox.get_nowait()
            except queue.Empty:
                break
            if msg is not None:
                self.seen.append(msg)
        return self.seen

    # --- typical client sessions -------------------------------------------

    def legacy_connect(self, version="2025-11-25", probe=False):
        if probe:
            self.request("server/discover", {"_meta": MODERN_META}, id_="server-discover-probe-1")
        params = dict(LEGACY_INIT, protocolVersion=version)
        init = self.request("initialize", params, id_=0)
        self.notify("notifications/initialized")
        tools = self.request("tools/list", id_=1)
        return init, tools

    def modern_connect(self):
        disc = self.request("server/discover", {"_meta": MODERN_META}, id_="server-discover-probe-1")
        tools = self.request("tools/list", {"_meta": MODERN_META}, id_=0)
        return disc, tools

    def call(self, name, arguments=None, modern=False, timeout=10.0):
        params = {"name": name, "arguments": arguments or {}}
        if modern:
            params["_meta"] = MODERN_META
        return self.request("tools/call", params, timeout=timeout)

    def close(self, timeout=10.0):
        try:
            self.proc.stdin.close()
        except BrokenPipeError:
            pass
        return self.proc.wait(timeout)

    def signal(self, sig):
        self.proc.send_signal(sig)


@pytest.fixture
def env(tmp_path):
    e = Env(tmp_path)
    yield e
    # Never leave a fake server behind (only the ones this test started).
    for pid in set(e.starts()):
        if alive(pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def text_of(resp):
    return resp["result"]["content"][0]["text"]
