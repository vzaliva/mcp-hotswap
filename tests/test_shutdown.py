"""Shutdown: client EOF, signals (one SIGTERM at most), wrapper SIGKILL."""

import os
import signal
import sys
import time

from conftest import LAUNCHER, alive, wait_until


def started(env, **kw):
    """A wrapper whose real server is running (no cache: starts at once)."""
    w = env.wrapper(**kw)
    w.legacy_connect()
    pid = env.starts()[-1]
    assert alive(pid)
    return w, pid


def test_client_eof_closes_server_stdin_without_signals(env):
    w, pid = started(env)
    assert w.close() == 0
    assert not alive(pid)
    assert env.signals(pid) == []
    assert [e["why"] for e in env.events("exit") if e["pid"] == pid] == ["eof"]


def test_client_eof_while_cold_exits_at_once(env):
    w = env.wrapper()
    w.legacy_connect()
    w.close()
    w = env.wrapper()
    w.legacy_connect()  # from cache
    t = time.time()
    assert w.close() == 0
    assert time.time() - t < 2


def test_client_signal_burst_reaches_server_as_eof_only(env):
    """Observed client shutdown: SIGINT, SIGTERM (+100 ms), SIGTERM (+350 ms)."""
    w, pid = started(env)
    w.signal(signal.SIGINT)
    time.sleep(0.1)
    w.signal(signal.SIGTERM)
    time.sleep(0.25)
    w.signal(signal.SIGTERM)
    assert w.proc.wait(5) == 0
    assert wait_until(lambda: not alive(pid), 3)
    assert env.signals(pid) == []  # in its own session; it only saw stdin close
    assert "signal sig=SIGINT" in env.debug()


def test_exactly_one_sigterm_when_server_ignores_eof(env):
    env.vars["FAKE_IGNORE_EOF"] = "1"
    w, pid = started(env, )
    w.signal(signal.SIGTERM)
    w.signal(signal.SIGTERM)
    w.signal(signal.SIGINT)
    assert w.proc.wait(5) == 0
    assert wait_until(lambda: not alive(pid), 3)
    assert env.signals(pid) == [signal.SIGTERM]


def test_sigkill_when_server_ignores_eof_and_sigterm(env):
    env.vars.update(FAKE_IGNORE_EOF="1", FAKE_IGNORE_TERM="1")
    w, pid = started(env, )
    t = time.time()
    w.close()
    w.proc.wait(10)
    assert wait_until(lambda: not alive(pid), 3)
    assert env.signals(pid) == [signal.SIGTERM]
    assert time.time() - t >= 2  # waited out the grace periods
    assert "how=sigkill" in env.debug()


def test_launcher_child_gets_single_sigterm(env):
    """With a signal-forwarding launcher (uvx, npx), the real server must
    get one SIGTERM, not one from the group plus one forwarded."""
    env.vars["FAKE_IGNORE_EOF"] = "1"
    w = env.wrapper("--stop-grace", "0.3s", server=[sys.executable, LAUNCHER])
    w.legacy_connect()
    real = env.starts()[-1]
    assert w.close() == 0
    assert wait_until(lambda: not alive(real), 3)
    assert env.signals(real) == [signal.SIGTERM]


def test_server_exits_when_wrapper_is_sigkilled(env):
    w, pid = started(env)
    os.kill(w.proc.pid, signal.SIGKILL)
    w.proc.wait(5)
    assert wait_until(lambda: not alive(pid), 5), "server must see stdin EOF and exit"
    assert env.signals(pid) == []


def test_idle_stop_uses_eof_then_single_sigterm(env):
    env.vars["FAKE_IGNORE_EOF"] = "1"
    w = env.wrapper()
    w.legacy_connect()
    w.close()
    w = env.wrapper("--idle", "0.5s", "--stop-grace", "0.3s")
    w.legacy_connect()
    w.call("echo")
    pid = env.starts()[-1]
    assert wait_until(lambda: not alive(pid), 6)
    assert env.signals(pid) == [signal.SIGTERM]
    w.call("echo")  # transparently restarted
    assert len(env.starts()) == 3
    w.close()
