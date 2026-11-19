"""Discovery answered from the cache, without starting the server."""

import sys

from conftest import FAKE, MODERN_META


def prime(env, connect="legacy", **kw):
    w = env.wrapper(**kw)
    if connect == "legacy":
        w.legacy_connect()
    elif connect == "probe-legacy":
        w.legacy_connect(probe=True)
    else:
        w.modern_connect()
    assert w.close() == 0
    return w


def test_first_session_passes_through_and_fills_cache(env):
    w = env.wrapper()
    init, tools = w.legacy_connect()
    assert init["result"]["serverInfo"]["name"] == "fake"
    assert [t["name"] for t in tools["result"]["tools"]][0] == "echo"
    assert len(env.starts()) == 1  # no cache yet: started at once
    assert w.close() == 0
    files = list(env.cache.glob("*.json"))
    assert len(files) == 1
    assert oct(files[0].stat().st_mode & 0o777) == "0o600"


def test_cached_legacy_session_does_not_spawn(env):
    prime(env)
    first = env.received()
    w = env.wrapper()
    init, tools = w.legacy_connect()
    assert w.request("ping")["result"] == {}
    assert w.request("logging/setLevel", {"level": "debug"})["result"] == {}
    assert init["result"]["protocolVersion"] == "2025-11-25"
    assert len(tools["result"]["tools"]) == 6
    assert w.close() == 0
    assert len(env.starts()) == 1, "second session must not start the server"
    assert env.received() == first
    assert "cache-hit method=initialize" in env.debug()


def test_discover_probe_then_legacy_fallback_is_cached(env):
    """Modern clients probe server/discover first; a legacy server answers -32601."""
    prime(env, connect="probe-legacy")
    w = env.wrapper()
    probe = w.request("server/discover", {"_meta": MODERN_META}, id_="server-discover-probe-1")
    assert probe["error"]["code"] == -32601
    init, tools = w.legacy_connect()
    assert "result" in init and "result" in tools
    w.close()
    assert len(env.starts()) == 1


def test_modern_session_is_cached(env):
    env.vars["FAKE_ERA"] = "dual"
    prime(env, connect="modern")
    w = env.wrapper()
    disc, tools = w.modern_connect()
    assert disc["result"]["supportedVersions"] == ["2026-07-28"]
    assert tools["result"]["cacheScope"] == "private"
    w.close()
    assert len(env.starts()) == 1


def test_both_eras_share_one_cache_file(env):
    """A modern and a legacy client can use the same dual-era server."""
    env.vars["FAKE_ERA"] = "dual"
    prime(env, connect="modern")
    prime(env, connect="legacy")
    w = env.wrapper()
    w.modern_connect()
    w.close()
    w = env.wrapper()
    w.legacy_connect()
    w.close()
    assert len(env.starts()) == 2
    assert len(list(env.cache.glob("*.json"))) == 1


def test_meta_progress_token_does_not_split_the_cache(env):
    """Some clients send tools/list with _meta.progressToken, others send no params."""
    prime(env)
    w = env.wrapper()
    w.request("initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
                             "clientInfo": {"name": "codex", "version": "9"}}, id_=0)
    w.notify("notifications/initialized")
    tools = w.request("tools/list", {"_meta": {"progressToken": 0}}, id_=1)
    assert "result" in tools
    w.close()
    assert len(env.starts()) == 1


def test_other_protocol_version_spawns(env):
    """A version the cache has not seen is not guessed: the server answers."""
    prime(env)
    w = env.wrapper()
    init = w.request("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                    "clientInfo": {"name": "x", "version": "1"}}, id_=0)
    assert init["result"]["protocolVersion"] == "2025-06-18"
    w.close()
    assert len(env.starts()) == 2


def test_key_env_selects_cache_file(env):
    w = env.wrapper("--key-env", "SOME_HOME", extra_env={"SOME_HOME": "/a"})
    w.legacy_connect()
    w.close()
    w = env.wrapper("--key-env", "SOME_HOME", extra_env={"SOME_HOME": "/b"})
    w.legacy_connect()
    w.close()
    assert len(env.starts()) == 2
    assert len(list(env.cache.glob("*.json"))) == 2
    # Variables not named with --key-env do not matter.
    w = env.wrapper("--key-env", "SOME_HOME", extra_env={"SOME_HOME": "/a", "UNRELATED": "x"})
    w.legacy_connect()
    w.close()
    assert len(env.starts()) == 2


def test_transient_list_error_is_not_cached(env):
    env.vars["FAKE_LIST_ERROR"] = "-32603"
    prime(env)
    w = env.wrapper()
    _, tools = w.legacy_connect()
    assert tools["error"]["code"] == -32603
    w.close()
    assert len(env.starts()) == 2


def test_method_not_found_list_is_cached(env):
    """Codex asks for resource lists even when the server has none."""
    w = env.wrapper()
    w.legacy_connect()
    assert w.request("resources/list")["error"]["code"] == -32601
    w.close()
    w = env.wrapper()
    w.legacy_connect()
    assert w.request("resources/list")["error"]["code"] == -32601
    w.close()
    assert len(env.starts()) == 1


def test_paginated_list_is_joined(env):
    env.vars["FAKE_PAGE_SIZE"] = "2"
    w = env.wrapper()
    w.legacy_connect()  # first page (with nextCursor) passes through
    cursor = "2"
    names = []
    while cursor:
        page = w.request("tools/list", {"cursor": cursor})
        names += [t["name"] for t in page["result"]["tools"]]
        cursor = page["result"].get("nextCursor")
    w.close()
    w = env.wrapper()
    _, tools = w.legacy_connect()
    w.close()
    assert len(env.starts()) == 1
    assert "nextCursor" not in tools["result"]
    assert [t["name"] for t in tools["result"]["tools"]] == ["echo", "sleep", "ask_roots", "counter", "pid", "crash"]


def test_show_cache(env):
    import subprocess
    prime(env)
    out = subprocess.run([sys.executable, "-m", "mcp_hotswap", "--cache-dir", str(env.cache), "--show-cache",
                          "--", sys.executable, FAKE], capture_output=True, text=True).stdout
    assert "initialize" in out and "tools/list" in out


def test_concurrent_first_sessions_start_one_server(env):
    """Several sessions (e.g. sub-agents) opening at once with no cache:
    one starts the server and records it, the others wait and use the cache."""
    import threading
    env.vars["FAKE_START_DELAY"] = "1.0"
    ws = [env.wrapper() for _ in range(3)]
    results = []

    def connect(w):
        results.append(w.legacy_connect())

    threads = [threading.Thread(target=connect, args=(w,)) for w in ws]
    for t in threads:
        t.start()
    for t in threads:
        t.join(20)
    assert len(results) == 3
    assert all("result" in tools for _, tools in results)
    assert len(env.starts()) == 1
    for w in ws:
        w.close()
