"""Spawn on first use, handshake replay, relaying, refresh, idle stop."""

import json
import threading
import time

from conftest import MODERN_META, alive, text_of, wait_until


def primed(env, connect="legacy", **kw):
    w = env.wrapper(**kw)
    w.legacy_connect() if connect == "legacy" else w.modern_connect()
    w.close()
    return len(env.starts())


def test_spawn_on_call_replays_handshake_with_own_ids(env):
    primed(env)
    w = env.wrapper()
    w.legacy_connect()
    w.request("logging/setLevel", {"level": "warning"})
    assert len(env.starts()) == 1
    resp = w.call("echo", {"x": 1})
    assert json.loads(text_of(resp)) == {"x": 1}
    pid = env.starts()[-1]
    msgs = env.received(pid)
    methods = [m.get("method") for m in msgs]
    assert methods[:4] == ["initialize", "notifications/initialized", "logging/setLevel", "tools/call"] or \
        methods[:4] == ["initialize", "notifications/initialized", "tools/call", "logging/setLevel"]
    init = msgs[0]
    assert isinstance(init["id"], str) and init["id"].startswith("mcp-hotswap:")
    assert init["params"]["protocolVersion"] == "2025-11-25"
    assert init["params"]["clientInfo"]["name"] == "test-client"
    call = next(m for m in msgs if m.get("method") == "tools/call")
    assert call["id"] == resp["id"]  # the client's own id, untouched
    # Nothing addressed to the wrapper's ids leaked to the client.
    for m in w.drain():
        assert not str(m.get("id", "")).startswith("mcp-hotswap:")
    w.close()


def test_modern_session_spawns_without_handshake(env):
    env.vars["FAKE_ERA"] = "dual"
    primed(env, connect="modern")
    w = env.wrapper()
    w.modern_connect()
    resp = w.call("echo", {"y": 2}, modern=True)
    assert json.loads(text_of(resp)) == {"y": 2}
    pid = env.starts()[-1]
    methods = [m.get("method") for m in env.received(pid)]
    assert "initialize" not in methods and "notifications/initialized" not in methods
    assert methods[0] == "tools/call"
    w.close()


def test_requests_queued_during_start_are_flushed_in_order(env):
    primed(env)
    env.vars["FAKE_START_DELAY"] = "1.0"
    w = env.wrapper()
    w.legacy_connect()
    ids = [w.request_async("tools/call", {"name": "echo", "arguments": {"n": i}}) for i in range(3)]
    # Discovery is still answered from the cache while the server starts.
    assert "result" in w.request("tools/list", timeout=0.8)
    for i, id_ in enumerate(ids):
        assert json.loads(text_of(w.wait_response(id_))) == {"n": i}
    pid = env.starts()[-1]
    calls = [m["params"]["arguments"]["n"] for m in env.received(pid) if m.get("method") == "tools/call"]
    assert calls == [0, 1, 2]
    w.close()


def test_cancelled_queued_request_is_dropped(env):
    primed(env)
    env.vars["FAKE_START_DELAY"] = "1.0"
    w = env.wrapper()
    w.legacy_connect()
    a = w.request_async("tools/call", {"name": "echo", "arguments": {"a": 1}})
    b = w.request_async("tools/call", {"name": "echo", "arguments": {"b": 1}})
    w.notify("notifications/cancelled", {"requestId": a, "reason": "test"})
    w.wait_response(b)
    pid = env.starts()[-1]
    sent = [m.get("id") for m in env.received(pid) if m.get("method") == "tools/call"]
    assert sent == [b]
    w.close()


def test_server_to_client_request_is_relayed(env):
    primed(env)
    w = env.wrapper()
    w.legacy_connect()
    call_id = w.request_async("tools/call", {"name": "ask_roots", "arguments": {}})
    ask = w.wait_for(lambda m: m.get("method") == "roots/list")
    assert ask["id"].startswith("srv-")
    w.send({"jsonrpc": "2.0", "id": ask["id"], "result": {"roots": [{"uri": "file:///tmp"}]}})
    resp = w.wait_response(call_id)
    assert json.loads(text_of(resp))["result"]["roots"][0]["uri"] == "file:///tmp"
    w.close()


def test_refresh_sends_list_changed_and_rewrites_cache(env, tmp_path):
    tools_file = tmp_path / "tools.json"
    tools_file.write_text(json.dumps(["echo"]))
    env.vars["FAKE_TOOLS"] = str(tools_file)
    env.vars["FAKE_LIST_CHANGED"] = "1"
    primed(env)
    tools_file.write_text(json.dumps(["echo", "newtool"]))  # "upgrade" the server
    w = env.wrapper()
    _, tools = w.legacy_connect()
    assert [t["name"] for t in tools["result"]["tools"]] == ["echo"]  # stale, from cache
    w.call("echo")
    w.wait_for(lambda m: m.get("method") == "notifications/tools/list_changed")
    fresh = w.request("tools/list")
    assert [t["name"] for t in fresh["result"]["tools"]] == ["echo", "newtool"]
    w.close()
    w = env.wrapper()
    _, tools = w.legacy_connect()
    w.close()
    assert [t["name"] for t in tools["result"]["tools"]] == ["echo", "newtool"]
    assert len(env.starts()) == 2  # the third session was served from the refreshed cache


def test_refresh_without_change_sends_nothing(env):
    env.vars["FAKE_LIST_CHANGED"] = "1"
    primed(env)
    w = env.wrapper()
    w.legacy_connect()
    w.call("echo")
    time.sleep(0.5)
    assert not any(m.get("method", "").endswith("list_changed") for m in w.drain())
    w.close()


def test_server_upgrade_forgets_unchecked_entries(env):
    """A changed server drops cache entries the refresh could not re-check
    (here: the modern probe, which a legacy session must never send)."""
    w = env.wrapper()
    w.legacy_connect(probe=True)  # legacy server: the probe gets -32601
    w.close()
    env.vars.update(FAKE_VERSION="2.0", FAKE_ERA="dual")  # upgraded in place
    w = env.wrapper()
    w.request("server/discover", {"_meta": MODERN_META}, id_="server-discover-probe-1")
    w.legacy_connect()
    w.call("echo")
    w.close()
    pid = env.starts()[-1]
    assert "server/discover" not in [m.get("method") for m in env.received(pid)]
    w = env.wrapper()
    probe = w.request("server/discover", {"_meta": MODERN_META}, id_="server-discover-probe-1")
    w.close()
    assert len(env.starts()) == 3, "the dropped probe entry is learned again"
    assert probe["result"]["supportedVersions"] == ["2026-07-28"]


def test_modern_refresh_updates_cache_without_notification(env, tmp_path):
    tools_file = tmp_path / "tools.json"
    tools_file.write_text(json.dumps(["echo"]))
    env.vars.update(FAKE_ERA="dual", FAKE_TOOLS=str(tools_file), FAKE_LIST_CHANGED="1")
    primed(env, connect="modern")
    tools_file.write_text(json.dumps(["echo", "newtool"]))
    w = env.wrapper()
    w.modern_connect()
    w.call("echo", modern=True)
    assert wait_until(lambda: "cache-refreshed" in env.debug(), 5)
    assert not any(m.get("method", "").endswith("list_changed") for m in w.drain())
    w.close()
    w = env.wrapper()
    _, tools = w.modern_connect()
    w.close()
    assert [t["name"] for t in tools["result"]["tools"]] == ["echo", "newtool"]


def test_idle_stop_and_transparent_respawn(env):
    primed(env)
    w = env.wrapper("--idle", "1s")
    w.legacy_connect()
    assert text_of(w.call("counter")) == "1"
    first = env.starts()[-1]
    assert wait_until(lambda: not alive(first), 6), "server should stop after the idle timeout"
    assert [e["why"] for e in env.events("exit") if e["pid"] == first] == ["eof"]
    assert env.signals(first) == []
    # The next call starts a fresh server, with the handshake replayed.
    assert text_of(w.call("counter")) == "1"  # (state does not survive a stop)
    second = env.starts()[-1]
    assert second != first
    methods = [m.get("method") for m in env.received(second)]
    assert methods[:2] == ["initialize", "notifications/initialized"]
    assert "spawn trigger=tools/call" in env.debug()
    w.close()


def test_idle_cached_discovery_does_not_start_or_keep_alive(env):
    primed(env)
    w = env.wrapper("--idle", "1s")
    w.legacy_connect()
    w.call("echo")
    pid = env.starts()[-1]
    # Polling the cache does not count as activity.
    deadline = time.time() + 2.5
    while time.time() < deadline and alive(pid):
        w.request("ping")
        time.sleep(0.2)
    assert not alive(pid)
    w.close()


def test_no_idle_stop_while_a_request_is_in_flight(env):
    primed(env)
    w = env.wrapper("--idle", "1s")
    w.legacy_connect()
    resp = w.call("sleep", {"seconds": 2.5}, timeout=10)
    assert text_of(resp) == "slept"
    assert len(env.starts()) == 2
    pid = env.starts()[-1]
    assert env.signals(pid) == []
    assert [e["why"] for e in env.events("exit") if e["pid"] == pid] == []
    assert wait_until(lambda: not alive(pid), 5)
    w.close()


def test_no_idle_stop_while_the_client_owes_an_answer(env):
    primed(env)
    w = env.wrapper("--idle", "1s")
    w.legacy_connect()
    call_id = w.request_async("tools/call", {"name": "ask_roots", "arguments": {}})
    ask = w.wait_for(lambda m: m.get("method") == "roots/list")
    pid = env.starts()[-1]
    time.sleep(2.0)
    assert alive(pid)
    w.send({"jsonrpc": "2.0", "id": ask["id"], "result": {"roots": []}})
    w.wait_response(call_id)
    assert wait_until(lambda: not alive(pid), 5)
    w.close()


def test_idle_off_by_default(env):
    primed(env)
    w = env.wrapper()
    w.legacy_connect()
    w.call("echo")
    pid = env.starts()[-1]
    time.sleep(1.5)
    assert alive(pid)
    w.close()


def test_server_crash_is_reported_and_next_call_respawns(env):
    primed(env)
    w = env.wrapper()
    w.legacy_connect()
    resp = w.call("crash")
    assert resp["error"]["code"] == -32603
    assert text_of(w.call("echo", {"ok": True})) == '{"ok": true}'
    assert len(env.starts()) == 3
    w.close()


def test_unknown_method_and_batch_start_the_server(env):
    primed(env)
    w = env.wrapper()
    w.legacy_connect()
    assert w.request("completion/complete", {})["error"]["code"] == -32601
    assert len(env.starts()) == 2
    w.close()
    w = env.wrapper()
    w.legacy_connect()
    w.send([{"jsonrpc": "2.0", "id": 77, "method": "ping"}])
    time.sleep(0.5)
    assert w.proc.poll() is None  # did not crash on a batch
    assert len(env.starts()) == 3
    w.close()


def test_start_failure_is_reported(env):
    w = env.wrapper(server=["/nonexistent/mcp-server"])
    resp = w.request("initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
                                    "clientInfo": {"name": "x", "version": "1"}}, id_=0)
    assert resp["error"]["code"] == -32603
    assert w.close() == 0


def test_cancelled_in_flight_request_does_not_block_idle_stop(env):
    primed(env)
    w = env.wrapper("--idle", "1s")
    w.legacy_connect()
    w.call("echo")
    pid = env.starts()[-1]
    slow = w.request_async("tools/call", {"name": "sleep", "arguments": {"seconds": 30}})
    time.sleep(0.3)
    w.notify("notifications/cancelled", {"requestId": slow})
    assert wait_until(lambda: not alive(pid), 8), "a cancelled request must not keep the server alive"
    w.close()


def test_shutdown_during_spawn_stops_the_new_server(env):
    primed(env)
    env.vars["FAKE_START_DELAY"] = "3"
    w = env.wrapper()
    w.legacy_connect()
    w.request_async("tools/call", {"name": "echo", "arguments": {}})
    assert wait_until(lambda: env.starts()[1:] != [], 5)
    pid = env.starts()[-1]
    assert w.close() == 0
    assert wait_until(lambda: not alive(pid), 8)


def test_server_list_changed_is_never_answered_stale(env, tmp_path):
    """After the server announces a change, an immediate re-fetch must not
    get the old list from the cache."""
    tools_file = tmp_path / "tools.json"
    tools_file.write_text(json.dumps(["echo", "add_tool"]))
    env.vars.update(FAKE_TOOLS=str(tools_file), FAKE_LIST_CHANGED="1")
    primed(env)
    w = env.wrapper()
    w.legacy_connect()
    w.call("add_tool", {"name": "fresh"})
    w.wait_for(lambda m: m.get("method") == "notifications/tools/list_changed")
    tools = w.request("tools/list")
    assert "fresh" in [t["name"] for t in tools["result"]["tools"]]
    assert wait_until(lambda: "refresh checked=tools/list" in env.debug(), 5)
    w.close()
    w = env.wrapper()
    _, tools = w.legacy_connect()
    w.close()
    assert "fresh" in [t["name"] for t in tools["result"]["tools"]]


def test_request_during_idle_stop_restarts_the_server(env):
    """A request that arrives while an idle stop is under way waits for the
    stop to finish and then starts a fresh server; the client sees no error."""
    env.vars["FAKE_IGNORE_EOF"] = "1"
    primed(env)
    w = env.wrapper("--idle", "0.5s", "--stop-grace", "1.5s")
    w.legacy_connect()
    w.call("echo")
    first = env.starts()[-1]
    assert wait_until(lambda: "stop reason=idle" in env.debug(), 5)
    resp = w.call("echo", {"again": 1}, timeout=15)
    assert json.loads(text_of(resp)) == {"again": 1}
    assert not alive(first)
    assert env.starts()[-1] != first
    w.close()


def test_client_ids_with_the_internal_prefix_get_their_replies(env):
    primed(env)
    w = env.wrapper("--idle", "1s")
    w.legacy_connect()
    resp = w.request("tools/call", {"name": "echo", "arguments": {"z": 1}}, id_="mcp-hotswap:client-123")
    assert json.loads(text_of(resp)) == {"z": 1}
    pid = env.starts()[-1]
    assert wait_until(lambda: not alive(pid), 5), "the request must not stay in flight"
    w.close()


def test_server_ping_during_handshake_replay_is_answered(env):
    env.vars["FAKE_PING_ON_INIT"] = "1"
    primed(env)
    w = env.wrapper("--start-timeout", "5s")
    w.legacy_connect()
    resp = w.call("echo", {"p": 1})
    assert json.loads(text_of(resp)) == {"p": 1}
    assert len(env.starts()) == 2
    w.close()


def test_failed_refresh_after_list_changed_never_serves_stale(env, tmp_path):
    tools_file = tmp_path / "tools.json"
    tools_file.write_text(json.dumps(["echo", "add_tool"]))
    env.vars.update(FAKE_TOOLS=str(tools_file), FAKE_LIST_CHANGED="1")
    primed(env)
    w = env.wrapper()
    w.legacy_connect()
    w.call("add_tool", {"name": "fresh", "break_list": True})
    w.wait_for(lambda m: m.get("method") == "notifications/tools/list_changed")
    assert wait_until(lambda: "refresh-failed" in env.debug(), 5)
    # The live server is asked (and fails) rather than the stale cache answering.
    assert w.request("tools/list")["error"]["code"] == -32603
    w.close()
    # The stale entry is gone: the next session asks a real server.
    w = env.wrapper()
    _, tools = w.legacy_connect()
    w.close()
    assert "fresh" in [t["name"] for t in tools["result"]["tools"]]
    assert len(env.starts()) == 3


def test_pinned_tools_disable_idle_stop(env):
    primed(env)
    w = env.wrapper("--idle", "0.5s", "--pin", "counter,ask_roots", "--pin", "sleep")
    w.legacy_connect()
    w.call("echo")
    first = env.starts()[-1]
    assert wait_until(lambda: not alive(first), 5), "unpinned tools still allow idle stop"
    w.call("counter")
    second = env.starts()[-1]
    time.sleep(2.0)
    assert alive(second), "a pinned tool keeps the server for the session"
    assert text_of(w.call("counter")) == "2"  # state kept
    assert "pinned tool=counter" in env.debug()
    w.close()


def test_pin_from_any_listed_flag(env):
    primed(env)
    w = env.wrapper("--idle", "0.5s", "--pin", "counter", "--pin", "sleep")
    w.legacy_connect()
    w.call("sleep", {"seconds": 0.1})
    pid = env.starts()[-1]
    time.sleep(1.5)
    assert alive(pid)
    w.close()


def test_server_upgrade_with_new_catalog_notifies_the_client(env, tmp_path):
    """A version bump plus a new tool: the refresh still re-checks the list
    the client was served and tells it, even though the changed handshake
    already dropped the old entries from the cache file."""
    tools_file = tmp_path / "tools.json"
    tools_file.write_text(json.dumps(["echo"]))
    env.vars.update(FAKE_TOOLS=str(tools_file), FAKE_LIST_CHANGED="1")
    primed(env)
    tools_file.write_text(json.dumps(["echo", "newtool"]))
    env.vars["FAKE_VERSION"] = "2.0"
    w = env.wrapper()
    _, tools = w.legacy_connect()
    assert [t["name"] for t in tools["result"]["tools"]] == ["echo"]
    w.call("echo")
    w.wait_for(lambda m: m.get("method") == "notifications/tools/list_changed")
    assert [t["name"] for t in w.request("tools/list")["result"]["tools"]] == ["echo", "newtool"]
    pid = env.starts()[-1]
    assert any(m.get("method") == "tools/list" for m in env.received(pid))
    w.close()
    w = env.wrapper()
    init, tools = w.legacy_connect()
    w.close()
    assert init["result"]["serverInfo"]["version"] == "2.0"
    assert [t["name"] for t in tools["result"]["tools"]] == ["echo", "newtool"]
    assert len(env.starts()) == 2


def test_pin_applies_to_batched_calls(env):
    primed(env)
    w = env.wrapper("--idle", "0.3s", "--pin", "counter")
    w.legacy_connect()
    w.send([{"jsonrpc": "2.0", "id": 900, "method": "tools/call",
             "params": {"name": "counter", "arguments": {}}}])  # queued while starting
    assert text_of(w.wait_response(900)) == "1"
    pid = env.starts()[-1]
    time.sleep(1.2)
    assert alive(pid), "a pinned tool in a batch pins the server"
    w.send([{"jsonrpc": "2.0", "id": 901, "method": "tools/call",
             "params": {"name": "counter", "arguments": {}}}])  # while running
    assert text_of(w.wait_response(901)) == "2"
    w.close()


def test_pin_applies_to_batch_sent_while_running(env):
    primed(env)
    w = env.wrapper("--idle", "0.3s", "--pin", "counter")
    w.legacy_connect()
    w.call("echo")
    w.send([{"jsonrpc": "2.0", "id": 902, "method": "tools/call",
             "params": {"name": "counter", "arguments": {}}}])
    w.wait_response(902)
    pid = env.starts()[-1]
    time.sleep(1.2)
    assert alive(pid)
    w.close()


def test_late_reply_to_timed_out_internal_request_is_not_leaked(env):
    primed(env)
    env.vars["FAKE_START_DELAY"] = "0.4"
    w = env.wrapper("--start-timeout", "0.1s", "--stop-grace", "1s")
    w.legacy_connect()
    resp = w.call("echo")
    assert resp["error"]["code"] == -32603
    seen = w.drain(1.5)
    assert not [m for m in seen if str(m.get("id", "")).startswith("mcp-hotswap:")], seen
    assert wait_until(lambda: "late-internal-reply" in env.debug(), 3)
    w.close()


def test_older_list_reply_never_overwrites_a_newer_one(env, tmp_path):
    """Startup refresh asks tools/list; its (old) reply is held back while the
    server adds a tool and announces it. The newer list must survive the old
    reply arriving last: in the session, on disk and in the next session."""
    tools_file = tmp_path / "tools.json"
    tools_file.write_text(json.dumps(["echo", "add_tool"]))
    release = tmp_path / "release"
    env.vars.update(FAKE_TOOLS=str(tools_file), FAKE_LIST_CHANGED="1")
    primed(env)
    env.vars["FAKE_HOLD_FIRST_LIST"] = str(release)
    w = env.wrapper()
    w.legacy_connect()
    w.call("echo")  # starts the server; its refresh's tools/list is held
    pid = env.starts()[-1]
    assert wait_until(lambda: any(m.get("method") == "tools/list" for m in env.received(pid)), 5)
    w.call("add_tool", {"name": "fresh"})
    w.wait_for(lambda m: m.get("method") == "notifications/tools/list_changed")
    cache_file = next(env.cache.glob("*.json"))
    assert wait_until(lambda: "fresh" in cache_file.read_text(), 5)
    release.write_text("go")  # the old reply now arrives
    assert wait_until(lambda: "refresh-superseded" in env.debug(), 5)
    assert "fresh" in cache_file.read_text()
    assert "fresh" in [t["name"] for t in w.request("tools/list")["result"]["tools"]]
    w.close()
    del env.vars["FAKE_HOLD_FIRST_LIST"]
    w = env.wrapper()
    _, tools = w.legacy_connect()
    w.close()
    assert "fresh" in [t["name"] for t in tools["result"]["tools"]]
    assert len(env.starts()) == 2


def test_older_passthrough_list_reply_is_not_recorded(env, tmp_path):
    """The same ordering rule for a client's own tools/list relayed live."""
    tools_file = tmp_path / "tools.json"
    tools_file.write_text(json.dumps(["echo", "add_tool"]))
    release = tmp_path / "release"
    env.vars.update(FAKE_TOOLS=str(tools_file), FAKE_LIST_CHANGED="1", FAKE_HOLD_FIRST_LIST=str(release))
    w = env.wrapper()  # no cache: everything goes to the live server
    w.request("initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
                             "clientInfo": {"name": "t", "version": "1"}}, id_=0)
    w.notify("notifications/initialized")
    old = w.request_async("tools/list")  # held: computed now, delivered later
    w.call("add_tool", {"name": "fresh"})
    w.wait_for(lambda m: m.get("method") == "notifications/tools/list_changed")
    assert "fresh" in [t["name"] for t in w.request("tools/list")["result"]["tools"]]
    release.write_text("go")
    w.wait_response(old)
    assert wait_until(lambda: "reply-superseded" in env.debug(), 5)
    w.close()
    cache_file = next(env.cache.glob("*.json"))
    assert "fresh" in cache_file.read_text()


def test_pages_from_before_and_after_list_changed_are_not_joined(env, tmp_path):
    tools_file = tmp_path / "tools.json"
    tools_file.write_text(json.dumps(["echo", "add_tool", "old", "tail"]))
    env.vars.update(FAKE_TOOLS=str(tools_file), FAKE_LIST_CHANGED="1", FAKE_PAGE_SIZE="2")
    w = env.wrapper()  # no cache: pages pass through and are joined
    w.request("initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
                             "clientInfo": {"name": "t", "version": "1"}}, id_=0)
    w.notify("notifications/initialized")
    first = w.request("tools/list")
    assert [t["name"] for t in first["result"]["tools"]] == ["echo", "add_tool"]
    tools_file.write_text(json.dumps(["new", "tail"]))
    w.call("add_tool", {"name": "fresh"})  # catalog is now new, tail, fresh
    w.wait_for(lambda m: m.get("method") == "notifications/tools/list_changed")
    rest = w.request("tools/list", {"cursor": first["result"]["nextCursor"]})
    assert [t["name"] for t in rest["result"]["tools"]] == ["fresh"]
    w.close()
    assert "pages=joined" not in env.debug()
    w = env.wrapper()
    _, tools = w.legacy_connect()  # must come from a real server, not a mixed cache
    w.close()
    assert [t["name"] for t in tools["result"]["tools"]] == ["new", "tail"]
    assert len(env.starts()) == 2


def test_pages_within_one_generation_are_still_joined(env, tmp_path):
    env.vars.update(FAKE_PAGE_SIZE="2", FAKE_LIST_CHANGED="1")
    w = env.wrapper()
    w.legacy_connect()
    cursor = "2"
    while cursor:
        cursor = w.request("tools/list", {"cursor": cursor})["result"].get("nextCursor")
    w.close()
    assert "pages=joined" in env.debug()


def test_debug_log_traces_commands_and_swaps(env):
    primed(env)
    w = env.wrapper("--idle", "0.5s")
    w.legacy_connect()
    w.call("echo", {"a": 1})
    call_id = w.request_async("tools/call", {"name": "ask_roots", "arguments": {}})
    ask = w.wait_for(lambda m: m.get("method") == "roots/list")
    w.send({"jsonrpc": "2.0", "id": ask["id"], "result": {"roots": []}})
    w.wait_response(call_id)
    pid = env.starts()[-1]
    assert wait_until(lambda: not alive(pid), 5)
    w.close()
    log = env.debug()
    assert "client name=test-client version=1.0 protocol=2025-11-25" in log
    assert "request id=0 method=initialize state=cold" in log
    assert "request id=101 method=tools/call state=cold tool=echo" in log
    assert "spawn trigger=tools/call" in log
    assert "reply id=101 method=tools/call ms=" in log and "outcome=ok tool=echo" in log
    assert "server-request id=" in log and "method=roots/list" in log
    assert "client-answer id=" in log
    assert "stop reason=idle" in log and "stopped reason=idle" in log and "how=eof" in log
