"""A fake stdio MCP server for the tests.

Configured through env vars (all optional):

FAKE_LOG          JSONL event log: start, recv, signal, exit (each with pid)
FAKE_ERA          legacy | modern | dual (default legacy)
FAKE_TOOLS        JSON file with the tool names to list (default: echo...)
FAKE_PAGE_SIZE    paginate tools/list
FAKE_LIST_CHANGED advertise tools.listChanged
FAKE_VERSION      serverInfo.version
FAKE_START_DELAY  seconds to sleep before reading stdin
FAKE_IGNORE_EOF   keep running after stdin closes
FAKE_IGNORE_TERM  log SIGTERM but keep running
FAKE_LIST_ERROR   answer tools/list with this error code
FAKE_PING_ON_INIT ping the client and wait for its answer before answering initialize
"""

import json
import os
import signal
import sys
import threading
import time

LOG = os.environ.get("FAKE_LOG")
ERA = os.environ.get("FAKE_ERA", "legacy")
MODERN = "2026-07-28"
LEGACY = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"]
lock = threading.Lock()
state = {"initialized": False, "era": None, "counter": 0}
waiting = {}  # our request id -> (event, slot)
next_id = [0]


def log(ev, **kw):
    if LOG:
        with lock, open(LOG, "a") as f:
            f.write(json.dumps({"ev": ev, "pid": os.getpid(), "t": time.time(), **kw}) + "\n")


batch = threading.local()  # while answering a batch, replies are collected here


def send(msg):
    sink = getattr(batch, "replies", None)
    if sink is not None and isinstance(msg, dict) and "method" not in msg:
        sink.append(msg)
        return
    data = json.dumps(msg) + "\n"
    with lock:
        sys.stdout.write(data)
        sys.stdout.flush()


def reply(id_, result=None, error=None):
    msg = {"jsonrpc": "2.0", "id": id_}
    if error is not None:
        msg["error"] = error
    else:
        msg["result"] = result
    send(msg)


def tool_names():
    path = os.environ.get("FAKE_TOOLS")
    if path:
        with open(path) as f:
            return json.load(f)
    return ["echo", "sleep", "ask_roots", "counter", "pid", "crash"]


def tools():
    return [{"name": n, "description": f"{n} tool", "inputSchema": {"type": "object"}} for n in tool_names()]


def caps():
    c = {"tools": {"listChanged": bool(os.environ.get("FAKE_LIST_CHANGED"))}, "logging": {}}
    return c


def server_info():
    return {"name": "fake", "version": os.environ.get("FAKE_VERSION", "1.0")}


def ask_client(method, params):
    next_id[0] += 1
    id_ = f"srv-{next_id[0]}"
    ev = threading.Event()
    slot = {}
    waiting[id_] = (ev, slot)
    send({"jsonrpc": "2.0", "id": id_, "method": method, "params": params})
    ev.wait(30)
    return slot.get("msg")


def call_tool(id_, params, modern):
    name = params.get("name")
    args = params.get("arguments") or {}
    text = None
    if name == "echo":
        text = json.dumps(args)
    elif name == "sleep":
        time.sleep(float(args.get("seconds", 1)))
        text = "slept"
    elif name == "ask_roots":
        answer = ask_client("roots/list", {})
        text = json.dumps(answer)
    elif name == "counter":
        state["counter"] += 1
        text = str(state["counter"])
    elif name == "pid":
        text = str(os.getpid())
    elif name == "add_tool":
        path = os.environ["FAKE_TOOLS"]
        with open(path) as f:
            names = json.load(f)
        with open(path + ".tmp", "w") as f:  # atomic: a list may be read concurrently
            json.dump(names + [args["name"]], f)
        os.replace(path + ".tmp", path)
        if args.get("break_list"):
            state["list_broken"] = True
        send({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"})
        text = "added"
    elif name == "crash":
        log("exit", why="crash")
        os._exit(3)
    else:
        reply(id_, error={"code": -32602, "message": f"unknown tool {name}"})
        return
    result = {"content": [{"type": "text", "text": text}]}
    if modern:
        result["resultType"] = "complete"
    reply(id_, result)


def list_tools(id_, params, modern):
    code = os.environ.get("FAKE_LIST_ERROR") or ("-32603" if state.get("list_broken") else None)
    if code:
        reply(id_, error={"code": int(code), "message": "list failed"})
        return
    all_tools = tools()
    size = int(os.environ.get("FAKE_PAGE_SIZE") or 0)
    start = int((params or {}).get("cursor") or 0)
    if size:
        page = all_tools[start:start + size]
        result = {"tools": page}
        if start + size < len(all_tools):
            result["nextCursor"] = str(start + size)
    else:
        result = {"tools": all_tools}
    if modern:
        result.update({"resultType": "complete", "ttlMs": 0, "cacheScope": "private"})
    reply(id_, result)


def handle(msg):
    method = msg.get("method")
    id_ = msg.get("id")
    params = msg.get("params") or {}
    if method is None:
        entry = waiting.pop(id_, None)
        if entry:
            entry[1]["msg"] = msg
            entry[0].set()
        return
    if id_ is None:
        if method == "notifications/initialized":
            state["initialized"] = True
        return
    meta = params.get("_meta") if isinstance(params, dict) else None
    modern = isinstance(meta, dict) and "io.modelcontextprotocol/protocolVersion" in meta
    if method == "server/discover":
        if ERA == "legacy":
            reply(id_, error={"code": -32601, "message": "Method not found"})
        else:
            state["era"] = state["era"] or "modern"
            reply(id_, {"supportedVersions": [MODERN], "capabilities": caps(), "resultType": "complete",
                        "ttlMs": 0, "cacheScope": "private", "_meta": {"io.modelcontextprotocol/serverInfo": server_info()}})
        return
    if method == "initialize":
        if ERA == "modern" or state["era"] == "modern":
            reply(id_, error={"code": -32022, "message": "initialize not accepted"})
            return
        state["era"] = "legacy"
        asked = params.get("protocolVersion")
        version = asked if asked in LEGACY else LEGACY[0]
        answer = {"protocolVersion": version, "capabilities": caps(), "serverInfo": server_info()}
        if os.environ.get("FAKE_PING_ON_INIT"):
            def ping_first():
                if ask_client("ping", {}) is None:
                    log("ping-unanswered")
                    return
                reply(id_, answer)
            threading.Thread(target=ping_first, daemon=True).start()
            return
        reply(id_, answer)
        return
    if method == "ping":
        reply(id_, {})
        return
    if not modern and state["era"] != "legacy":
        reply(id_, error={"code": -32002, "message": "not initialized"})
        return
    if modern:
        state["era"] = "modern"
    if method == "tools/list":
        list_tools(id_, params, modern)
    elif method == "tools/call":
        if getattr(batch, "replies", None) is not None:
            call_tool(id_, params, modern)  # inside a batch: answer in this thread
        else:
            threading.Thread(target=call_tool, args=(id_, params, modern), daemon=True).start()
    elif method == "logging/setLevel":
        reply(id_, {})
    else:
        reply(id_, error={"code": -32601, "message": "Method not found"})


def on_signal(sig, _frame):
    log("signal", sig=sig)
    if sig == signal.SIGTERM and os.environ.get("FAKE_IGNORE_TERM"):
        return
    log("exit", why=f"signal-{sig}")
    os._exit(128 + sig)


def main():
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, on_signal)
    log("start", argv=sys.argv, ppid=os.getppid(), pgid=os.getpgrp())
    delay = float(os.environ.get("FAKE_START_DELAY") or 0)
    if delay:
        time.sleep(delay)
    for line in sys.stdin:
        msg = json.loads(line)
        log("recv", msg=msg)
        if isinstance(msg, list):  # JSON-RPC batch: one array of responses
            batch.replies = []
            for item in msg:
                handle(item)
            replies, batch.replies = batch.replies, None
            if replies:
                send(replies)
            continue
        handle(msg)
    log("eof")
    if os.environ.get("FAKE_IGNORE_EOF"):
        while True:
            time.sleep(1)
    log("exit", why="eof")


if __name__ == "__main__":
    main()
