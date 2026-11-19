"""The relay between the MCP client (our stdin/stdout) and the real server.

States: ``cold`` (no server; discovery answered from the cache),
``starting`` (server spawned, handshake being replayed, client requests
queued), ``running`` (raw frames relayed both ways), ``stopping`` (idle
stop in progress; requests queue up and restart the server afterwards) and
``closing`` (the client went away; nothing restarts).

Frames are parsed only to read ``id`` and ``method``; the original bytes
are what gets forwarded, so unknown fields survive.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import signal
import sys
from dataclasses import dataclass, field

from . import cache as C
from .child import LINE_LIMIT, Child
from .log import Log

INTERNAL_PREFIX = "mcp-hotswap:"
REFRESH_WAIT = 2.0  # at client EOF, how long a running cache refresh may finish
QUEUE_LIMIT = 64 * 1024 * 1024
SIGNAL_EOF_GRACE = 0.25
COLLECT_WAIT = 15.0  # how long a first session waits for another to build the cache
COLLECT_HOLD = 30.0  # longest a collector keeps others waiting

LIST_ITEMS = {
    "tools/list": "tools",
    "prompts/list": "prompts",
    "resources/list": "resources",
    "resources/templates/list": "resourceTemplates",
}
LIST_KIND = {
    "tools/list": "tools",
    "prompts/list": "prompts",
    "resources/list": "resources",
    "resources/templates/list": "resources",
}
CHANGED_NOTIFICATION = {
    "notifications/tools/list_changed": ("tools/list",),
    "notifications/prompts/list_changed": ("prompts/list",),
    "notifications/resources/list_changed": ("resources/list", "resources/templates/list"),
}


@dataclass
class Options:
    argv: list[str]
    idle: float | None = None
    start_timeout: float = 120.0
    eof_grace: float = 2.0
    term_grace: float = 3.0
    refresh_timeout: float = 30.0
    pin: frozenset = frozenset()


@dataclass
class Pending:
    method: str | None
    key: str | None = None
    cursor: object = None
    gen: int = 0  # list generation when the request was forwarded


@dataclass
class Pages:
    """A list being collected page by page while the client pages through it."""

    key: str
    first: dict
    items: list = field(default_factory=list)
    next: object = None


def idkey(value) -> str:
    # 1 and "1" are different JSON-RPC ids.
    return json.dumps(value)


def error_frame(id_, code: int, message: str, data=None) -> bytes:
    err = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return encode({"jsonrpc": "2.0", "id": id_, "error": err})


def encode(msg) -> bytes:
    return (json.dumps(msg, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


class Proxy:
    def __init__(self, opts: Options, cache: C.Cache, log: Log):
        self.opts = opts
        self.cache = cache
        self.log = log
        self.state = "cold"
        self.child: Child | None = None
        self.child_count = 0
        self.writer: asyncio.StreamWriter | None = None
        self.queue: list[tuple[bytes, object]] = []
        self.queue_bytes = 0
        self.start_task: asyncio.Task | None = None
        self.handshake_reply: tuple[str, dict] | None = None
        self.collect_fd: int | None = None
        self.dirty: set[str] = set()  # lists the server said changed, not yet re-fetched
        # Bumped by every list_changed from the server. A list reply from a
        # fetch that began before the latest bump is older than what the
        # server has announced and must not be stored, even if it arrives
        # after the newer one.
        self.gen: dict[str, int] = {}
        self.refresh_tasks: set[asyncio.Task] = set()
        self.expired: set[str] = set()  # internal ids that timed out; late replies are dropped
        self.pinned = False  # a --pin tool was called: no idle stop for this server
        # Internal ids carry a per-process random part, so a client cannot
        # pick one by accident; replies are routed by exact id, not prefix.
        self.internal_base = f"{INTERNAL_PREFIX}{secrets.token_hex(4)}:"
        self.done = asyncio.Event()

        # Session state, as seen by the client.
        self.client_init: dict | None = None  # the client's initialize request
        self.init_answered = False  # the client got its initialize reply
        self.negotiated: str | None = None  # protocolVersion in that reply
        self.served_caps: dict = {}
        self.client_initialized = False
        self.log_level: dict | None = None
        self.modern_meta: dict | None = None
        self.modern_version: str | None = None
        self.served: dict[str, str] = {}  # key -> reply the client got from the cache

        # In flight.
        self.client_pending: dict[str, Pending] = {}
        self.server_pending: set[str] = set()
        self.internal: dict[str, asyncio.Future] = {}
        self.internal_seq = 0
        self.pages: dict[str, Pages] = {}
        self.last_activity = 0.0
        self.idle_task: asyncio.Task | None = None
        self.closing_reason: str | None = None

    # ------------------------------------------------------------------ setup

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        self.last_activity = loop.time()
        reader = await self._open_stdin(loop)
        self.writer = await self._open_stdout(loop)
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            loop.add_signal_handler(sig, self._on_signal, sig)
        self.log("start", argv=json.dumps(self.opts.argv), cache=self.cache.path,
                 entries=len(self.cache.entries), idle=self.opts.idle)
        if self.opts.idle:
            self.idle_task = asyncio.ensure_future(self._idle_loop())
        reader_task = asyncio.ensure_future(self._client_loop(reader))
        await self.done.wait()
        reader_task.cancel()
        if self.idle_task:
            self.idle_task.cancel()

    async def _open_stdin(self, loop) -> asyncio.StreamReader:
        reader = asyncio.StreamReader(limit=LINE_LIMIT)
        try:
            await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin.buffer)
        except ValueError:
            # Not a pipe, socket or tty (a regular file): read it in a thread.
            def pump():
                for line in sys.stdin.buffer:
                    loop.call_soon_threadsafe(reader.feed_data, line)
                loop.call_soon_threadsafe(reader.feed_eof)
            loop.run_in_executor(None, pump)
        return reader

    async def _open_stdout(self, loop) -> asyncio.StreamWriter:
        transport, protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, sys.stdout.buffer)
        return asyncio.StreamWriter(transport, protocol, None, loop)

    # ------------------------------------------------------------ client side

    async def _client_loop(self, reader: asyncio.StreamReader) -> None:
        while True:
            try:
                raw = await reader.readline()
            except (ValueError, asyncio.LimitOverrunError):
                Log.warn("client sent a line over the size limit; closing")
                raw = b""
            if not raw:
                self.log("client-eof")
                await self.shutdown("client-eof", fast=False)
                return
            if not raw.strip():
                continue
            if not raw.endswith(b"\n"):
                raw += b"\n"
            self.log.frame("c>", raw)
            try:
                msg = json.loads(raw)
            except ValueError:
                msg = None
            try:
                await self._on_client(raw, msg)
            except Exception as exc:  # never let one frame kill the session
                self.log("error", where="client", exc=repr(exc))
                Log.warn(f"internal error handling a client message: {exc!r}")

    async def _on_client(self, raw: bytes, msg) -> None:
        if self.state == "closing":
            return
        if not isinstance(msg, dict):
            # A batch (dropped from recent MCP revisions) or garbage: the
            # real server decides what to make of it.
            await self._forward_or_queue(raw, msg, trigger="batch" if isinstance(msg, list) else "invalid")
            return
        method = msg.get("method")
        has_id = "id" in msg
        if method is None:
            await self._client_response(raw, msg)
            return
        if not isinstance(method, str):
            await self._forward_or_queue(raw, msg, trigger="invalid")
            return
        if not has_id:
            await self._client_notification(raw, msg, method)
            return
        self._note_meta(msg)
        if self.state == "running" and not await self._answer_locally(msg, method):
            await self._forward_request(raw, msg)
            return
        if self.state != "running":
            await self._request_while_down(raw, msg, method)

    async def _answer_locally(self, msg: dict, method: str) -> bool:
        """While the server runs, still answer ping and cached discovery.

        Neither is real work: a client that polls them must not keep an
        idle server alive. The cache was re-checked when this server
        started, and a list_changed from the server triggers another check.
        """
        if method == "ping":
            await self._to_client(encode({"jsonrpc": "2.0", "id": msg["id"], "result": {}}))
            return True
        if method == C.INITIALIZE or method in self.dirty:
            return False
        key = C.request_key(msg, self.negotiated)
        reply = self.cache.get(key)
        if reply is None:
            return False
        self.log("cache-hit", method=method, state=self.state)
        self.served[key] = C.dumps(reply)
        await self._to_client(encode({"jsonrpc": "2.0", "id": msg["id"], **reply}))
        return True

    def _note_meta(self, msg: dict) -> None:
        params = msg.get("params")
        meta = params.get("_meta") if isinstance(params, dict) else None
        if isinstance(meta, dict) and isinstance(meta.get(C.META_VERSION), str):
            self.modern_meta = {k: v for k, v in meta.items() if k != "progressToken"}
            self.modern_version = meta[C.META_VERSION]

    async def _client_response(self, raw: bytes, msg: dict) -> None:
        """The client answering a server-to-client request."""
        k = idkey(msg.get("id"))
        asked = k in self.server_pending
        self.server_pending.discard(k)
        if self.child is not None and (self.state == "running" or (self.state == "starting" and asked)):
            # While starting, the server may ask something (a ping) before it
            # answers the replayed initialize: relay the answer at once, or
            # each side waits for the other.
            self._touch()
            await self._to_child(raw)
        elif self.state == "starting":
            self._enqueue(raw, msg)
        else:
            self.log("drop", what="client-response", id=msg.get("id"))

    async def _client_notification(self, raw: bytes, msg: dict, method: str) -> None:
        if method == "notifications/initialized":
            self.client_initialized = True
            if self.state == "running":
                self._touch()
                await self._to_child(raw)
            # Otherwise swallowed; replayed to the server when it starts.
            return
        if method == "notifications/cancelled":
            params = msg.get("params") if isinstance(msg.get("params"), dict) else {}
            target = idkey(params.get("requestId"))
            for i, (_, queued) in enumerate(self.queue):
                if isinstance(queued, dict) and "method" in queued and idkey(queued.get("id")) == target:
                    raw_q, _ = self.queue.pop(i)
                    self.queue_bytes -= len(raw_q)
                    self.log("cancel-queued", id=params.get("requestId"))
                    return
            if self.state == "running" and target in self.client_pending:
                # The server will not answer a cancelled request; do not let
                # it count as in flight forever.
                self.client_pending.pop(target, None)
                self._touch()
                await self._to_child(raw)
            return
        if self.state == "running":
            self._touch()
            await self._to_child(raw)
        elif self.state == "starting":
            self._enqueue(raw, msg)
        else:
            self.log("drop", what=method)

    async def _request_while_down(self, raw: bytes, msg: dict, method: str) -> None:
        id_ = msg["id"]
        if method == "ping":
            await self._to_client(encode({"jsonrpc": "2.0", "id": id_, "result": {}}))
            return
        if method == "logging/setLevel":
            self.log_level = msg.get("params") if isinstance(msg.get("params"), dict) else None
            await self._to_client(encode({"jsonrpc": "2.0", "id": id_, "result": {}}))
            return
        key = C.request_key(msg, self.negotiated)
        reply = self.cache.get(key)
        if reply is None and key is not None and self.state == "cold":
            reply = await self._wait_for_collector(key)
        if method == C.INITIALIZE:
            self.client_init = msg
            self.init_answered = False
            if reply is not None and "result" not in reply:
                reply = None  # an initialize error is never replayed
        if reply is not None:
            self.log("cache-hit", method=method, state=self.state)
            if method == C.INITIALIZE:
                result = reply["result"]
                self.negotiated = result.get("protocolVersion") if isinstance(result, dict) else None
                self.served_caps = result.get("capabilities") or {} if isinstance(result, dict) else {}
                self.init_answered = True
            self.served[key] = C.dumps(reply)
            await self._to_client(encode({"jsonrpc": "2.0", "id": id_, **reply}))
            return
        self.log("cache-miss" if key else "spawn-request", method=method, state=self.state)
        await self._forward_or_queue(raw, msg, trigger=method)

    async def _wait_for_collector(self, key: str) -> dict | None:
        """On a discovery miss, let a wrapper that is already collecting finish.

        Another session may have just written the entry, or may be starting
        the server to record it. Wait (bounded) for that, then look again;
        only start our own server if the entry is still missing. The lock is
        then ours until our server's first list reply is recorded.
        """
        self.cache.reload()
        reply = self.cache.get(key)
        if reply is not None or self.collect_fd is not None:
            return reply
        loop = asyncio.get_running_loop()
        deadline = loop.time() + COLLECT_WAIT
        waited = False
        while True:
            fd = self.cache.try_collect()
            if fd is not None:
                break
            if loop.time() >= deadline:
                self.log("collect-wait-timeout")
                return None
            waited = True
            await asyncio.sleep(0.1)
        if waited:
            self.log("collect-waited")
            self.cache.reload()
            reply = self.cache.get(key)
            if reply is not None:
                C.Cache.end_collect(fd)
                return reply
        self.collect_fd = fd
        loop.call_later(COLLECT_HOLD, self._end_collect)
        return None

    def _end_collect(self) -> None:
        if self.collect_fd is not None:
            C.Cache.end_collect(self.collect_fd)
            self.collect_fd = None

    async def _forward_or_queue(self, raw: bytes, msg, trigger: str) -> None:
        if self.state == "running":
            if isinstance(msg, dict):
                await self._forward_request(raw, msg)
            else:
                self._track_batch(msg)
                self._touch()
                await self._to_child(raw)
            return
        if self.queue_bytes + len(raw) > QUEUE_LIMIT:
            if isinstance(msg, dict) and "id" in msg:
                await self._to_client(error_frame(msg["id"], -32603, "mcp-hotswap: too much input queued while the server starts"))
            return
        self._enqueue(raw, msg)
        if self.state == "cold":
            self._start(trigger)

    def _enqueue(self, raw: bytes, msg) -> None:
        self.queue.append((raw, msg))
        self.queue_bytes += len(raw)

    async def _forward_request(self, raw: bytes, msg: dict) -> None:
        method = msg.get("method")
        if method is None:
            await self._client_response(raw, msg)
            return
        if "id" in msg:
            key = C.request_key(msg, self.negotiated)
            cursor = None
            if method in LIST_ITEMS and isinstance(msg.get("params"), dict):
                cursor = msg["params"].get("cursor")
            self.client_pending[idkey(msg["id"])] = Pending(method, key, cursor, self.gen.get(method, 0))
            if method == C.INITIALIZE:
                self.client_init = msg
            elif method == "logging/setLevel" and isinstance(msg.get("params"), dict):
                self.log_level = msg["params"]
            elif method == "tools/call":
                self._check_pin(msg)
        elif method == "notifications/initialized":
            self.client_initialized = True
        self._touch()
        await self._to_child(raw)

    def _track_batch(self, batch) -> None:
        if isinstance(batch, list):
            for item in batch:
                if isinstance(item, dict) and "method" in item and "id" in item:
                    self.client_pending[idkey(item["id"])] = Pending(None)
                    if item.get("method") == "tools/call":
                        self._check_pin(item)

    def _check_pin(self, msg: dict) -> None:
        if not self.opts.pin or self.pinned:
            return
        params = msg.get("params")
        name = params.get("name") if isinstance(params, dict) else None
        if name in self.opts.pin:
            self.pinned = True
            self.log("pinned", tool=name)

    # ------------------------------------------------------------- start/stop

    def _start(self, trigger: str) -> None:
        self.state = "starting"
        self.start_task = asyncio.ensure_future(self._start_child(trigger))

    async def _start_child(self, trigger: str) -> None:
        self.child_count += 1
        try:
            child = await Child.spawn(self.opts.argv, self.child_count)
        except OSError as exc:
            Log.warn(f"cannot start {self.opts.argv[0]!r}: {exc}")
            self.log("spawn-failed", trigger=trigger, error=repr(exc))
            await self._fail_queue(-32603, f"mcp-hotswap: cannot start the MCP server: {exc}")
            self._end_collect()
            self.state = "cold"
            return
        self.child = child
        self.log("spawn", trigger=trigger, pid=child.pid, n=child.number)
        asyncio.ensure_future(self._child_loop(child))
        replay = self.client_init is not None and self.init_answered
        if replay:
            if not await self._replay_handshake(child):
                return
        if self.state != "starting" or self.child is not child:
            return
        self.state = "running"
        self._touch()
        queued, self.queue, self.queue_bytes = self.queue, [], 0
        for raw, msg in queued:
            if isinstance(msg, dict) and "method" in msg:
                await self._forward_request(raw, msg)
            elif isinstance(msg, dict):
                await self._client_response(raw, msg)
            else:
                self._track_batch(msg)
                await self._to_child(raw)
        self.log("ready", pid=child.pid, flushed=len(queued))
        if replay or (self.modern_meta is not None and self.client_init is None and self.served):
            self._start_refresh(child)

    async def _replay_handshake(self, child: Child) -> bool:
        params = dict(self.client_init.get("params") or {})
        if self.negotiated:
            params["protocolVersion"] = self.negotiated
        reply = await self._internal(child, C.INITIALIZE, params, self.opts.start_timeout)
        if reply is None or "result" not in reply:
            err = (reply or {}).get("error") or {"code": -32603, "message": "mcp-hotswap: the MCP server did not answer initialize"}
            self.log("handshake-failed", pid=child.pid, error=json.dumps(err))
            Log.warn(f"the MCP server failed the initialize handshake: {err.get('message')}")
            await self._fail_queue(err.get("code", -32603), err.get("message", "initialize failed"), err.get("data"))
            if self.child is child:
                await self._stop_child(child, "handshake-failed")
            return False
        result = reply["result"]
        if isinstance(result, dict) and self.negotiated and result.get("protocolVersion") != self.negotiated:
            self.log("version-changed", served=self.negotiated, now=result.get("protocolVersion"))
        key = C.request_key(self.client_init, None)
        self.handshake_reply = (key, {"result": result})
        if self.cache.get(key) is not None and C.dumps(self.cache.get(key)) != C.dumps({"result": result}):
            # The server changed. Record it now, forgetting what cannot be
            # re-checked, so the next session re-learns it even if this one
            # ends before the background refresh completes.
            self.cache.update({key: {"result": result}}, forget_unchecked=True)
            self.log("cache-refreshed", changed="initialize")
        if self.client_initialized:
            await child.send(encode({"jsonrpc": "2.0", "method": "notifications/initialized"}))
        if self.log_level is not None:
            asyncio.ensure_future(self._internal(child, "logging/setLevel", self.log_level, self.opts.refresh_timeout))
        return True

    async def _fail_queue(self, code: int, message: str, data=None) -> None:
        queued, self.queue, self.queue_bytes = self.queue, [], 0
        for _, msg in queued:
            if isinstance(msg, dict) and "method" in msg and "id" in msg:
                await self._to_client(error_frame(msg["id"], code, message, data))
            elif isinstance(msg, list):
                for item in msg:
                    if isinstance(item, dict) and "method" in item and "id" in item:
                        await self._to_client(error_frame(item["id"], code, message, data))

    async def _stop_child(self, child: Child, reason: str, fast: bool = False) -> None:
        first = not child.stopping
        if first:
            if self.state != "closing" and self.child is child:
                self.state = "stopping"
            self.log("stop", reason=reason, pid=child.pid)
        eof_grace = min(self.opts.eof_grace, SIGNAL_EOF_GRACE) if fast else self.opts.eof_grace
        how = await child.stop(eof_grace, self.opts.term_grace)
        if first:
            self.log("stopped", reason=reason, pid=child.pid, how=how, status=child.proc.returncode)
            self._child_gone(child)

    def _child_gone(self, child: Child) -> None:
        if self.child is not child:
            return
        self.child = None
        for fut in self.internal.values():
            if not fut.done():
                fut.set_result(None)
        self.internal.clear()
        self.server_pending.clear()
        self.pages.clear()
        self.dirty.clear()
        self.expired.clear()
        self.pinned = False  # whatever it held is gone with it
        self._end_collect()
        if self.state == "closing":
            return
        self.state = "cold"
        if self.queue:
            self._start("queued")

    async def _child_loop(self, child: Child) -> None:
        while True:
            raw = await child.readline()
            if not raw:
                break
            if self.child is not child:
                continue
            self.log.frame("s>", raw)
            try:
                await self._on_child(child, raw)
            except Exception as exc:
                self.log("error", where="server", exc=repr(exc))
                Log.warn(f"internal error handling a server message: {exc!r}")
        if self.child is child and not child.stopping and self.state != "closing":
            # The server went away on its own (crash, or it closed stdout).
            self.log("server-exit", pid=child.pid, state=self.state)
            Log.warn("the MCP server exited unexpectedly; it will be restarted on the next request")
            lost = list(self.client_pending.keys())
            self.client_pending.clear()
            for k in lost:
                await self._to_client(error_frame(json.loads(k), -32603, "mcp-hotswap: the MCP server exited before answering"))
            if self.state == "starting":
                await self._fail_queue(-32603, "mcp-hotswap: the MCP server exited during startup")
            await self._stop_child(child, "server-exit")

    async def _on_child(self, child: Child, raw: bytes) -> None:
        try:
            msg = json.loads(raw)
        except ValueError:
            self._touch()
            await self._to_client(raw)
            return
        if isinstance(msg, list):  # batch response
            for item in msg:
                if isinstance(item, dict) and "method" not in item:
                    self.client_pending.pop(idkey(item.get("id")), None)
            self._touch()
            await self._to_client(raw)
            return
        if not isinstance(msg, dict):
            await self._to_client(raw)
            return
        method = msg.get("method")
        if method is None:
            id_ = msg.get("id")
            if isinstance(id_, str) and idkey(id_) not in self.client_pending:
                if id_ in self.internal:
                    fut = self.internal.pop(id_)
                    if not fut.done():
                        fut.set_result(msg)
                    return
                if id_ in self.expired:
                    # We gave up waiting; the client never sent this id.
                    self.expired.discard(id_)
                    self.log("late-internal-reply", id=id_)
                    return
            self._touch()
            pending = self.client_pending.pop(idkey(id_), None)
            await self._to_client(raw)
            if pending is not None:
                self._record(pending, msg)
            return
        self._touch()
        if "id" in msg:
            self.server_pending.add(idkey(msg["id"]))
        elif method == "notifications/cancelled":
            params = msg.get("params") if isinstance(msg.get("params"), dict) else {}
            self.server_pending.discard(idkey(params.get("requestId")))
        elif method in CHANGED_NOTIFICATION:
            # Until the cache is re-fetched, these lists go to the live server.
            self.dirty.update(CHANGED_NOTIFICATION[method])
            for m in CHANGED_NOTIFICATION[method]:
                self.gen[m] = self.gen.get(m, 0) + 1
            self._start_refresh(child, only=CHANGED_NOTIFICATION[method])
        await self._to_client(raw)

    # ---------------------------------------------------------- cache upkeep

    def _record(self, pending: Pending, msg: dict) -> None:
        """Store a discovery reply that passed through from the live server."""
        method = pending.method
        if method in LIST_ITEMS and pending.gen != self.gen.get(method, 0):
            self.log("reply-superseded", method=method)
            return
        if method == C.INITIALIZE:
            result = msg.get("result")
            if isinstance(result, dict):
                self.negotiated = result.get("protocolVersion")
                self.served_caps = result.get("capabilities") or {}
                self.init_answered = True
                # The key could not use the version before; recompute.
                pending.key = C.request_key(self.client_init or {"method": C.INITIALIZE}, None)
        if pending.key is None and method in LIST_ITEMS and pending.cursor is not None:
            self._collect_page(method, msg, pending.cursor)
            return
        if pending.key is None or method is None:
            return
        if method in LIST_ITEMS and isinstance(msg.get("result"), dict) and msg["result"].get("nextCursor"):
            self.pages[method] = Pages(pending.key, msg["result"], list(msg["result"].get(LIST_ITEMS[method]) or []),
                                       msg["result"]["nextCursor"])
            return
        reply = C.cacheable_reply(method, msg)
        if reply is not None:
            self.dirty.discard(method)
            changed = self.cache.update({pending.key: reply})
            if changed:
                self.log("cache-store", method=method)
            if method in LIST_ITEMS:
                self._end_collect()

    def _collect_page(self, method: str, msg: dict, cursor) -> None:
        pages = self.pages.get(method)
        result = msg.get("result")
        if pages is None or pages.next != cursor or not isinstance(result, dict):
            self.pages.pop(method, None)
            return
        pages.items += list(result.get(LIST_ITEMS[method]) or [])
        pages.next = result.get("nextCursor")
        if not pages.next:
            del self.pages[method]
            full = {k: v for k, v in pages.first.items() if k != "nextCursor"}
            full[LIST_ITEMS[method]] = pages.items
            self.cache.update({pages.key: {"result": full}})
            self.log("cache-store", method=method, pages="joined")
            self._end_collect()

    async def _internal(self, child: Child, method: str, params, timeout: float) -> dict | None:
        while True:
            self.internal_seq += 1
            id_ = f"{self.internal_base}{self.internal_seq}"
            if idkey(id_) not in self.client_pending:
                break
        fut = asyncio.get_running_loop().create_future()
        self.internal[id_] = fut
        msg = {"jsonrpc": "2.0", "id": id_, "method": method}
        if params is not None:
            msg["params"] = params
        raw = encode(msg)
        self.log.frame("h>", raw)
        if not await child.send(raw):
            self.internal.pop(id_, None)
            return None
        try:
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            self.log("internal-timeout", method=method)
            self.expired.add(id_)
            return None
        finally:
            self.internal.pop(id_, None)

    async def _fetch_list(self, child: Child, method: str, params) -> dict | None:
        """Fetch a whole list (all pages) as one reply body."""
        full = None
        items: list = []
        cursor = None
        for _ in range(1000):
            p = dict(params or {})
            if cursor is not None:
                p["cursor"] = cursor
            reply = await self._internal(child, method, p or None, self.opts.refresh_timeout)
            if reply is None:
                return None
            if "result" not in reply or not isinstance(reply["result"], dict):
                return C.cacheable_reply(method, reply)
            result = reply["result"]
            if full is None:
                full = {k: v for k, v in result.items() if k != "nextCursor"}
            items += list(result.get(LIST_ITEMS[method]) or [])
            cursor = result.get("nextCursor")
            if not cursor:
                full[LIST_ITEMS[method]] = items
                return {"result": full}
        return None

    def _start_refresh(self, child: Child, only: tuple[str, ...] | None = None) -> None:
        task = asyncio.ensure_future(self._refresh(child, only))
        self.refresh_tasks.add(task)
        task.add_done_callback(self.refresh_tasks.discard)

    async def _refresh(self, child: Child, only: tuple[str, ...] | None = None) -> None:
        """Re-ask the live server everything the cache holds for this session.

        Runs after the server was started for a session that had been
        answered from the cache (or when the server announces a change).
        Requests use internal ids and are built from this session's own
        protocol era: a legacy session never sends ``server/discover`` (a
        dual-era server would switch that process to the modern protocol
        and then refuse the session), and a modern session's requests carry
        the client's ``_meta``.
        """
        legacy = self.client_init is not None
        version = self.negotiated if legacy else self.modern_version
        if version is None:
            return
        put: dict[str, dict] = {}
        handshake = self.handshake_reply
        if legacy and handshake and only is None:
            put[handshake[0]] = handshake[1]
            self.handshake_reply = None
        failed: list[str] = []
        started: dict[str, int] = {}
        # Everything the cache holds, plus what this client was served: a
        # changed handshake may already have dropped those entries from the
        # file, but the client still holds the old replies and must hear
        # about changes.
        keys = list(self.cache.entries)
        keys += [k for k in self.served if k not in self.cache.entries]
        for key in keys:
            if self.child is not child or self.state == "closing":
                return
            v, method, params = C.key_parts(key)
            if v != version or method == C.INITIALIZE:
                continue
            if only is not None and method not in only:
                continue
            if method == C.DISCOVER and legacy:
                continue
            p = dict(params or {})
            if not legacy:
                p["_meta"] = self.modern_meta
            started[key] = self.gen.get(method, 0)
            if method == C.DISCOVER:
                reply = await self._internal(child, method, p, self.opts.refresh_timeout)
                reply = C.cacheable_reply(method, reply) if reply else None
            elif method in LIST_ITEMS:
                reply = await self._fetch_list(child, method, p)
            else:
                continue
            if reply is not None:
                put[key] = reply
            else:
                failed.append(key)
        # Drop results overtaken by a later list_changed (checked here, with
        # no await before the cache write below).
        stale = [k for k in list(put) + failed if started.get(k, 0) != self.gen.get(C.key_parts(k)[1], 0)]
        if stale:
            self.log("refresh-superseded", methods=",".join(sorted({C.key_parts(k)[1] for k in stale})))
            for k in stale:
                put.pop(k, None)
            failed = [k for k in failed if k not in stale]
        if only is not None:
            # A list the server declared changed but that could not be
            # fetched again must not be served from the cache: neither now
            # (it stays dirty, so requests go to the live server) nor in a
            # later session (its entry is dropped).
            refreshed = {C.key_parts(k)[1] for k in put}
            self.dirty.difference_update(m for m in only if m in refreshed and not any(C.key_parts(k)[1] == m for k in failed))
            if failed:
                self.cache.forget(failed)
                self.log("refresh-failed", dropped=",".join(C.key_parts(k)[1] for k in failed))
        if not put or self.child is not child:
            return
        changed = self.cache.update(put, forget_unchecked=only is None, keep=set(stale))
        self.log("refresh", checked=",".join(C.key_parts(k)[1] for k in put), changed=len(changed))
        if changed:
            self.log("cache-refreshed", changed=",".join(C.key_parts(k)[1] for k in changed))
        await self._notify_changes(put)

    async def _notify_changes(self, put: dict[str, dict]) -> None:
        kinds = set()
        for key, reply in put.items():
            served = self.served.get(key)
            if served is None:
                continue
            fresh = C.dumps(reply)
            if served != fresh:
                self.served[key] = fresh
                method = C.key_parts(key)[1]
                if method in LIST_KIND:
                    kinds.add(LIST_KIND[method])
        if not kinds:
            return
        if self.client_init is None:
            # Modern clients only take change notifications on a
            # subscriptions/listen stream; the new lists reach the next session.
            self.log("list-changed-unsent", kinds=",".join(sorted(kinds)), why="modern-session")
            return
        for kind in sorted(kinds):
            cap = self.served_caps.get(kind)
            if isinstance(cap, dict) and cap.get("listChanged"):
                self.log("list-changed", kind=kind)
                await self._to_client(encode({"jsonrpc": "2.0", "method": f"notifications/{kind}/list_changed"}))
            else:
                self.log("list-changed-unsent", kind=kind, why="not-advertised")

    # ------------------------------------------------------------------- idle

    def _touch(self) -> None:
        self.last_activity = asyncio.get_running_loop().time()

    def _busy(self) -> bool:
        return bool(self.client_pending or self.server_pending or self.internal or self.queue)

    async def _idle_loop(self) -> None:
        loop = asyncio.get_running_loop()
        idle = self.opts.idle
        while True:
            wait = self.last_activity + idle - loop.time()
            if self.state != "running":
                wait = min(idle, 5.0)
            elif wait <= 0 or self._busy():
                wait = min(idle, 1.0)
            await asyncio.sleep(wait)
            child = self.child
            if self.state != "running" or child is None or self._busy() or self.pinned:
                continue
            if loop.time() - self.last_activity >= idle:
                await self._stop_child(child, "idle")

    # --------------------------------------------------------------- shutdown

    def _on_signal(self, sig: int) -> None:
        name = signal.Signals(sig).name
        self.log("signal", sig=name, state=self.state)
        if self.closing_reason is None:
            asyncio.ensure_future(self.shutdown(name, fast=True))

    async def shutdown(self, reason: str, fast: bool) -> None:
        if self.closing_reason is not None:
            return
        self.closing_reason = reason
        if not fast and self.refresh_tasks:
            # Let a cache refresh in progress finish (bounded), so what it
            # learned about a changed server is not lost.
            await asyncio.wait(list(self.refresh_tasks), timeout=REFRESH_WAIT)
        self.state = "closing"
        if self.start_task is not None and not self.start_task.done() and self.child is None:
            # A spawn is under way: let it produce the process so it can be stopped.
            await asyncio.wait([self.start_task], timeout=1.0)
        child = self.child
        if child is not None:
            await self._stop_child(child, reason, fast=fast)
        self.log("exit", reason=reason)
        self.done.set()

    # -------------------------------------------------------------------- I/O

    async def _to_child(self, raw: bytes) -> None:
        child = self.child
        if child is None:
            return
        if not await child.send(raw):
            self.log("drop", what="write-to-server-failed")

    async def _to_client(self, raw: bytes) -> None:
        if self.writer is None or self.state == "closing" and self.closing_reason == "client-gone":
            return
        self.log.frame("<c", raw)
        try:
            self.writer.write(raw)
            await self.writer.drain()
        except (BrokenPipeError, ConnectionResetError, RuntimeError):
            if self.closing_reason is None:
                asyncio.ensure_future(self.shutdown("client-gone", fast=False))


def run(opts: Options, cache: C.Cache, log: Log) -> int:
    proxy = Proxy(opts, cache, log)
    try:
        asyncio.run(proxy.run())
    except KeyboardInterrupt:
        pass
    return 0
