"""Discovery cache: maps a normalised discovery request to the server's reply.

One file per server identity (command, arguments, selected env vars). An
entry is keyed by (protocol version, method, params without ``_meta``) and
holds the whole reply body, ``{"result": ...}`` or ``{"error": ...}``, so the
wrapper can answer a probe exactly as the real server would, including the
"method not found" a legacy server returns to ``server/discover``.

Only discovery replies are stored. Tool results, resource contents, prompts
and anything carrying credentials never are.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import tempfile
import time

FORMAT = 1

DISCOVER = "server/discover"
INITIALIZE = "initialize"
LIST_METHODS = (
    "tools/list",
    "prompts/list",
    "resources/list",
    "resources/templates/list",
)
CACHEABLE_METHODS = (DISCOVER, INITIALIZE) + LIST_METHODS

META_VERSION = "io.modelcontextprotocol/protocolVersion"

# Error replies worth replaying. They are answers about the protocol, not
# about the moment: a legacy server's "method not found" for the
# server/discover probe, or for a list it does not implement (Codex asks
# for resource lists regardless of capabilities). Anything else (internal
# errors, auth failures, timeouts) is never cached: a cached transient error
# on tools/list would hide the tools and nothing would ever start the server
# to heal it.
CACHEABLE_ERRORS = {
    DISCOVER: {-32601, -32602, -32600, -32022},
    INITIALIZE: set(),
}
LIST_CACHEABLE_ERRORS = {-32601}


def dumps(obj) -> str:
    """Compact canonical JSON, used for keys and for comparing replies."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def strip_meta(params):
    """Params without ``_meta``; ``None`` when nothing else is left."""
    if not isinstance(params, dict):
        return params
    rest = {k: v for k, v in params.items() if k != "_meta"}
    return rest or None


def request_version(msg: dict, session_version: str | None) -> str | None:
    """Protocol version a request is made under.

    Modern (2026-07-28+) requests carry it in ``_meta``; ``initialize`` names
    it in its params; any other legacy request inherits the version
    negotiated in the session.
    """
    params = msg.get("params")
    if isinstance(params, dict):
        meta = params.get("_meta")
        if isinstance(meta, dict) and isinstance(meta.get(META_VERSION), str):
            return meta[META_VERSION]
        if msg.get("method") == INITIALIZE and isinstance(params.get("protocolVersion"), str):
            return params["protocolVersion"]
    return session_version


def request_key(msg: dict, session_version: str | None) -> str | None:
    """Cache key for a client request, or ``None`` if it is not cacheable."""
    method = msg.get("method")
    if method not in CACHEABLE_METHODS:
        return None
    params = msg.get("params")
    if params is not None and not isinstance(params, dict):
        return None
    norm = strip_meta(params)
    if method == INITIALIZE:
        # clientInfo and capabilities change with every client release; the
        # server's answer is assumed to depend on the version only.
        norm = None
    elif method in LIST_METHODS and isinstance(norm, dict) and norm.get("cursor") is not None:
        # Cursors belong to one server process: never served from the cache.
        return None
    version = request_version(msg, session_version)
    return dumps([version, method, norm])


def key_parts(key: str):
    version, method, params = json.loads(key)
    return version, method, params


def cacheable_reply(method: str, msg: dict) -> dict | None:
    if "result" in msg:
        result = msg["result"]
        if not isinstance(result, dict):
            return None
        if method in LIST_METHODS and result.get("nextCursor"):
            return None  # partial page; complete lists are assembled elsewhere
        return {"result": result}
    err = msg.get("error")
    if isinstance(err, dict) and isinstance(err.get("code"), int):
        allowed = LIST_CACHEABLE_ERRORS if method in LIST_METHODS else CACHEABLE_ERRORS.get(method, set())
        if err["code"] in allowed:
            return {"error": err}
    return None


def cache_dir(override: str | None = None) -> str:
    if override:
        return override
    base = os.environ.get("XDG_CACHE_HOME") or os.path.join(os.path.expanduser("~"), ".cache")
    return os.path.join(base, "mcp-hotswap")


def identity(argv: list[str], key_env: list[str], key_cwd: bool) -> dict:
    resolved = shutil.which(argv[0]) or argv[0]
    env = {}
    for name in sorted(set(key_env)):
        value = os.environ.get(name)
        # Values are hashed, not stored: a selected variable may be a secret.
        env[name] = None if value is None else hashlib.sha256(value.encode()).hexdigest()[:16]
    ident = {"argv": list(argv), "resolved": os.path.realpath(resolved), "env": env}
    if key_cwd:
        ident["cwd"] = os.getcwd()
    return ident


def cache_path(directory: str, ident: dict) -> str:
    digest = hashlib.sha256(dumps(ident).encode()).hexdigest()[:20]
    name = "".join(c if c.isalnum() or c in "-_." else "_" for c in os.path.basename(ident["argv"][0]))
    return os.path.join(directory, f"{name or 'server'}-{digest}.json")


class Cache:
    """In-memory view of one cache file, written atomically under a lock."""

    def __init__(self, path: str, ident: dict, enabled: bool = True):
        self.path = path
        self.ident = ident
        self.enabled = enabled
        self.entries: dict[str, dict] = {}
        if enabled:
            self.entries = self._read()

    def _read(self) -> dict:
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, ValueError):
            return {}
        if not isinstance(data, dict) or data.get("format") != FORMAT or data.get("identity") != self.ident:
            return {}
        entries = data.get("entries")
        return entries if isinstance(entries, dict) else {}

    def reload(self) -> None:
        if self.enabled:
            self.entries = self._read()

    def try_collect(self) -> int | None:
        """Try to become the one wrapper that builds this cache.

        Returns a lock fd to pass to ``end_collect``, or ``None`` if another
        wrapper is collecting right now. Concurrent first sessions (several
        sub-agents opening at once) then wait for that one instead of each
        starting the real server.
        """
        if not self.enabled:
            return -1
        os.makedirs(os.path.dirname(self.path), mode=0o700, exist_ok=True)
        fd = os.open(self.path + ".collect", os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fd
        except OSError:
            os.close(fd)
            return None

    @staticmethod
    def end_collect(fd: int | None) -> None:
        if fd is not None and fd >= 0:
            try:
                os.close(fd)  # releases the flock
            except OSError:
                pass

    def get(self, key: str | None) -> dict | None:
        if key is None:
            return None
        entry = self.entries.get(key)
        if isinstance(entry, dict) and isinstance(entry.get("reply"), dict):
            reply = entry["reply"]
            if ("result" in reply) != ("error" in reply):
                return reply
        return None

    def update(self, put: dict[str, dict], forget_unchecked: bool = False, keep=()) -> list[str]:
        """Merge new replies into the file; return keys whose reply changed.

        ``put`` maps key -> reply body. With ``forget_unchecked``, if any
        reply changed, every entry not in ``put`` is removed: the server
        turned out to be different, so whatever could not be re-checked
        (other protocol versions, the other era's probe) is forgotten and
        re-learned from the real server the next time a client asks.
        Another wrapper may have written the file since we read it, so the
        merge happens on a fresh read under an exclusive lock.
        """
        if not self.enabled:
            return []
        os.makedirs(os.path.dirname(self.path), mode=0o700, exist_ok=True)
        lock_path = self.path + ".lock"
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            entries = self._read()
            changed = []
            for key, reply in put.items():
                old = entries.get(key)
                if not isinstance(old, dict) or dumps(old.get("reply")) != dumps(reply):
                    changed.append(key)
                entries[key] = {"reply": reply, "time": int(time.time())}
            dropped = False
            if forget_unchecked and changed:
                for key in list(entries):
                    if key not in put and key not in keep:
                        del entries[key]
                        dropped = True
            if changed or dropped or not os.path.exists(self.path):
                self._write(entries)
            self.entries = entries
            return changed
        finally:
            os.close(fd)

    def forget(self, keys) -> None:
        """Drop entries (replies known to be stale that could not be re-fetched)."""
        if not self.enabled:
            return
        lock_path = self.path + ".lock"
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            entries = self._read()
            gone = [k for k in keys if k in entries]
            for k in gone:
                del entries[k]
            if gone:
                self._write(entries)
            self.entries = entries
        finally:
            os.close(fd)

    def _write(self, entries: dict) -> None:
        data = {"format": FORMAT, "identity": self.ident, "entries": entries}
        directory = os.path.dirname(self.path)
        fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=1, sort_keys=True, ensure_ascii=False)
                f.write("\n")
            os.chmod(tmp, 0o600)
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
