"""Command line: ``mcp-hotswap [OPTIONS] -- COMMAND [ARG...]``."""

from __future__ import annotations

import argparse
import json
import re
import sys

from . import __version__

UNITS = {"ms": 0.001, "s": 1, "m": 60, "h": 3600}


def duration(text: str) -> float:
    """``15m``, ``90s``, ``1h``, ``500ms`` or plain seconds; 0 means off."""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h)?\s*", text)
    if not m:
        raise argparse.ArgumentTypeError(f"bad duration {text!r} (use e.g. 90s, 15m, 1h)")
    return float(m.group(1)) * UNITS[m.group(2) or "s"]


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="mcp-hotswap",
        usage="mcp-hotswap [OPTIONS] -- COMMAND [ARG...]",
        description=(
            "Lazy-start wrapper for a stdio MCP server: answers discovery from a "
            "cache, starts the server on the first real request and (with --idle) "
            "stops it again when idle."
        ),
    )
    p.add_argument("--idle", type=duration, default=None, metavar="DURATION",
                   help="stop the server after this long with nothing in flight and "
                        "nothing relayed (default: never). Do not use with servers "
                        "that keep state between calls.")
    p.add_argument("--pin", action="append", default=[], metavar="TOOL[,TOOL...]",
                   help="once one of these tools is called, never idle-stop that "
                        "server (it may hold state the tool created); repeatable")
    p.add_argument("--key-env", action="append", default=[], metavar="NAME",
                   help="include this env var in the cache identity (repeatable); "
                        "name the variables that change what the server offers, "
                        "e.g. MCP_DBLP_HOME")
    p.add_argument("--key-cwd", action="store_true",
                   help="include the working directory in the cache identity")
    p.add_argument("--cache-dir", metavar="DIR",
                   help="cache directory (default: $XDG_CACHE_HOME/mcp-hotswap or ~/.cache/mcp-hotswap)")
    p.add_argument("--no-cache", action="store_true",
                   help="never answer from or write the cache (plain pass-through)")
    p.add_argument("--debug-log", metavar="PATH",
                   help="append spawns, stops, cache hits and misses to PATH")
    p.add_argument("--trace", action="store_true",
                   help="with --debug-log, also record every frame in both directions")
    p.add_argument("--start-timeout", type=duration, default=120.0, metavar="DURATION",
                   help="how long to wait for the server to answer the replayed "
                        "initialize (default: 120s)")
    p.add_argument("--stop-grace", type=duration, default=2.0, metavar="DURATION",
                   help="after closing the server's stdin, wait this long before "
                        "the single SIGTERM (default: 2s; at most 0.25s when the "
                        "wrapper itself was signalled)")
    p.add_argument("--kill-grace", type=duration, default=3.0, metavar="DURATION",
                   help="after SIGTERM, wait this long before SIGKILL (default: 3s)")
    p.add_argument("--show-cache", action="store_true",
                   help="print the cache file path and its entries for COMMAND, then exit")
    p.add_argument("--version", action="version", version=f"mcp-hotswap {__version__}")
    p.add_argument("command", nargs=argparse.REMAINDER, help=argparse.SUPPRESS)
    return p


def main(argv: list[str] | None = None) -> None:
    args = parser().parse_args(argv)
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser().error("missing server command (put it after --)")

    from . import cache as C

    ident = C.identity(command, args.key_env, args.key_cwd)
    path = C.cache_path(C.cache_dir(args.cache_dir), ident)
    store = C.Cache(path, ident, enabled=not args.no_cache)

    if args.show_cache:
        print(path)
        for key in sorted(store.entries):
            version, method, params = C.key_parts(key)
            reply = store.entries[key].get("reply", {})
            kind = "result" if "result" in reply else f"error {reply.get('error', {}).get('code')}"
            extra = f" {json.dumps(params)}" if params else ""
            print(f"  {version or '-'}  {method}{extra}  -> {kind}")
        return

    from .log import Log
    from .proxy import Options, run

    log = Log(args.debug_log, trace=args.trace)
    opts = Options(
        argv=command,
        idle=args.idle or None,
        start_timeout=args.start_timeout,
        eof_grace=args.stop_grace,
        term_grace=args.kill_grace,
        pin=frozenset(t.strip() for item in args.pin for t in item.split(",") if t.strip()),
    )
    sys.exit(run(opts, store, log))
