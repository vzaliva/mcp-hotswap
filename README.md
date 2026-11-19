# mcp-hotswap

Lazy-start wrapper for stdio MCP servers. It answers the client's discovery
requests from a cache, so an unused server never starts. It starts the real
server on the first real request and, if asked, stops it again when it is
idle.

MCP clients start every configured stdio server when a session opens and keep
it until the session ends, whether or not a tool is ever called. With several
sessions open, each one holds its own copy of every server. Behind
mcp-hotswap, a session that never uses a server costs one small wrapper
process (about 15 MB) instead of the server.

- **Cached discovery.** `server/discover`, `initialize`, `tools/list`,
  `prompts/list`, `resources/list`, `resources/templates/list`, `ping` and
  `logging/setLevel` are answered without starting the server. This covers
  both the stateless 2026-07-28 protocol and the older handshake-based ones.
- **Start on first use.** Any other request starts the server. For legacy
  clients the wrapper first replays the client's `initialize` under its own
  request ids. Requests that arrive while the server starts are queued. From
  then on frames are relayed unchanged in both directions, including
  server-to-client requests.
- **Self-updating cache.** Each time the server starts, the wrapper asks it
  again for what the cache holds and rewrites the cache if anything changed.
  For legacy sessions it also sends `notifications/tools/list_changed` (and
  the prompts/resources equivalents). The cache never expires on its own.
  Before the first cache exists, the wrapper starts the server at once and
  records its replies. If several sessions or sub-agents open at the same
  time, one of them builds the cache and the others wait for it.
- **Optional idle stop.** With `--idle`, the server is stopped once nothing has
  been in flight in either direction and nothing has been relayed for that
  long. The next request starts it again, without the client noticing.
- **Careful shutdown.** The wrapper closes the server's stdin, waits, sends one
  SIGTERM, waits, then SIGKILLs the server's process group. The server runs in
  its own session, so it never gets the client's signals. Some clients send
  SIGINT and then two SIGTERMs, and some Python servers spin on a double
  SIGTERM.

Python 3.11+, standard library only, Linux and macOS.

## Install

```sh
uv tool install git+https://github.com/vzaliva/mcp-hotswap
# or from a checkout:  uv tool install .
```

## Configure

Put `mcp-hotswap [options] --` in front of the server's command and keep
everything else as it was. Claude Code (`~/.claude.json`):

```json
"mcp-dblp": {
  "type": "stdio",
  "command": "mcp-hotswap",
  "args": ["--idle", "15m", "--key-env", "MCP_DBLP_HOME", "--", "mcp-dblp"],
  "env": { "MCP_DBLP_HOME": "/home/me/.cache/mcp-dblp" }
},
"scholar_mcp": {
  "type": "stdio",
  "command": "mcp-hotswap",
  "args": ["--", "scholar-mcp", "--transport=stdio"],
  "env": { "SCHOLAR_MCP_TRANSPORT": "stdio" }
}
```

Codex (`~/.codex/config.toml`):

```toml
[mcp_servers.mcp-dblp]
command = "mcp-hotswap"
args = ["--idle", "15m", "--key-env", "MCP_DBLP_HOME", "--", "mcp-dblp"]
env = { MCP_DBLP_HOME = "/home/me/.cache/mcp-dblp" }
```

If the client cannot find `mcp-hotswap` on its `PATH`, give the full path
(`~/.local/bin/mcp-hotswap` after `uv tool install`).

Options:

| option | meaning |
|---|---|
| `--idle 15m` | stop the server after 15 minutes idle (default: never) |
| `--pin TOOL[,TOOL...]` | once one of these tools is called, never idle-stop that server (repeatable) |
| `--key-env NAME` | env var that changes what the server offers; part of the cache identity (repeatable) |
| `--key-cwd` | make the working directory part of the cache identity |
| `--debug-log PATH` | log starts, stops and cache hits/misses (`--trace`: every frame too) |
| `--cache-dir DIR` | default `$XDG_CACHE_HOME/mcp-hotswap`, else `~/.cache/mcp-hotswap` |
| `--start-timeout`, `--stop-grace`, `--kill-grace` | handshake timeout (120s); wait after closing stdin (2s) and after SIGTERM (3s) |
| `--show-cache` | print the cache file and its entries for the command, then exit |
| `--no-cache` | plain pass-through |

The cache identity is the command, its arguments and the env vars named with
`--key-env`. The rest of the environment is ignored, because clients put
per-session values there (session ids, sockets, `PWD`). So name the variables
that select a different server, such as an index location. If you forget
one, the first session after a change still sees the old tool list until the
server starts; after that the cache is corrected.

## Idle stop and stateful servers

Idle stop is off by default. A stopped server loses everything it held in
memory: background jobs, open sessions, anything set up by earlier calls.
Only use `--idle` for servers whose calls are independent, such as mcp-dblp.
A running request blocks the stop, but a background job the server started
on its own does not.

For servers where only some tools create state, name those tools with
`--pin`. Once one of them is called, the server stays up for the rest of the
session, while sessions that use only the other tools still unload.
scholar-mcp keeps ingested papers in memory: `ingest_paper_fulltext` starts
a background job, and `get_ingestion_status` and
`extract_granular_paper_details` read its results. Its other tools take
everything from their arguments:

```json
"args": ["--idle", "15m", "--pin", "ingest_paper_fulltext", "--",
         "scholar-mcp", "--transport=stdio"]
```

`--pin` accepts several tools, comma-separated or as repeated flags. Pin
every tool that creates state the server must keep.

See [docs/design.md](docs/design.md) for how it works and
[docs/prior-art.md](docs/prior-art.md) for the lessons taken from similar
tools and from what current clients actually send.
