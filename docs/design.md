# Design

One `mcp-hotswap` process sits between one MCP client connection and one
real stdio server. It owns at most one server process at a time. The
lessons that shaped these choices, and recordings of what Claude Code and
Codex actually send, are in [prior-art.md](prior-art.md).

```
client ──stdin/stdout── mcp-hotswap ──pipes── real server (own session)
                            │
                ~/.cache/mcp-hotswap/<cmd>-<hash>.json
```

## States

| state | server | client requests |
|---|---|---|
| `cold` | none | discovery, `ping`, `logging/setLevel` answered locally; anything else is queued and starts the server |
| `starting` | spawned, handshake being replayed | discovery still from the cache; everything else queued |
| `running` | up | relayed; `ping` and cached discovery still answered locally |
| `stopping` | idle stop in progress | queued; the server is started again once the stop finishes |
| `closing` | being shut down | ignored; nothing restarts |

Frames are parsed only to read `id`, `method` and a few params. The original
bytes are what gets forwarded, so unknown fields and extensions survive.

## The cache

One JSON file per server identity: the command as given, its resolved path,
and the hashed values of the env vars named with `--key-env` (plus the cwd
with `--key-cwd`). The whole environment cannot be used, because clients put
per-session values there. Files are 0600 in a 0700 directory, written to a
temp file and renamed into place. Writes take an exclusive `flock` and merge
into a fresh read, so concurrent sessions do not lose each other's updates.

An entry maps a normalised request to the server's whole reply:

- **key** = `[protocol version, method, params without _meta]`. The version
  comes from `_meta["io.modelcontextprotocol/protocolVersion"]` (modern,
  2026-07-28 and later), from `initialize`'s params, or, for other legacy
  requests, from the version negotiated in the session. `initialize` is keyed
  on the version alone, so a client upgrade (new `clientInfo`) still hits.
  `_meta` is dropped because clients put per-request values there (Codex
  sends `progressToken`).
- **reply** = `{"result": ...}`, or `{"error": ...}` for errors that are
  answers about the protocol rather than the moment. That means
  `-32601/-32602/-32600/-32022` for `server/discover` (how a legacy server
  answers the probe Claude Code sends first) and `-32601` for lists (Codex
  asks for resource lists a server may not have). Other errors are never
  stored. A cached transient error on `tools/list` would hide the tools, and
  nothing would ever start the server to correct it.
- **Methods**: `server/discover`, `initialize`, `tools/list`,
  `prompts/list`, `resources/list`, `resources/templates/list`. Tool results,
  resource contents and prompts are never stored.
- **Pagination**: lists are stored as one complete page without
  `nextCursor`. Cursors belong to one server process. Pages that pass through
  are joined as the client fetches them, and the refresh fetches all pages
  itself. A request that carries a cursor is never answered from the cache.

A request with a version the cache has not seen is a miss, and a miss
starts the server. So a client that asks for a different protocol version
gets an answer from the real server instead of a guess, and that answer is
recorded. Claude Code (modern for dual-era servers) and Codex (legacy) can
share one cache file, because their entries have different keys.

`ttlMs` and `cacheScope` in modern list results are passed to the client
unchanged but do not expire our cache: mcp-dblp returns `ttlMs: 0`, which
would mean never answering from it. Freshness comes from the refresh below.
`cacheScope: "private"` only forbids shared caches, and this one is per user.

## Many sessions at once

Every new session, and every sub-agent with its own MCP connections, gets its
own wrapper. Each wrapper answers discovery from the cache and starts
nothing, so opening a session costs a wrapper start (about 40 ms for
discovery) instead of a server start. In one observed run, Claude Code
2.1.328 sub-agents reused the parent session's MCP connections (no new
wrapper). Codex sub-agents open
their own, and each one is answered from the cache. The cache has no
expiry; it is re-checked whenever a server really starts (see Refresh).

The exception is a cache miss with several sessions opening at once (the
very first run, or a new protocol version). Without coordination, every one
of them would start the real server. On a discovery miss, a wrapper first
re-reads the cache file, then takes a per-server collector lock
(`<cache>.collect`, `flock`). The first wrapper to get the lock starts the
server and records its replies, and releases the lock once a list reply is
stored, its server stops, or 30 s pass. The others wait for the lock (at
most 15 s), then re-read the cache and answer from it. Only if the entry is
still missing do they start their own server.

## Starting the server

The first request that cannot be answered locally spawns the server in a new
session (its own process group). What happens next depends on the session's
protocol era:

- **Legacy** (the client sent `initialize`, answered from the cache): the
  wrapper sends the client's `initialize` params with the protocol version
  it already gave the client, under an id like `mcp-hotswap:1`. Then come
  `notifications/initialized` (if the client sent it) and the remembered
  `logging/setLevel`. Internal ids are `mcp-hotswap:<random>:<n>`, never
  equal to an outstanding client id. A server reply goes to the wrapper only
  if its id exactly matches an outstanding internal request, or one that
  timed out (a late reply is dropped, not leaked to the client). Any other
  reply, even one whose id starts with `mcp-hotswap:`, goes to the client. If
  the server asks the client something (e.g. a ping) before it answers the
  replayed `initialize`, the client's answer is relayed at once, not queued. If the
  server rejects `initialize`, the error goes back to every queued request
  under the client's ids, and the server is stopped.
- **Modern** (requests carry `_meta`): no handshake, because each request
  describes itself.
- **No cache yet**: the client's own handshake is the first thing queued, so
  the server sees exactly what the client sent, and the replies are recorded.

Then the queue is flushed in order. A `notifications/cancelled` for a
request still in the queue removes that request. Cancelling a request
already forwarded passes the notification on and stops counting the request
as in flight.

## Refresh

After a start that followed answers from the cache, the wrapper asks the
live server again, with internal ids, for everything the cache holds for
this session's protocol version, and compares the replies. A legacy session
never sends `server/discover` to its server: a dual-era server such as
mcp-dblp switches a process to the modern protocol once it sees one modern
request, and would then refuse the session.

- **The replayed `initialize` reply differs from the cache**: this is saved
  at once (dropping entries that cannot be re-checked), before the queued
  requests are released. A session that ends straight after its first call
  still leaves a corrected cache. At client EOF the wrapper also gives a
  running refresh up to 2 s to finish. The refresh re-checks everything the
  client was served as well as what the cache file still holds, so the lists
  dropped this way are fetched again and compared.
- **Something changed**: the cache is rewritten, and entries that could not
  be re-checked (other versions, the other era's probe) are dropped, to be
  learned again from the real server the next time a client asks. This is
  how an upgraded server corrects a stale cache within one session.
- **A list the client got from the cache changed**: a legacy session that
  advertised `listChanged` gets `notifications/<kind>/list_changed`. Modern
  clients only accept change notifications on a `subscriptions/listen`
  stream, so for them the corrected lists take effect in the next session.
- **The server sends a `list_changed` itself**: the notification is
  forwarded, the affected lists are fetched again into the cache, and until
  that finishes the client's requests for them go to the live server, so an
  immediate re-fetch never gets the old list. If the re-fetch fails, the
  stale entry is dropped from the cache file and those lists keep going to
  the live server.

## Idle stop

With `--idle T`, the server is stopped when all of the following hold:
nothing is in flight (client requests, server-to-client requests, our own
internal requests, queued requests), and nothing has been relayed for `T`.
`ping` and discovery answered from the cache do not count as activity. A
client that polls them neither starts the server nor keeps it alive.

`--pin TOOL[,TOOL...]` (repeatable) names tools that create state the
server must keep. Once a `tools/call` for one of them is forwarded (alone
or inside a batch), idle stop is off for that server process. The pin ends only when the process
ends for another reason (crash or session end), because the state is then
gone anyway. For scholar-mcp, pinning `ingest_paper_fulltext` covers the
ingestion jobs, which keep running after the call has answered, so the
in-flight rule alone does not protect them.

The stop is the normal stop sequence. The next request starts the server
again exactly as above, replaying the handshake for legacy sessions, so the
client never sees a disconnect. State the server held in memory is lost.
mcp-doze leaves idle stop out for that reason; here it is opt-in per server
and off by default. A long-lived request (such as `subscriptions/listen`)
keeps the server up.

## Stopping the server

1. Close its stdin and wait `--stop-grace` (2 s; 0.25 s when the wrapper
   itself was signalled, because the client is about to kill it).
2. Send **one** SIGTERM to the direct child, never to its group.
3. Wait `--kill-grace` (3 s), then SIGKILL the process group. Members of the
   group still alive after the direct child exited get the same grace
   period, then the sweep.

Signalling the group with SIGTERM while the child is a launcher that forwards
signals (`uvx`, `npx`, `uv run`) would deliver SIGTERM twice to the real
server. That is what makes lean-lsp-mcp spin at 100% CPU. A test with a
forwarding launcher checks that the real server gets exactly one.

Triggers: the client closes our stdin, our stdout breaks, or we get
SIGINT/SIGTERM/SIGHUP. All triggers lead to the same sequence, run once.
Later signals are only logged. That matters because Claude Code sends SIGINT,
SIGTERM 100 ms later and another SIGTERM at 350 ms, then SIGKILL at about
500 ms, all without closing stdin. Under Claude Code the server therefore
normally exits on the EOF from step 1 (mcp-dblp takes about 55 ms,
scholar-mcp about 5 ms). If the wrapper is SIGKILLed first, the kernel closes
the pipe, and the server sees the same EOF.

## Logging

The server's stderr is inherited, so it reaches the client's MCP log
directly. The wrapper writes to stderr only when something goes wrong (it
cannot start the server, the handshake fails, the server exits unexpectedly).
`--debug-log PATH` records every start (with the method that triggered it),
stop (with reason and how it ended), cache hit and miss, refresh and signal.
Add `--trace` to record every frame in both directions. A `spawn` line whose
trigger is a discovery method in a session that already has a cache means
the client sent something at connect time that the cache does not cover.

## Decisions and deviations from the brief

- **Request-to-reply cache, both eras.** The brief's list of cached methods
  assumed the `initialize` handshake. Claude Code 2.1.328 instead probes with
  `server/discover` and uses the stateless 2026-07-28 protocol whenever the
  server supports it. Keying on (version, method, params) covers both eras
  without special cases.
- **Env in the cache key only via `--key-env`.** The brief asked for the
  server's env in the key, but the env a client passes contains per-session
  values, so the whole env would mean a cache miss every session. Pass
  `--key-env MCP_DBLP_HOME` for dblp.
- **Discovery is answered from the cache even while the server runs**, so
  polling does not defeat idle stop. `ping` is always answered locally.
- **A changed server does not close the connection** (as mcp-doze does). The
  queue is released first, and the refresh corrects the cache in the
  background.
- **`--pin`** for servers where only some tools create state.
- **Collector lock on cache misses**: concurrent first sessions (or
  sub-agents) start one server, not one each.
- **Batches** are relayed to a started server and never crash the wrapper.
- **Server-to-client requests** (sampling, roots, elicitation) are relayed
  both ways. They count as in flight until the client answers.

## Limits

- One client per wrapper; no sharing of a server between sessions.
- A long-lived request keeps an idle server up, and a modern client's
  `subscriptions/listen` is not answered from the cache. Neither current
  client sends one to these servers.
- Modern sessions get no `list_changed` from a refresh (see above).
- A server that answers the `server/discover` probe with an error outside
  the cached codes, or not at all, is started by every session that probes
  (the client waits for the probe to time out). The debug log shows
  `spawn trigger=server/discover` in every session. A probe that is never
  answered also counts as in flight, so it blocks idle stop.
- Lines over 64 MiB end the connection.
- POSIX only (process groups and signals); tested on macOS. The Linux run is
  pending.
