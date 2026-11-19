# Prior art and lessons

Four projects wrap a stdio MCP server, answer discovery from a cache and start
the real server on first use. They were read for their design notes, issues
and fix commits (the code was not reused). This file lists what applies to
mcp-hotswap and what we decided because of it. It ends with what the current
clients were observed to send, which turned out to matter more than any of
the four projects.

## mmng-a/mcp-doze (Rust, Nov 2026)

The most careful of the four: explicit states (cold, starting, running,
closing), raw frames forwarded unchanged, a bounded startup queue, a
per-identity lock for cache collection, cache files with mode 0600.

Adopted:

- **Explicit state machine.** Ours is `cold → starting → running →
  stopping → cold` plus a terminal `closing`; `stopping` exists because we
  also stop on idle, which doze does not.
- **Forward the original bytes.** Each line is parsed only to read `id` and
  `method`; the raw line goes to the other side, so unknown fields and
  extensions survive.
- **Replay `initialize` with the version already negotiated with the
  client** (the version in the reply we gave from the cache), under an id the
  client cannot use.
- **Cancellation of a queued request removes it from the queue** instead of
  starting the server for it.
- **Do not replay pagination cursors.** Cursors belong to one server
  process. We store each list as one complete page (all pages concatenated,
  no `nextCursor`), so a client never holds a cursor from the cache. A
  request that carries a cursor goes to a live server.
- **Never cache execution results or credentials.** Only discovery replies
  are stored; tool results, resource contents and prompts never are.
- **Cache identity from selected env vars, not the whole environment** (see
  "Observed client behaviour" for why this is forced on us).
- **Return upstream initialization errors with the client's request ids.**
- **The backend gets its own process group.**
- **A per-identity lock for cache collection.** When several sessions (or
  sub-agents) open at once and the cache has no entry yet, only the one
  holding the lock starts the server; the others wait, re-read the cache and
  answer from it.

Rejected or changed:

- **Group-wide SIGTERM.** doze makes the wrapper the group leader, puts the
  backend in the same group and sends SIGTERM to the group. With a launcher
  backend (`uvx`, `npx`, `uv run`) the real server then gets SIGTERM twice,
  once from the group signal and once forwarded by the launcher, which is the
  suspected trigger of the lean-lsp-mcp spin (oOo0oOo/lean-lsp-mcp#246). doze
  also sends SIGTERM immediately after closing stdin, with no time to exit on
  EOF. We start the child in a new session (its own group, which also keeps
  the client's signals away from it), close its stdin, wait, send **one**
  SIGTERM to the direct child only, wait, and use the group only for the
  final SIGKILL sweep.
- **Closing the connection on a catalog mismatch.** doze verifies the live
  initialize and lists against the cache before releasing queued work and
  drops the connection if they differ. We release the queue first (lower
  latency for the call that woke the server), refresh in the background with
  internal ids, rewrite the cache atomically and send `list_changed` when a
  list differs. A changed `initialize` result (e.g. a version bump) only
  updates the cache for the next session; there is no notification for it.
- **No idle timeout** is one of doze's deliberate boundaries: a restarted
  backend loses its state and anything the client set up. We do want idle
  stop, so it is opt-in per server, never stops with a request in flight in
  either direction, and replays the handshake on respawn. For stateful
  servers (scholar's ingestion jobs) it stays off. Under the 2026-07-28
  protocol, which is stateless and tells clients to restart a server that
  exits, idle stop is much less risky than under the legacy one.
- **Separate collector process and helper pipe for cache collection.** Our
  cache is built by passing the first session through to the real server and
  recording its replies, then refreshed whenever a session starts the server.
  No extra process.

## sleticalboy/lazy-mcp-wrapper (Go)

Grew into a daemon with shared backends, HTTP proxying, OAuth and a setup
command. Most of that is out of scope. Lessons:

- **Its idle timer only counts time since the last message.** A tool call
  that runs longer than the timeout gets its server killed underneath it.
  Ours stops only when nothing is in flight in either direction (client
  requests, server-to-client requests and our own internal requests) *and*
  nothing was relayed for the whole timeout.
- **Its stop sends a signal straight away without closing stdin first.** We
  follow the spec order (close stdin, wait, then signal).
- **It answers `server/discover` itself** and has a plan for the stateless
  2026-07-28 protocol. Its stateless-protocol notes pointed us to the spec
  change that turned out to drive much of our design (below).
- **It honours `ttlMs` / `cacheScope` from list results** (2026-07-28). We
  deliberately do not use `ttlMs` to expire the cache: mcp-dblp returns
  `ttlMs: 0`, which would mean never serving from the cache. Freshness comes
  from re-validating every time the server starts (see `docs/design.md`).
  `cacheScope: "private"` forbids *shared* caches; ours is per user (0600
  files in the user's cache dir), so it qualifies as private.
- **`node_repl`-style stateful servers are a bad fit for idle stop**; it says
  so in its README, as do we.
- It defaults to `Content-Length` framing for backends. The stdio transport
  is newline-delimited JSON; we only support that.

## tkukushkin/lazy-mcp (Go)

Simple and close to our design; its fix commits are the most useful part:

- **"Cache server/discover probe instead of going live on it"** (Oct 2026):
  Claude Code started sending `server/discover` before `initialize`. lazy-mcp
  treated it as a real request, started the server on every session and
  saved an empty cache. Fix: cache the probe's whole response, *including the
  error* legacy servers return (`-32601`), and never save an empty cache.
  Same lesson for us, generalised: the cache maps a normalised discovery
  request to its whole reply, result or error.
- **"Harden server/discover handling"**: a `server/discover` notification
  (no id) must not crash anything; a probe without params must still be
  cached.
- **"Cache all lists when a list request triggers going live"** (Nov 2026):
  Codex asked every server for `resources/list` and `resources/templates/list`
  even when the server does not advertise resources. Those replies are errors
  from the real server; if they are not cached, every session starts the
  server again. Our refresh re-asks every list the cache already holds, not
  only the advertised ones, and method-not-found style errors to discovery
  requests are cached.
- **"Increase max message size from 1MB to 10MB"**: some servers send huge
  replies. We read lines with a 64 MiB limit.
- **Write the cache as soon as each discovery reply arrives**, not at session
  end (sessions end with SIGKILL often enough).
- Its cache key is command + args only, and it has no idle stop, no `ping`
  handling and no documented child shutdown.

## developit/mcp-later (bash)

- **"fix hanging after first tool call"**: background `cat` did not inherit
  stdin, and the server never saw EOF. Lesson: be explicit about who owns
  which end of each pipe and make sure the child sees EOF on shutdown. Our
  child's stdin is a pipe owned only by the wrapper; closing it is the first
  shutdown step.
- **Its tests found that `pgrep -f server-name` also matches unrelated
  processes** (the pipeline, the test runner). Our tests identify the child
  by the pid it reports in its own event log, never by `pgrep`, and never
  touch processes they did not start (the user's live sessions run the same
  servers).
- **A 24 h cache TTL** makes rarely used servers start once a day for
  nothing. We keep the cache until the real server shows something
  different.

## Observed client behaviour (Nov 2026)

Recorded with a pass-through logger between the client and the real
servers (`--trace` in our debug log does the same):

**Claude Code 2.1.328**

- Sends `server/discover` first (id `"server-discover-probe-1"`, modern
  `_meta` with protocolVersion `2026-07-28`). mcp-dblp 2.1.0 answers it (it is
  a modern, dual-era server); Claude then sends only `tools/list` with the
  same `_meta`: no `initialize`, no `initialized`. scholar-mcp 1.2.0 answers
  `-32601`; Claude falls back to `initialize` (protocolVersion `2025-11-25`),
  `notifications/initialized`, `tools/list`.
- Integer ids for normal requests, starting at 0.
- No `ping`, `logging/setLevel` or resource/prompt lists for these servers
  at connect.
- Sub-agents (the Agent tool) reuse the parent session's MCP connections: no
  extra server or wrapper process per sub-agent. (Codex sub-agents, per
  mcp-doze, inherit the MCP configuration and start their own.)
- **Shutdown never closes stdin.** It sends SIGINT, SIGTERM 100 ms later,
  another SIGTERM about 350 ms after the SIGINT and SIGKILL at about 500 ms.
  So Claude Code itself delivers two SIGTERMs to every server; a wrapper
  that absorbs signals and talks to the child only through stdin plus one
  SIGTERM shields servers like lean-lsp-mcp from that. The 500 ms budget is
  shorter than any sensible grace period, so under Claude Code the effective
  shutdown signal for the real server is the stdin EOF the wrapper produces
  on the first signal (or that the kernel produces when the wrapper is
  SIGKILLed).

**Codex CLI 0.170.0**

- Legacy handshake only: `initialize` (protocolVersion `2025-06-18`),
  `notifications/initialized`, `tools/list` with
  `params._meta.progressToken: 0`. mcp-dblp answers the legacy handshake too.
  So the cache key must ignore `_meta` (except the protocol version), and
  one server can have entries for both eras.
- Each server in its own process group; shutdown is one SIGTERM, then stdin
  is closed.

**Environment.** The env a server receives differs between two Claude
sessions in `CLAUDE_CODE_SESSION_ID`, `CLAUDE_PID`, messaging socket and
token, `PWD`, terminal and SSH variables, and so on. Hashing the whole
environment would give every session its own cache, so the cache identity
uses the command, its arguments and only the env vars named with
`--key-env`.
