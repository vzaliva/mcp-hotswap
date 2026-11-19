# AGENTS.md

Guidance for agents working on this repository.

## What this is

`mcp-hotswap` is a lazy-start wrapper for stdio MCP servers. It answers
discovery from a cache, starts the real server on the first real request and
optionally stops it when idle. Read `README.md` for the behaviour,
`docs/design.md` for how it works and why, and `docs/prior-art.md` for the
traps other projects hit and what real clients were observed to send.

## Layout

```
src/mcp_hotswap/cli.py     argument parsing, --show-cache
src/mcp_hotswap/proxy.py   state machine and relay (cold/starting/running/stopping/closing)
src/mcp_hotswap/cache.py   cache key, cacheable replies, atomic locked writes, collector lock
src/mcp_hotswap/child.py   spawn in a new session; stop sequence (EOF, one SIGTERM, group SIGKILL)
src/mcp_hotswap/log.py     --debug-log / --trace
tests/fake_server.py       fake MCP server (legacy, modern, dual era), configured by FAKE_* env vars
tests/fake_launcher.py     signal-forwarding launcher, stands in for uvx/npx
```

## Commands

```sh
uv sync                    # dev environment (pytest)
uv run pytest -q           # full suite, about 40 s
uv run mcp-hotswap --help
uv build                   # wheel and sdist
```

Before reporting work as done, run the suite on the minimum supported Python
too: `UV_PROJECT_ENVIRONMENT=<scratch dir> uv run --python 3.11 pytest -q`.

## Rules for the code

- Python >= 3.11, **standard library only**. Keep imports minimal: an idle
  wrapper should stay around 15 MB.
- POSIX only (Linux and macOS). No Linux-only tricks such as
  `PR_SET_PDEATHSIG`.
- Forward the original frame bytes. Parse only to read `id`, `method` and the
  params you need; never re-serialise a relayed message.
- Shutdown: close the child's stdin, wait, send **one** SIGTERM to the
  **direct child only**, wait, then SIGKILL the group. Never send SIGTERM to
  the process group and never send it twice. A test with the fake launcher
  guards this.
- Cache only discovery replies (and only the error codes listed in
  `cache.py`), never tool results, resource contents or anything that may
  hold credentials.
- A legacy session must never send a modern request (`server/discover`,
  `_meta` protocol version) to its server process, and the reverse.
- Every behaviour change gets a test against the fake server. Prove that a
  new test can fail: break the code briefly and watch the test catch it.

## Rules for tests

- Tests identify server processes by the pid in the fake server's own event
  log. Never use `pgrep -f` / `pkill -f` by name, and never signal processes
  a test did not start. The user's live sessions run the same real servers.
- Each test gets a private cache dir via the `env` fixture; never touch
  `~/.cache/mcp-hotswap`.

## Testing with real clients

- Isolate every run: `claude -p ... --model haiku --strict-mcp-config
  --mcp-config <temp file>`, or `codex exec --ignore-user-config --ephemeral
  -m <smallest model>`. Use the cheapest model and minimal prompts.
- Point `--cache-dir` and `--debug-log` at a scratch directory.
- **Do not edit live client configs** (`~/.claude.json`,
  `~/.claude/settings.json`, `~/.codex/config.toml`). Switching them to the
  wrapper is for the user to approve. Back up any config before an approved
  edit (`<file>.bak-<what>-<date>`).

## Git and publishing

- **No destructive or outward-facing git operations without explicit
  authorization from the user**: no commits, pushes, force-pushes, rebases,
  resets, branch deletions, tag pushes or history rewrites. Leave work
  uncommitted in the working tree unless the user explicitly asks for a
  commit. Authorization for one operation does not carry over to the next.
- Do not publish to PyPI or any other registry without explicit
  authorization.
- **No attribution in commit messages**: no `Co-Authored-By`, no
  "Generated with" lines, no session links, no mention of AI or of any
  assistant, in commits, comments or files.
- Commit message style, when a commit is authorized: lowercase subject; a
  prose body explaining why, and the trap being avoided.
