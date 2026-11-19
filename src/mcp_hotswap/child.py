"""The real MCP server process: spawn and the stop sequence.

The child runs in a new session, so it has its own process group and the
signals the MCP client sends to the wrapper (one observed client sends
SIGINT and then two SIGTERMs) never reach it. The wrapper talks to it only through
its stdin and at most one SIGTERM:

1. close its stdin (the spec's graceful shutdown signal) and wait;
2. send one SIGTERM to the direct child, never to the group, and wait;
3. SIGKILL the whole group as the last-resort sweep.

Signalling the group with SIGTERM while the child is a launcher that
forwards signals (uvx, npx, uv run) would deliver SIGTERM twice to the
real server, which makes some Python/anyio servers spin at 100% CPU.
"""

from __future__ import annotations

import asyncio
import os
import signal

LINE_LIMIT = 64 * 1024 * 1024


class Child:
    def __init__(self, proc: asyncio.subprocess.Process, number: int):
        self.proc = proc
        self.pid = proc.pid
        self.number = number
        self.termed = False
        self.stop_task: asyncio.Task | None = None
        self.wait_task = asyncio.ensure_future(proc.wait())

    @classmethod
    async def spawn(cls, argv: list[str], number: int) -> "Child":
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=None,  # straight through to the wrapper's stderr
            start_new_session=True,
            limit=LINE_LIMIT,
        )
        return cls(proc, number)

    @property
    def exited(self) -> bool:
        return self.wait_task.done()

    async def send(self, raw: bytes) -> bool:
        stdin = self.proc.stdin
        if stdin is None or stdin.is_closing():
            return False
        try:
            stdin.write(raw)
            await stdin.drain()
            return True
        except (BrokenPipeError, ConnectionResetError, RuntimeError):
            return False

    async def readline(self) -> bytes:
        try:
            return await self.proc.stdout.readline()
        except (ValueError, asyncio.LimitOverrunError):
            # A line over the limit cannot be relayed; treat the stream as broken.
            return b""

    def close_stdin(self) -> None:
        stdin = self.proc.stdin
        if stdin is not None and not stdin.is_closing():
            try:
                stdin.close()
            except (BrokenPipeError, ConnectionResetError, RuntimeError):
                pass

    async def _wait(self, timeout: float) -> bool:
        if self.exited:
            return True
        try:
            await asyncio.wait_for(asyncio.shield(self.wait_task), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def _group_alive(self) -> bool:
        try:
            os.killpg(self.pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    @property
    def stopping(self) -> bool:
        return self.stop_task is not None

    async def stop(self, eof_grace: float, term_grace: float) -> str:
        """Run the stop sequence once; later callers wait for the same run."""
        if self.stop_task is None:
            self.stop_task = asyncio.ensure_future(self._stop(eof_grace, term_grace))
        return await asyncio.shield(self.stop_task)

    async def _stop(self, eof_grace: float, term_grace: float) -> str:
        how = "eof"
        self.close_stdin()
        if not await self._wait(eof_grace):
            how = "sigterm"
            if not self.termed and not self.exited:
                self.termed = True
                try:
                    self.proc.send_signal(signal.SIGTERM)
                except ProcessLookupError:
                    pass
            if not await self._wait(term_grace):
                how = "sigkill"
                self._kill_group()
                await self._wait(5.0)
        # The direct child is gone; descendants left in its group (a
        # launcher's real server, a stray helper) get the rest of the grace
        # period, then the SIGKILL sweep.
        if self._group_alive():
            loop = asyncio.get_running_loop()
            deadline = loop.time() + term_grace
            while self._group_alive() and loop.time() < deadline:
                await asyncio.sleep(0.05)
            if self._group_alive():
                how += "+group-sigkill"
                self._kill_group()
        return how

    def _kill_group(self) -> None:
        try:
            os.killpg(self.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            try:
                self.proc.kill()
            except ProcessLookupError:
                pass
