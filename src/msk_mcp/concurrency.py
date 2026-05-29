from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import TypeVar

T = TypeVar("T")


class Bouncer:
    """Two-tier concurrency bound: total tool calls + a stricter inner cap on CLI subprocesses.

    A CLI tool acquires the tool semaphore (broad cap) and the CLI executor then acquires
    the cli semaphore (JVM fan-out cap). AdminClient tools only acquire the tool semaphore.
    """

    def __init__(self, tool_limit: int, cli_limit: int) -> None:
        if tool_limit < 1 or cli_limit < 1:
            raise ValueError("limits must be >= 1")
        self._tool_sem = asyncio.Semaphore(tool_limit)
        self._cli_sem = asyncio.Semaphore(cli_limit)

    @property
    def cli_semaphore(self) -> asyncio.Semaphore:
        return self._cli_sem

    async def run_tool(self, awaitable: Awaitable[T], timeout: float) -> T:
        async with self._tool_sem:
            return await asyncio.wait_for(awaitable, timeout)
