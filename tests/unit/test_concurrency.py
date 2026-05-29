from __future__ import annotations

import asyncio

import pytest

from msk_mcp.concurrency import Bouncer


async def test_run_tool_returns_result():
    b = Bouncer(tool_limit=2, cli_limit=2)

    async def work() -> int:
        return 7

    assert await b.run_tool(work(), timeout=1.0) == 7


async def test_run_tool_enforces_timeout():
    b = Bouncer(tool_limit=2, cli_limit=2)

    async def slow() -> None:
        await asyncio.sleep(1.0)

    with pytest.raises(asyncio.TimeoutError):
        await b.run_tool(slow(), timeout=0.05)


async def test_tool_semaphore_caps_concurrency():
    b = Bouncer(tool_limit=2, cli_limit=2)
    in_flight = 0
    peak = 0

    async def work() -> None:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.05)
        in_flight -= 1

    await asyncio.gather(*(b.run_tool(work(), timeout=2.0) for _ in range(6)))
    assert peak <= 2


def test_invalid_limits_rejected():
    with pytest.raises(ValueError):
        Bouncer(tool_limit=0, cli_limit=1)
    with pytest.raises(ValueError):
        Bouncer(tool_limit=1, cli_limit=0)


async def test_cli_semaphore_exposed_for_executor():
    b = Bouncer(tool_limit=10, cli_limit=1)
    sem = b.cli_semaphore
    await sem.acquire()
    # second acquire should not complete until release
    task = asyncio.create_task(sem.acquire())
    await asyncio.sleep(0.05)
    assert not task.done()
    sem.release()
    await asyncio.wait_for(task, timeout=0.1)
    sem.release()
