from __future__ import annotations

import asyncio

import pytest

from msk_mcp.cli_executor import CliExecutor
from msk_mcp.errors import ErrorType, MskToolError


def _executor(limit: int = 4) -> CliExecutor:
    return CliExecutor(asyncio.Semaphore(limit))


async def test_run_returns_stdout_on_success():
    ex = _executor()
    result = await ex.run(["/bin/sh", "-c", "echo hello"], timeout=5.0)
    assert result.returncode == 0
    assert b"hello" in result.stdout


async def test_run_propagates_correlation_id_into_env():
    ex = _executor()
    result = await ex.run(
        ["/bin/sh", "-c", "echo $MSK_MCP_CID"],
        timeout=5.0,
        correlation_id="trace-xyz",
    )
    assert b"trace-xyz" in result.stdout


async def test_run_raises_timeout_and_kills_process():
    ex = _executor()
    with pytest.raises(MskToolError) as ei:
        # Sleep longer than the timeout; the executor should SIGTERM/SIGKILL.
        await ex.run(["/bin/sh", "-c", "sleep 5"], timeout=0.2)
    assert ei.value.error_type == ErrorType.TIMEOUT


async def test_run_raises_execution_failure_for_missing_binary():
    ex = _executor()
    with pytest.raises(MskToolError) as ei:
        await ex.run(["/this/binary/does/not/exist", "--help"], timeout=2.0)
    assert ei.value.error_type == ErrorType.EXECUTION_FAILURE


async def test_run_classifies_auth_failure_from_stderr():
    ex = _executor()
    with pytest.raises(MskToolError) as ei:
        await ex.run(
            ["/bin/sh", "-c", "echo 'SASL authentication failed' 1>&2; exit 2"],
            timeout=2.0,
        )
    assert ei.value.error_type == ErrorType.AUTH_FAILURE
    assert "SASL" in ei.value.raw_stderr


async def test_run_classifies_authorization_from_stderr():
    ex = _executor()
    with pytest.raises(MskToolError) as ei:
        await ex.run(
            ["/bin/sh", "-c", "echo 'TopicAuthorizationException: Not authorized' 1>&2; exit 1"],
            timeout=2.0,
        )
    assert ei.value.error_type == ErrorType.AUTHORIZATION


async def test_run_classifies_network_failure_from_stderr():
    ex = _executor()
    with pytest.raises(MskToolError) as ei:
        await ex.run(
            ["/bin/sh", "-c", "echo 'connection refused' 1>&2; exit 1"],
            timeout=2.0,
        )
    assert ei.value.error_type == ErrorType.NETWORK_TIMEOUT


async def test_run_classifies_generic_failure_when_no_pattern_matches():
    ex = _executor()
    with pytest.raises(MskToolError) as ei:
        await ex.run(
            ["/bin/sh", "-c", "echo 'something weird' 1>&2; exit 1"],
            timeout=2.0,
        )
    assert ei.value.error_type == ErrorType.EXECUTION_FAILURE


async def test_cli_semaphore_caps_concurrent_subprocesses():
    sem = asyncio.Semaphore(1)
    ex = CliExecutor(sem)

    async def slow() -> bytes:
        result = await ex.run(["/bin/sh", "-c", "sleep 0.1; echo done"], timeout=5.0)
        return result.stdout

    # Three concurrent calls with limit=1 should serialize.
    start = asyncio.get_event_loop().time()
    await asyncio.gather(slow(), slow(), slow())
    elapsed = asyncio.get_event_loop().time() - start
    assert elapsed >= 0.3 - 0.05  # 3 * 0.1, allow some scheduling slack


async def test_run_rejects_empty_argv():
    ex = _executor()
    with pytest.raises(ValueError):
        await ex.run([], timeout=1.0)
