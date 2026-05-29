from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from msk_mcp.cli_executor import CliResult
from msk_mcp.server import create_mcp

SAMPLE_LOG_DIRS_OUTPUT = """\
Querying brokers for log directories information
""" + json.dumps(
    {
        "version": 1,
        "brokers": [
            {
                "broker": 1,
                "logDirs": [
                    {
                        "logDir": "/data/kafka",
                        "error": None,
                        "partitions": [
                            {
                                "partition": "orders-0",
                                "size": 1024,
                                "offsetLag": 0,
                                "isFuture": False,
                            },
                            {
                                "partition": "orders-1",
                                "size": 2048,
                                "offsetLag": 99,
                                "isFuture": True,
                            },
                        ],
                    }
                ],
            }
        ],
    }
)


async def _call_tool(mcp, name: str, args: dict) -> dict:
    raw = await mcp.call_tool(name, args)
    if isinstance(raw, tuple):
        contents, structured = raw
        if structured:
            return structured
        items = contents
    else:
        items = raw
    for item in items or []:
        text = getattr(item, "text", None)
        if text:
            return json.loads(text)
    raise AssertionError(f"No text content in tool result: {raw!r}")


async def test_describe_log_dirs_via_mcp(app_context):
    """Verifies the CLI subprocess execution path is wired all the way through FastMCP.

    We monkeypatch the cli_executor.run method to return a canned CLI result, so the
    test exercises argv construction, JSON parsing, stuck-reassignment detection, and
    the FastMCP→tool plumbing without spawning a real JVM.
    """
    app_context.cli_executor.run = AsyncMock(
        return_value=CliResult(
            returncode=0,
            stdout=SAMPLE_LOG_DIRS_OUTPUT.encode(),
            stderr=b"",
            duration_ms=5,
        )
    )

    mcp = create_mcp(app_context)
    result = await _call_tool(
        mcp,
        "describe_log_dirs",
        {"cluster_id": "poc-dev"},
    )

    assert "error" not in result
    assert len(result["brokers"]) == 1
    assert result["brokers"][0]["broker_id"] == 1
    # Stuck reassignment surfaced separately
    assert len(result["stuck_reassignments"]) == 1
    assert result["stuck_reassignments"][0]["topic"] == "orders"
    assert result["stuck_reassignments"][0]["partition"] == 1


async def test_describe_log_dirs_passes_cluster_config_path(app_context):
    """Verifies the tool passes --command-config pointing at the rendered properties file."""
    captured = {}

    async def _fake_run(argv, timeout, correlation_id=None, cwd=None):
        captured["argv"] = argv
        return CliResult(
            returncode=0, stdout=SAMPLE_LOG_DIRS_OUTPUT.encode(), stderr=b"", duration_ms=1
        )

    app_context.cli_executor.run = _fake_run

    mcp = create_mcp(app_context)
    await _call_tool(mcp, "describe_log_dirs", {"cluster_id": "poc-dev"})

    argv = captured["argv"]
    assert "--command-config" in argv
    cfg_path = argv[argv.index("--command-config") + 1]
    assert cfg_path.endswith("poc-dev.properties")
    assert "--describe" in argv
