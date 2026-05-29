from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from msk_mcp.cli_executor import CliResult
from msk_mcp.config import load_registry
from msk_mcp.tools.describe_log_dirs import describe_log_dirs


def _registry(tmp_path) -> "ClustersRegistry":  # type: ignore[name-defined]
    p = tmp_path / "clusters.yaml"
    p.write_text(
        """
clusters:
  poc-dev:
    bootstrap_servers: b-1.example:9098
    region: us-east-1
    auth_type: IAM
"""
    )
    return load_registry(p)


def _properties_mgr(tmp_path) -> MagicMock:
    m = MagicMock()
    m.get_path.return_value = tmp_path / "poc-dev.properties"
    return m


SAMPLE_OUTPUT = """\
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
                            {"partition": "orders-0", "size": 1024, "offsetLag": 0, "isFuture": False},
                            {"partition": "orders-1", "size": 2048, "offsetLag": 5, "isFuture": True},
                        ],
                    }
                ],
            },
            {
                "broker": 2,
                "logDirs": [
                    {
                        "logDir": "/data/kafka",
                        "error": "KafkaStorageException",
                        "partitions": [],
                    }
                ],
            },
        ],
    }
)


def _executor_returning(stdout: bytes) -> MagicMock:
    e = MagicMock()
    e.run = AsyncMock(return_value=CliResult(returncode=0, stdout=stdout, stderr=b"", duration_ms=10))
    return e


async def test_describe_log_dirs_parses_brokers_and_partitions(tmp_path):
    reg = _registry(tmp_path)
    ex = _executor_returning(SAMPLE_OUTPUT.encode())
    result = await describe_log_dirs(
        registry=reg,
        properties=_properties_mgr(tmp_path),
        executor=ex,
        kafka_bin_path="/opt/kafka/bin",
        timeout=10.0,
        cluster_id="poc-dev",
    )
    assert "error" not in result
    assert len(result["brokers"]) == 2
    assert result["brokers"][0]["broker_id"] == 1
    assert result["brokers"][0]["log_dirs"][0]["partitions"][0]["topic"] == "orders"
    assert result["brokers"][0]["log_dirs"][0]["partitions"][0]["partition"] == 0


async def test_describe_log_dirs_surfaces_stuck_reassignments(tmp_path):
    reg = _registry(tmp_path)
    ex = _executor_returning(SAMPLE_OUTPUT.encode())
    result = await describe_log_dirs(
        registry=reg,
        properties=_properties_mgr(tmp_path),
        executor=ex,
        kafka_bin_path="/opt/kafka/bin",
        timeout=10.0,
        cluster_id="poc-dev",
    )
    assert len(result["stuck_reassignments"]) == 1
    assert result["stuck_reassignments"][0]["broker_id"] == 1
    assert result["stuck_reassignments"][0]["topic"] == "orders"
    assert result["stuck_reassignments"][0]["partition"] == 1


async def test_describe_log_dirs_surfaces_disk_errors(tmp_path):
    reg = _registry(tmp_path)
    ex = _executor_returning(SAMPLE_OUTPUT.encode())
    result = await describe_log_dirs(
        registry=reg,
        properties=_properties_mgr(tmp_path),
        executor=ex,
        kafka_bin_path="/opt/kafka/bin",
        timeout=10.0,
        cluster_id="poc-dev",
    )
    assert len(result["disk_errors"]) == 1
    assert result["disk_errors"][0]["broker_id"] == 2
    assert "Storage" in result["disk_errors"][0]["error"]


async def test_describe_log_dirs_topic_filter(tmp_path):
    output = json.dumps(
        {
            "brokers": [
                {
                    "broker": 1,
                    "logDirs": [
                        {
                            "logDir": "/d",
                            "error": None,
                            "partitions": [
                                {"partition": "orders-0", "size": 1, "offsetLag": 0, "isFuture": False},
                                {"partition": "events-0", "size": 1, "offsetLag": 0, "isFuture": False},
                            ],
                        }
                    ],
                }
            ]
        }
    )
    reg = _registry(tmp_path)
    ex = _executor_returning(("hdr\n" + output).encode())
    result = await describe_log_dirs(
        registry=reg,
        properties=_properties_mgr(tmp_path),
        executor=ex,
        kafka_bin_path="/opt/kafka/bin",
        timeout=10.0,
        cluster_id="poc-dev",
        topic_filter="orders",
    )
    parts = result["brokers"][0]["log_dirs"][0]["partitions"]
    assert len(parts) == 1
    assert parts[0]["topic"] == "orders"


async def test_describe_log_dirs_handles_garbage_output(tmp_path):
    reg = _registry(tmp_path)
    ex = _executor_returning(b"some random text without json")
    result = await describe_log_dirs(
        registry=reg,
        properties=_properties_mgr(tmp_path),
        executor=ex,
        kafka_bin_path="/opt/kafka/bin",
        timeout=10.0,
        cluster_id="poc-dev",
    )
    assert result["error"] is True
    assert result["error_type"] == "EXECUTION_FAILURE"


async def test_describe_log_dirs_passes_broker_list_arg(tmp_path):
    reg = _registry(tmp_path)
    ex = _executor_returning(SAMPLE_OUTPUT.encode())
    await describe_log_dirs(
        registry=reg,
        properties=_properties_mgr(tmp_path),
        executor=ex,
        kafka_bin_path="/opt/kafka/bin",
        timeout=10.0,
        cluster_id="poc-dev",
        broker_ids="1,2",
    )
    args, kwargs = ex.run.call_args
    argv = args[0]
    assert "--broker-list" in argv
    assert argv[argv.index("--broker-list") + 1] == "1,2"
