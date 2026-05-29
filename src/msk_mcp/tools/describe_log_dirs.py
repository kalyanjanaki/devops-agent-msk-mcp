from __future__ import annotations

import json
from typing import Any

from msk_mcp.cli_executor import CliExecutor
from msk_mcp.client_properties import ClientPropertiesManager
from msk_mcp.config import ClustersRegistry
from msk_mcp.errors import ErrorType, MskToolError, tool_error_handler
from msk_mcp.logging_setup import current_correlation_id


@tool_error_handler
async def describe_log_dirs(
    *,
    registry: ClustersRegistry,
    properties: ClientPropertiesManager,
    executor: CliExecutor,
    kafka_bin_path: str,
    timeout: float,
    cluster_id: str,
    broker_ids: str | None = None,
    topic_filter: str | None = None,
) -> dict[str, Any]:
    """Run kafka-log-dirs.sh --describe and return parsed structured output.

    Surfaces stuck reassignments (`isFuture: true`) and per-log-dir disk errors prominently
    so the agent can diagnose without scanning the whole payload.
    """
    cfg = registry.get(cluster_id)
    cfg_path = properties.get_path(cluster_id)

    argv = [
        f"{kafka_bin_path}/kafka-log-dirs.sh",
        "--bootstrap-server",
        cfg.bootstrap_servers,
        "--command-config",
        str(cfg_path),
        "--describe",
    ]
    if broker_ids:
        argv += ["--broker-list", broker_ids]

    result = await executor.run(argv, timeout=timeout, correlation_id=current_correlation_id())

    raw = result.stdout.decode(errors="replace")
    payload = _extract_json(raw)

    brokers_out: list[dict[str, Any]] = []
    stuck: list[dict[str, Any]] = []
    disk_errors: list[dict[str, Any]] = []

    for b in payload.get("brokers", []) or []:
        broker_id = b.get("broker")
        log_dirs_out: list[dict[str, Any]] = []
        for ld in b.get("logDirs", []) or []:
            partitions_out: list[dict[str, Any]] = []
            ld_error = ld.get("error")
            if ld_error:
                disk_errors.append(
                    {
                        "broker_id": broker_id,
                        "path": ld.get("logDir"),
                        "error": ld_error,
                    }
                )
            for p in ld.get("partitions", []) or []:
                topic_part = p.get("partition", "")
                topic = topic_part.rsplit("-", 1)[0] if "-" in topic_part else None
                if topic_filter and topic != topic_filter:
                    continue
                entry = {
                    "topic": topic,
                    "partition": _safe_int(topic_part.rsplit("-", 1)[-1]),
                    "size_bytes": p.get("size"),
                    "offset_lag": p.get("offsetLag"),
                    "is_future": bool(p.get("isFuture")),
                }
                partitions_out.append(entry)
                if entry["is_future"]:
                    stuck.append(
                        {"broker_id": broker_id, "path": ld.get("logDir"), **entry}
                    )
            log_dirs_out.append(
                {
                    "path": ld.get("logDir"),
                    "error": ld_error,
                    "partitions": partitions_out,
                }
            )
        brokers_out.append({"broker_id": broker_id, "log_dirs": log_dirs_out})

    summary = (
        f"brokers={len(brokers_out)}, "
        f"stuck_reassignments={len(stuck)}, "
        f"disk_errors={len(disk_errors)}"
    )

    return {
        "cluster_id": cluster_id,
        "brokers": brokers_out,
        "stuck_reassignments": stuck,
        "disk_errors": disk_errors,
        "summary": summary,
    }


def _extract_json(stdout: str) -> dict[str, Any]:
    """kafka-log-dirs.sh prints a header line, then a single JSON object.

    Strip everything before the first '{' and parse the rest.
    """
    idx = stdout.find("{")
    if idx < 0:
        raise MskToolError(
            ErrorType.EXECUTION_FAILURE,
            "kafka-log-dirs.sh output did not contain a JSON object",
            raw_stderr=stdout[:500],
        )
    try:
        return json.loads(stdout[idx:])
    except json.JSONDecodeError as e:
        raise MskToolError(
            ErrorType.EXECUTION_FAILURE,
            f"Failed to parse kafka-log-dirs.sh JSON: {e}",
            raw_stderr=stdout[:500],
        ) from e


def _safe_int(s: str) -> int | None:
    try:
        return int(s)
    except (TypeError, ValueError):
        return None
