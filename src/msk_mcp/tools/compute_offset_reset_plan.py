"""Read-only consumer offset reset planner.

This tool NEVER mutates broker state. It runs `kafka-consumer-groups.sh
--reset-offsets ... --dry-run`, parses the proposed plan, and returns it
together with the exact CLI command a human would run with `--execute` to
apply it. The MCP server itself does not execute the remediation — the
human takes responsibility for that step.

See feedback memory: MSK MCP server is debugging-only; mutations are
returned as command strings for humans to run.
"""

from __future__ import annotations

from typing import Any, Literal

from msk_mcp.cli_executor import CliExecutor
from msk_mcp.client_properties import ClientPropertiesManager
from msk_mcp.config import ClustersRegistry
from msk_mcp.errors import ErrorType, MskToolError, tool_error_handler
from msk_mcp.logging_setup import current_correlation_id

ResetStrategy = Literal["to-latest", "to-earliest", "to-offset", "shift-by"]

_VALID_STRATEGIES: frozenset[str] = frozenset(
    {"to-latest", "to-earliest", "to-offset", "shift-by"}
)


@tool_error_handler
async def compute_offset_reset_plan(
    *,
    registry: ClustersRegistry,
    properties: ClientPropertiesManager,
    executor: CliExecutor,
    kafka_bin_path: str,
    timeout: float,
    cluster_id: str,
    group_id: str,
    topic_name: str,
    reset_strategy: str,
    offset_value: int | None = None,
) -> dict[str, Any]:
    """Compute what a consumer-group offset reset WOULD do, without applying it.

    Always runs `--dry-run`. Returns the per-partition before/after offsets
    AND a `remediation_command` string the human can copy-paste with
    `--execute` to actually apply the change. The MCP server never executes
    the mutation.
    """
    _validate_strategy(reset_strategy, offset_value)

    cfg = registry.get(cluster_id)
    cfg_path = properties.get_path(cluster_id)

    argv = _build_argv(
        kafka_bin_path=kafka_bin_path,
        bootstrap_servers=cfg.bootstrap_servers,
        client_properties_path=str(cfg_path),
        group_id=group_id,
        topic_name=topic_name,
        reset_strategy=reset_strategy,
        offset_value=offset_value,
        execute=False,  # ALWAYS dry-run from this tool
    )

    result = await executor.run(
        argv, timeout=timeout, correlation_id=current_correlation_id()
    )

    plan = _parse_plan(result.stdout.decode(errors="replace"))

    remediation_argv = _build_argv(
        kafka_bin_path=kafka_bin_path,
        bootstrap_servers=cfg.bootstrap_servers,
        client_properties_path=str(cfg_path),
        group_id=group_id,
        topic_name=topic_name,
        reset_strategy=reset_strategy,
        offset_value=offset_value,
        execute=True,  # the string we hand to the human
    )

    return {
        "cluster_id": cluster_id,
        "group_id": group_id,
        "topic": topic_name,
        "strategy": reset_strategy,
        "offset_value": offset_value,
        "dry_run": True,
        "partition_offsets": plan,
        "remediation_command": " ".join(_quote_for_shell(a) for a in remediation_argv),
        "warning": _build_warning(plan),
        "summary": _summarize(plan),
    }


def _validate_strategy(strategy: str, offset_value: int | None) -> None:
    if strategy not in _VALID_STRATEGIES:
        raise MskToolError(
            ErrorType.INVALID_PARAMS,
            f"reset_strategy must be one of {sorted(_VALID_STRATEGIES)}, got {strategy!r}",
        )
    if strategy in ("to-offset", "shift-by") and offset_value is None:
        raise MskToolError(
            ErrorType.INVALID_PARAMS,
            f"reset_strategy={strategy} requires offset_value",
        )


def _build_argv(
    *,
    kafka_bin_path: str,
    bootstrap_servers: str,
    client_properties_path: str,
    group_id: str,
    topic_name: str,
    reset_strategy: str,
    offset_value: int | None,
    execute: bool,
) -> list[str]:
    argv = [
        f"{kafka_bin_path}/kafka-consumer-groups.sh",
        "--bootstrap-server",
        bootstrap_servers,
        "--command-config",
        client_properties_path,
        "--group",
        group_id,
        "--topic",
        topic_name,
        "--reset-offsets",
    ]
    if reset_strategy == "to-latest":
        argv.append("--to-latest")
    elif reset_strategy == "to-earliest":
        argv.append("--to-earliest")
    elif reset_strategy == "to-offset":
        argv += ["--to-offset", str(offset_value)]
    elif reset_strategy == "shift-by":
        argv += ["--shift-by", str(offset_value)]
    argv.append("--execute" if execute else "--dry-run")
    return argv


def _parse_plan(stdout: str) -> list[dict[str, Any]]:
    """Parse the dry-run output table.

    Sample (clean) output:
      GROUP                  TOPIC          PARTITION  NEW-OFFSET
      orders-processor       orders         0          12345
      orders-processor       orders         1          67890

    Real-world wrinkle: kafka-consumer-groups.sh sometimes emits the
    entire table on ONE line (header and all data rows mashed together
    with runs of whitespace, no newlines between rows). We handle both
    forms by locating the header marker, splitting the rest of the
    stream into whitespace-separated tokens, and consuming them in
    groups of `len(header_cols)`.
    """
    # Find the header. It always contains the literal "NEW-OFFSET" column.
    idx = stdout.find("NEW-OFFSET")
    if idx < 0:
        return []
    # Determine header column names — read backwards from NEW-OFFSET to the
    # nearest line break (or start of string) to capture preceding columns.
    line_start = stdout.rfind("\n", 0, idx) + 1  # 0 if no newline
    # The header may end at the next whitespace after NEW-OFFSET (when data
    # is on the same line) or at the next newline (when output is normal).
    after_header_token = idx + len("NEW-OFFSET")
    header_text = stdout[line_start:after_header_token]
    header_cols = header_text.split()
    if not header_cols or "PARTITION" not in header_cols:
        return []

    # Everything after the header token is data — flatten into tokens.
    rest = stdout[after_header_token:]
    tokens = rest.split()
    # Filter out anything that looks like log noise (e.g. lines starting with
    # '[' from a logger message). A pragmatic heuristic: data rows always
    # have an integer in the PARTITION column and an integer in NEW-OFFSET.
    cols_per_row = len(header_cols)
    if cols_per_row < 4:
        return []

    partition_idx = header_cols.index("PARTITION")
    new_offset_idx = header_cols.index("NEW-OFFSET")
    topic_idx = header_cols.index("TOPIC") if "TOPIC" in header_cols else 1

    plan: list[dict[str, Any]] = []
    i = 0
    while i + cols_per_row <= len(tokens):
        group = tokens[i : i + cols_per_row]
        # Validate this looks like a row: PARTITION and NEW-OFFSET must parse as int.
        try:
            partition = int(group[partition_idx])
            new_offset = int(group[new_offset_idx])
        except (ValueError, IndexError):
            # Not a data row — drop one token and try again. This is robust
            # against stray log lines that bleed into the table area.
            i += 1
            continue
        topic = group[topic_idx] if topic_idx < len(group) else None
        plan.append({
            "topic": topic,
            "partition": partition,
            "new_offset": new_offset,
        })
        i += cols_per_row

    plan.sort(key=lambda r: (r["topic"] or "", r["partition"]))
    return plan


def _build_warning(plan: list[dict[str, Any]]) -> str:
    if not plan:
        return (
            "No partition reset plan parsed from dry-run output. "
            "Verify the consumer group exists on this cluster."
        )
    return (
        "DRY RUN ONLY — no offsets have changed. To apply this reset, a human "
        "must run the command in remediation_command. Resetting consumer "
        "offsets can cause messages to be skipped or reprocessed; confirm "
        "business impact before running --execute."
    )


def _summarize(plan: list[dict[str, Any]]) -> str:
    if not plan:
        return "DRY RUN: no plan parsed (group or topic may not exist)"
    return (
        f"DRY RUN: would set offsets on {len(plan)} partition(s); "
        "see remediation_command to apply"
    )


def _quote_for_shell(arg: str) -> str:
    """Quote a CLI argument so the resulting string is safe to copy-paste into bash."""
    if not arg:
        return "''"
    if all(c.isalnum() or c in "@%+=:,./-_" for c in arg):
        return arg
    # Use single quotes; escape any embedded single quotes.
    return "'" + arg.replace("'", "'\\''") + "'"
