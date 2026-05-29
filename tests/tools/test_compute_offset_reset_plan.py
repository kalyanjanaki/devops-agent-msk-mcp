from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from msk_mcp.cli_executor import CliResult
from msk_mcp.config import load_registry
from msk_mcp.tools.compute_offset_reset_plan import (
    _parse_plan,
    _quote_for_shell,
    compute_offset_reset_plan,
)


def _registry(tmp_path):
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


def _executor_returning(stdout: bytes) -> MagicMock:
    e = MagicMock()
    e.run = AsyncMock(
        return_value=CliResult(returncode=0, stdout=stdout, stderr=b"", duration_ms=10)
    )
    return e


# --- _parse_plan unit tests ---


def test_parse_plan_basic_table():
    out = """\

GROUP                  TOPIC                  PARTITION  NEW-OFFSET
orders-processor       orders                 0          12345
orders-processor       orders                 1          67890
"""
    plan = _parse_plan(out)
    assert plan == [
        {"topic": "orders", "partition": 0, "new_offset": 12345},
        {"topic": "orders", "partition": 1, "new_offset": 67890},
    ]


def test_parse_plan_handles_single_line_real_output():
    """Real-world: kafka-consumer-groups.sh emits the entire table on ONE line
    (header and all rows mashed together with whitespace, no newlines between
    rows). This is what we observed against MSK 3.8 on Kafka CLI 3.8.0.
    """
    out = (
        "[2026-05-29 15:44:09,195] WARN [AdminClient] some warning\n"
        "\n"
        "GROUP                          TOPIC                          PARTITION  NEW-OFFSET     "
        "amazon.msk.canary.group.broker-1 __amazon_msk_canary            5          0              "
        "amazon.msk.canary.group.broker-1 __amazon_msk_canary            3          0              "
        "amazon.msk.canary.group.broker-1 __amazon_msk_canary            4          90721          "
        "amazon.msk.canary.group.broker-1 __amazon_msk_canary            1          412404         "
        "amazon.msk.canary.group.broker-1 __amazon_msk_canary            2          0              "
        "amazon.msk.canary.group.broker-1 __amazon_msk_canary            0          735249"
    )
    plan = _parse_plan(out)
    assert len(plan) == 6
    by_p = {p["partition"]: p["new_offset"] for p in plan}
    assert by_p == {0: 735249, 1: 412404, 2: 0, 3: 0, 4: 90721, 5: 0}
    # All rows attributed to the canary topic.
    assert all(p["topic"] == "__amazon_msk_canary" for p in plan)


def test_parse_plan_handles_extra_chatter():
    """The CLI sometimes emits log lines before/after the table."""
    out = """\
WARN: doing some setup
Some informational line
GROUP                  TOPIC          PARTITION  NEW-OFFSET
g1                     orders         0          100
"""
    plan = _parse_plan(out)
    assert len(plan) == 1
    assert plan[0]["new_offset"] == 100


def test_parse_plan_returns_empty_when_no_table():
    plan = _parse_plan("Error: group not found")
    assert plan == []


def test_parse_plan_sorts_by_partition():
    out = """\
GROUP TOPIC PARTITION NEW-OFFSET
g orders 5 50
g orders 0 0
g orders 2 20
"""
    plan = _parse_plan(out)
    assert [p["partition"] for p in plan] == [0, 2, 5]


# --- _quote_for_shell ---


def test_quote_for_shell_passes_safe_args():
    assert _quote_for_shell("simple") == "simple"
    assert _quote_for_shell("path/with-dashes_and.dots") == "path/with-dashes_and.dots"


def test_quote_for_shell_quotes_unsafe_args():
    assert _quote_for_shell("has space") == "'has space'"
    assert _quote_for_shell("a;b") == "'a;b'"


def test_quote_for_shell_handles_embedded_single_quote():
    # The result must round-trip through bash safely.
    quoted = _quote_for_shell("don't")
    assert quoted == "'don'\\''t'"


# --- compute_offset_reset_plan integration with mocked executor ---


SAMPLE_DRY_RUN_OUTPUT = b"""\

GROUP                  TOPIC          PARTITION  NEW-OFFSET
orders-processor       orders         0          12345
orders-processor       orders         1          67890
"""


async def test_dry_run_flag_always_on(tmp_path):
    """Critical contract test: this tool MUST NOT pass --execute."""
    reg = _registry(tmp_path)
    ex = _executor_returning(SAMPLE_DRY_RUN_OUTPUT)
    await compute_offset_reset_plan(
        registry=reg,
        properties=_properties_mgr(tmp_path),
        executor=ex,
        kafka_bin_path="/opt/kafka/bin",
        timeout=30.0,
        cluster_id="poc-dev",
        group_id="orders-processor",
        topic_name="orders",
        reset_strategy="to-latest",
    )
    args, _ = ex.run.call_args
    argv = args[0]
    assert "--dry-run" in argv
    assert "--execute" not in argv


async def test_returns_dry_run_true_in_response(tmp_path):
    """Output schema invariant: dry_run is always True."""
    reg = _registry(tmp_path)
    ex = _executor_returning(SAMPLE_DRY_RUN_OUTPUT)
    result = await compute_offset_reset_plan(
        registry=reg,
        properties=_properties_mgr(tmp_path),
        executor=ex,
        kafka_bin_path="/opt/kafka/bin",
        timeout=30.0,
        cluster_id="poc-dev",
        group_id="g",
        topic_name="t",
        reset_strategy="to-earliest",
    )
    assert result["dry_run"] is True
    assert "error" not in result


async def test_remediation_command_uses_execute(tmp_path):
    """The hand-off-to-human string MUST include --execute."""
    reg = _registry(tmp_path)
    ex = _executor_returning(SAMPLE_DRY_RUN_OUTPUT)
    result = await compute_offset_reset_plan(
        registry=reg,
        properties=_properties_mgr(tmp_path),
        executor=ex,
        kafka_bin_path="/opt/kafka/bin",
        timeout=30.0,
        cluster_id="poc-dev",
        group_id="g",
        topic_name="t",
        reset_strategy="to-latest",
    )
    cmd = result["remediation_command"]
    assert "--execute" in cmd
    assert "--dry-run" not in cmd
    assert "kafka-consumer-groups.sh" in cmd
    assert "--reset-offsets" in cmd
    assert "--to-latest" in cmd


async def test_partition_offsets_parsed(tmp_path):
    reg = _registry(tmp_path)
    ex = _executor_returning(SAMPLE_DRY_RUN_OUTPUT)
    result = await compute_offset_reset_plan(
        registry=reg,
        properties=_properties_mgr(tmp_path),
        executor=ex,
        kafka_bin_path="/opt/kafka/bin",
        timeout=30.0,
        cluster_id="poc-dev",
        group_id="orders-processor",
        topic_name="orders",
        reset_strategy="to-latest",
    )
    assert len(result["partition_offsets"]) == 2
    assert result["partition_offsets"][0] == {
        "topic": "orders", "partition": 0, "new_offset": 12345
    }


async def test_invalid_strategy_returns_envelope(tmp_path):
    reg = _registry(tmp_path)
    ex = _executor_returning(b"")
    result = await compute_offset_reset_plan(
        registry=reg,
        properties=_properties_mgr(tmp_path),
        executor=ex,
        kafka_bin_path="/opt/kafka/bin",
        timeout=30.0,
        cluster_id="poc-dev",
        group_id="g",
        topic_name="t",
        reset_strategy="to-cosmic-rays",
    )
    assert result["error"] is True
    assert result["error_type"] == "INVALID_PARAMS"


async def test_to_offset_requires_offset_value(tmp_path):
    reg = _registry(tmp_path)
    ex = _executor_returning(b"")
    result = await compute_offset_reset_plan(
        registry=reg,
        properties=_properties_mgr(tmp_path),
        executor=ex,
        kafka_bin_path="/opt/kafka/bin",
        timeout=30.0,
        cluster_id="poc-dev",
        group_id="g",
        topic_name="t",
        reset_strategy="to-offset",
    )
    assert result["error"] is True
    assert result["error_type"] == "INVALID_PARAMS"


async def test_to_offset_argv_includes_value(tmp_path):
    reg = _registry(tmp_path)
    ex = _executor_returning(SAMPLE_DRY_RUN_OUTPUT)
    await compute_offset_reset_plan(
        registry=reg,
        properties=_properties_mgr(tmp_path),
        executor=ex,
        kafka_bin_path="/opt/kafka/bin",
        timeout=30.0,
        cluster_id="poc-dev",
        group_id="g",
        topic_name="t",
        reset_strategy="to-offset",
        offset_value=500,
    )
    args, _ = ex.run.call_args
    argv = args[0]
    assert "--to-offset" in argv
    assert argv[argv.index("--to-offset") + 1] == "500"


async def test_shift_by_argv_includes_signed_value(tmp_path):
    reg = _registry(tmp_path)
    ex = _executor_returning(SAMPLE_DRY_RUN_OUTPUT)
    await compute_offset_reset_plan(
        registry=reg,
        properties=_properties_mgr(tmp_path),
        executor=ex,
        kafka_bin_path="/opt/kafka/bin",
        timeout=30.0,
        cluster_id="poc-dev",
        group_id="g",
        topic_name="t",
        reset_strategy="shift-by",
        offset_value=-1000,
    )
    args, _ = ex.run.call_args
    argv = args[0]
    assert "--shift-by" in argv
    assert argv[argv.index("--shift-by") + 1] == "-1000"


async def test_warning_present_in_response(tmp_path):
    reg = _registry(tmp_path)
    ex = _executor_returning(SAMPLE_DRY_RUN_OUTPUT)
    result = await compute_offset_reset_plan(
        registry=reg,
        properties=_properties_mgr(tmp_path),
        executor=ex,
        kafka_bin_path="/opt/kafka/bin",
        timeout=30.0,
        cluster_id="poc-dev",
        group_id="g",
        topic_name="t",
        reset_strategy="to-latest",
    )
    assert "DRY RUN" in result["warning"]
    assert "human" in result["warning"].lower()


async def test_unknown_cluster_returns_envelope(tmp_path):
    reg = _registry(tmp_path)
    ex = _executor_returning(b"")
    result = await compute_offset_reset_plan(
        registry=reg,
        properties=_properties_mgr(tmp_path),
        executor=ex,
        kafka_bin_path="/opt/kafka/bin",
        timeout=30.0,
        cluster_id="missing",
        group_id="g",
        topic_name="t",
        reset_strategy="to-latest",
    )
    assert result["error"] is True
