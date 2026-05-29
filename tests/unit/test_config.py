from __future__ import annotations

from pathlib import Path

import pytest

from msk_mcp.config import (
    AuthType,
    IamCluster,
    MtlsCluster,
    ScramCluster,
    Settings,
    UnknownClusterError,
    load_registry,
    load_settings,
)


def test_load_settings_uses_defaults_when_env_empty():
    s = load_settings(env={})
    assert s.port == 8080
    assert s.tool_concurrency == 16
    assert s.cli_concurrency == 4
    assert s.default_timeout_seconds == 30.0
    assert s.log_level == "INFO"
    assert s.clusters_config_path == Path("/etc/msk-mcp/clusters.yaml")


def test_load_settings_reads_env_overrides():
    s = load_settings(
        env={
            "MSK_MCP_PORT": "9090",
            "MSK_MCP_TOOL_CONCURRENCY": "32",
            "MSK_MCP_CLI_CONCURRENCY": "2",
            "MSK_MCP_DEFAULT_TIMEOUT_SECONDS": "10",
            "MSK_MCP_LOG_LEVEL": "DEBUG",
        }
    )
    assert s.port == 9090
    assert s.tool_concurrency == 32
    assert s.cli_concurrency == 2
    assert s.default_timeout_seconds == 10.0
    assert s.log_level == "DEBUG"


def test_settings_rejects_non_positive():
    with pytest.raises(ValueError):
        Settings(tool_concurrency=0)
    with pytest.raises(ValueError):
        Settings(cli_concurrency=0)
    with pytest.raises(ValueError):
        Settings(default_timeout_seconds=0)


def test_load_registry_iam_cluster(tmp_path):
    p = tmp_path / "clusters.yaml"
    p.write_text(
        """
clusters:
  poc-dev:
    bootstrap_servers: b-1.example:9098,b-2.example:9098
    region: us-east-1
    auth_type: IAM
    description: dev
"""
    )
    reg = load_registry(p)
    assert reg.ids() == ["poc-dev"]
    c = reg.get("poc-dev")
    assert isinstance(c, IamCluster)
    assert c.auth_type == AuthType.IAM
    assert c.region == "us-east-1"


def test_load_registry_scram_cluster(tmp_path):
    p = tmp_path / "clusters.yaml"
    p.write_text(
        """
clusters:
  cust-foo:
    bootstrap_servers: b-1.example:9096
    region: us-east-1
    auth_type: SASL_SCRAM
    scram_secret_arn: arn:aws:secretsmanager:us-east-1:123:secret:foo
"""
    )
    reg = load_registry(p)
    c = reg.get("cust-foo")
    assert isinstance(c, ScramCluster)
    assert c.scram_secret_arn.endswith(":foo")


def test_load_registry_mtls_cluster(tmp_path):
    p = tmp_path / "clusters.yaml"
    p.write_text(
        """
clusters:
  cust-bar:
    bootstrap_servers: b-1.example:9094
    region: us-east-1
    auth_type: MTLS
    cert_path: /a/b.crt
    key_path: /a/b.key
    ca_path: /a/ca.crt
"""
    )
    reg = load_registry(p)
    c = reg.get("cust-bar")
    assert isinstance(c, MtlsCluster)
    assert c.cert_path == "/a/b.crt"


def test_load_registry_rejects_empty(tmp_path):
    p = tmp_path / "clusters.yaml"
    p.write_text("clusters: {}\n")
    with pytest.raises(ValueError):
        load_registry(p)


def test_load_registry_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_registry(tmp_path / "nope.yaml")


def test_unknown_cluster_lists_valid_ids(tmp_path):
    p = tmp_path / "clusters.yaml"
    p.write_text(
        """
clusters:
  a:
    bootstrap_servers: b-1.example:9098
    region: us-east-1
    auth_type: IAM
  b:
    bootstrap_servers: b-1.example:9098
    region: us-east-1
    auth_type: IAM
"""
    )
    reg = load_registry(p)
    with pytest.raises(UnknownClusterError) as ei:
        reg.get("missing")
    assert ei.value.valid_ids == ["a", "b"]
