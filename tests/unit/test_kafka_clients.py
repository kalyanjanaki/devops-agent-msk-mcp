from __future__ import annotations

from unittest.mock import patch

import pytest

from msk_mcp.config import ClustersRegistry
from msk_mcp.errors import ErrorType, MskToolError
from msk_mcp.kafka_clients import AdminClientFactory


def _registry(yaml_text: str, tmp_path) -> ClustersRegistry:
    p = tmp_path / "clusters.yaml"
    p.write_text(yaml_text)
    from msk_mcp.config import load_registry

    return load_registry(p)


def test_iam_client_built_with_oauth_callback(tmp_path):
    reg = _registry(
        """
clusters:
  poc-dev:
    bootstrap_servers: b-1.example:9098
    region: us-east-1
    auth_type: IAM
""",
        tmp_path,
    )
    factory = AdminClientFactory(reg)

    captured: dict = {}

    def _fake_admin_client(conf):
        captured["conf"] = conf
        return object()

    with patch("msk_mcp.kafka_clients.AdminClient", side_effect=_fake_admin_client):
        factory.get("poc-dev")

    conf = captured["conf"]
    assert conf["bootstrap.servers"] == "b-1.example:9098"
    assert conf["security.protocol"] == "SASL_SSL"
    assert conf["sasl.mechanism"] == "OAUTHBEARER"
    assert callable(conf["oauth_cb"])


def test_clients_are_cached_per_cluster(tmp_path):
    reg = _registry(
        """
clusters:
  a:
    bootstrap_servers: b-1.example:9098
    region: us-east-1
    auth_type: IAM
""",
        tmp_path,
    )
    factory = AdminClientFactory(reg)

    sentinel = object()
    with patch("msk_mcp.kafka_clients.AdminClient", return_value=sentinel) as m:
        c1 = factory.get("a")
        c2 = factory.get("a")
    assert c1 is c2 is sentinel
    assert m.call_count == 1


def test_scram_stub_raises_msk_error(tmp_path):
    reg = _registry(
        """
clusters:
  s:
    bootstrap_servers: b-1.example:9096
    region: us-east-1
    auth_type: SASL_SCRAM
    scram_secret_arn: arn:aws:secretsmanager:us-east-1:123:secret:foo
""",
        tmp_path,
    )
    factory = AdminClientFactory(reg)
    with pytest.raises(MskToolError) as ei:
        factory.get("s")
    assert ei.value.error_type == ErrorType.EXECUTION_FAILURE
    assert "SASL_SCRAM" in ei.value.message


def test_mtls_stub_raises_msk_error(tmp_path):
    reg = _registry(
        """
clusters:
  m:
    bootstrap_servers: b-1.example:9094
    region: us-east-1
    auth_type: MTLS
    cert_path: /a/b.crt
    key_path: /a/b.key
    ca_path: /a/ca.crt
""",
        tmp_path,
    )
    factory = AdminClientFactory(reg)
    with pytest.raises(MskToolError) as ei:
        factory.get("m")
    assert ei.value.error_type == ErrorType.EXECUTION_FAILURE
    assert "MTLS" in ei.value.message
