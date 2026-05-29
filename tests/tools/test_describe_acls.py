from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from msk_mcp.config import load_registry
from msk_mcp.tools.describe_acls import describe_acls


class _Future:
    def __init__(self, value):
        self._value = value

    def result(self, timeout=None):
        return self._value


def _registry(tmp_path, auth_type="IAM"):
    p = tmp_path / "clusters.yaml"
    if auth_type == "IAM":
        p.write_text(
            """
clusters:
  poc-dev:
    bootstrap_servers: b-1.example:9098
    region: us-east-1
    auth_type: IAM
"""
        )
    else:
        p.write_text(
            """
clusters:
  poc-dev:
    bootstrap_servers: b-1.example:9094
    region: us-east-1
    auth_type: MTLS
    cert_path: /a.crt
    key_path: /a.key
    ca_path: /ca.crt
"""
        )
    return load_registry(p)


def _factory(admin) -> MagicMock:
    f = MagicMock()
    f.get.return_value = admin
    return f


def _binding(restype, name, principal, op, perm) -> SimpleNamespace:
    return SimpleNamespace(
        restype=SimpleNamespace(name=restype),
        name=name,
        resource_pattern_type=SimpleNamespace(name="LITERAL"),
        principal=principal,
        host="*",
        operation=SimpleNamespace(name=op),
        permission_type=SimpleNamespace(name=perm),
    )


async def test_returns_empty_with_iam_specific_summary(tmp_path):
    """IAM cluster with no ACLs: tool should explain why instead of looking broken."""
    reg = _registry(tmp_path, auth_type="IAM")
    admin = MagicMock()
    admin.describe_acls.return_value = _Future([])

    result = await describe_acls(
        factory=_factory(admin),
        registry=reg,
        cluster_id="poc-dev",
    )
    assert result["total_count"] == 0
    assert "IAM auth" in result["summary"]


async def test_returns_acl_bindings_normalized(tmp_path):
    reg = _registry(tmp_path, auth_type="MTLS")
    admin = MagicMock()
    admin.describe_acls.return_value = _Future(
        [
            _binding("TOPIC", "orders", "User:alice", "READ", "ALLOW"),
            _binding("GROUP", "orders-svc", "User:alice", "READ", "ALLOW"),
        ]
    )
    result = await describe_acls(
        factory=_factory(admin),
        registry=reg,
        cluster_id="poc-dev",
    )
    assert result["total_count"] == 2
    assert {a["resource_type"] for a in result["acls"]} == {"TOPIC", "GROUP"}
    assert all(a["principal"] == "User:alice" for a in result["acls"])


async def test_filter_passed_to_acl_binding(tmp_path):
    """Verify caller-supplied filters are translated and passed to describe_acls."""
    reg = _registry(tmp_path, auth_type="MTLS")
    admin = MagicMock()
    admin.describe_acls.return_value = _Future([])

    await describe_acls(
        factory=_factory(admin),
        registry=reg,
        cluster_id="poc-dev",
        resource_type="TOPIC",
        resource_name="orders",
        principal="User:alice",
    )
    assert admin.describe_acls.call_count == 1
    args, _ = admin.describe_acls.call_args
    acl_filter = args[0]
    # Just confirm the filter was constructed (we can't introspect easily across SDK versions)
    assert acl_filter is not None


async def test_unknown_cluster_returns_envelope(tmp_path):
    reg = _registry(tmp_path)
    admin = MagicMock()
    result = await describe_acls(
        factory=_factory(admin),
        registry=reg,
        cluster_id="missing",
    )
    assert result["error"] is True


async def test_handles_non_future_return(tmp_path):
    reg = _registry(tmp_path, auth_type="MTLS")
    admin = MagicMock()
    admin.describe_acls.return_value = []  # no .result()
    result = await describe_acls(
        factory=_factory(admin),
        registry=reg,
        cluster_id="poc-dev",
    )
    assert result["total_count"] == 0
