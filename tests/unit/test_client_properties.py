from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from msk_mcp.client_properties import ClientPropertiesManager
from msk_mcp.config import load_registry
from msk_mcp.errors import MskToolError


def _registry(yaml_text: str, tmp_path) -> Path:
    p = tmp_path / "clusters.yaml"
    p.write_text(yaml_text)
    return load_registry(p)


def test_render_all_creates_iam_properties_file(tmp_path):
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
    out = tmp_path / "props"
    mgr = ClientPropertiesManager(reg, out)
    mgr.render_all()

    p = mgr.get_path("poc-dev")
    assert p == out / "poc-dev.properties"
    assert p.exists()
    content = p.read_text()
    assert "security.protocol=SASL_SSL" in content
    assert "sasl.mechanism=AWS_MSK_IAM" in content
    assert "IAMLoginModule" in content
    assert "IAMClientCallbackHandler" in content


def test_iam_properties_file_is_mode_0600(tmp_path):
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
    mgr = ClientPropertiesManager(reg, tmp_path / "props")
    mgr.render_all()

    p = mgr.get_path("poc-dev")
    mode = stat.S_IMODE(os.stat(p).st_mode)
    assert mode == 0o600


def test_render_all_skips_unsupported_clusters_without_failing(tmp_path):
    reg = _registry(
        """
clusters:
  ok-iam:
    bootstrap_servers: b-1.example:9098
    region: us-east-1
    auth_type: IAM
  legacy-scram:
    bootstrap_servers: b-1.example:9096
    region: us-east-1
    auth_type: SASL_SCRAM
    scram_secret_arn: arn:aws:secretsmanager:us-east-1:123:secret:foo
""",
        tmp_path,
    )
    out = tmp_path / "props"
    mgr = ClientPropertiesManager(reg, out)
    mgr.render_all()  # must not raise

    assert (out / "ok-iam.properties").exists()
    assert not (out / "legacy-scram.properties").exists()


def test_get_path_for_unsupported_cluster_raises(tmp_path):
    reg = _registry(
        """
clusters:
  legacy:
    bootstrap_servers: b-1.example:9096
    region: us-east-1
    auth_type: SASL_SCRAM
    scram_secret_arn: arn:aws:secretsmanager:us-east-1:123:secret:foo
""",
        tmp_path,
    )
    mgr = ClientPropertiesManager(reg, tmp_path / "props")
    mgr.render_all()
    with pytest.raises(MskToolError):
        mgr.get_path("legacy")
