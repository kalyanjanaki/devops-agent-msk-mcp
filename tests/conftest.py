from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from msk_mcp.cli_executor import CliExecutor
from msk_mcp.client_properties import ClientPropertiesManager
from msk_mcp.concurrency import Bouncer
from msk_mcp.config import ClustersRegistry, Settings, load_registry
from msk_mcp.server import AppContext


@pytest.fixture
def iam_registry(tmp_path: Path) -> ClustersRegistry:
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


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        clusters_config_path=tmp_path / "clusters.yaml",
        port=18080,
        tool_concurrency=4,
        cli_concurrency=2,
        default_timeout_seconds=5.0,
        log_level="WARNING",
        kafka_bin_path=Path("/opt/kafka/bin"),
        client_properties_dir=tmp_path / "props",
    )


@pytest.fixture
def app_context(iam_registry: ClustersRegistry, settings: Settings, tmp_path: Path) -> AppContext:
    """A wired AppContext but with NO real AdminClient or CLI binary calls.

    Tests that exercise tools must monkeypatch context.factory.get and/or context.cli_executor.run.
    """
    properties = ClientPropertiesManager(iam_registry, settings.client_properties_dir)
    properties.render_all()
    bouncer = Bouncer(settings.tool_concurrency, settings.cli_concurrency)

    class _StubFactory:
        def get(self, cluster_id):  # pragma: no cover — overridden in each test
            raise RuntimeError("StubFactory.get not configured for test")

    return AppContext(
        settings=settings,
        registry=iam_registry,
        factory=_StubFactory(),
        properties=properties,
        bouncer=bouncer,
        cli_executor=CliExecutor(bouncer.cli_semaphore),
    )
