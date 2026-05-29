from __future__ import annotations

import logging
import os
from pathlib import Path

from msk_mcp.config import (
    ClusterConfig,
    ClustersRegistry,
    IamCluster,
    MtlsCluster,
    ScramCluster,
)
from msk_mcp.errors import ErrorType, MskToolError

logger = logging.getLogger(__name__)


_IAM_PROPERTIES = """\
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
"""


class ClientPropertiesManager:
    """Renders one client.properties file per cluster on disk for use with --command-config.

    Files are written with mode 0600 in the directory configured by Settings
    (default /tmp/msk-mcp). Path lookup is by cluster_id; CLI tools pass
    --command-config <path> to authenticate the same way the AdminClient does.
    """

    def __init__(self, registry: ClustersRegistry, output_dir: Path) -> None:
        self._registry = registry
        self._output_dir = output_dir
        self._paths: dict[str, Path] = {}

    def render_all(self) -> None:
        """Write one .properties file per cluster. Called once at server startup."""
        self._output_dir.mkdir(parents=True, exist_ok=True)
        for cluster_id in self._registry.ids():
            cfg = self._registry.get(cluster_id)
            try:
                content = _render(cfg)
            except MskToolError as e:
                logger.warning(
                    "skipping_unsupported_cluster",
                    extra={"cluster_id": cluster_id, "reason": e.message},
                )
                continue
            path = self._output_dir / f"{cluster_id}.properties"
            path.write_text(content)
            os.chmod(path, 0o600)
            self._paths[cluster_id] = path

    def get_path(self, cluster_id: str) -> Path:
        if cluster_id not in self._paths:
            cfg = self._registry.get(cluster_id)
            content = _render(cfg)
            self._output_dir.mkdir(parents=True, exist_ok=True)
            path = self._output_dir / f"{cluster_id}.properties"
            path.write_text(content)
            os.chmod(path, 0o600)
            self._paths[cluster_id] = path
        return self._paths[cluster_id]


def _render(cfg: ClusterConfig) -> str:
    if isinstance(cfg, IamCluster):
        return _IAM_PROPERTIES
    if isinstance(cfg, ScramCluster):
        raise MskToolError(
            ErrorType.EXECUTION_FAILURE,
            "SASL_SCRAM client.properties rendering not yet implemented in this POC.",
        )
    if isinstance(cfg, MtlsCluster):
        raise MskToolError(
            ErrorType.EXECUTION_FAILURE,
            "MTLS client.properties rendering not yet implemented in this POC.",
        )
    raise MskToolError(  # pragma: no cover — exhaustive
        ErrorType.EXECUTION_FAILURE,
        f"Unknown cluster auth type: {cfg!r}",
    )
