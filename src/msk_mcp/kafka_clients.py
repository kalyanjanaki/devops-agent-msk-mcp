from __future__ import annotations

from typing import Any

from confluent_kafka.admin import AdminClient

from msk_mcp.config import (
    AuthType,
    ClusterConfig,
    ClustersRegistry,
    IamCluster,
    MtlsCluster,
    ScramCluster,
)
from msk_mcp.errors import ErrorType, MskToolError


class AdminClientFactory:
    """Builds and caches one confluent_kafka AdminClient per cluster_id.

    AdminClient is thread-safe; we keep one per cluster for the process lifetime.
    The IAM token provider refreshes credentials internally on a timer.
    """

    def __init__(self, registry: ClustersRegistry) -> None:
        self._registry = registry
        self._clients: dict[str, AdminClient] = {}

    def get(self, cluster_id: str) -> AdminClient:
        if cluster_id not in self._clients:
            cfg = self._registry.get(cluster_id)
            self._clients[cluster_id] = self._build(cfg)
        return self._clients[cluster_id]

    def _build(self, cfg: ClusterConfig) -> AdminClient:
        conf = self._base_config(cfg)
        if isinstance(cfg, IamCluster):
            conf.update(_iam_config(cfg))
        elif isinstance(cfg, ScramCluster):
            conf.update(_scram_config(cfg))
        elif isinstance(cfg, MtlsCluster):
            conf.update(_mtls_config(cfg))
        else:  # pragma: no cover — exhaustive
            raise MskToolError(
                ErrorType.EXECUTION_FAILURE,
                f"Unsupported auth_type for cluster: {cfg!r}",
            )
        return AdminClient(conf)

    @staticmethod
    def _base_config(cfg: ClusterConfig) -> dict[str, Any]:
        return {
            "bootstrap.servers": cfg.bootstrap_servers,
        }


def _iam_config(cfg: IamCluster) -> dict[str, Any]:
    # Imported lazily so unit tests that don't exercise IAM don't require the package.
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    except ImportError as e:  # pragma: no cover — only happens if dep missing
        raise MskToolError(
            ErrorType.EXECUTION_FAILURE,
            f"aws-msk-iam-sasl-signer-python is required for IAM auth: {e}",
        ) from e

    region = cfg.region

    def _oauth_cb(_oauthbearer_config: str) -> tuple[str, float]:
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
        # confluent-kafka expects (token, expiry_seconds_since_epoch)
        return token, expiry_ms / 1000.0

    return {
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": _oauth_cb,
    }


def _scram_config(_cfg: ScramCluster) -> dict[str, Any]:
    # Stub for POC: SCRAM is not yet implemented.
    raise MskToolError(
        ErrorType.EXECUTION_FAILURE,
        "SASL_SCRAM auth is not yet implemented in this POC. "
        "Use auth_type: IAM for now.",
        suggestion="See the project roadmap; SCRAM support arrives after the walking skeleton.",
    )


def _mtls_config(_cfg: MtlsCluster) -> dict[str, Any]:
    # Stub for POC: mTLS is not yet implemented.
    raise MskToolError(
        ErrorType.EXECUTION_FAILURE,
        "MTLS auth is not yet implemented in this POC. "
        "Use auth_type: IAM for now.",
        suggestion="See the project roadmap; mTLS support arrives after the walking skeleton.",
    )


_AUTH_DISPATCH = {
    AuthType.IAM: _iam_config,
    AuthType.SASL_SCRAM: _scram_config,
    AuthType.MTLS: _mtls_config,
}
