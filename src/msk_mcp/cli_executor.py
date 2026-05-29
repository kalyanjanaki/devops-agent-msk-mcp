from __future__ import annotations

import asyncio
import logging
import os
import signal
import time
from dataclasses import dataclass

from msk_mcp.errors import ErrorType, MskToolError

logger = logging.getLogger(__name__)


@dataclass
class CliResult:
    returncode: int
    stdout: bytes
    stderr: bytes
    duration_ms: int


class CliExecutor:
    """Async wrapper around `kafka-*.sh` style subprocesses.

    - Acquires the CLI semaphore (caps JVM fan-out — separate from the broader tool semaphore).
    - On timeout: SIGTERM -> wait 5s -> SIGKILL.
    - Propagates a correlation_id into the subprocess env as MSK_MCP_CID.
    - Maps non-zero exits / timeouts to MskToolError with stderr-pattern-based classification.
    """

    GRACE_PERIOD_SECONDS = 5.0

    def __init__(self, cli_semaphore: asyncio.Semaphore, classpath: str | None = None) -> None:
        self._sem = cli_semaphore
        self._classpath = classpath

    async def run(
        self,
        argv: list[str],
        timeout: float,
        correlation_id: str | None = None,
        cwd: str | None = None,
    ) -> CliResult:
        if not argv:
            raise ValueError("argv must not be empty")

        env = os.environ.copy()
        if correlation_id:
            env["MSK_MCP_CID"] = correlation_id
        if self._classpath:
            existing = env.get("CLASSPATH")
            env["CLASSPATH"] = f"{self._classpath}:{existing}" if existing else self._classpath

        async with self._sem:
            start = time.monotonic()
            try:
                proc = await asyncio.create_subprocess_exec(
                    *argv,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env,
                    cwd=cwd,
                )
            except FileNotFoundError as e:
                raise MskToolError(
                    ErrorType.EXECUTION_FAILURE,
                    f"CLI binary not found: {argv[0]}",
                    suggestion=(
                        "Verify Kafka CLI is installed and "
                        "MSK_MCP_KAFKA_BIN_PATH is correct."
                    ),
                ) from e
            except PermissionError as e:
                raise MskToolError(
                    ErrorType.EXECUTION_FAILURE,
                    f"CLI binary not executable: {argv[0]}: {e}",
                ) from e

            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout)
            except TimeoutError:
                stdout, stderr = await self._terminate(proc)
                duration_ms = int((time.monotonic() - start) * 1000)
                raise MskToolError(
                    ErrorType.TIMEOUT,
                    f"CLI exceeded timeout of {timeout}s",
                    raw_stderr=stderr.decode(errors="replace"),
                    suggestion="Re-run with a higher timeout or scope down the request.",
                ) from None

            duration_ms = int((time.monotonic() - start) * 1000)

        result = CliResult(
            returncode=proc.returncode if proc.returncode is not None else -1,
            stdout=stdout,
            stderr=stderr,
            duration_ms=duration_ms,
        )

        if result.returncode != 0:
            raise _classify_cli_failure(result, argv)

        return result

    async def _terminate(self, proc: asyncio.subprocess.Process) -> tuple[bytes, bytes]:
        """Try SIGTERM with a grace period; escalate to SIGKILL if needed.

        Returns whatever stdout/stderr was emitted before the process exited.
        """
        try:
            proc.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            return b"", b""

        try:
            return await asyncio.wait_for(proc.communicate(), self.GRACE_PERIOD_SECONDS)
        except TimeoutError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            try:
                return await proc.communicate()
            except Exception:  # pragma: no cover — best-effort drain
                return b"", b""


_AUTH_PATTERNS = ("authentication failed", "sasl", "oauth", "iam")
_AUTHZ_PATTERNS = ("authorization", "topicauthorizationexception", "groupauthorizationexception")
_NETWORK_PATTERNS = (
    "no route to host",
    "connection refused",
    "connection timed out",
    "name or service not known",
    "could not be established",
)


def _classify_cli_failure(result: CliResult, argv: list[str]) -> MskToolError:
    stderr_text = result.stderr.decode(errors="replace")
    haystack = stderr_text.lower()

    if any(p in haystack for p in _AUTH_PATTERNS):
        return MskToolError(
            ErrorType.AUTH_FAILURE,
            f"CLI auth failed (rc={result.returncode})",
            raw_stderr=stderr_text,
            suggestion=(
                "Check the cluster's auth config and that "
                "aws-msk-iam-auth JAR is on the classpath."
            ),
        )
    if any(p in haystack for p in _AUTHZ_PATTERNS):
        return MskToolError(
            ErrorType.AUTHORIZATION,
            f"CLI authorization denied (rc={result.returncode})",
            raw_stderr=stderr_text,
            suggestion="Principal authenticated but lacks permission for this operation.",
        )
    if any(p in haystack for p in _NETWORK_PATTERNS):
        return MskToolError(
            ErrorType.NETWORK_TIMEOUT,
            f"CLI network failure (rc={result.returncode})",
            raw_stderr=stderr_text,
            suggestion="Check broker reachability and security groups.",
        )
    return MskToolError(
        ErrorType.EXECUTION_FAILURE,
        f"CLI failed (rc={result.returncode}): {' '.join(argv)}",
        raw_stderr=stderr_text,
    )
