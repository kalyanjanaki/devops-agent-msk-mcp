from __future__ import annotations

import functools
import logging
from collections.abc import Awaitable, Callable
from enum import Enum
from typing import Any, ParamSpec

logger = logging.getLogger(__name__)


class ErrorType(str, Enum):
    AUTH_FAILURE = "AUTH_FAILURE"
    NETWORK_TIMEOUT = "NETWORK_TIMEOUT"
    AUTHORIZATION = "AUTHORIZATION"
    INVALID_PARAMS = "INVALID_PARAMS"
    EXECUTION_FAILURE = "EXECUTION_FAILURE"
    TIMEOUT = "TIMEOUT"


class MskToolError(Exception):
    def __init__(
        self,
        error_type: ErrorType,
        message: str,
        suggestion: str = "",
        raw_stderr: str = "",
    ):
        self.error_type = error_type
        self.message = message
        self.suggestion = suggestion
        self.raw_stderr = raw_stderr
        super().__init__(message)

    def to_envelope(self) -> dict[str, Any]:
        return {
            "error": True,
            "error_type": self.error_type.value,
            "error_message": self.message,
            "raw_stderr": self.raw_stderr,
            "suggestion": self.suggestion,
        }


P = ParamSpec("P")


def _classify_kafka_exception(e: BaseException) -> MskToolError:
    text = str(e).lower()
    if "authentication" in text or "sasl" in text or "oauth" in text:
        return MskToolError(
            ErrorType.AUTH_FAILURE,
            f"Authentication failed: {e}",
            suggestion=(
                "Check the cluster's auth config "
                "(IAM permissions / SCRAM secret / mTLS cert)."
            ),
        )
    if (
        "authoriz" in text
        or "topicauthorizationexception" in text
        or "groupauthorizationexception" in text
    ):
        return MskToolError(
            ErrorType.AUTHORIZATION,
            f"Not authorized: {e}",
            suggestion="The principal authenticated but lacks permission for this operation.",
        )
    if "timed out" in text or "timeout" in text:
        return MskToolError(
            ErrorType.NETWORK_TIMEOUT,
            f"Network/operation timeout: {e}",
            suggestion="Check connectivity to the bootstrap servers and broker security groups.",
        )
    if "unknown_topic_or_partition" in text or "unknowntopicor" in text:
        return MskToolError(
            ErrorType.INVALID_PARAMS,
            f"Unknown topic or partition: {e}",
            suggestion="Verify the topic name exists on the cluster.",
        )
    if "groupidnotfound" in text or "group not found" in text:
        return MskToolError(
            ErrorType.INVALID_PARAMS,
            f"Consumer group not found: {e}",
            suggestion="Verify the group_id exists; use list_consumer_groups.",
        )
    return MskToolError(
        ErrorType.EXECUTION_FAILURE,
        f"Kafka operation failed: {e}",
    )


def tool_error_handler(
    fn: Callable[P, Awaitable[dict[str, Any]]],
) -> Callable[P, Awaitable[dict[str, Any]]]:
    @functools.wraps(fn)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> dict[str, Any]:
        try:
            return await fn(*args, **kwargs)
        except MskToolError as e:
            logger.warning(
                "tool_error",
                extra={"error_type": e.error_type.value, "detail": e.message},
            )
            return e.to_envelope()
        except TimeoutError:
            err = MskToolError(
                ErrorType.TIMEOUT,
                "Tool exceeded timeout",
                suggestion=(
                    "Re-run with a higher MSK_MCP_DEFAULT_TIMEOUT_SECONDS, "
                    "or scope down filters."
                ),
            )
            logger.warning("tool_timeout")
            return err.to_envelope()
        except Exception as e:
            mapped: MskToolError
            if e.__class__.__module__.startswith("confluent_kafka"):
                mapped = _classify_kafka_exception(e)
            else:
                mapped = MskToolError(
                    ErrorType.EXECUTION_FAILURE,
                    f"Unhandled error: {e}",
                )
            logger.exception("tool_unhandled_exception")
            return mapped.to_envelope()

    return wrapper
