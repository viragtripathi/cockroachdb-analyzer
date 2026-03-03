"""Transaction retry logic for CockroachDB.

Implements exponential backoff with jitter for handling:
- 40001 serialization failures (SERIALIZABLE isolation)
- Transient connection errors
- Network timeouts

Based on CockroachDB best practices from cockroachdb/langchain-cockroachdb.
"""

import logging
import random
import time
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

_TRANSIENT_PATTERNS = (
    "restart transaction",
    "serialization failure",
    "connection",
    "timeout",
    "closed",
    "broken pipe",
    "connection reset",
    "too many clients",
    "server closed",
    "query_wait",
    "ssl connection has been closed",
    "could not connect",
    "network",
    "eof",
)


def is_retryable_error(error: Exception) -> bool:
    """Check if error is transient and should be retried."""
    error_code = getattr(error, "sqlstate", None) or getattr(error, "pgcode", None)
    if error_code == "40001":
        return True

    error_str = str(error).lower()
    return any(pattern in error_str for pattern in _TRANSIENT_PATTERNS)


def retry_with_backoff(
    max_retries: int = 5,
    initial_backoff: float = 0.1,
    max_backoff: float = 10.0,
    backoff_multiplier: float = 2.0,
    jitter: bool = True,
) -> Callable[..., Any]:
    """Retry sync function with exponential backoff on transient CockroachDB errors."""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            backoff = initial_backoff
            last_exception: Exception | None = None

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    if not is_retryable_error(e):
                        raise

                    if attempt >= max_retries - 1:
                        logger.error(
                            "Max retries (%d) exceeded in %s: %s",
                            max_retries,
                            func.__name__,
                            e,
                        )
                        raise

                    actual_backoff = (
                        backoff * (0.5 + random.random() * 0.5) if jitter else backoff
                    )
                    logger.warning(
                        "Retry %d/%d for %s after %.2fs. Error: %s",
                        attempt + 1,
                        max_retries,
                        func.__name__,
                        actual_backoff,
                        e,
                    )
                    time.sleep(actual_backoff)
                    backoff = min(backoff * backoff_multiplier, max_backoff)

            if last_exception:
                raise last_exception
            msg = f"Retry loop completed without result in {func.__name__}"
            raise RuntimeError(msg)

        return wrapper

    return decorator
