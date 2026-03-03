"""Tests for retry logic."""


import pytest

from crdb_analyzer.retry import is_retryable_error, retry_with_backoff


class TestIsRetryableError:
    def test_serialization_failure(self):
        err = Exception("some error")
        err.sqlstate = "40001"
        assert is_retryable_error(err) is True

    def test_pgcode_40001(self):
        err = Exception("some error")
        err.pgcode = "40001"
        assert is_retryable_error(err) is True

    def test_restart_transaction(self):
        assert is_retryable_error(Exception("restart transaction")) is True

    def test_connection_error(self):
        assert is_retryable_error(Exception("connection reset by peer")) is True

    def test_timeout_error(self):
        assert is_retryable_error(Exception("timeout expired")) is True

    def test_non_retryable(self):
        assert is_retryable_error(Exception("syntax error")) is False
        assert is_retryable_error(ValueError("bad value")) is False


class TestRetryWithBackoff:
    def test_success_no_retry(self):
        call_count = 0

        @retry_with_backoff(max_retries=3, initial_backoff=0.001)
        def succeeds():
            nonlocal call_count
            call_count += 1
            return "ok"

        assert succeeds() == "ok"
        assert call_count == 1

    def test_retries_on_transient_error(self):
        call_count = 0

        @retry_with_backoff(max_retries=3, initial_backoff=0.001)
        def fails_then_succeeds():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("restart transaction")
            return "ok"

        assert fails_then_succeeds() == "ok"
        assert call_count == 3

    def test_raises_non_retryable_immediately(self):
        call_count = 0

        @retry_with_backoff(max_retries=5, initial_backoff=0.001)
        def fails_non_retryable():
            nonlocal call_count
            call_count += 1
            raise ValueError("syntax error in SQL")

        with pytest.raises(ValueError, match="syntax error"):
            fails_non_retryable()
        assert call_count == 1

    def test_exhausts_retries(self):
        call_count = 0

        @retry_with_backoff(max_retries=3, initial_backoff=0.001)
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise Exception("connection reset")

        with pytest.raises(Exception, match="connection reset"):
            always_fails()
        assert call_count == 3
