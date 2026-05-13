from ccflow_etl import RetryPolicy


def test_retry_policy_classifies_status_and_exceptions():
    policy = RetryPolicy(max_attempts=3, retry_status_codes=[429], retry_exception_types=["TimeoutError"])

    assert policy.should_retry_status(status_code=429, attempt=1) is True
    assert policy.should_retry_status(status_code=500, attempt=1) is False
    assert policy.should_retry_status(status_code=429, attempt=3) is False
    assert policy.should_retry_exception(TimeoutError("timed out"), attempt=2) is True
    assert policy.should_retry_exception(ValueError("bad"), attempt=1) is False
