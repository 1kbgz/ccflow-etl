from ccflow_etl import ExecutionPolicy


def test_execution_policy_computes_concurrency_and_rate_delay():
    policy = ExecutionPolicy(max_concurrency=4, requests_per_interval=2, interval_seconds=1.0, min_interval_seconds=0.1)

    assert policy.effective_max_concurrency(default=8) == 4
    assert policy.rate_delay_seconds(previous_started_at=None, now=10.0) == 0.0
    assert policy.rate_delay_seconds(previous_started_at=10.0, now=10.2) == 0.3
    assert policy.rate_delay_seconds(previous_started_at=10.0, now=10.5) == 0.0


def test_execution_policy_uses_min_interval_without_bucket_rate():
    policy = ExecutionPolicy(min_interval_seconds=0.25)

    assert policy.effective_max_concurrency(default=3) == 3
    assert policy.rate_delay_seconds(previous_started_at=5.0, now=5.1) == 0.15
