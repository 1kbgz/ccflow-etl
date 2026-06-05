from ccflow_etl import CheckpointRecord


def test_checkpoint_record_carries_status_and_metadata():
    record = CheckpointRecord(
        key="example_provider:sample_records:item-001:2024-01-03",
        status="succeeded",
        updated_at="2026-05-24T00:00:00+00:00",
        metadata={"path": "sample_records/item-001/2024-01-03.json"},
    )

    assert record.key == "example_provider:sample_records:item-001:2024-01-03"
    assert record.status == "succeeded"
    assert record.metadata == {"path": "sample_records/item-001/2024-01-03.json"}
