from ccflow_etl import SQLiteCheckpointStore


def test_sqlite_checkpoint_store_marks_and_reads_completed_units(tmp_path):
    store = SQLiteCheckpointStore(path=str(tmp_path / "checkpoints.sqlite"))

    store.mark_succeeded("massive:daily:AAA:2024-01-03", metadata={"path": "daily/AAA/2024-01-03.json"})

    record = store.get("massive:daily:AAA:2024-01-03")
    assert record is not None
    assert record.key == "massive:daily:AAA:2024-01-03"
    assert record.status == "succeeded"
    assert record.metadata == {"path": "daily/AAA/2024-01-03.json"}
    assert store.should_skip("massive:daily:AAA:2024-01-03") is True
    assert store.should_skip("massive:daily:AAA:2024-01-04") is False
