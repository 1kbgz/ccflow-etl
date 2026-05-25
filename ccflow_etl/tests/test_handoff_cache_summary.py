import json

from ccflow_etl import CacheGetContext, CacheGetModel, CachePutContext, CachePutModel, ETLArtifact, LocalCacheStore, RunSummary


def test_local_cache_put_and_get_exposes_typed_artifacts(tmp_path):
    cache_path = tmp_path / "massive" / "stocks" / "raw" / "2024-01-03" / "AAA.json"
    store = LocalCacheStore()

    put_result = CachePutModel(store=store, format="json")(
        CachePutContext(path=cache_path, payload={"ticker": "AAA"}, dataset="stocks", stage="extract")
    )

    assert put_result.status == "written"
    assert put_result.artifact == ETLArtifact(
        key=str(cache_path),
        dataset="stocks",
        stage="extract",
        uri=str(cache_path),
        media_type="application/json",
        status="written",
    )

    get_result = CacheGetModel(store=store, format="json")(CacheGetContext(path=cache_path, dataset="stocks", stage="extract"))

    assert get_result.status == "hit"
    assert get_result.payload == {"ticker": "AAA"}
    assert get_result.artifact.status == "hit"
    assert json.loads(cache_path.read_text()) == {"ticker": "AAA"}


def test_run_summary_counts_statuses_and_handoff_stages():
    summary = RunSummary.from_items(
        [
            {"status": "planned"},
            {"status": "checkpoint"},
            {"status": "exists"},
            {"status": "written"},
            {"status": "failed"},
        ],
        artifacts=[
            ETLArtifact(key="raw", stage="extract", status="written"),
            ETLArtifact(key="normalized", stage="transform", status="written"),
            ETLArtifact(key="sqlite", stage="load", status="upserted"),
        ],
    )

    assert summary.total == 5
    assert summary.planned == 1
    assert summary.skipped == 2
    assert summary.succeeded == 1
    assert summary.failed == 1
    assert summary.by_status == {"checkpoint": 1, "exists": 1, "failed": 1, "planned": 1, "written": 1}
    assert summary.by_stage == {"extract": 1, "load": 1, "transform": 1}
    assert summary.legacy_counts() == {"planned": 1, "skipped": 2, "succeeded": 1, "failed": 1, "retried": 0, "cancelled": 0}
