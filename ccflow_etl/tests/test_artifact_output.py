from ccflow_etl import (
    ArtifactExistsContext,
    ArtifactExistsModel,
    ArtifactPublishContext,
    ArtifactPublishModel,
    ArtifactWriteContext,
    ArtifactWriteModel,
    ETLArtifact,
    NoOpArtifactStore,
)


class RecordingArtifactStore:
    def __init__(self, existing=None):
        self.existing = set(existing or ())
        self.writes = []
        self.publishes = []

    def artifact_uri(self, key):
        return f"recording://{key}"

    def exists(self, key):
        return key in self.existing

    def write(self, key, payload, media_type=None, metadata=None):
        self.writes.append((key, payload, media_type, metadata or {}))
        self.existing.add(key)
        return {"etag": "abc123"}

    def publish(self, key, source_key=None, source_uri=None, metadata=None):
        self.publishes.append((key, source_key, source_uri, metadata or {}))
        self.existing.add(key)
        return {"version": "1"}


def test_artifact_models_plan_write_and_publish_without_backend_assumptions():
    store = RecordingArtifactStore(existing={"outputs/existing.json"})

    exists_result = ArtifactExistsModel(store=store)(ArtifactExistsContext(key="outputs/existing.json"))
    dry_run = ArtifactWriteModel(store=store)(
        ArtifactWriteContext(key="outputs/planned.json", payload=b"{}", media_type="application/json", dataset="sample_records", dry_run=True)
    )
    write_result = ArtifactWriteModel(store=store)(
        ArtifactWriteContext(key="outputs/new.json", payload=b"{}", media_type="application/json", dataset="sample_records")
    )
    publish_result = ArtifactPublishModel(store=store)(
        ArtifactPublishContext(key="outputs/final.json", source_key="tmp/final.json", dataset="sample_records")
    )
    existing_write = ArtifactWriteModel(store=store)(
        ArtifactWriteContext(key="outputs/existing.json", payload=b"{}", media_type="application/json", dataset="sample_records")
    )

    assert exists_result.exists is True
    assert exists_result.status == "exists"
    assert dry_run.status == "planned"
    assert dry_run.artifact.status == "planned"
    assert write_result.status == "written"
    assert write_result.metadata == {"etag": "abc123"}
    assert write_result.artifact.uri == "recording://outputs/new.json"
    assert publish_result.status == "published"
    assert publish_result.metadata == {"version": "1"}
    assert existing_write.status == "exists"
    assert store.writes == [("outputs/new.json", b"{}", "application/json", {})]
    assert store.publishes == [("outputs/final.json", "tmp/final.json", None, {})]


def test_noop_artifact_store_returns_explain_safe_artifacts():
    result = ArtifactPublishModel(store=NoOpArtifactStore())(
        ArtifactPublishContext(key="outputs/final.json", source_uri="memory://tmp/final.json", dataset="sample_records")
    )

    assert result.status == "noop"
    assert result.uri == "noop://artifact/outputs/final.json"
    assert result.artifact == ETLArtifact(
        key="outputs/final.json",
        stage="load",
        dataset="sample_records",
        uri="noop://artifact/outputs/final.json",
        status="noop",
    )
