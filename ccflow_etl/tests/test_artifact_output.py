import pytest

from ccflow_etl import (
    ArtifactExistsContext,
    ArtifactExistsModel,
    ArtifactMaterializeContext,
    ArtifactMaterializeModel,
    ArtifactPublishContext,
    ArtifactPublishModel,
    ArtifactReadContext,
    ArtifactReadModel,
    ArtifactWriteContext,
    ArtifactWriteFileContext,
    ArtifactWriteFileModel,
    ArtifactWriteModel,
    ETLArtifact,
    NoOpArtifactStore,
)


class RecordingArtifactStore:
    def __init__(self, existing=None):
        self.existing = set(existing or ())
        self.writes = []
        self.file_reads = []
        self.file_writes = []
        self.publishes = []

    def artifact_uri(self, key):
        return f"recording://{key}"

    def exists(self, key):
        return key in self.existing

    def read(self, key):
        if key not in self.existing:
            raise FileNotFoundError(key)
        return b'{"ticker":"AAA"}'

    def write(self, key, payload, media_type=None, metadata=None):
        self.writes.append((key, payload, media_type, metadata or {}))
        self.existing.add(key)
        return {"etag": "abc123"}

    def read_file(self, key, path):
        if key not in self.existing:
            raise FileNotFoundError(key)
        self.file_reads.append((key, path))
        path.write_bytes(b"artifact-file")
        return {"source_version": "1", "status": "materialized"}

    def write_file(self, key, path, media_type=None, metadata=None):
        self.file_writes.append((key, path, media_type, metadata or {}))
        self.existing.add(key)
        return {"etag": "file-etag", "path": str(path), "size": path.stat().st_size}

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
    read_result = ArtifactReadModel(store=store)(ArtifactReadContext(key="outputs/existing.json"))
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
    assert read_result.status == "read"
    assert read_result.payload == b'{"ticker":"AAA"}'
    assert read_result.uri == "recording://outputs/existing.json"
    assert write_result.status == "written"
    assert write_result.metadata == {"etag": "abc123"}
    assert write_result.artifact.uri == "recording://outputs/new.json"
    assert publish_result.status == "published"
    assert publish_result.metadata == {"version": "1"}
    assert existing_write.status == "exists"
    assert store.writes == [("outputs/new.json", b"{}", "application/json", {})]
    assert store.publishes == [("outputs/final.json", "tmp/final.json", None, {})]


def test_artifact_read_model_propagates_missing_store_error():
    store = RecordingArtifactStore()

    with pytest.raises(FileNotFoundError):
        ArtifactReadModel(store=store)(ArtifactReadContext(key="outputs/missing.json"))


def test_artifact_models_materialize_and_write_local_files(tmp_path):
    store = RecordingArtifactStore(existing={"raw/daily.csv.gz"})
    materialized_path = tmp_path / "raw" / "daily.csv.gz"

    materialized = ArtifactMaterializeModel(store=store)(
        ArtifactMaterializeContext(key="raw/daily.csv.gz", path=materialized_path, metadata={"dataset": "daily_bars"})
    )
    written = ArtifactWriteFileModel(store=store)(
        ArtifactWriteFileContext(
            key="curated/daily.parquet",
            path=materialized_path,
            media_type="application/vnd.apache.parquet",
            dataset="daily_bars",
        )
    )

    assert materialized.status == "materialized"
    assert materialized.path == str(materialized_path)
    assert materialized.size == len(b"artifact-file")
    assert materialized.metadata == {"dataset": "daily_bars", "source_version": "1"}
    assert materialized_path.read_bytes() == b"artifact-file"
    assert written.status == "written"
    assert written.size == len(b"artifact-file")
    assert written.metadata == {"etag": "file-etag"}
    assert written.artifact.dataset == "daily_bars"
    assert len(store.file_reads) == 1
    assert store.file_writes == [("curated/daily.parquet", materialized_path, "application/vnd.apache.parquet", {})]


def test_artifact_file_models_skip_io_for_dry_run_and_existing_files(tmp_path):
    store = RecordingArtifactStore(existing={"curated/daily.parquet"})
    local_path = tmp_path / "daily.parquet"
    local_path.write_bytes(b"existing")

    planned = ArtifactMaterializeModel(store=store)(
        ArtifactMaterializeContext(key="raw/daily.csv.gz", path=tmp_path / "planned.csv.gz", dry_run=True)
    )
    existing_materialization = ArtifactMaterializeModel(store=store)(
        ArtifactMaterializeContext(key="raw/daily.csv.gz", path=local_path)
    )
    existing_write = ArtifactWriteFileModel(store=store)(
        ArtifactWriteFileContext(key="curated/daily.parquet", path=local_path)
    )

    assert planned.status == "planned"
    assert existing_materialization.status == "exists"
    assert existing_write.status == "exists"
    assert store.file_reads == []
    assert store.file_writes == []


def test_artifact_materialize_cleans_partial_file_on_failure(tmp_path):
    class FailingStore(RecordingArtifactStore):
        def read_file(self, key, path):
            path.write_bytes(b"partial")
            raise OSError("download failed")

    target = tmp_path / "daily.csv.gz"

    with pytest.raises(OSError, match="download failed"):
        ArtifactMaterializeModel(store=FailingStore())(ArtifactMaterializeContext(key="raw/daily.csv.gz", path=target))

    assert not target.exists()
    assert list(tmp_path.iterdir()) == []


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
