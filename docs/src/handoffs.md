# Handoffs, Formats, And Reliability

`ccflow-etl` provides small models for passing data and execution metadata between stages. These pieces are domain-neutral and are designed to work with connector-owned stores.

## Cache Handoffs And Formats

Use cache put/get models when persisted payloads need ETL metadata. They return `ETLArtifact` records with stable keys, dataset names, stages, URIs, media types, and statuses.

Stores only need a byte-oriented interface:

- `exists(key)`
- `put_bytes(key, payload, content_type=None)`
- `get_bytes(key)`
- `uri(key)`

`ccflow-etl` owns the format conversion through `PayloadCodec`.

```python
from ccflow_etl import CacheGetContext, CacheGetModel, LocalCacheStore

result = CacheGetModel(store=LocalCacheStore(), format="json")(
    CacheGetContext(path="./stats.json", dataset="text_stats", stage="load")
)
if result.status == "hit":
    print(result.payload)
```

`CachePutModel` and `CacheGetModel` accept `format="json"`, `format="csv"`, `format="text"`, `format="binary"`, `format="parquet"`, or a compressed form such as `format=["json", "gzip"]`. The selected codec determines suffixes and media types, so adding a format does not require new cache model classes.

The packaged `cache=noop` group registers `NoOpCacheStore` plus JSON cache get/put models. It is the default for the packaged base config, so pipelines can depend on a cache registry without forcing local or durable storage.

Connector packages can expose durable implementations of the byte-store contract:

```python
from ccflow_etl import CacheGetContext, CacheGetModel
from ccflow_s3 import S3CacheStore, S3Client

store = S3CacheStore(client=S3Client(), bucket="bucket", prefix="cache")
result = CacheGetModel(store=store, format="json")(CacheGetContext(key="text_stats/2026-05-01"))
```

```python
from ccflow_db import SQLiteCacheStore, SQLiteConfig
from ccflow_etl import CacheGetContext, CacheGetModel

store = SQLiteCacheStore(config=SQLiteConfig(path="./cache.sqlite"), table="cache_entries")
result = CacheGetModel(store=store, format="json")(CacheGetContext(key="text_stats/2026-05-01"))
```

Typical unit statuses are:

- `planned`: should run now.
- `exists`: skipped because the output artifact already exists.
- `database`: skipped because a database output already has the expected row.
- `written` or `upserted`: completed successfully.
- `failed`, `retried`, or `cancelled`: execution did not finish cleanly.

## Dataset Metadata

Concrete dataset models should expose their own metadata, such as semantic name, schema version, partition keys, cadence, media types, expectations, provider hints, and output hints. `ccflow-etl` does not add a separate dataset/provider/schema registry; use ccflow registration and Hydra config groups to select the concrete model.

```python
class SampleRecordsModel:
    dataset_name = "sample_records"
    provider_name = "example_provider"
    schema_name = "sample_record"
    schema_version = "1"
    partition_keys = ["date", "item_id"]

    def dataset_metadata(self):
        return {
            "name": self.dataset_name,
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "partition_keys": self.partition_keys,
        }
```

## Artifact IO

Artifact IO models provide a small contract for durable reads and writes without making any storage backend part of `ccflow-etl`.

Artifact store implementations should provide:

- `exists(key)`
- `write(key, payload, media_type=None, metadata=None)`
- `publish(key, source_key=None, source_uri=None, metadata=None)`
- `artifact_uri(key)`

Connector packages can implement this contract for object stores, databases, warehouses, or local files. Applications can use `NoOpArtifactStore` for explain-safe plans.

```python
from ccflow_etl import ArtifactWriteContext, ArtifactWriteModel, NoOpArtifactStore

result = ArtifactWriteModel(store=NoOpArtifactStore())(
    ArtifactWriteContext(
        key="outputs/sample-records/2025-01-02.json",
        payload=b"{}",
        media_type="application/json",
        dataset="sample_records",
        dry_run=True,
    )
)
print(result.artifact.model_dump(mode="json"))
```

## Retries

Use `ccflow` retry wrappers around callables that may fail transiently. `RetryModel` makes retry behavior part of the graph, while `RetryEvaluator` applies retry behavior through evaluator configuration. Connector packages own protocol-specific classification such as HTTP status codes.

```python
from ccflow.models import RetryModel

retrying_model = RetryModel(
    model=my_callable,
    max_attempts=3,
    wait_initial=0.5,
    wait_multiplier=2.0,
    wait_jitter=0.1,
)
```

## Run Summaries

`RunSummary` turns item statuses and artifact stages into consistent reporting fields:

```python
from ccflow_etl import ETLArtifact, RunSummary

summary = RunSummary.from_items(
    [{"status": "planned"}, {"status": "exists"}, {"status": "written"}],
    artifacts=[ETLArtifact(key="stats", stage="load", status="written")],
)
print(summary.model_dump(mode="json"))
```

Use `summary.legacy_counts()` when a caller needs only `planned`, `skipped`, `succeeded`, `failed`, `retried`, and `cancelled` counts.
