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

## Checkpoints And Skip Decisions

`CheckpointDecisionModel` combines checkpoint stores and destination existence checks into planned or skipped units. Use it before calling expensive or non-idempotent work. Stores only need to provide `should_skip(key)`; connector packages can provide durable implementations.

The packaged `checkpoint=noop` group registers `NoOpCheckpointStore` and keeps all units runnable. Connector packages can replace it with durable groups such as `checkpoint=s3` or `checkpoint=sqlite`.

Typical unit statuses are:

- `planned`: should run now.
- `checkpoint`: skipped because a checkpoint says the unit already succeeded.
- `exists`: skipped because the destination already exists.
- `database`: skipped because a database destination already has the expected row.
- `written` or `upserted`: completed successfully.
- `failed`, `retried`, or `cancelled`: execution did not finish cleanly.

## Retries

Use `RetryPolicy` and `RetryModel` around callables that may fail transiently. Policies classify retryable status codes, timeout exceptions, and other exception types, then produce event summaries with attempt counts and backoff decisions.

```python
from ccflow_etl import RetryModel, RetryPolicy

retrying_model = RetryModel(
    model=my_callable,
    policy=RetryPolicy(max_attempts=3, initial_delay_seconds=0.5, backoff_multiplier=2.0, jitter_ratio=0.1),
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
