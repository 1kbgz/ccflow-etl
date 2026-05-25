# ccflow-etl

Domain-neutral ETL building blocks for `ccflow` callable models.

[![Build Status](https://github.com/1kbgz/ccflow-etl/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/1kbgz/ccflow-etl/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/1kbgz/ccflow-etl/branch/main/graph/badge.svg)](https://codecov.io/gh/1kbgz/ccflow-etl)
[![License](https://img.shields.io/github/license/1kbgz/ccflow-etl)](https://github.com/1kbgz/ccflow-etl)
[![PyPI](https://img.shields.io/pypi/v/ccflow-etl.svg)](https://pypi.python.org/pypi/ccflow-etl)

## What It Provides

`ccflow-etl` is a small library of reusable ETL support primitives for projects built on `ccflow`. It does not ship a generic `ETL` class or skeletal extract/transform/load base classes. Real pipelines should be concrete `CallableModel` graphs owned by the application or domain package, with reusable behavior pulled from this package where it helps.

The package currently provides:

- Shared CLI entry points: `cc-etl` and `cc-etl-explain`.
- Date expansion: `Interval`, `BaseCalendar`, built-in calendars, `BackfillContext`, and `BackfillModel` for fixed, business-day, calendar-boundary, and custom-calendar backfills.
- Handoff metadata: `ETLArtifact` for typed stage artifacts.
- Format-aware writes and cache handoffs: `LocalWriteModel`, `CachePutModel` / `CacheGetModel`, `PayloadCodec`, and `LocalCacheStore` for JSON, CSV, text, binary, gzip, and pyarrow-backed parquet payloads over byte-oriented stores.
- Checkpointing: `CheckpointRecord`, checkpoint statuses, and `CheckpointDecisionModel` for idempotent skip decisions; connector-backed stores live in connector packages.
- Retry orchestration: `RetryPolicy`, `RetryModel`, retry event summaries, timeout categories, and backoff/jitter helpers.
- Run reporting: `RunSummary` for structured counts by status and artifact stage.

`ccflow-etl` is domain-neutral. It does not contain application workflows, provider-specific clients, private deployment assumptions, domain specific functionality (e.g. financial concepts, locale-specific information, etc). Domain-specific functionality may be available in separate packages.

## Installation

```bash
pip install ccflow-etl
```

Connector-backed cache and checkpoint stores are installed from the connector packages that own their I/O:

```bash
pip install ccflow-s3
pip install ccflow-db
```

## CLI Basics

`cc-etl` runs a configured `ccflow` callable model through Hydra. The packaged default config is intentionally tiny: it writes a local JSON payload using `LocalWriteModel(format="json")`.

```bash
cc-etl context.path=./example-output.json context.payload.message='hello from ccflow-etl'
```

Use `cc-etl-explain` to inspect the resolved config without running the callable:

```bash
cc-etl-explain context.path=./example-output.json
```

Most projects should provide their own Hydra config directory and still use the shared entry point:

```bash
cc-etl --config-path ./config --config-name text_stats context.input_path=./notes.txt context.output_path=./stats.json
```

A minimal config has three important pieces:

```yaml
# config/text_stats.yaml
model:
  _target_: text_pipeline.TextStatsModel

cli:
  model:
    _target_: ccflow.FlowOptions
    evaluator:
      _target_: ccflow.evaluators.MultiEvaluator
      evaluators:
        - _target_: ccflow.evaluators.GraphEvaluator
        - _target_: ccflow.evaluators.MemoryCacheEvaluator
        - _target_: ccflow.evaluators.LoggingEvaluator
    cacheable: true

callable: /model
context:
  input_path: ./notes.txt
  output_path: ./stats.json
  min_length: 1
```

`model` defines the callable. `callable` points at the registered model to run. `context` is validated against the callable model's `context_type`.

## Writing A Simple Pipeline

The recommended pattern is to write a concrete `CallableModel` for the workflow you actually need. This example reads a text file, computes word counts, writes JSON locally, and returns artifact metadata plus a run summary.

```python
# text_pipeline.py
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Type

from ccflow import CallableModel, ContextBase, ContextType, Flow, GenericResult, ResultType
from pydantic import Field

from ccflow_etl import CachePutContext, CachePutModel, ETLArtifact, LocalCacheStore, RunSummary


class TextStatsContext(ContextBase):
    input_path: Path
    output_path: Path
    min_length: int = 1
    date: date | None = None


class TextStatsModel(CallableModel):
    writer: CachePutModel = Field(default_factory=lambda: CachePutModel(store=LocalCacheStore(), format="json"))

    @property
    def context_type(self) -> Type[ContextType]:
        return TextStatsContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: TextStatsContext) -> GenericResult:
        text = context.input_path.read_text()
        words = [word.lower() for word in text.split() if len(word) >= context.min_length]
        counts = Counter(words)
        output_path = context.output_path
        if context.date is not None:
            output_path = output_path.with_name(f"{output_path.stem}-{context.date.isoformat()}{output_path.suffix}")
        payload = {
            "input_path": str(context.input_path),
            "date": context.date.isoformat() if context.date else None,
            "word_count": len(words),
            "unique_words": len(counts),
            "top_words": counts.most_common(10),
        }

        write_result = self.writer(
            CachePutContext(
                path=output_path,
                payload=payload,
                dataset="text_stats",
                stage="load",
                overwrite=True,
            )
        )
        artifacts = [write_result.artifact]
        summary = RunSummary.from_items([{"status": write_result.status}], artifacts=artifacts)

        return GenericResult(
            value={
                "payload": payload,
                "artifacts": [artifact.model_dump(mode="json") for artifact in artifacts],
                "run_summary": summary.model_dump(mode="json"),
            }
        )
```

This is deliberately not a subclass of a generic ETL shell. The model name, context, output shape, and dependencies should describe the domain workflow directly. `ccflow-etl` supplies reusable parts: the writer, artifact contract, and summary model.

Run it through the shared CLI with your config:

```bash
echo 'small tools make larger workflows easier to trust' > notes.txt
cc-etl --config-path ./config --config-name text_stats context.input_path=./notes.txt context.output_path=./stats.json
```

## Backfills

Use `BackfillModel` when the same callable should run once per date or datetime step. `BackfillContext` gets its steps from a calendar. The default is daily, interval strings such as `2M` are accepted through `IntervalCalendar`, and downstream packages can provide their own `BaseCalendar` subclasses.

A literal `backfill_text_stats` config would wrap the same `/model` in a `BackfillModel` and pass the underlying text-stats context as the third item in the compact backfill context list:

```yaml
# config/backfill_text_stats.yaml
model:
    _target_: text_pipeline.TextStatsModel

cli:
    model:
        _target_: ccflow.FlowOptions
        evaluator:
            _target_: ccflow.evaluators.MultiEvaluator
            evaluators:
                - _target_: ccflow.evaluators.GraphEvaluator
                - _target_: ccflow.evaluators.MemoryCacheEvaluator
                - _target_: ccflow.evaluators.LoggingEvaluator
        cacheable: true

backfill:
    _target_: ccflow_etl.BackfillModel
    model: /model

callable: /backfill
context:
    - 2026-05-01
    - 2026-05-03
    - input_path: ./notes.txt
        output_path: ./stats.json
        min_length: 1
    - forward
    - daily
```

Run that explicit config with:

```bash
cc-etl --config-path ./config --config-name backfill_text_stats
```

That shape is useful to understand the pieces, but most projects should not create a separate backfill config for every callable. `ccflow-etl` ships config groups for the common execution shapes:

- `callable/callable`: run `/model` directly.
- `backfill/default`: wrap `/model` in `BackfillModel`.
- `backfill/daily`: the same wrapper for daily-style backfills, so CLI users can select `backfill=daily`.

The `context` key remains the `ccflow` runtime context. For a backfill run, the root `context` is a `BackfillContext`; the wrapped callable's context is nested under `context.context`, or passed as the third item in the compact list form. The packaged backfill groups do not create a separate `backfill.context` shadow namespace.

The backfill groups also make built-in calendars available in the root registry under `/calendars`: `daily`, `hourly`, `weekly`, `weekdays`, `business_daily`, and `monday_friday`. Set `context.calendar` to one of those paths when you want the calendar object to choose steps instead of the interval shorthand:

```bash
cc-etl --config-path ./config --config-name text_stats_runner backfill=daily +context.start_datetime=2026-05-01 +context.end_datetime=2026-05-15 +context.calendar=/calendars/weekdays +context.context.input_path=./notes.txt +context.context.output_path=./stats.json +context.context.min_length=1
```

For static runner configs that set `context.*` values in the file, compose the packaged group before `_self_` so local values override group defaults:

```yaml
defaults:
    - /backfill: daily
    - _self_
```

The wrapper uses the same `/cli/model` `FlowOptions` as direct execution, so a local `GraphEvaluator` can evaluate each step from the callable dependency graph. With the packaged groups available in your config search path, the same model can be run directly or as a daily backfill from the CLI:

```yaml
# config/text_stats_runner.yaml
defaults:
    - _self_
    - /callable: callable
    - optional /backfill: null

hydra:
    searchpath:
        - pkg://ccflow_etl.config

model:
    _target_: text_pipeline.TextStatsModel

cli:
    model:
        _target_: ccflow.FlowOptions
        evaluator:
            _target_: ccflow.evaluators.MultiEvaluator
            evaluators:
                - _target_: ccflow.evaluators.GraphEvaluator
                - _target_: ccflow.evaluators.MemoryCacheEvaluator
                - _target_: ccflow.evaluators.LoggingEvaluator
        cacheable: true
```

```bash
cc-etl --config-path ./config --config-name text_stats_runner +context.input_path=./notes.txt +context.output_path=./stats.json +context.min_length=1
cc-etl --config-path ./config --config-name text_stats_runner backfill=daily +context.start_datetime=2026-05-01 +context.end_datetime=2026-05-03 +context.interval=daily +context.context.input_path=./notes.txt +context.context.output_path=./stats.json +context.context.min_length=1
```

Backfill contexts can use aliases such as `daily`, fixed intervals such as `1D`, `6h`, or `30min`, month intervals such as `2M`, business days such as `1B`, and calendar boundaries such as `MS`, `ME`, `BMS`, `BME`, `QS`, `QE`, `YS`, and `YE`. `calendar` takes precedence over `interval` when both are provided.

## Config Scopes

`ccflow-etl` keeps its packaged config scopes small and domain-neutral:

- `callable/*`: choose the callable path to run, or keep compatibility aliases.
- `backfill/*`: wrap `/model` with `BackfillModel`; runtime dates still live in root `context` because that is what `ccflow` executes.
- `calendars/*`: register reusable calendar objects under `/calendars/*`.

Connection and credential scopes should live with the packages that own those concepts. Use connector packages for reusable `connections/rest`, `connections/s3`, and `connections/db` groups, and keep `credentials/*` configs as references to environment variables, secret names, or runtime-injected values rather than literal secrets. Domain packages can compose those groups into provider-specific configs without making `ccflow-etl` depend on HTTP, S3, database, or finance libraries.

## Checkpoints And Skip Decisions

`CheckpointDecisionModel` combines checkpoint stores and destination existence checks into planned or skipped units. Use it before calling expensive or non-idempotent work. Stores only need to provide `should_skip(key)`; connector-backed stores such as `ccflow_db.SQLiteCheckpointStore` provide the durable implementation.

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

## Cache Handoffs And Formats

Use cache put/get models when persisted payloads need ETL metadata: they return `ETLArtifact` records with stable keys, dataset names, stages, URIs, media types, and statuses. `ccflow-etl` owns the format conversion; stores only need byte-oriented `exists(key)`, `put_bytes(key, payload, content_type=...)`, `get_bytes(key)`, and `uri(key)` methods.

```python
from ccflow_etl import CacheGetContext, CacheGetModel, LocalCacheStore

result = CacheGetModel(store=LocalCacheStore(), format="json")(CacheGetContext(path="./stats.json", dataset="text_stats", stage="load"))
if result.status == "hit":
    print(result.payload)
```

`CachePutModel` / `CacheGetModel` accept `format="json"`, `format="csv"`, `format="text"`, `format="binary"`, `format="parquet"`, or a compressed form such as `format=["json", "gzip"]`. The selected `PayloadCodec` determines suffixes and media types, so adding a format does not require new cache model classes.

```python
from ccflow_s3 import S3CacheStore, S3Client
from ccflow_etl import CacheGetContext, CacheGetModel

store = S3CacheStore(client=S3Client(), bucket="bucket", prefix="cache")
result = CacheGetModel(store=store, format="json")(CacheGetContext(key="text_stats/2026-05-01"))
```

```python
from ccflow_db import SQLiteCacheStore, SQLiteConfig
from ccflow_etl import CacheGetContext, CacheGetModel

store = SQLiteCacheStore(config=SQLiteConfig(path="./cache.sqlite"), table="cache_entries")
result = CacheGetModel(store=store, format="json")(CacheGetContext(key="text_stats/2026-05-01"))
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

## Development

Run the package tests with:

```bash
python -m pytest ccflow_etl/tests -q
```

Run Ruff before release:

```bash
python -m ruff check ccflow_etl
```

Default tests should use synthetic local fixtures and must not require live HTTP, S3, database, Celery, or provider credentials. Integration tests that need external services should be opt-in and skipped by default.

`ccflow-etl` provides public, domain-neutral ETL building blocks for `ccflow` callable models. It should own reusable concepts such as extract/transform/load composition, backfill planning, checkpointing, caching, retry policy models, idempotency metadata, and CLI workflows.

`ccflow-etl` should not contain finance-specific calendars, market-data provider behavior, connector-specific client code, or application-specific workflows. Connector packages and domain packages should depend on these ETL contracts where useful.

> [!NOTE]
> This library was generated using [copier](https://copier.readthedocs.io/en/stable/) from the [Base Python Project Template repository](https://github.com/python-project-templates/base).
