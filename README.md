# ccflow-etl

ETL Tools for ccflow

[![Build Status](https://github.com/1kbgz/ccflow-etl/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/1kbgz/ccflow-etl/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/1kbgz/ccflow-etl/branch/main/graph/badge.svg)](https://codecov.io/gh/1kbgz/ccflow-etl)
[![License](https://img.shields.io/github/license/1kbgz/ccflow-etl)](https://github.com/1kbgz/ccflow-etl)
[![PyPI](https://img.shields.io/pypi/v/ccflow-etl.svg)](https://pypi.python.org/pypi/ccflow-etl)

## Overview

`ccflow-etl` provides public, domain-neutral ETL building blocks for `ccflow` callable models. It should own reusable concepts such as extract/transform/load composition, backfill planning, checkpointing, caching, retry policy models, idempotency metadata, and CLI workflows.

`ccflow-etl` should not contain finance-specific calendars, market-data provider behavior, connector-specific client code, or application-specific workflows. Connector packages and domain packages should depend on these ETL contracts where useful.

## Current Status

- Implemented: `ETL`, `ExtractModel`, `TransformModel`, `LoadModel`, `BackfillContext`, `BackfillModel`, interval parsing, business-day context expansion, `SQLiteCheckpointStore`, transport-neutral `RetryPolicy`, and the `cc-etl` Hydra CLI entry points.
- Partial: current ETL stage models establish ordering and status shells, backfill can generate concrete contexts, SQLite checkpoints can mark/read completed units, and connector packages can consume retry classification, but durable data handoff, cache stores, retry execution orchestration, structured summaries, and generic planner/executor resume semantics still need implementation.
- Missing: local/S3/database cache adapters, broader checkpoint adapters, structured run summaries, generalized skip policies, dry-run planning, backoff/jitter scheduling, and cross-package integration examples.

## Dependency Contract

- Depends on `ccflow` for model, context, result, evaluator, and Hydra integration primitives.
- May define generic interfaces that connector packages implement.
- Must not depend on finance packages or application-specific packages.

## Test Convention

Default tests should use synthetic local fixtures and must not require live HTTP, S3, database, Celery, or provider credentials. Integration tests that need external services should be opt-in and skipped by default.

> [!NOTE]
> This library was generated using [copier](https://copier.readthedocs.io/en/stable/) from the [Base Python Project Template repository](https://github.com/python-project-templates/base).
