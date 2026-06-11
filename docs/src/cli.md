# CLI And Config

`ccflow-etl` installs two shared Hydra entry points:

- `cc-etl`: run a configured `ccflow` callable model.
- `cc-etl-explain`: inspect the resolved config without running the callable.

The packaged default config is intentionally small. It writes a local JSON payload with `LocalWriteModel(format="json")`:

```bash
cc-etl +context.path=./example-output.json +context.payload.message='hello from ccflow-etl' +context.overwrite=true
cc-etl-explain +context.path=./example-output.json
```

`cc-etl-explain --no-gui` prints the merged Hydra config. Run-level reporting belongs in ccflow evaluators rather than package-local task payloads.

Most projects should provide their own Hydra config directory and still use the shared entry point:

```bash
cc-etl --config-path ./config --config-name text_stats +context.input_path=./notes.txt +context.output_path=./stats.json
```

## Minimal Runner Config

A runner config usually defines a callable model, the `ccflow` execution options, the callable path to run, and the root runtime context passed to `ccflow.cfg_run`:

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

`model` defines the callable. `callable` points at the registered object to run. The top-level `context` key is reserved by `ccflow`; it is the runtime payload validated against the callable model's `context_type`.

## Packaged Config Groups

`ccflow-etl` ships small, domain-neutral config groups:

- `callable/callable`: run `/model` directly.
- `backfills/default`: register reusable `BackfillModel` objects under `/backfills/...`.
- `calendars/*`: register reusable calendar objects for backfilling.
- `credentials/default`: register generic credential shapes for packages to extend.
- `cache/noop`: register a no-op cache store plus matching get/put models.

To use those groups from a project config, add the package config directory to the Hydra search path:

```yaml
# config/text_stats_runner.yaml
defaults:
  - _self_
  - /callable: callable
  - /backfills: default

hydra:
  searchpath:
    - pkg://ccflow_etl.config

model:
  _target_: text_pipeline.TextStatsModel

task: ${model}
callable: ${oc.select:backfill,/task}

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

Run the configured model directly:

```bash
cc-etl --config-path ./config --config-name text_stats_runner +context.input_path=./notes.txt +context.output_path=./stats.json +context.min_length=1
```

Run the same model as a backfill:

```bash
cc-etl --config-path ./config --config-name text_stats_runner +backfill=/backfills/daily +context.start_datetime=2026-05-01 +context.end_datetime=2026-05-03 +context.interval=daily +context.template.input_path=./notes.txt +context.template.output_path=./stats.json +context.template.min_length=1
```

Connector packages can add their own package config directories through Hydra lerna plugins. For example, `ccflow-s3` contributes `cache=s3`, and `ccflow-db` can contribute `cache=sqlite`. The packaged `ccflow-etl` base defaults remain no-op, so local runners can opt into durable stores by changing only config groups.

For static runner configs that set root `context` values in the file, compose the packaged group before `_self_` so local runtime values override group defaults:

```yaml
defaults:
  - /backfills: default
  - _self_

backfill: /backfills/daily
callable: ${oc.select:backfill,/task}
```
