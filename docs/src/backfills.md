# Backfills And Calendars

Use `BackfillModel` when the same callable should run once per date or datetime step. `BackfillContext` gets its steps from a calendar. The default is daily, interval strings such as `2M` are accepted through `IntervalCalendar`, and packages can provide their own `BaseCalendar` subclasses.

## Compact Contexts

A compact backfill context list has this shape:

```yaml
context:
  - 2026-05-01
  - 2026-05-03
  - input_path: ./notes.txt
    output_path: ./stats.json
    min_length: 1
  - forward
  - daily
```

The first two items are the start and end datetimes. The third item is the wrapped callable's template context. The fourth item is the direction, and the fifth item is either an interval or a calendar.

## Explicit Wrapper Config

A literal config can wrap a callable model in `BackfillModel`:

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

Run it with:

```bash
cc-etl --config-path ./config --config-name backfill_text_stats
```

## Packaged Backfill Groups

Most projects can use the packaged groups instead of creating a separate backfill config for every callable. The root `context` remains the `ccflow` runtime context. For a backfill run, it is a `BackfillContext`; the wrapped callable's seed context is nested under `context.template`, or passed as the third item in compact list form.

The packaged backfill groups do not create a separate `backfill.context` shadow namespace.

Built-in calendars are available under `/calendars`: `daily`, `hourly`, `weekly`, `weekdays`, `business_daily`, and `monday_friday`. Set `context.calendar` to one of those paths when you want the calendar object to choose steps instead of the interval shorthand:

```bash
cc-etl --config-path ./config --config-name text_stats_runner backfill=daily +context.start_datetime=2026-05-01 +context.end_datetime=2026-05-15 +context.calendar=/calendars/weekdays +context.template.input_path=./notes.txt +context.template.output_path=./stats.json +context.template.min_length=1
```

## Supported Intervals

Backfill contexts can use aliases such as `daily`, fixed intervals such as `1D`, `6h`, or `30min`, month intervals such as `2M`, business days such as `1B`, and calendar boundaries such as `MS`, `ME`, `BMS`, `BME`, `QS`, `QE`, `YS`, and `YE`.

`calendar` takes precedence over `interval` when both are provided.
