# Building Pipelines

The recommended pattern is to write a concrete `CallableModel` for the workflow you need. The model name, context, output shape, and dependencies should describe the actual workflow. `ccflow-etl` supplies reusable pieces such as writers, artifact metadata, cache handoffs, execution policy, and summaries. Generic retry wrappers come from `ccflow`.

This example reads a text file, computes word counts, writes JSON locally, and returns artifact metadata plus a run summary.

```python
# text_pipeline.py
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Type

from ccflow import CallableModel, ContextBase, ContextType, Flow, GenericResult, ResultType
from pydantic import Field

from ccflow_etl import CachePutContext, CachePutModel, LocalCacheStore, RunSummary


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
        summary = RunSummary.from_items([{"status": write_result.status}], artifacts=[write_result.artifact])

        return GenericResult(
            value={
                "payload": payload,
                "artifacts": [write_result.artifact.model_dump(mode="json")],
                "run_summary": summary.model_dump(mode="json"),
            }
        )
```

Run it through the shared CLI with a matching config:

```bash
echo 'small tools make larger workflows easier to trust' > notes.txt
cc-etl --config-path ./config --config-name text_stats +context.input_path=./notes.txt +context.output_path=./stats.json
```

## Design Guidelines

Keep reusable infrastructure in `ccflow-etl` and keep workflow-specific behavior in the package or application that owns the workflow. Durable I/O implementations should live in connector packages; `ccflow-etl` works against small contracts such as byte stores and checkpoint stores.

Avoid adding a new generic base class unless it removes real duplication. A concrete `CallableModel` graph is usually easier to read, test, and compose than a broad extract/transform/load shell.
