import json

import pyarrow.parquet as pq
from ccflow import CallableModel, ContextBase, ContextType, Flow, GenericResult, ResultType

from ccflow_etl import ExtractTaskModel, LocalFileOutput, NoOpArtifactStore


class SampleDataset(CallableModel):
    return_type: str = "json"

    @property
    def context_type(self):
        return ContextBase

    @property
    def result_type(self):
        return GenericResult

    @Flow.call
    def __call__(self, context: ContextType) -> ResultType:
        extension = "json" if self.return_type == "json" else self.return_type
        return GenericResult(
            value={
                "dataset": "sample",
                "output_keys": [f"sample/output.{extension}"],
                "results": [{"ticker": "AAA", "value": 1}],
            }
        )


def test_extract_task_plans_output_writes_without_touching_artifact_store():
    result = ExtractTaskModel(dataset=SampleDataset(), output=NoOpArtifactStore(uri_prefix="noop://output"), explain=True)(ContextBase()).value

    assert result["output_writes"][0]["status"] == "planned"
    assert result["output_writes"][0]["artifact"]["uri"] == "noop://output/sample/output.json"


def test_extract_task_writes_json_payload_to_local_output(tmp_path):
    result = ExtractTaskModel(dataset=SampleDataset(), output=LocalFileOutput(path=tmp_path))(ContextBase()).value

    output_path = tmp_path / "sample" / "output.json"
    assert result["output_writes"][0]["status"] == "written"
    assert json.loads(output_path.read_text()) == {"ticker": "AAA", "value": 1}


def test_extract_task_writes_non_json_return_types_with_payload_codec(tmp_path):
    planned = ExtractTaskModel(dataset=SampleDataset(return_type="parquet"), output=LocalFileOutput(path=tmp_path), explain=True)(ContextBase()).value

    assert planned["output_writes"][0]["artifact"]["media_type"] == "application/vnd.apache.parquet"

    written = ExtractTaskModel(dataset=SampleDataset(return_type="parquet"), output=LocalFileOutput(path=tmp_path))(ContextBase()).value

    assert written["output_writes"][0]["status"] == "written"
    assert pq.read_table(tmp_path / "sample" / "output.parquet").to_pylist() == [{"ticker": "AAA", "value": 1}]
