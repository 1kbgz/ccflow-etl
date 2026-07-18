from ccflow import FlowOptions
from ccflow.evaluators import GraphEvaluator, LoggingEvaluator, MemoryCacheEvaluator, MultiEvaluator
from ccflow.utils.reporting import InMemoryReporter, LoggingReporter, ReportPhase

from ccflow_etl import ArtifactWriteContext, ArtifactWriteModel, LocalFileOutput, load_config


def test_artifact_results_emit_reporting_lifecycle_events(tmp_path):
    output = LocalFileOutput(path=tmp_path)
    output.write("existing.json", b"{}")
    model = ArtifactWriteModel(store=output)
    reporter = InMemoryReporter()
    evaluator = LoggingEvaluator(reporter=reporter)

    result = model(
        ArtifactWriteContext(key="existing.json", payload=b"{}", dataset="sample"),
        _options={"evaluator": evaluator},
    )

    assert result.status == "exists"
    assert [event.phase for event in reporter.events] == [ReportPhase.START, ReportPhase.SUCCESS, ReportPhase.END]
    assert all(event.model_name == "ArtifactWriteModel" for event in reporter.events)


def test_base_config_enables_ccflow_reporting_events():
    registry = load_config([], overwrite=True)
    options = registry["/cli/model"]

    assert isinstance(options, FlowOptions)
    assert isinstance(options.evaluator, MultiEvaluator)
    assert [type(evaluator) for evaluator in options.evaluator.evaluators] == [
        GraphEvaluator,
        MemoryCacheEvaluator,
        LoggingEvaluator,
    ]
    assert isinstance(options.evaluator.evaluators[-1].reporter, LoggingReporter)
