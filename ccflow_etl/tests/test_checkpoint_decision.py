from ccflow_etl import CheckpointDecisionContext, CheckpointDecisionModel, CheckpointDecisionUnit


class FakeCheckpointStore:
    def __init__(self, succeeded_keys):
        self.succeeded_keys = set(succeeded_keys)

    def should_skip(self, key):
        return key in self.succeeded_keys


def test_checkpoint_decision_model_skips_succeeded_checkpoint_units(tmp_path):
    store = FakeCheckpointStore({"massive:daily_bars:AAA:2024-01-03"})

    result = CheckpointDecisionModel()(
        CheckpointDecisionContext(
            units=[
                CheckpointDecisionUnit(key="massive:daily_bars:AAA:2024-01-03"),
                CheckpointDecisionUnit(key="massive:daily_bars:AAA:2024-01-04"),
            ],
            checkpoint_store=store,
        )
    )

    assert [decision.status for decision in result.decisions] == ["checkpoint", "runnable"]
    assert result.runnable_keys == ["massive:daily_bars:AAA:2024-01-04"]


def test_checkpoint_decision_model_skips_existing_destinations_without_overwrite(tmp_path):
    output_path = tmp_path / "daily_bars" / "AAA" / "2024-01-03.json"
    output_path.parent.mkdir(parents=True)
    output_path.write_text("{}\n")

    result = CheckpointDecisionModel()(
        CheckpointDecisionContext(units=[CheckpointDecisionUnit(key="massive:daily_bars:AAA:2024-01-03", output_path=output_path)])
    )

    assert result.decisions[0].status == "exists"
    assert result.runnable_keys == []


def test_checkpoint_decision_model_overwrite_keeps_existing_destinations_runnable(tmp_path):
    output_path = tmp_path / "daily_bars" / "AAA" / "2024-01-03.json"
    output_path.parent.mkdir(parents=True)
    output_path.write_text("{}\n")

    result = CheckpointDecisionModel()(
        CheckpointDecisionContext(
            units=[CheckpointDecisionUnit(key="massive:daily_bars:AAA:2024-01-03", output_path=output_path)],
            overwrite=True,
        )
    )

    assert result.decisions[0].status == "runnable"
    assert result.runnable_keys == ["massive:daily_bars:AAA:2024-01-03"]
