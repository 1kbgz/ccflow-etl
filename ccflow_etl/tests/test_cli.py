import sys
from pathlib import Path
from shutil import copytree
from unittest.mock import patch

from ccflow.utils.hydra import cfg_run, load_config as base_load_config

from ccflow_etl import (
    APIKeySecretCredentials,
    APITokenCredentials,
    BackfillModel,
    DailyCalendar,
    ExecutionPolicy,
    LocalWriteModel,
    NoCredentials,
    NoOpCacheStore,
    NoOpCheckpointStore,
    OAuthCredentials,
    UsernamePasswordCredentials,
    WeekdayCalendar,
    load_config,
)
from ccflow_etl.cli import main


class TestBasic:
    def test_basic_example(self):
        cfg = load_config([], overwrite=True)
        assert isinstance(cfg["model"], LocalWriteModel)

    def test_base_config_registers_credentials_and_noop_stores(self):
        cfg = load_config([], overwrite=True)

        assert isinstance(cfg["credentials/none"], NoCredentials)
        assert isinstance(cfg["credentials/username_password"], UsernamePasswordCredentials)
        assert isinstance(cfg["credentials/api_token"], APITokenCredentials)
        assert isinstance(cfg["credentials/api_key_secret"], APIKeySecretCredentials)
        assert isinstance(cfg["credentials/oauth"], OAuthCredentials)
        assert isinstance(cfg["execution"], ExecutionPolicy)
        assert isinstance(cfg["cache/store"], NoOpCacheStore)
        assert isinstance(cfg["checkpoint/store"], NoOpCheckpointStore)

    def test_packaged_backfill_interval_configs_register_default_intervals(self):
        for name, expected_offset in {
            "hourly": "h",
            "first_day_of_month": "MS",
            "last_day_of_month": "ME",
        }.items():
            cfg = load_config(
                [
                    "model._target_=ccflow_etl.tests.test_backfill.EchoDateModel",
                    f"backfill={name}",
                ],
                overwrite=True,
            )
            assert isinstance(cfg["backfill"], BackfillModel)
            assert cfg["backfill"].interval.offset == expected_offset
            assert cfg["backfill"].interval.n == 1

    def test_basic_cli(self, tmp_path):
        output_path = tmp_path / "example.json"
        with patch.object(sys, "argv", ["ccflow-etl", f"+context.path={output_path}", "+context.payload.message=hello", "+context.overwrite=true"]):
            ret = main()
            assert ret is None
        assert output_path.exists()

    def test_basic_cli_accepts_callable_group_alias(self, tmp_path):
        output_path = tmp_path / "example.json"
        with patch.object(
            sys,
            "argv",
            ["ccflow-etl", "callable=callable", f"+context.path={output_path}", "+context.payload.message=hello", "+context.overwrite=true"],
        ):
            ret = main()
            assert ret is None
        assert output_path.exists()

    def test_backfill_default_group_wraps_configured_model(self):
        root_config_dir = str(Path(__file__).parents[1] / "config")
        result = base_load_config(
            root_config_dir=root_config_dir,
            root_config_name="base",
            overrides=[
                "model._target_=ccflow_etl.tests.test_backfill.EchoDateModel",
                "backfill=default",
                "+context=[2024-01-02,2024-01-03]",
            ],
            basepath=root_config_dir,
            debug=False,
        )

        output = cfg_run(result.cfg)

        cfg = load_config(
            [
                "model._target_=ccflow_etl.tests.test_backfill.EchoDateModel",
                "backfill=default",
                "+context=[2024-01-02,2024-01-03]",
            ],
            overwrite=True,
        )
        assert isinstance(cfg["backfill"], BackfillModel)
        assert isinstance(cfg["calendars/daily"], DailyCalendar)
        assert isinstance(cfg["calendars/weekdays"], WeekdayCalendar)
        daily_cfg = load_config(
            [
                "model._target_=ccflow_etl.tests.test_backfill.EchoDateModel",
                "backfill=daily",
                "+context=[2024-01-02,2024-01-03]",
            ],
            overwrite=True,
        )
        assert isinstance(daily_cfg["backfill"], BackfillModel)
        assert output.value == {
            "steps": 2,
            "outputs": [
                {"context": {"date": "2024-01-02", "type_": "ccflow.context.DateContext"}, "value": {"date": "2024-01-02"}},
                {"context": {"date": "2024-01-03", "type_": "ccflow.context.DateContext"}, "value": {"date": "2024-01-03"}},
            ],
        }

    def test_backfill_group_can_use_registry_calendar(self):
        root_config_dir = str(Path(__file__).parents[1] / "config")
        result = base_load_config(
            root_config_dir=root_config_dir,
            root_config_name="base",
            overrides=[
                "model._target_=ccflow_etl.tests.test_backfill.EchoDateModel",
                "backfill=daily",
                "+context.start_datetime=2024-01-05",
                "+context.end_datetime=2024-01-09",
                "+context.template.date=2024-01-05",
                "+context.calendar=/calendars/weekdays",
            ],
            basepath=root_config_dir,
            debug=False,
        )

        output = cfg_run(result.cfg)

        assert output.value == {
            "steps": 3,
            "outputs": [
                {"context": {"date": "2024-01-05", "type_": "ccflow.context.DateContext"}, "value": {"date": "2024-01-05"}},
                {"context": {"date": "2024-01-08", "type_": "ccflow.context.DateContext"}, "value": {"date": "2024-01-08"}},
                {"context": {"date": "2024-01-09", "type_": "ccflow.context.DateContext"}, "value": {"date": "2024-01-09"}},
            ],
        }

    def test_backfill_daily_group_wraps_configured_model_without_context_preset(self):
        root_config_dir = str(Path(__file__).parents[1] / "config")
        backfill_config = Path(root_config_dir) / "backfill" / "daily.yaml"
        backfill_config_text = backfill_config.read_text()
        callable_config_text = (Path(root_config_dir) / "callable" / "local_write.yaml").read_text()
        assert "context:" not in backfill_config_text
        assert "context:" not in callable_config_text
        assert "start_datetime: ${backfill.start_datetime}" not in backfill_config_text

        result = base_load_config(
            root_config_dir=root_config_dir,
            root_config_name="base",
            overrides=[
                "model._target_=ccflow_etl.tests.test_backfill.EchoDateModel",
                "backfill=daily",
                "+context=[2024-01-02,2024-01-03]",
            ],
            basepath=root_config_dir,
            debug=False,
        )

        assert result.cfg["callable"] == "/backfill"
        output = cfg_run(result.cfg)

        cfg = load_config(
            [
                "model._target_=ccflow_etl.tests.test_backfill.EchoDateModel",
                "backfill=daily",
                "+context=[2024-01-02,2024-01-03]",
            ],
            overwrite=True,
        )
        assert isinstance(cfg["backfill"], BackfillModel)
        assert output.value == {
            "steps": 2,
            "outputs": [
                {"context": {"date": "2024-01-02", "type_": "ccflow.context.DateContext"}, "value": {"date": "2024-01-02"}},
                {"context": {"date": "2024-01-03", "type_": "ccflow.context.DateContext"}, "value": {"date": "2024-01-03"}},
            ],
        }

    def test_external_runner_config_can_use_packaged_callable_groups(self, tmp_path):
        config_path = tmp_path / "config"
        config_path.mkdir()
        (config_path / "runner.yaml").write_text(
            """
defaults:
    - _self_
    - /callable: callable
    - optional /backfill: null

hydra:
  searchpath:
    - pkg://ccflow_etl.config

model:
  _target_: ccflow_etl.tests.test_backfill.EchoDateModel

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
""".lstrip()
        )

        with patch.object(sys, "argv", ["ccflow-etl", "--config-path", str(config_path), "--config-name", "runner", "+context.date=2024-01-02"]):
            assert main() is None

        with patch.object(
            sys,
            "argv",
            [
                "ccflow-etl",
                "--config-path",
                str(config_path),
                "--config-name",
                "runner",
                "backfill=daily",
                "+context=[2024-01-02,2024-01-03]",
            ],
        ):
            assert main() is None

    def test_basic_cli_accepts_relative_filesystem_config_path(self, tmp_path, monkeypatch):
        copytree(Path(__file__).parents[1] / "config", tmp_path / "external_config")
        monkeypatch.chdir(tmp_path)
        output_path = tmp_path / "relative-config-example.json"

        with patch.object(
            sys,
            "argv",
            [
                "ccflow-etl",
                "--config-path",
                "./external_config",
                "--config-name",
                "base",
                f"+context.path={output_path}",
                "+context.payload.message=hello",
                "+context.overwrite=true",
                f"hydra.run.dir={tmp_path / 'hydra'}",
                "hydra.output_subdir=null",
            ],
        ):
            ret = main()

        assert ret is None
        assert output_path.exists()
