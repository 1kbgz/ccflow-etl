from ccflow import LazyRegistry, ModelRegistry
from omegaconf import OmegaConf

from ccflow_etl.cli import load_registry, run

CONFIG = {
    "calendars": {
        "daily": {"_target_": "ccflow_etl.DailyCalendar"},
        "weekly": {"_target_": "ccflow_etl.WeeklyCalendar"},
    },
    "task": {"_target_": "ccflow_etl.tests.test_backfill.EchoDateModel"},
    "callable": "/task",
    "context": ["2025-01-02"],
}


def test_load_registry_mounts_nested_registries_lazily_into_root():
    root = load_registry(OmegaConf.create(CONFIG))
    calendars = root["calendars"]

    assert isinstance(calendars, LazyRegistry)
    assert not calendars.is_loaded("daily")
    assert root["/calendars/daily"] is calendars["daily"]
    assert calendars.is_loaded("daily")
    assert not calendars.is_loaded("weekly")


def test_run_executes_the_callable_without_instantiating_unused_models():
    result = run(OmegaConf.create(CONFIG))

    assert result.value is not None
    assert not ModelRegistry.root()["calendars"].is_loaded("weekly")
