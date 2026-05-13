import sys
from pathlib import Path

import hydra
from ccflow.utils.hydra import cfg_explain_cli, cfg_run

__all__ = (
    "explain",
    "main",
)


def _normalize_config_path(value: str) -> str:
    path = Path(value).expanduser()
    if not path.is_absolute() and path.exists():
        return str(path.resolve())
    return value


def _normalize_config_path_argv(argv: list[str]) -> list[str]:
    normalized: list[str] = []
    for index, arg in enumerate(argv):
        if arg in {"--config-path", "-cp"}:
            normalized.append(arg)
        elif arg.startswith("--config-path="):
            key, value = arg.split("=", 1)
            normalized.append(f"{key}={_normalize_config_path(value)}")
        elif arg.startswith("-cp="):
            key, value = arg.split("=", 1)
            normalized.append(f"{key}={_normalize_config_path(value)}")
        elif index > 0 and argv[index - 1] in {"--config-path", "-cp"}:
            normalized.append(_normalize_config_path(arg))
        elif arg.startswith("+callable="):
            normalized.append(arg[1:])
        elif any(arg.startswith(f"+backfill.{key}=") for key in ("start_datetime", "end_datetime", "direction", "interval")):
            normalized.append(arg[1:])
        else:
            normalized.append(arg)
    return normalized


def explain():
    cfg_explain_cli(config_path="config", config_name="base", hydra_main=_main)


@hydra.main(config_path="config", config_name="base", version_base=None)
def _main(cfg):
    return cfg_run(cfg)


def main():
    original_argv = sys.argv
    normalized_argv = _normalize_config_path_argv(original_argv)
    if normalized_argv == original_argv:
        return _main()
    try:
        sys.argv = normalized_argv
        return _main()
    finally:
        sys.argv = original_argv
