import sys
from pathlib import Path
from pprint import pprint

import hydra
from ccflow.utils.hydra import cfg_run, get_args_parser_default_ui, load_config, resolve_config_paths, ui_launcher_default
from omegaconf import OmegaConf
from omegaconf.errors import OmegaConfBaseException

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
        elif arg.startswith(("--config-path=", "-cp=")):
            key, value = arg.split("=", 1)
            normalized.append(f"{key}={_normalize_config_path(value)}")
        elif index > 0 and argv[index - 1] in {"--config-path", "-cp"}:
            normalized.append(_normalize_config_path(arg))
        else:
            normalized.append(arg)
    return normalized


def explain():
    parser = get_args_parser_default_ui()
    args = parser.parse_args()
    root_config_dir, root_config_name = resolve_config_paths(args, "config", "base", _main)
    result = load_config(
        root_config_dir=root_config_dir,
        root_config_name=root_config_name,
        config_dir=args.config_dir,
        config_name=args.config_dir_config_name,
        overrides=args.overrides,
        basepath=args.basepath,
        debug=True,
    )
    try:
        merged_cfg = result.merge()
    except OmegaConfBaseException:
        merged_cfg = OmegaConf.to_container(result.cfg, resolve=True)

    if args.no_gui:
        pprint(merged_cfg, width=120, indent=2)
        return
    try:
        ui_launcher_default(merged_cfg, **vars(args))
    except ImportError:
        raise ValueError("Cannot launch UI. Use --no-gui to print the results.") from None


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
