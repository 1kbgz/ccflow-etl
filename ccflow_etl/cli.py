import sys
from pathlib import Path
from pprint import pprint

import hydra
from ccflow import LazyRegistry, ModelRegistry
from ccflow.utils.hydra import get_args_parser_default_ui, load_config, resolve_config_paths, ui_launcher_default
from omegaconf import OmegaConf
from omegaconf.errors import OmegaConfBaseException

__all__ = (
    "explain",
    "load_registry",
    "main",
    "run",
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


def load_registry(cfg, overwrite: bool = True) -> ModelRegistry:
    """Load a composed config into the root registry, deferring each model until it is first used.

    The config is loaded into a ``LazyRegistry`` and its top-level entries are mounted into the root registry.
    Nested registries (calendars, backfills, credentials, and so on) stay lazy, so only the models a run touches
    are instantiated and only the modules they need are imported, while lookups through
    ``ModelRegistry.root()`` keep working.
    """
    container = OmegaConf.to_container(cfg, resolve=True) if OmegaConf.is_config(cfg) else cfg
    lazy = LazyRegistry()
    # ccflow accepts plain mappings here but currently annotates only DictConfig.
    lazy.load_config(cfg=container, overwrite=True)  # ty: ignore[invalid-argument-type]
    root = ModelRegistry.root()
    for name in lazy.models:
        root.add(name, lazy[name], overwrite=overwrite)
    return root


def run(cfg):
    """Run a composed config like ``ccflow.utils.hydra.cfg_run``, with the registry loaded lazily."""
    # Staged here until lazy loading moves into ccflow's runner; reuses ccflow's run helpers so behavior matches.
    from ccflow.utils.hydra import _run_get, _run_model

    if not OmegaConf.is_config(cfg):
        cfg = OmegaConf.create(cfg)
    load_registry(cfg)
    return _run_get(cfg) if "get" in cfg else _run_model(cfg)


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
    return run(cfg)


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
