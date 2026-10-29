__version__ = "0.4.2"


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import *
    from .models import *


def __getattr__(name: str) -> object:
    # Submodules are imported on first access so that lerna can locate
    # ccflow_etl.config without importing ccflow
    from importlib import import_module

    namespace = globals()
    for submodule in ("config", "models"):
        module = import_module(f"{__name__}.{submodule}")
        exported = getattr(module, "__all__", None) or [n for n in vars(module) if not n.startswith("_")]
        namespace.update({n: getattr(module, n) for n in exported})
    if name in namespace:
        return namespace[name]
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
