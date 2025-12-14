import hydra
from ccflow.utils.hydra import cfg_explain_cli, cfg_run

__all__ = (
    "explain",
    "main",
)


def explain():
    cfg_explain_cli(config_path="config", config_name="base", hydra_main=main)


@hydra.main(config_path="config", config_name="base", version_base=None)
def main(cfg):
    return cfg_run(cfg)
