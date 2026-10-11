import subprocess
import sys


def test_locating_config_package_does_not_import_ccflow():
    # lerna resolves pkg://ccflow_etl.config on every hydra app startup
    code = "import importlib.resources, sys; importlib.resources.files('ccflow_etl.config'); assert 'ccflow' not in sys.modules"
    subprocess.run([sys.executable, "-c", code], check=True)
