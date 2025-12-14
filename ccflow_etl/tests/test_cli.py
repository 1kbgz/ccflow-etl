import sys
from unittest.mock import patch

from ccflow_etl import ETL, load_config
from ccflow_etl.cli import main


class TestBasic:
    def test_basic_example(self):
        cfg = load_config(["+context=[[[]],[[]],[[]]]"], overwrite=True)
        assert isinstance(cfg["etl"], ETL)

    def test_basic_cli(self):
        with patch.object(sys, "argv", ["ccflow-etl", "+context=[[[]],[[]],[[]]]"]):
            ret = main()
            assert ret is None
