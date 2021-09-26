import glob
import os
import pytest
import pandas as pd

from unittest.mock import patch
from dags.etl.spark_initial_load import create_initial_load
from dags.etl.spark_daily import create_daily_app


def test_create_initial_load(spark_session, mariadb, data_path, input_path, output_path):

    with patch('dags.utils.common.get_spark_session',
               spark_session):
        with patch('dags.utils.common.write_mariadb',
                   mariadb):
            result = create_initial_load(input_path, output_path)
            assert result == "Done"

            assert len(glob.glob(output_path + "/*.parquet")) > 0


def test_create_daily_app(spark_session, mariadb, data_path, input_path, output_path):

    with patch('dags.utils.common.get_spark_session',
               spark_session):
        with patch('dags.utils.common.write_mariadb',
                   mariadb):
            result = create_daily_app(input_path, output_path)
            assert result == "Done"

            assert len(glob.glob(output_path + "/*.parquet")) > 0
