import logging
import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(name="data_path", autouse=True, scope="session")
def fixture_data_path():
    return os.path.join(os.getcwd(), "dags", "tests", "fixture")


@pytest.fixture(name="output_path", scope="session")
def fixture_output(data_path):
    return 'file://' + os.path.join(data_path, "result")


@pytest.fixture(name="input_path", scope="session")
def fixture_input(data_path):
    return 'file://' + os.path.join(data_path, "*.json")


@pytest.fixture(name="spark_session", scope="session")
def fixture_spark_session(request):
    """Fixture for creating a spark context."""

    spark = (SparkSession.builder
             .master('local[1]')
             .config('spark.jars.packages', "io.delta:delta-core_2.12:1.0.0")
             .config("spark.jars", "dags/jars/aws-java-sdk-1.11.534.jar,dags/jars/aws-java-sdk-bundle-1.11.874.jar,dags/jars/delta-core_2.12-1.0.0.jar,dags/jars/hadoop-aws-3.2.0.jar")
             .appName('pytest-pyspark-local-testing')
             .config("spark.network.timeout", "10000s")
             .config("spark.sql.files.ignoreMissingFiles", "true")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
             .enableHiveSupport()
             .getOrCreate())
    request.addfinalizer(lambda: spark.stop())
    return spark


@pytest.fixture(name="mariadb", scope="session")
def fixture_mariadb():
    return "ok"
