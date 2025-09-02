
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder
             .appName("pytest-xpert_group_etl")
             .getOrCreate())
    yield spark
    spark.stop()
