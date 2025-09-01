from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.config("spark.ui.enabled", "false").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def test_dir() -> Path:
    return Path(__file__).parent
