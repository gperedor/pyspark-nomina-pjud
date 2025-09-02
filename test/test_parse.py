import datetime
from pathlib import Path
from test.common import EXPECTED_SCHEMA

import pyspark.sql
import pytest
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType
from pyspark.testing.utils import assertDataFrameEqual

from pyspark_nomina_pjud.batch_parse.parse import CorporationsManifestParser


@pytest.fixture
def data_dir(test_dir: Path) -> Path:
    return test_dir / "data" / "manifest" / "input"


def test_rejects_nulls(spark: pyspark.sql.SparkSession, data_dir: Path) -> None:
    parser = CorporationsManifestParser(
        spark=spark, input_file=str(data_dir / "with_invalid_nulls.csv")
    )
    (valid, rejected) = parser.parse()

    expected = spark.createDataFrame(
        [
            (
                2022,
                2222222,
                "2",
                "COSOPAC",
                1,
                10,
                datetime.date.fromisoformat("2010-01-01"),
                None,
                datetime.date.fromisoformat("2010-01-01"),
                None,
                "ABC",
                "ABCD",
                10,
                None,
                None,
                None,
                None,
                "X",
                "Y",
                "Z",
                "P",
                "R",
            )
        ],
        EXPECTED_SCHEMA,
    )

    assertDataFrameEqual(actual=valid, expected=expected)
    assert rejected.count() == 1


def test_rejects_invalid_dates(spark: pyspark.sql.SparkSession, data_dir: Path) -> None:
    parser = CorporationsManifestParser(
        spark=spark, input_file=str(data_dir / "with_invalid_dates.csv")
    )
    (valid, rejected) = parser.parse()

    expected = spark.createDataFrame(
        [
            (
                2022,
                2222222,
                "2",
                "COSOPAC",
                1,
                10,
                datetime.date.fromisoformat("2010-01-01"),
                None,
                datetime.date.fromisoformat("2010-01-01"),
                None,
                "ABC",
                "ABCD",
                10,
                None,
                None,
                None,
                None,
                "X",
                "Y",
                "Z",
                "P",
                "R",
            )
        ],
        schema=EXPECTED_SCHEMA,
    )

    assertDataFrameEqual(actual=valid, expected=expected)
    assert rejected.count() == 1


def test_rejects_duplicates(spark: pyspark.sql.SparkSession, data_dir: Path) -> None:
    parser = CorporationsManifestParser(
        spark=spark, input_file=str(data_dir / "with_duplicates.csv")
    )
    (valid, rejected) = parser.parse()

    expected = spark.createDataFrame(
        [
            (
                2022,
                111111,
                "1",
                'ACME "Inc',
                1,
                10,
                datetime.date.fromisoformat("2010-01-01"),
                None,
                datetime.date.fromisoformat("2007-01-01"),
                None,
                "ABC",
                "ABCD",
                10,
                None,
                "E",
                "EF",
                "EFG",
                "X",
                "Y",
                "Z",
                None,
                "O",
            ),
            (
                2022,
                2222222,
                "2",
                "COSOPAC",
                1,
                10,
                datetime.date.fromisoformat("2010-01-01"),
                None,
                datetime.date.fromisoformat("2010-01-01"),
                None,
                "ABC",
                "ABCD",
                10,
                None,
                None,
                None,
                None,
                "X",
                "Y",
                "Z",
                "P",
                "R",
            ),
        ],
        schema=EXPECTED_SCHEMA,
    )

    assertDataFrameEqual(actual=valid, expected=expected)
    assert rejected.count() == 1


def test_parses_correct_df(spark: pyspark.sql.SparkSession, data_dir: Path) -> None:
    parser = CorporationsManifestParser(
        spark=spark, input_file=str(data_dir / "happy_path.csv")
    )
    (actual, _) = parser.parse()

    expected = spark.createDataFrame(
        [
            (
                2022,
                111111,
                "1",
                'ACME "Inc',
                1,
                10,
                datetime.date.fromisoformat("2010-01-01"),
                None,
                datetime.date.fromisoformat("2007-01-01"),
                None,
                "ABC",
                "ABCD",
                10,
                None,
                "E",
                "EF",
                "EFG",
                "X",
                "Y",
                "Z",
                None,
                "O",
            ),
            (
                2022,
                2222222,
                "2",
                "COSOPAC",
                1,
                10,
                datetime.date.fromisoformat("2010-01-01"),
                None,
                datetime.date.fromisoformat("2010-01-01"),
                None,
                "ABC",
                "ABCD",
                10,
                None,
                None,
                None,
                None,
                "X",
                "Y",
                "Z",
                "P",
                "R",
            ),
        ],
        schema=EXPECTED_SCHEMA,
    )

    assertDataFrameEqual(actual=actual, expected=expected)
