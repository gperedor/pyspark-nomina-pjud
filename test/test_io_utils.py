import datetime
import os
from pathlib import Path
from test.common import EXPECTED_SCHEMA

import pyspark.sql
from pyspark.testing.utils import assertDataFrameEqual

from pyspark_nomina_pjud.batch_parse.io_utils import write_records


def test_write_single_file(spark: pyspark.sql.SparkSession, tmp_path: Path):
    records = spark.createDataFrame(
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
    run_date = datetime.date.fromisoformat("2025-01-01")
    run_id = "abc"

    data_dir, n_files = write_records(
        records=records,
        output_directory_root=str(tmp_path.absolute()),
        run_id=run_id,
        run_date=run_date,
    )

    actual = spark.read.csv(data_dir, schema=EXPECTED_SCHEMA, header=True)

    assertDataFrameEqual(actual=actual, expected=records)
    assert n_files == 1
    assert len([f for f in os.listdir(data_dir) if f.endswith(".csv")]) == 1


def test_write_n_files(spark: pyspark.sql.SparkSession, tmp_path: Path):
    records = spark.createDataFrame(
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
        EXPECTED_SCHEMA,
    )
    run_date = datetime.date.fromisoformat("2025-01-01")
    run_id = "abc"

    data_dir, n_files = write_records(
        records=records,
        output_directory_root=str(tmp_path.absolute()),
        run_id=run_id,
        run_date=run_date,
        max_rows_per_file=1,
    )

    actual = spark.read.csv(data_dir, schema=EXPECTED_SCHEMA, header=True)

    assertDataFrameEqual(actual=actual, expected=records)
    assert n_files == 2
    assert len([f for f in os.listdir(data_dir) if f.endswith(".csv")]) == 2


def test_write_one_file_too_small(spark: pyspark.sql.SparkSession, tmp_path: Path):
    records = spark.createDataFrame(
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
    run_date = datetime.date.fromisoformat("2025-01-01")
    run_id = "abc"

    data_dir, n_files = write_records(
        records=records,
        output_directory_root=str(tmp_path.absolute()),
        run_id=run_id,
        run_date=run_date,
        max_rows_per_file=10000000000,
    )

    actual = spark.read.csv(data_dir, schema=EXPECTED_SCHEMA, header=True)

    assertDataFrameEqual(actual=actual, expected=records)
    assert n_files == 1
    assert len([f for f in os.listdir(data_dir) if f.endswith(".csv")]) == 1
