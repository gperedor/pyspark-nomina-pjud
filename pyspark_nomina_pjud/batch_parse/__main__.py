import argparse
import logging
import sys
import uuid

import pyspark.sql
from pyspark.logger import PySparkLogger

from pyspark_nomina_pjud.batch_parse import io_utils
from pyspark_nomina_pjud.batch_parse.parse import CorporationsManifestParser


def main(
    spark: pyspark.sql.SparkSession,
    input_file: str,
    output_directory_root: str,
    max_file_size_rows: int,
) -> None:

    logger = PySparkLogger.getLogger("ConsoleLogger")
    logger.setLevel(getattr(logging, "INFO"))

    run_id = str(uuid.uuid4())

    logger.info(f"Initiating run, run_id: {run_id}", run_id=run_id)

    logger.info(
        f"Instantiating parser to process file: {input_file}", input_file=input_file
    )

    parser = CorporationsManifestParser(spark, input_file)

    (records, rejects) = parser.parse()

    logger.info(
        f"Writing to {output_directory_root}",
        output_directory_root=output_directory_root,
    )

    rejects_count = rejects.count()

    logger.info(f"Rejected records count: {rejects_count}", rejects_count=rejects_count)

    io_utils.write_records(
        records, output_directory_root, run_id, max_rows_per_file=max_file_size_rows
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="batch_parse",
        description="Parses a CSV file containing the SII Nomina Persona Juridica into cleaned up CSV files",
    )
    parser.add_argument("input_file", help="Path to input file")
    parser.add_argument(
        "output_directory_root", help="Output directory root to write to"
    )
    parser.add_argument(
        "-P", "--parallelism", help="Number of Spark workers", type=int, default=1
    )
    parser.add_argument(
        "-M",
        "--max-file-size-rows",
        help="Maximum number of rows per file",
        type=int,
        default=-1,
    )

    args = parser.parse_args()

    spark = (
        pyspark.sql.SparkSession.builder.master(f"local[{args.parallelism}]")
        .config("spark.log.level", "INFO")
        .getOrCreate()
    )

    main(
        spark=spark,
        input_file=args.input_file,
        output_directory_root=args.output_directory_root,
        max_file_size_rows=args.max_file_size_rows,
    )
