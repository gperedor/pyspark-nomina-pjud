import datetime
from typing import Tuple

import pyspark.sql


def write_records(
    records: pyspark.sql.DataFrame,
    output_directory_root: str,
    run_id: str,
    run_date: datetime.date | None = None,
    file_kind: str = "records",
    max_rows_per_file: int = -1,
) -> Tuple[str, int]:
    """
    Writes the supplied records to the output directory root, to a path as follows

        output_directory_root/run_date/run_id/*.csv

    The number of files depends on the value of max_rows_per_file, one file if value < 1

    Parameters
    ----------
    records:    pyspark.sql.DataFrame
                The records to write

    output_directory_root:  str
                            The directory root to write the data to

    run_id: str
            A unique identifier, for the run_date, for this run and output

    run_date:   datetime.date
                The execution date, defaults to today's date if not supplied

    file_kind:  str
                A string stating the type of file to be written, given that the pipeline
                produces valid and rejected records

    max_rows_per_file:  int
                        The maximum number of rows to output to any resulting file, will be
                        used as the basis to split the output into multiple files.
                        Defaults to -1, meaning all rows will go into a single file.

    Returns
    -------
    output_directory:   str
                        The directory the data files were written to
    files_written:      int
                        The number of files written


    """

    if run_date is None:
        output_date = datetime.date.today()
    else:
        output_date = run_date

    if max_rows_per_file > 0:
        n_partitions = max(int(records.count() / max_rows_per_file), 1)
    else:
        n_partitions = 1

    df = records.repartition(n_partitions)

    output_directory = (
        output_directory_root
        + "/"
        + output_date.strftime("%Y-%m-%d")
        + "/"
        + run_id
        + "/"
        + file_kind
    )
    df.write.csv(output_directory, encoding="utf-8", header=True)

    return output_directory, n_partitions
