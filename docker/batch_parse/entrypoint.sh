#!/bin/sh
python -m pyspark_nomina_pjud.batch_parse -P ${PARALLELISM:-1} -M ${MAX_FILE_SIZE_ROWS:-20000} "${BATCH_PARSE_INPUT_FILE}" /data/out
