import logging
from functools import reduce
from typing import Tuple

import pyspark.sql
import pyspark.sql.functions
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType


class CorporationsManifestParser:

    # Note: "Rubro" fields are actually nullable, but have a sentinel value that's
    # not of interest
    INPUT_SCHEMA = StructType(
        [
            StructField("Año comercial", LongType(), nullable=False),
            StructField("RUT", LongType(), nullable=False),
            StructField("DV", StringType(), nullable=False),
            StructField("Razón social", StringType(), nullable=False),
            StructField("Tramo según ventas", LongType(), nullable=False),
            StructField("Número de trabajadores dependie", LongType(), nullable=False),
            StructField("Fecha inicio de actividades vige", DateType(), nullable=True),
            StructField("Fecha término de giro", DateType(), nullable=True),
            StructField("Fecha primera inscripción de ac", DateType(), nullable=True),
            StructField("Tipo término de giro", StringType(), nullable=True),
            StructField("Tipo de contribuyente", StringType(), nullable=False),
            StructField("Subtipo de contribuyente", StringType(), nullable=False),
            StructField("Tramo capital propio positivo", LongType(), nullable=True),
            StructField("Tramo capital propio negativo", LongType(), nullable=True),
            StructField("Rubro económico", StringType(), nullable=True),
            StructField("Subrubro económico", StringType(), nullable=True),
            StructField("Actividad económica", StringType(), nullable=True),
            StructField("Región", StringType(), nullable=False),
            StructField("Provincia", StringType(), nullable=False),
            StructField("Comuna", StringType(), nullable=False),
            StructField("R_PRESUNTA", StringType(), nullable=True),
            StructField("OTROS_REGIMENES", StringType(), nullable=True),
        ]
    )

    SCHEMA_MAPPING: dict[str, str] = dict(
        [
            ("Año comercial", "ano_comercial"),
            ("RUT", "rut"),
            ("DV", "dv"),
            ("Razón social", "razon_social"),
            ("Tramo según ventas", "tramo_segun_ventas"),
            ("Número de trabajadores dependie", "numero_de_trabajadores_dependie"),
            ("Fecha inicio de actividades vige", "fecha_inicio_de_actividades_vige"),
            ("Fecha término de giro", "fecha_termino_de_giro"),
            ("Fecha primera inscripción de ac", "fecha_primera_inscripcion_de_ac"),
            ("Tipo término de giro", "tipo_termino_de_giro"),
            ("Tipo de contribuyente", "tipo_de_contribuyente"),
            ("Subtipo de contribuyente", "subtipo_de_contribuyente"),
            ("Tramo capital propio positivo", "tramo_capital_propio_positivo"),
            ("Tramo capital propio negativo", "tramo_capital_propio_negativo"),
            ("Rubro económico", "rubro_economico"),
            ("Subrubro económico", "subrubro_economico"),
            ("Actividad económica", "actividad_economica"),
            ("Región", "region"),
            ("Provincia", "provincia"),
            ("Comuna", "comuna"),
            ("R_PRESUNTA", "r_presunta"),
            ("OTROS_REGIMENES", "otros_regimenes"),
        ]
    )

    def __init__(
        self,
        spark: pyspark.sql.SparkSession,
        input_file: str,
        file_enc: str = "utf-8",
    ) -> None:
        """
        Parameters
        ----------
        spark:  pyspark.sql.SparkSession
                SparkSession instance

        input_file: str
                    input file path

        file_enc:   str
                    encoding of file, defaults to utf-8
        """
        self.spark = spark
        self.input_file = input_file

        self.df = (
            spark.read.option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("charset", file_enc)
            .schema(CorporationsManifestParser.INPUT_SCHEMA)
            .csv(path=input_file, sep="\t")
        )

    def __or_clause(self, *clauses: pyspark.sql.Column) -> pyspark.sql.Column:
        return reduce(
            lambda acc, col: (acc | col), clauses, pyspark.sql.functions.lit(False)
        )

    def __split_validate_null_violations(
        self, df: pyspark.sql.DataFrame, schema: StructType
    ) -> Tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame]:
        import pyspark.sql.functions as _f

        not_null_cols = [f.name for f in schema.fields if not f.nullable]

        invalid_clauses = [_f.col(col).isNull() for col in not_null_cols]
        invalid_condition = self.__or_clause(*invalid_clauses)

        return (df.where(~invalid_condition), df.where(invalid_condition))

    def __split_validate_date_fields(
        self, df: pyspark.sql.DataFrame, schema: StructType
    ) -> Tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame]:
        import pyspark.sql.functions as _f

        cols = [f.name for f in schema.fields if isinstance(f.dataType, DateType)]

        # invalid row if any of the supplied date fields is before the independence of Chile
        invalid_clauses = [
            ((_f.col(c) < _f.lit("1810-01-01")) & _f.col(c).isNotNull()) for c in cols
        ]
        invalid_condition = self.__or_clause(*invalid_clauses)

        return (df.where(~invalid_condition), df.where(invalid_condition))

    def _split_validate_duplicates(
        self, df: pyspark.sql.DataFrame, schema: StructType
    ) -> Tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame]:
        import pyspark.sql.functions as _f

        w = (
            pyspark.sql.Window()
            .partitionBy(df.ano_comercial, df.rut)
            .orderBy(df.fecha_inicio_de_actividades_vige.desc())
        )
        enumerated = df.withColumn("r", _f.row_number().over(w))
        return (
            enumerated.where(enumerated.r == 1).drop("r"),
            enumerated.where(enumerated.r > 1).drop("r"),
        )

    def parse(self) -> Tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame]:
        """
        Parses the "Nomina Persona Juridica" manifest of incorporated entities
        published by Chile's SII.

        It rejects malformed records, including those lacking required fields
        and having invalid dates, as well as it deduplicates on the basis of the
        publication year and the RUT field.

        Returns
        -------
        valid:  pyspark.sql.DataFrame
                Parsed, accepted records

        rejects:    pyspark.sql.DataFrame
                    Rejected rows
        """
        import pyspark.sql.functions as _f

        renamed_df = self.df.select(
            [
                _f.col(orig).alias(renamed)
                for orig, renamed in CorporationsManifestParser.SCHEMA_MAPPING.items()
            ]
        )

        new_schema = StructType(
            [
                StructField(
                    CorporationsManifestParser.SCHEMA_MAPPING[f.name],
                    f.dataType,
                    f.nullable,
                )
                for f in CorporationsManifestParser.INPUT_SCHEMA
            ]
        )

        null_mapped_df = (
            renamed_df.withColumn(
                "rubro_economico",
                _f.when(
                    renamed_df.rubro_economico == _f.lit("Valor por Defecto"), None
                ).otherwise(renamed_df.rubro_economico),
            )
            .withColumn(
                "subrubro_economico",
                _f.when(
                    renamed_df.subrubro_economico == _f.lit("Valor por Defecto"), None
                ).otherwise(renamed_df.subrubro_economico),
            )
            .withColumn(
                "actividad_economica",
                _f.when(
                    renamed_df.actividad_economica == _f.lit("Valor por Defecto"), None
                ).otherwise(renamed_df.actividad_economica),
            )
        )

        (null_valid, null_rejects) = self.__split_validate_null_violations(
            null_mapped_df, new_schema
        )

        (date_valid, date_rejects) = self.__split_validate_date_fields(
            null_valid, new_schema
        )

        (valid, dupe_rejects) = self._split_validate_duplicates(date_valid, new_schema)

        rejects = null_rejects.union(date_rejects).union(dupe_rejects)

        valid = self.spark.createDataFrame(valid.rdd, new_schema)

        return (valid, rejects)
