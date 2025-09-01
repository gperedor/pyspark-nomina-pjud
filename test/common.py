from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

EXPECTED_SCHEMA: StructType = StructType(
    [
        StructField("ano_comercial", LongType(), nullable=False),
        StructField("rut", LongType(), nullable=False),
        StructField("dv", StringType(), nullable=False),
        StructField("razon_social", StringType(), nullable=False),
        StructField("tramo_segun_ventas", LongType(), nullable=False),
        StructField("numero_de_trabajadores_dependie", LongType(), nullable=False),
        StructField("fecha_inicio_de_actividades_vige", DateType(), nullable=True),
        StructField("fecha_termino_de_giro", DateType(), nullable=True),
        StructField("fecha_primera_inscripcion_de_ac", DateType(), nullable=True),
        StructField("tipo_termino_de_giro", StringType(), nullable=True),
        StructField("tipo_de_contribuyente", StringType(), nullable=False),
        StructField("subtipo_de_contribuyente", StringType(), nullable=False),
        StructField("tramo_capital_propio_positivo", LongType(), nullable=True),
        StructField("tramo_capital_propio_negativo", LongType(), nullable=True),
        StructField("rubro_economico", StringType(), nullable=True),
        StructField("subrubro_economico", StringType(), nullable=True),
        StructField("actividad_economica", StringType(), nullable=True),
        StructField("region", StringType(), nullable=False),
        StructField("provincia", StringType(), nullable=False),
        StructField("comuna", StringType(), nullable=False),
        StructField("r_presunta", StringType(), nullable=True),
        StructField("otros_regimenes", StringType(), nullable=True),
    ]
)
