"""Enriquecimiento analítico de la deuda bruta sin alterar la granularidad mensual.

Granularidad de salida: una fila por mes (CICLO + MES), igual que la entrada.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}

OUTPUT_COLUMNS = [
    "PERIODO", "CICLO", "MES", "MES_NUM", "MONTO",
    "MONTO_PERIODO_ANTERIOR", "VARIACION_MONTO", "VARIACION_PCT",
    "MONTO_ANIO_ANTERIOR", "VARIACION_ANUAL_PCT",
    "PROMEDIO_MOVIL_3_MONTO",
]


def month_number(df):
    """Número de mes (1-12) a partir del nombre en español; nulo si no se reconoce."""
    mapping = F.create_map(*[x for k, v in MESES.items() for x in (F.lit(k), F.lit(v))])
    return mapping[F.lower(F.trim(F.col("MES")))].cast("int")


def with_periodo(df):
    """Agrega MES_NUM y PERIODO (primer día del mes, tipo date) a partir de CICLO y MES."""
    df = df.withColumn("MES_NUM", month_number(df))
    return df.withColumn(
        "PERIODO",
        F.when(
            F.col("CICLO").isNotNull() & F.col("MES_NUM").isNotNull(),
            F.to_date(
                F.concat_ws(
                    "-",
                    F.col("CICLO").cast("string"),
                    F.lpad(F.col("MES_NUM").cast("string"), 2, "0"),
                    F.lit("01"),
                ),
                "yyyy-MM-dd",
            ),
        ),
    )


def transform(df):
    print("[TRANSFORM] Construyendo PERIODO y comparando periodos mensuales")
    # Una sola serie mensual: se conserva cada fila, no se realiza groupBy.
    # Sin partitionBy la ventana usa una sola partición; es aceptable con ~186 filas.
    ordered = Window.orderBy("PERIODO")
    rolling = ordered.rowsBetween(-2, 0)
    prev = F.col("MONTO_PERIODO_ANTERIOR")
    prev_year = F.col("MONTO_ANIO_ANTERIOR")

    return (
        with_periodo(df)
        # Variación mensual (lag 1)
        .withColumn("MONTO_PERIODO_ANTERIOR", F.lag("MONTO", 1).over(ordered))
        .withColumn("VARIACION_MONTO", F.col("MONTO") - prev)
        .withColumn("VARIACION_PCT", F.round((F.col("MONTO") - prev) / prev * 100, 4))
        # Variación anual (mismo mes del año anterior, lag 12)
        .withColumn("MONTO_ANIO_ANTERIOR", F.lag("MONTO", 12).over(ordered))
        .withColumn(
            "VARIACION_ANUAL_PCT",
            F.round((F.col("MONTO") - prev_year) / prev_year * 100, 4),
        )
        # Promedio móvil de 3 meses (ventana parcial en los primeros dos meses)
        .withColumn("PROMEDIO_MOVIL_3_MONTO", F.round(F.avg("MONTO").over(rolling), 2))
        .select(*OUTPUT_COLUMNS)
        .orderBy("PERIODO")
    )
