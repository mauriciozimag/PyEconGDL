"""Lectura del dataset de deuda bruta (Parquet mensual ya limpiado)."""
from pathlib import Path

from pyspark.sql import functions as F


def extract(spark, input_path):
    # Fallar con un mensaje claro si el archivo aún no se ha copiado.
    if not Path(input_path).exists():
        raise FileNotFoundError(
            f"No existe la entrada: {input_path}. "
            "Copia aquí archivo_deuda_bruta.parquet (data/raw/)."
        )
    print(f"[EXTRACT] Leyendo {input_path}")
    df = spark.read.parquet(input_path)
    # Parquet ya trae sus tipos, pero se fijan los esperados por si pandas
    # los guardó distinto (p. ej. CICLO como bigint).
    return df.select(
        F.col("CICLO").cast("int").alias("CICLO"),
        F.col("MES").cast("string").alias("MES"),
        F.col("MONTO").cast("long").alias("MONTO"),
    )