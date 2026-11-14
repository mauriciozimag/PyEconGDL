"""Punto de entrada: ETL mensual de deuda bruta (Actividad 11)."""
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.ingestion.extract import extract
from src.quality.validate import validate_input, validate_output
from src.transformations.enrich import transform


INPUT_PATH = "data/raw/archivo_deuda_bruta.parquet"
OUTPUT_PATH = "data/processed/deuda_bruta_enriquecida.parquet"
REPORT_PATH = "reports/ultima_ejecucion.json"
GRANULARIDAD = "mensual (una fila por CICLO + MES)"


def save_report(status, input_rows=None, output_rows=None, error=None,
                output_path=None, periodo_min=None, periodo_max=None,
                report_path=REPORT_PATH):
    """Guarda evidencia mínima incluso si una etapa falla."""
    report = {
        "fecha_utc": datetime.now(timezone.utc).isoformat(),
        "estado": status,
        "registros_entrada": input_rows,
        "registros_salida": output_rows,
        "granularidad": GRANULARIDAD,
        "ruta_salida": output_path,
        "periodo_inicial": periodo_min,
        "periodo_final": periodo_max,
        "error": str(error) if error is not None else None,
    }
    destination = Path(report_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return report


def load(df, output_path):
    """Publica la salida; overwrite no es transaccional."""
    print("[LOAD] Guardando resultado Parquet")
    df.write.mode("overwrite").parquet(output_path)


def verify_load(spark, output_path, expected_rows, expected_columns):
    """Relee el Parquet publicado y comprueba conteo y esquema."""
    print("[LOAD] Verificando que la salida puede volver a leerse")
    saved = spark.read.parquet(output_path)
    if saved.count() != expected_rows:
        raise ValueError(f"El Parquet releído tiene {saved.count()} filas, se esperaban {expected_rows}")
    if saved.columns != expected_columns:
        raise ValueError(f"El Parquet releído tiene columnas distintas: {saved.columns}")
    return saved


def run_pipeline(spark, input_path=INPUT_PATH, output_path=OUTPUT_PATH, report_path=REPORT_PATH):
    """Coordina etapas y detiene la publicación si falla una validación."""
    if Path(input_path).resolve() == Path(output_path).resolve():
        raise ValueError("Entrada y salida deben tener rutas distintas")

    input_rows = None
    output_rows = None
    periodo_min = None
    periodo_max = None
    spark.sparkContext.setJobGroup("CLASE11_PIPELINE", "Pipeline mensual de deuda bruta")
    print("=== INICIO DEL PIPELINE ===")
    try:
        df = extract(spark, input_path)
        input_rows = validate_input(df)
        print(f"[VALIDATE] Entrada correcta: {input_rows} registros")

        result = transform(df)
        output_rows = validate_output(result, input_rows)
        print(f"[VALIDATE] Salida correcta: {output_rows} registros")

        bounds = result.agg(F.min("PERIODO"), F.max("PERIODO")).first()
        periodo_min, periodo_max = str(bounds[0]), str(bounds[1])

        load(result, output_path)
        verify_load(spark, output_path, output_rows, result.columns)
        save_report("OK", input_rows, output_rows, output_path=output_path,
                    periodo_min=periodo_min, periodo_max=periodo_max,
                    report_path=report_path)
        print(f"[OK] Resultado: {output_path}")
        print("=== FIN DEL PIPELINE ===")
        return result
    except Exception as exc:
        save_report("ERROR", input_rows, output_rows, error=exc,
                    output_path=output_path, periodo_min=periodo_min,
                    periodo_max=periodo_max, report_path=report_path)
        print(f"[ERROR] {type(exc).__name__}: {exc}")
        raise
    finally:
        spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
        spark.sparkContext.setLocalProperty("spark.job.description", None)


def main():
    # La configuración conserva el entorno local de los laboratorios previos.
    spark = (
        SparkSession.builder
        .appName("Clase11_Pipeline_Deuda_Bruta")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    try:
        run_pipeline(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
