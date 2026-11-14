"""Validaciones de calidad de la deuda bruta mensual y de su enriquecimiento."""
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

from src.transformations.enrich import MESES, OUTPUT_COLUMNS, with_periodo

REQUIRED = ("CICLO", "MES", "MONTO")


def has_rows(df):
    return df.limit(1).count() > 0


def validate_input(df):
    print("[VALIDATE] Revisando entrada")
    missing = sorted(set(REQUIRED) - set(df.columns))
    if missing:
        raise ValueError(f"Faltan columnas obligatorias: {missing}")
    if not has_rows(df):
        raise ValueError("El dataset está vacío")

    # Tipos: evita sumas/promedios sobre textos.
    for name in ("CICLO", "MONTO"):
        if not isinstance(df.schema[name].dataType, NumericType):
            raise ValueError(f"La columna {name} debe ser numérica")

    # Nulos en columnas obligatorias.
    if df.filter(F.col("CICLO").isNull() | F.col("MES").isNull() | F.col("MONTO").isNull()).limit(1).count():
        raise ValueError("Hay nulos en CICLO, MES o MONTO")

    # Nombres de mes reconocibles (se comparan en minúsculas, sin espacios).
    invalid_months = (
        df.select(F.lower(F.trim(F.col("MES"))).alias("m"))
        .filter(~F.col("m").isin(list(MESES)))
        .distinct()
        .collect()
    )
    if invalid_months:
        raise ValueError(f"MES con valores no reconocidos: {[r['m'] for r in invalid_months]}")

    # La deuda bruta debe ser positiva.
    if df.filter(F.col("MONTO") <= 0).limit(1).count():
        raise ValueError("Hay montos de deuda menores o iguales a cero")

    prepared = with_periodo(df)
    if prepared.filter(F.col("PERIODO").isNull()).limit(1).count():
        raise ValueError("No se pudo construir PERIODO a partir de CICLO y MES")

    # Un solo registro por mes.
    if prepared.groupBy("PERIODO").count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("Existen meses duplicados (CICLO, MES)")

    # Serie continua: sin meses faltantes entre el primero y el último.
    total, first, last = prepared.agg(
        F.count("*"), F.min("PERIODO"), F.max("PERIODO")
    ).first()
    expected = (last.year - first.year) * 12 + (last.month - first.month) + 1
    if total != expected:
        raise ValueError(
            f"La serie mensual tiene huecos: {total} filas para {expected} meses "
            f"entre {first} y {last}"
        )

    # El conteo histórico era 186; aquí se mide, no se impone.
    return total


def validate_output(df, expected_rows):
    print("[VALIDATE] Revisando salida")
    if list(df.columns) != OUTPUT_COLUMNS:
        raise ValueError(f"Columnas inesperadas en la salida: {df.columns}")

    actual = df.count()
    if actual != expected_rows:
        raise ValueError(f"Cambió el número de filas: {expected_rows} → {actual}")
    if df.groupBy("PERIODO").count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("La salida contiene periodos duplicados")

    # Columnas que nunca deben ser nulas.
    never_null = ("PERIODO", "CICLO", "MES", "MES_NUM", "MONTO", "PROMEDIO_MOVIL_3_MONTO")
    cond = None
    for name in never_null:
        c = F.col(name).isNull()
        cond = c if cond is None else (cond | c)
    if df.filter(cond).limit(1).count():
        raise ValueError("El enriquecimiento produjo nulos inesperados")

    # Nulos esperados por el lag: 1 en la variación mensual y 12 en la anual.
    # NO son errores de calidad, pero su cantidad debe ser exacta.
    nulls = df.agg(
        F.sum(F.col("VARIACION_PCT").isNull().cast("int")).alias("mensual"),
        F.sum(F.col("VARIACION_ANUAL_PCT").isNull().cast("int")).alias("anual"),
    ).first()
    if nulls["mensual"] != min(1, actual):
        raise ValueError(f"Nulos inesperados en VARIACION_PCT: {nulls['mensual']}")
    if nulls["anual"] != min(12, actual):
        raise ValueError(f"Nulos inesperados en VARIACION_ANUAL_PCT: {nulls['anual']}")
    return actual
