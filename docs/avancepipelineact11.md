## Avance del Pipeline Modular — Fuente piloto: Deuda Pública (actividad 11)

Como fase piloto del proyecto, se implementó y validó el flujo modular de principio a fin en **PySpark** utilizando la serie histórica de **Deuda Bruta** (SHCP). Lo anterior para verificar la arquitectura antes de incorporar el resto de las variables macroeconómicas.  

### 1. Fuente y conteos de entrada
* **Archivo fuente:** `data/raw/archivo_deuda_bruta.parquet` (proveniente de los registros históricos de deuda pública de la SHCP).
* **Registros de entrada validados:** `186` registros mensuales.
* **Cobertura temporal:** De `2011-01-01` a `2026-06-01`.

### 2. Decisiones de limpieza y validación (`validate_input` y `validate_output`)
* **Control de rutas:** Se configuraron rutas relativas (`data/raw/...` y `data/processed/...`) para garantizar la portabilidad entre distintos sistemas operativos (Windows, macOS y Linux).
* **Reglas de calidad:** 
  * Validación previa (`validate_input`) para confirmar la existencia de registros y el esquema esperado antes de procesar.
  * Validación posterior (`validate_output`) para asegurar que la transformación conserve íntegra la granularidad sin duplicar ni perder filas (`186` registros de entrada = `186` registros de salida).
  * Relectura automática (`verify_load`) tras la escritura en disco para comprobar que el Parquet generado es legible y mantiene sus columnas originales.

### 3. Transformaciones aplicadas (`src/transformations/enrich.py`)
* **Homologación temporal:** Construcción de la columna estandarizada `PERIODO` a partir de las variables `CICLO` y `MES`.
* **Cálculos analíticos:** Comparación intermensual sobre la serie de deuda bruta para preparar el cruce con indicadores monetarios y macroeconómicos.

### 4. Resultados y evidencia de ejecución
* **Ubicación de la salida analítica:** `data/processed/deuda_bruta_enriquecida.parquet`.
* **Reporte de auditoría:** `reports/ultima_ejecucion.json`.
* **Granularidad definida:** Mensual (una fila única por `CICLO + MES`).
* **Estado de la última ejecución:** `"OK"` (`186` registros procesados, `0` errores).

### 5. Indicaciones para reproducir la ejecución
1. Acceder a la carpeta raíz del repositorio (`PyEconGDL/`):
   ```bash
   cd PyEconGDL
   ```
2. *(únicamente para Windows)* Confirma que se tienen configurados `winutils.exe` y `hadoop.dll` en `C:\hadoop\bin`, así como las variables de entorno `HADOOP_HOME` y `JAVA_HOME`.
3. Ejecutar el pipeline principal:
   ```bash
   python pipeline.py
   ```
4. Verificar la generación de los archivos `part-*.parquet` y `_SUCCESS` en `data/processed/deuda_bruta_enriquecida.parquet`, y confirmar `"estado": "OK"` en `reports/ultima_ejecucion.json`.
