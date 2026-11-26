# PyEconGDL

Repositorio colaborativo para un pipeline de ingeniería de datos en PySpark. Integra fuentes económicas de México para procesar, transformar y analizar información macroeconómica bajo un flujo de trabajo colaborativo en GitHub.

**Nombre del proyecto:** Mecanismo de transmisión de la política monetaria y su impacto en las finanzas públicas de México.  

## Integrantes y roles

Para garantizar el cumplimiento de las buenas prácticas de ingeniería de datos y documentación, el equipo se organiza bajo la siguiente estructura de funciones:  

**Developer:** MAURICIO GAMIZ SILVA  
**Technical reviewer:** MARIA ELBA BECERRA JIMENEZ  
**Documentation lead:** CESAR MICHAEL DURAN GUTIERREZ

## Presentación del proyecto

### Problema y relevancia

En la economía mexicana, las decisiones de política monetaria que toma el Banco de México (como los movimientos en la tasa de interés de referencia) desempeñan un papel fundamental para el control de la inflación y el rumbo del crecimiento del país. Sin embargo, medir el impacto real de estas decisiones es un reto complejo debido a que sus efectos no son inmediatos, sino que se transmiten a la economía con **rezagos temporales e indirectos**.

Adicionalmente, estos ciclos macroeconómicos alteran directamente las finanzas públicas del país: por ejemplo, afectan la cantidad de impuestos que el Gobierno Federal (SHCP) puede recaudar y al mismo tiempo modifican su capacidad de gasto (gasto público).

La problemática radica en que analizar este fenómeno requiere procesar, limpiar y alinear un gran volumen de series de tiempo e indicadores macroeconómicos provenientes de múltiples fuentes oficiales durante un periodo extenso (2011-2026). La relevancia de este proyecto consiste en **diseñar un pipeline de datos robusto y distribuido** que permita unificar esta información dispersa. Con esto, se busca evaluar cuantitativamente cómo y cuándo se transmiten los efectos de la tasa de interés hacia la inflación (INPC), la actividad económica (IGAE) y el presupuesto público, aportando una base de datos confiable para la toma de decisiones financieras y analíticas.

### Pregunta de investigación

¿En qué medida las decisiones de política monetaria en México (reflejadas en la tasa de interés de referencia) impactan la estabilidad de precios (INPC) y el dinamismo de la actividad económica (IGAE), y de qué manera este ciclo macroeconómico determina la recaudación de ingresos tributarios y la capacidad de gasto del Gobierno Federal (SHCP) durante el periodo 2011-2026?

### Objetivo

Diseñar e implementar un pipeline de datos distribuido en PySpark que integre, limpie y homologue series de tiempo macroeconómicas (2011-2026) para evaluar cuantitativamente el mecanismo de transmisión de la política monetaria en México, determinando los rezagos temporales y los efectos de los cambios en las tasas de interés sobre la inflación (INPC), el crecimiento económico (IGAE) y las finanzas públicas (ingresos y gastos de la SHCP).

### Fuentes de datos

* **Banco de México (Banxico):** - Tasa de Interés Interbancaria de Equilibrio.
* **Instituto Nacional de Estadística y Geografía (INEGI):** - Índice Nacional de Precios al Consumidor (INPC) y el Indicador Global de la Actividad Económica (IGAE).
* **Secretaría de Hacienda y Crédito Público (SHCP):** - Ingreso, gasto, financiamiento y resultados y deuda pública.

*Nota: El detalle técnico de la estructura, periodicidad, formatos y el diccionario de estas variables se encuentra documentado en el apartado correspondiente del repositorio.*

### Resultados esperados

Al concluir el desarrollo e implementación de este proyecto integrador, se contempla la obtención de los siguientes productos entregables, divididos en dos vertientes principales:

#### 1. Entregables técnicos y de ingeniería de datos

* **Pipeline de datos distribuido en PySpark:** Flujo automatizado, robusto y optimizado para la ingesta, limpieza, homologación y alineación temporal de las series macroeconómicas (2011-2026), estructurado bajo el esquema de capas de datos (`raw` y `processed`).
* **Dataset unificado de series de tiempo:** Repositorio centralizado de datos limpios, sin valores atípicos ni desajustes de periodicidad, con esquemas estrictamente definidos y preparado para consultas analíticas de alto rendimiento.
* **Repositorio de código con buenas prácticas:** Entorno de desarrollo documentado, con control de versiones transparente, historial de cambios mediante “pull requests” semanales y guías de optimización técnica para el procesamiento a gran escala.

#### 2. Entregables analíticos y de finanzas públicas

* **Evaluación de cambios en las finanzas públicas:** Análisis que determine la sensibilidad de los ingresos tributarios y la capacidad de gasto de la SHCP frente a las fluctuaciones del ciclo macroeconómico inducido por la política monetaria.
* **Evidencias gráficas y reportes estructurados:** Serie de reportes e indicadores procesados que respondan de manera concluyente a la pregunta de investigación, sirviendo como base empírica confiable para la toma de decisiones financieras en México.

### Arquitectura y etapas del procesamiento

El pipeline de datos del proyecto está diseñado bajo una arquitectura distribuida en **PySpark**, estructurada en cuatro secciones funcionales que transforman los registros crudos en conocimiento analítico a través de las siguientes etapas:

* **Fuentes:** Extracción de las series históricas (2011-2026) desde los portales oficiales de **Banxico** (tasa de interés), **INEGI** (INPC e IGAE) y la **SHCP** (deuda pública e ingresos y gastos públicos).

* **1. Ingesta (sección de datos transaccionales):** Carga distribuida hacia el entorno de Spark para la lectura inicial y estandarización de los formatos de entrada.  
* **2. Validación y limpieza (sección de procesamiento):** Aseguramiento de la calidad de los datos mediante el tratamiento de valores nulos y la homologación de fechas (`datetime`) para alinear las distintas periodicidades.  
* **3. Transformación (sección de procesamiento):** Aplicación de ingeniería de características para calcular variaciones porcentuales, tasas y rezagos temporales (funciones `lag()`).  
* **4. Integración (sección de procesamiento):** Modelado y cruce de las variables limpias para consolidar un **dataset maestro** unificado.
* **5. Almacenamiento (sección de datos analíticos):** Persistencia optimizada de la información procesada en un entorno tipo *data lakehouse* utilizando formatos columnares de alto rendimiento (**parquet / delta lake**).  
* **6. Análisis (sección de visualización y ciencia de datos):** Consumo del dataset maestro mediante *Jupyter Notebooks* (PySpark y Plotly) para ejecutar el estudio cuantitativo de impacto y rezagos.  

*Nota: El diagrama completo y las especificaciones técnicas de cada componente pueden consultarse en la sección de [Arquitectura del sistema](docs/arquitectura2.0.jpeg).*

### Estado del desarrollo

Actualmente el repositorio ha sido estructurado de forma que, conforme a buenas prácticas, cuenta con la documentación inicial que permite preparar la fase de implementación de los pipelines de datos.

## Documentación del proyecto

A continuación se presentan los accesos directos a los componentes técnicos detallados del proyecto integrador:

*[Arquitectura del sistema](docs/arquitectura2.0.jpeg)  
*[Fuentes de datos detalladas](docs/fuentes.md)  
*[Guía práctica de optimización](docs/guia_optimizacion.md)  
*[Roles de trabajo del equipo](docs/roles.md)  

## Avance del Pipeline Modular — Fuente piloto: Deuda Pública (actividad 11)

Como fase piloto del proyecto, se implementó y validó el flujo modular de principio a fin en *PySpark* utilizando la serie histórica de *Deuda Bruta* (SHCP). Lo anterior para verificar la arquitectura antes de incorporar el resto de las variables macroeconómicas.

El siguiente acceso directo contiene la descripción detallada de este avance:
*[Avance de pipeline modular](docs/avancepipelineact11.md)

## Sección de consumo analítico y resultados finales (actividad 12)

El consumo analítico del proyecto se desarrolla en el notebook **[`notebooks/reporte_de_proyecto.ipynb`](notebooks/reporte_de_proyecto.ipynb)** mediante **PySpark (Spark SQL)** directamente sobre el dataset maestro consolidado.

### Dataset final consumido

* **Archivo:** [`data/processed/macro_finanzas_consolidado.parquet`](data/processed/macro_finanzas_consolidado.parquet)
* **Cobertura y granularidad:** 186 observaciones mensuales (`2011-01` a `2026-06`) a nivel nacional; 174 meses con variaciones anuales calculables (`2012-01` a `2026-06`).
* **Variables principales:** `CICLO`, `TIIE` (Banxico), `VALOR_INPC` y `MONTO_IGAE_TOTAL` (INEGI), `DEUDA`, `INGRESO PUBLICO` y `GASTO PUBLICO` (SHCP).

---

### Preguntas específicas, hallazgos y evidencias

#### 1. Política monetaria y deuda pública (`TIIE` vs. `DEUDA`)

* **Pregunta:** ¿En qué medida los ciclos de endurecimiento monetario (incrementos en la TIIE) aceleran el crecimiento anual de la Deuda Pública de México entre 2012 y 2026, y con cuántos meses de rezago se refleja este encarecimiento?
* **Hallazgos principales:**
  * El crecimiento anual promedio de la deuda es de **10.47%**, siendo mayor bajo postura monetaria acomodaticia (**12.39%**, con `TIIE < 6.35%`) que bajo postura restrictiva (**8.60%**), lo que refleja que las expansiones de deuda responden principalmente a necesidades fiscales y choques presupuestarios.
  * La correlación contemporánea (`k = 0`) es de **-0.3460** y se atenúa progresivamente hasta **-0.1159 con un rezago de 12 meses**.
* **Evidencias gráficas:**
  * [Evolución de la TIIE y crecimiento anual de la Deuda Pública](docs/pregunta1/tiie_deuda_crecimiento.png)
  * [Correlación con rezagos: TIIE → Deuda Pública](docs/pregunta1/tiie_deuda_rezagos.png)

#### 2. Política monetaria e inflación (`TIIE` vs. `INPC`)

* **Pregunta:** ¿Ha sido la tasa de interés real positiva en México entre 2012 y 2026, y reacciona la TIIE a la inflación medida con el INPC?
* **Hallazgos principales:**
  * La tasa real ex post promedio fue de **1.91 puntos porcentuales**, registrando **41 meses** (**23.6%**) con tasa real negativa (concentrados en 2013–2014 y 2021–2022, con un mínimo de **-2.46 pp** en `2021-11`).
  * La correlación entre inflación y TIIE pasa de **0.38** contemporánea a **0.72 con 12 meses de rezago**, confirmando una política monetaria reactiva.
* **Evidencias gráficas:**
  * [TIIE frente a inflación anual y tasa real](docs/pregunta2/tiie_inpc_tasa_real.png)
  * [Correlación con rezagos: Inflación → TIIE](docs/pregunta2/tiie_inflacion_rezagos.png)

#### 3. Política monetaria y actividad económica (`TIIE` vs. `IGAE`)

* **Pregunta:** ¿En qué medida los incrementos en la TIIE frenan el dinamismo de la actividad económica (IGAE) entre 2012 y 2026, y con cuántos meses de rezago se manifiesta este enfriamiento?
* **Hallazgos principales:**
  * El crecimiento anual promedio del IGAE es de **1.54%** (**25.9%** de meses en contracción). Bajo postura restrictiva (`TIIE >= 6.35%`) el crecimiento promedio baja a **1.22%** frente a **1.87%** en periodos acomodaticios (brecha de **0.65 pp**).
  * La correlación contemporánea es prácticamente nula (**-0.0415**), alcanzando su máximo impacto negativo de **-0.2247 a los 9 meses de rezago**.
* **Evidencias gráficas:**
  * [Evolución de la TIIE y crecimiento anual del IGAE](docs/pregunta3/tiie_igae_actividad.png)
  * [Correlación con rezagos: TIIE → IGAE](docs/pregunta3/tiie_igae_rezagos.png)

---

### Limitaciones del análisis

* **Choques exógenos:** La pandemia de 2020 y el rebote estadístico de 2021 introducen valores extremos en las series de actividad económica y deuda.
* **Correlación vs. causalidad:** Las correlaciones bivariadas describen comovimientos y rezagos temporales, pero no aíslan factores externos (ciclo industrial de EE. UU., tipo de cambio o política fiscal).
* **Ventana efectiva:** el cálculo de variaciones interanuales (`LAG(..., 12)`) descarta los primeros 12 meses (`2011-01` a `2011-12`), dejando 174 meses efectivos.

---

### Instrucciones de ejecución y reproducibilidad

1. **Clonar el repositorio e instalar dependencias:**

   ```bash

   git clone https://github.com/mauriciozimag/PyEconGDL
   cd PyEconGDL
   pip install -r requirements.txt pandas matplotlib
   ```

2. **Verificar o generar el dataset consolidado:**
   * Asegurar la existencia de `data/processed/macro_finanzas_consolidado.parquet` (generado por `src/pipeline.py`).
3. **Ejecutar el consumo analítico:**
   * Abrir [`notebooks/reporte_de_proyecto.ipynb`](notebooks/reporte_de_proyecto.ipynb) con un kernel que tenga `pyspark`, `pandas` y `matplotlib`.
   * Ejecutar todas las celdas en orden: las consultas en Spark SQL validarán la integridad del Parquet y regenerarán las gráficas en `docs/pregunta1/`, `docs/pregunta2/` y `docs/pregunta3/`.

## Consulta de resultados

[Reporte de análisis](reports/reporte.pdf)
