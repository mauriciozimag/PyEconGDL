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
Para el desarrollo de este análisis macroeconómico y financiero, el proyecto se abastece de datos oficiales, públicos y centralizados provenientes de las organismos que a continuación se enlistan:

* **Banco de México (Banxico):** Provee el histórico y las proyecciones de las variables de política monetaria, fundamentalmente la **Tasa de Interés Interbancaria de Equilibrio**.
* **Instituto Nacional de Estadística y Geografía (INEGI):** Aporta los indicadores económicos clave para medir el impacto de las decisiones monetarias en la economía real, específicamente el **Índice Nacional de Precios al Consumidor (INPC)** para el análisis de la inflación, y el **Indicador Global de la Actividad Económica (IGAE)** para medir el dinamismo y crecimiento del país.
* **Secretaría de Hacienda y Crédito Público (SHCP):** Suministra los registros históricos del sector público presupuestario, permitiendo extraer las series de tiempo relacionadas con la **recaudación de ingresos tributarios** y el **ejercicio del gasto público** del Gobierno Federal.

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

* **Fuentes:** Extracción de las series históricas (2011-2026) desde los portales oficiales de **Banxico** (tasa de interés), **INEGI** (INPC e IGAE) y la **SHCP** (ingresos y gastos públicos).

* **1. Ingesta (sección de datos transaccionales):** Carga distribuida hacia el entorno de Spark para la lectura inicial y estandarización de los formatos de entrada.  
* **2. Validación y limpieza (sección de procesamiento):** Aseguramiento de la calidad de los datos mediante el tratamiento de valores nulos y la homologación de fechas (`datetime`) para alinear las distintas periodicidades.  
* **3. Transformación (sección de procesamiento):** Aplicación de ingeniería de características para calcular variaciones porcentuales, tasas y rezagos temporales (funciones `lag()`).  
* **4. Integración (sección de procesamiento):** Modelado y cruce de las variables limpias para consolidar un **dataset maestro** unificado.
* **5. Almacenamiento (sección de datos analíticos):** Persistencia optimizada de la información procesada en un entorno tipo *data lakehouse* utilizando formatos columnares de alto rendimiento (**parquet / delta lake**).  
* **6. Análisis (sección de visualización y ciencia de datos):** Consumo del dataset maestro mediante *Jupyter Notebooks* (PySpark y Plotly) para ejecutar el estudio cuantitativo de impacto y rezagos.  

*Nota: El diagrama completo y las especificaciones técnicas de cada componente pueden consultarse en la sección de [Arquitectura del sistema](docs/arquitectura.jpg).*

### Estado del desarrollo
Actualmente el repositorio ha sido estructurado de forma que, conforme a buenas prácticas, cuenta con la documentación inicial que permite preparar la fase de implementación de los pipelines de datos.


## Documentación del proyecto
A continuación se presentan los accesos directos a los componentes técnicos detallados del proyecto integrador:

*[Arquitectura del sistema](docs/Arquitectura.jpg)  
*[Fuentes de datos detalladas](docs/fuentes.md)  
*[Guía práctica de optimización](docs/guia_optimizacion.md)  
*[Roles de trabajo del equipo](docs/roles.md)  

