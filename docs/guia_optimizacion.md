# Guía Práctica de Buenas Prácticas y Optimización en Pipelines de Datos y Machine Learning

**Actividad 10 - Punto 3: Documento Técnico y Guía de Referencia**

## 1. Introducción

Un pipeline de procesamiento de datos y aprendizaje automático (*Data / ML Pipeline*) es una arquitectura orientada a automatizar el flujo de trabajo desde la ingesta de datos crudos hasta la inferencia o entrega de resultados. Garantizar la robustez, escalabilidad, mantenibilidad y modularidad del pipeline es fundamental para evitar problemas comunes como la fuga de datos (*data leakage*), cuellos de botella en el rendimiento y fallas en entornos de producción.

Esta guía establece los estándares de ingeniería de software, arquitectura de datos y optimización de cómputo aplicables a la construcción de pipelines profesionales.

## 2. Arquitectura y Modularidad del Pipeline

### 2.1. Descomposición en Etapas Claras (DAGs)

Un pipeline eficiente debe diseñarse como un Grafo Acíclico Dirigido (DAG, por sus siglas en inglés), dividiendo el flujo en etapas independientes, atómicas y reutilizables:

1. **Ingesta y Extracción:** Conexión y lectura de fuentes de datos (Bases de datos, APIs, Data Lakes).
2. **Validación y Limpieza:** Control de calidad de esquemas, tipos de datos y manejo de valores nulos.
3. **Ingeniería de Características (*Feature Engineering*):** Transformaciones, codificación y escalado.
4. **Entrenamiento / Inferencia:** Modelado estadístico o aplicación de algoritmos de ML.
5. **Almacenamiento y Notificación:** Exportación de resultados procesados y métricas a capas persistentes.

### 2.2. Encapsulamiento y Principio de Única Responsabilidad (SRP)

* **Abstracción mediante Clases y Funciones:** Cada componente del pipeline (ej. `DataCleaner`, `FeatureScaler`, `ModelTrainer`) debe estar aislado en módulos orientados a objetos o funciones puras.
* **Uso de Transformers Estándar:** Implementar clases compatibles con las APIs estándar (como `BaseEstimator` y `TransformerMixin` en Scikit-Learn) para integrar transformaciones directamente en el objeto final de pipeline.

### 2.3. Prevención de Fuga de Datos (*Data Leakage*)

* **Aislamiento de Ajuste y Transformación:** Las transformaciones (imputación de medias, escalado MinMax/Standard, codificación de variables categóricas) deben calcular estadísticos exclusivamente con los datos de entrenamiento (`fit`), aplicándolos posteriormente al conjunto de validación/prueba (`transform`).
* **Integración del Modelo en el Pipeline:** El modelo de machine learning debe acoplarse dentro de la misma estructura del pipeline para asegurar que los splits en validación cruzada no compartan información del conjunto completo.

## 3. Manejo de Estado, Reproducibilidad y Configuración

### 3.1. Externalización de Configuración

* **Archivos de Configuración Desacoplados:** Evite el uso de valores arbitrarios (*hardcoded*) dentro del código del pipeline. Utilice archivos `.yaml`, `.json` o `.toml` para definir hiperparámetros, rutas de acceso, variables de entorno y credenciales.
* **Parámetros Dinámicos:** Permita la inyección de argumentos por línea de comandos o variables de entorno para facilitar la ejecución programada y automatizada (CI/CD, Cron jobs).

### 3.2. Reproducibilidad Estricta

* **Control de Semillas Aleatorias:** Centralice y fije la semilla de aleatoriedad (`seed` / `random_state`) a lo largo de todos los componentes del pipeline (splits de datos, inicialización de modelos, muestreos).
* **Versionado de Artefactos:** Guarde el pipeline entrenado y sus transformaciones asociadas en un único archivo serializado (ej. `.joblib`, `.pkl`, `.onnx`), incluyendo la versión exacta de las librerías empleadas (`requirements.txt`).

## 4. Optimización de Rendimiento y Escalabilidad

### 4.1. Procesamiento Vectorizado y Paralelización

* **Operaciones Vectorizadas:** Elimine los bucles iterativos fila por fila. Utilice operaciones vectorizadas subyacentes en motores de procesamiento como Pandas, NumPy, PySpark o DuckDB.
* **Cómputo Multinúcleo y Distribuido:** Ajuste el parámetro `n_jobs` o implemente procesamiento paralelo (`multiprocessing` / Dask) en etapas intensivas como la ingeniería de características o la búsqueda de hiperparámetros (*GridSearch* / *RandomSearch*).

### 4.2. Eficiencia de Memoria y Carga Eficiente de Datos

* **Carga por Lotes (*Batching / Streaming*):** Para datasets que superan la memoria RAM disponible, procese la información en bloques o streaming utilizando herramientas orientadas a grandes volúmenes de datos.
* **Optimización de Tipos de Datos:** Reduzca la huella en memoria convirtiendo datos numéricos a la precisión adecuada (`float32`, `int16`) y columnas de texto repetitivo a variables de tipo `category`.

## 5. Monitoreo, Logging y Manejo de Errores

### 5.1. Rastreabilidad y Sistema de Logging

* **Implementación de `logging` Profesional:** Sustituya las sentencias `print()` por el módulo `logging` estructurado. Registre niveles de severidad (`INFO`, `WARNING`, `ERROR`, `CRITICAL`) con timestamps para auditar la ejecución.
* **Métricas de Rendimiento:** Registre los tiempos de ejecución por cada bloque del pipeline y el volumen de filas procesadas para identificar degradaciones de rendimiento en ejecuciones periódicas.

### 5.2. Puntos de Control (*Checkpointing*) e Idempotencia

* **Principio de Idempotencia:** El pipeline debe ser idempotente; su ejecución repetida con los mismos datos de entrada debe producir exactamente el mismo resultado final sin duplicar registros ni corromper estados.
* **Checkpoints de Almacenamiento Intermedio:** En pipelines extensos, guarde estados intermedios en formato Parquet o Feather en la salida de cada etapa clave para permitir la reanudación del proceso ante posibles fallos de ejecución.

## 6. Lista de Verificación (Check-List) para Producción

| Etapa | Criterio de Verificación | Estado |
| ----- | ----- | ----- |
| **Modularidad** | ¿El pipeline está descompuesto en componentes y módulos independientes? | \[ \] |
| **Fuga de Datos** | ¿Todas las transformaciones hacen `fit` exclusivamente en el set de entrenar? | \[ \] |
| **Configuración** | ¿Se utilizan archivos `.yaml`/`.json` en lugar de variables *hardcoded*? | \[ \] |
| **Idempotencia** | ¿El pipeline puede ejecutarse varias veces sin generar inconsistencias? | \[ \] |
| **Reproducibilidad** | ¿Están fijadas las semillas de aleatoriedad y se guarda el pipeline serializado? | \[ \] |
| **Logging** | ¿Existe un sistema de logs que capture errores, métricas y tiempos de ejecución? | \[ \] |

------

**Conceptos a tener en cuenta**

Granularidad: La granularidad de los datos se define como el nivel de detalle o especificidad que posee un dato o una estructura de datos. En este concepto se evalúa tanto la complejidad del tipo de dato como la forma o el tamaño en el que se dividen sus campos. Cuanto más subdividido y específico es un campo de información (por ejemplo, tener subdivisiones separadas para nombre, primer apellido y segundo apellido en lugar de un solo campo general), mayor es su nivel de granularidad.

Tipos de datos: El conocimiento de los tipos de datos es importante debido a que son fundamentales porque ayudan a los programas de computadora a procesar información de manera eficiente y precisa. Los tipos de datos permiten que los programas comprendan y trabajen con diferentes tipos de información, como números, texto, fechas y horas, y estructuras de datos complejas 

Particionamiento, procesamiento secuencial y procesamiento paralelo: La partición de datos consiste en dividir grandes conjuntos de información en fragmentos más pequeños y manejables, denominados particiones, los cuales se pueden almacenar, consultar y gestionar como si fueran tablas individuales. Para procesar estos datos, existen diferentes metodologías de ejecución. Por un lado, el procesamiento secuencial opera de manera lineal, exigiendo que cada tarea se complete de forma estricta antes de avanzar a la siguiente. En contraste, el procesamiento paralelo aprovecha múltiples unidades de cómputo para ejecutar varias instrucciones simultáneamente; de este modo, ofrece un rendimiento significativamente más rápido y eficiente en comparación con las limitaciones del esquema secuencial tradicional.

Particionar no es lo mismo que paralelizar. Particionar organiza los datos; paralelizar decide cuántas de esas particiones se procesan al mismo tiempo. El primero es un requisito práctico del segundo, pero no lo implica automáticamente.


Diagnosticar antes de paralelizar: Identificar primero si el sistema pierde tiempo esperando o procesando; esto determinará si se requiere hilos o procesos.

Medir el punto óptimo con datos reales: No dar por hecho que agregar más workers acelera el proceso. El número exacto solo se descubre con métricas sobre el volumen de producción.

Definir particiones equilibradas: Los bloques demasiado pequeños generan más costo logístico que trabajo útil, mientras que las particiones gigantes acaban con la RAM y limitan el trabajo en paralelo.

Monitorear la CPU, no solo el cronómetro: Un tiempo de ejecución largo con la CPU ociosa indica que se está perdiendo recursos en la coordinación (lo que exige empaquetar mejor las tareas), mientras que una CPU al 100% sí requiere más procesadores.

Escalar horizontalmente solo por necesidad: La migración se justifica únicamente cuando la capacidad máxima de un solo servidor no logra cumplir con los tiempos de respuesta requeridos de forma económica.

profiling ≠ limpieza. Detectar los duplicados en esta etapa no implicó eliminarlos de inmediato, primero fue necesario comprender si afectaban el caso de uso concreto.

