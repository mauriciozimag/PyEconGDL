# Investigación de fuentes adicionales

## Propuesta 1

Tasa de Interés Interbancaria de Equilibrio (TIIE) a 1 día

Institución: Banco de México (Banxico).  
Enlace oficial: Sistema de Información Económica (SIE) - Banxico (https://www.banxico.org.mx/SieInternet/consultarDirectorioInternetAction.do?accion=consultarCuadro&idCuadro=CF101&sector=18&locale=es).  
Variables: Tasa de Fondeo Bancario / TIIE de fondeo a 1 día hábil (porcentaje anual).  
Periodicidad: Diaria (días hábiles bancarios).  
Cobertura temporal: Enero de 2011 a diciembre de 2026.  
Formato de acceso: Archivo plano .csv (descargable vía interfaz web del SIE).  
Relación con las fuentes base: Representa el costo real y efectivo del dinero en el mercado interbancario a corto plazo. Al interactuar con las fuentes base, permite medir cómo el encarecimiento diario del crédito se traslada con el tiempo al consumo/producción (IGAE), frena la demanda para mitigar la inflación (INPC) y, consecuentemente, reduce la base gravable de la cual la SHCP obtiene ingresos por IVA e ISR.  

## Propuesta 2

Tasa de Interés de Referencia (Tasa Objetivo)

Institución: Banco de México (Banxico).  
Enlace oficial: Sistema de Información Económica (SIE) - Banxico (https://www.banxico.org.mx/SieInternet/consultarDirectorioInternetAction.do?accion=consultarCuadro&idCuadro=CF101&sector=18&locale=es).  
Variables: Tasa de interés objetivo (porcentaje anual determinado por la Junta de Gobierno).  
Periodicidad: Por anuncio / eventual (cada que sesiona la Junta de Gobierno de Banxico y emite un comunicado de política monetaria).  
Cobertura temporal: Enero de 2011 a diciembre de 2026.  
Formato de acceso: Archivo plano .csv (descargable vía interfaz web del SIE).  
Relación con las fuentes base: Es el instrumento de política monetaria dictado por el banco central. Sirve como la variable exógena principal para analizar si los choques regulatorios o decretos de Banxico logran coordinar las expectativas de inflación (INPC) y qué costo tienen en el crecimiento de la economía (IGAE) y el presupuesto del sector público (SHCP).  

## Selección de la fuente

El equipo ha seleccionado la Tasa de Interés Interbancaria de Equilibrio (TIIE) de fondeo a 1 día como la fuente pública adicional definitiva para el proyecto.

## Justificación

*A diferencia de la Tasa Objetivo (que es un decreto político/institucional estático hasta el siguiente anuncio), la TIIE de fondeo es un cálculo estadístico real basado en transacciones financieras efectivas al mayoreo ocurridas diariamente en el mercado mexicano. Captura la volatilidad real del dinero día a día, ofreciendo una métrica mucho más precisa para correlacionar con la economía.  
*Como se trata de una serie de tiempo diaria, representa un volumen de datos sustancialmente mayor (aproximadamente 250 registros por año de días hábiles bancarios corporativos) que contrasta con la granularidad mensual del INPC, IGAE y SHCP. Esto permite justificar el uso de PySpark para realizar transformaciones (Window functions), calcular promedios móviles mensuales, y ejecutar JOINs temporales complejos sin perder rendimiento por shuffle.  
*Así mismo, la fuente es altamente viable ya que Banxico permite la descarga directa en formato .csv limpio a través del SIE. Al no requerir autenticaciones web complejas o APIs de pago, se mitiga el riesgo de caídas en el pipeline y se garantiza la consistencia del histórico para todo el espectro temporal solicitado (2011-2026).

---

# Fuentes de datos utilizadas en el desarrollo del proyecto

El análisis empírico se sustenta en información macroeconómica y financiera publicada por organismos públicos autónomos y dependencias federales de México. La selección de estas fuentes garantiza la trazabilidad, consistencia metodológica y rigor estadístico necesarios para medir los rezagos entre las decisiones monetarias, la evolución de precios y el desempeño de las finanzas públicas:

* **Banco de México (Banxico):** Provee el histórico y las proyecciones de las variables de política monetaria, fundamentalmente la **Tasa de Interés Interbancaria de Equilibrio**.  
[Tasa de Interés Interbancaria de Equilibrio](https://www.banxico.org.mx/SieInternet/consultarDirectorioInternetAction.do?accion=consultarCuadro&idCuadro=CF101&sector=18&locale=es)
  

* **Instituto Nacional de Estadística y Geografía (INEGI):** Aporta los indicadores económicos clave para medir el impacto de las decisiones monetarias en la economía real, específicamente el **Índice Nacional de Precios al Consumidor (INPC)** para el análisis de la inflación, y el **Indicador Global de la Actividad Económica (IGAE)** para medir el dinamismo y crecimiento del país.  
[Índice Nacional de Precios al Consumidor](https://www.inegi.org.mx/programas/inpc/2018a/#datos_abiertos)  
[Indicador Global de la Actividad Económica](https://www.inegi.org.mx/temas/igae/)  
  

* **Secretaría de Hacienda y Crédito Público (SHCP):** Suministra los registros históricos del sector público presupuestario, permitiendo extraer las series de tiempo relacionadas con la **recaudación de ingresos tributarios**, el **ejercicio del gasto público** y **deuda pública** del Gobierno Federal.  
[Ingreso, gasto, financiamiento y resultados](https://www.secciones.hacienda.gob.mx/work/models/estadisticas_oportunas/datos_abiertos_eopf/ingreso_gasto_finan.csv)  
[Deuda pública](https://www.secciones.hacienda.gob.mx/work/models/estadisticas_oportunas/datos_abiertos_eopf/deuda_publica.csv)  
  

El proyecto se abastece de cinco series oficiales de acceso público homologadas a granularidad mensual: la **Tasa de Interés Interbancaria de Equilibrio (TIIE)** del Banco de México, capturada originalmente en por ciento anual con frecuencia diaria (formato CSV); el **Índice Nacional de Precios al Consumidor (INPC)** y el **Indicador Global de la Actividad Económica (IGAE)** del INEGI, publicados como números índice (base 2018 = 100) con periodicidad mensual (formatos CSV y XLSX respectivamente); y los **registros de ingresos, gasto público** y **deuda pública** de la SHCP, expresados en miles de pesos con periodicidad mensual en formato CSV.