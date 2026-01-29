# DOMINIO DE PROGRAMACIÓN FUNCIONAL Y REACTIVA
# Análsis y Limpieza de Dataset Movies

Utilizando **Scala** junto con **Cats Effect** y **FS2**, este sistema orquesta un flujo de trabajo completo que incluye:
* **Ingesta de Datos:** Procesamiento eficiente de archivos CSV.
* **Saneamiento:** Limpieza y normalización de registros.
* **Análisis:** Generación de métricas estadísticas y procesamiento de texto.

## 1. columnasNumericas.scala - Reporte de Métricas Descriptivas

Este módulo ejecuta la Fase 2 del Análisis Exploratorio de Datos (EDA), proporcionando un resumen matemático completo de las variables numéricas del dataset. A diferencia de un análisis básico, este script implementa una arquitectura modular separando la lógica matemática pura del flujo de ejecución de entrada/salida.

### Características Principales

* **Procesamiento Funcional:** Utiliza FS2 para la ingesta de datos en streaming, lo que permite procesar archivos grandes sin saturar la memoria.
* **Decodificación Automática:** Mapeo directo de CSV a Case Classes mediante fs2-data-csv.
* **Estadística Robusta:** Calcula cinco dimensiones clave para entender la distribución real de los datos, detectando sesgos que el promedio simple no muestra:
    * Tendencia Central: Promedio (Media) y Mediana.
    * Rango: Valores Mínimos y Máximos.
    * Dispersión: Desviación Estándar.

### Variables Analizadas

El script procesa automáticamente las siguientes columnas financieras y de métricas:

* **Budget** (Presupuesto)
* **Revenue** (Ingresos)
* **Popularity** (Índice de popularidad)
* **Runtime** (Duración en minutos)
* **Vote Average** (Promedio de votos)
* **Vote Count** (Cantidad total de votos)

### Ejemplo de Salida

Al ejecutar el script, se genera un reporte tabular alineado en la consola:

```text
===================================================================================================================
                               REPORTE ESTADÍSTICO DE PELÍCULAS
===================================================================================================================
| Budget       | Prom:      XX.XX | Med:     XX.XX  | Min:          0 | Max:        XX.XX | Desv:      XX.XX |
| Revenue      | Prom:      XX.XX | Med:     XX.XX  | Min:          0 | Max:        XX.XX | Desv:      XX.XX |
| Popularity   | Prom:      XX.XX | Med:     XX.XX  | Min:          0 | Max:        XX.XX | Desv:      XX.XX |
| Runtime      | Prom:      XX.XX | Med:     XX.XX  | Min:          5 | Max:        XX.XX | Desv:      XX.XX |
| Vote Avg     | Prom:      XX.XX | Med:     XX.XX  | Min:          0 | Max:        XX.XX | Desv:      XX.XX |
| Vote Count   | Prom:      XX.XX | Med:     XX.XX  | Min:          0 | Max:        XX.XX | Desv:      XX.XX |
-------------------------------------------------------------------------------------------------------------------
 Total registros analizados: XX,XXX
===================================================================================================================
```

---

# 5.4 Análisis de Datos en Columnas Tipo Texto

## Descripción General

Este módulo realiza un **análisis de distribución de frecuencias** sobre las columnas de tipo texto del dataset de películas. El objetivo es identificar los valores más comunes en campos categóricos y textuales, permitiendo entender patrones y características predominantes en los datos.

---

## ¿Qué hace el análisis?

El programa procesa el archivo CSV de películas y genera un reporte que muestra:

* **Top 10 valores más frecuentes** por cada columna de texto
* **Número de apariciones** de cada valor
* **Identificación de datos vacíos o nulos**
* **Total de registros procesados** exitosamente

---

## Columnas Analizadas

El análisis se enfoca en 5 campos textuales clave:

1. **Idioma Original** (`original_language`)
2. **Estado de Producción** (`status`)
3. **Colección** (`belongs_to_collection`)
4. **Eslogan** (`tagline`)
5. **Título de la Película** (`title`)

---

## Resultados Obtenidos

### 1. Idioma Original

**Insight:** Distribución de idiomas en las películas del dataset.
```
Top Frecuencias: Idioma Original
  1. en        | Apariciones: 2514
  2. fr        | Apariciones: 205
  3. de        | Apariciones: 106
  4. it        | Apariciones: 105
  5. ja        | Apariciones: 95
```

**Interpretación:**
* El **inglés (en)** es predominante con 2,514 películas (72% aprox.)
* Seguido por **francés (fr)**, **alemán (de)** e **italiano (it)**
* El dataset tiene una fuerte inclinación hacia producciones en inglés

---

### 2. Estado de Producción

**Insight:** Estado actual de las películas en la base de datos.
```
Top Frecuencias: Estado (Status)
  1. Released              | Apariciones: 3462
  2. Rumored               | Apariciones: 14
  3. Post Pro              | Apariciones: 5
  4. [Sin Datos/Vacío]     | Apariciones: 5
  5. In Produ              | Apariciones: 1
```

**Interpretación:**
* El **99.3%** de las películas ya están estrenadas ("Released")
* Solo un pequeño número está en rumores o post-producción
* Los datos están principalmente completos (solo 5 valores vacíos)

---

### 3. Colección

**Insight:** Pertenencia a franquicias o sagas cinematográficas.
```
Top Frecuencias: Colección
  1. [Sin Datos/Vacío]                         | Apariciones: 3082
  2. Why We Fight                              | Apariciones: 12
  3. Star Wars Collection                      | Apariciones: 6
  4. Children of the Corn Collection           | Apariciones: 6
  5. Rocky Collection                          | Apariciones: 6
```

**Interpretación:**
* La mayoría de películas **(88%)** son producciones independientes (no pertenecen a colecciones)
* Las franquicias identificadas incluyen grandes sagas como **Star Wars** y **Rocky**
* Los datos de colección están en formato JSON con metadata adicional

---

### 4. Eslogan (Tagline)

**Insight:** Frases promocionales de las películas.
```
Top Frecuencias: Tagline (Eslogan)
  1. [Sin Datos/Vacío]                                     | Apariciones: 1872
  2. There's More To The Legend Than Meets… The Throat!    | Apariciones: 2
  3. A Motion Picture As Unusual As The Roles...           | Apariciones: 2
```

**Interpretación:**
* El **53.7%** de las películas no tienen eslogan registrado
* Los eslóganes son altamente únicos (pocas repeticiones)
* Existe variabilidad significativa en las frases promocionales

---

### 5. Título de la Película

**Insight:** Títulos más repetidos en el dataset.
```
Top Frecuencias: Título de la Película
  1. Why We Fight: Divide and Conquer          | Apariciones: 12
  2. Nana, the True Key of Pleasure            | Apariciones: 8
  3. Breathless                                | Apariciones: 3
```

**Interpretación:**
* Existen **duplicados** en los títulos (posibles remakes o versiones)
* "Why We Fight: Divide and Conquer" aparece 12 veces (posible error de duplicación)
* La mayoría de títulos son únicos

---

## Resumen Estadístico
```
Total registros analizados correctamente: 3,487
```

**Calidad de los datos:**
* El proceso filtró automáticamente registros malformados
* Se analizaron exitosamente **3,487 películas**
* El análisis es tolerante a errores de formato en el CSV

---

## Aplicaciones Prácticas

Este análisis de frecuencias es útil para:

* **Análisis exploratorio inicial** del dataset
* **Detección de patrones** en categorías de texto
* **Identificación de valores atípicos** o inconsistencias
* **Validación de calidad** de datos
* **Decisiones de preprocesamiento** para modelos de ML

---

## Tecnología Utilizada

El análisis se implementa usando:

* **Scala 3** con programación funcional
* **fs2**: Procesamiento streaming de archivos
* **Cats Effect**: Manejo de efectos IO
* **fs2-data-csv**: Parsing eficiente de CSV

---

# 5.5 Limpieza de Datos

## Descripción General

Este módulo implementa un **pipeline completo de limpieza y depuración** del dataset de películas, abordando problemas comunes de calidad de datos como valores nulos, ceros, valores atípicos (outliers) y registros inconsistentes. El proceso genera un dataset optimizado listo para análisis estadístico y modelado predictivo.

---

## Objetivos del Proceso de Limpieza

El pipeline tiene tres objetivos principales:

1. **Evaluar la integridad** de los datos identificando valores problemáticos
2. **Detectar valores atípicos** usando métodos estadísticos robustos
3. **Generar un dataset limpio** eliminando registros inválidos de forma gradual

---

## Pipeline de Limpieza (4 Secciones)

### SECCIÓN 1: Evaluación de Integridad de Datos

Esta sección identifica problemas de calidad en cada columna del dataset.

#### Campos Numéricos Analizados
```
Campo                Total    Nulos    Ceros    Negativos    % Válido
budget              3,487        0    2,638            0       24.35%
revenue             3,487        0    2,692            0       22.80%
popularity          3,487        0        4            0       99.89%
runtime             3,487        0      113            0       96.76%
vote_average        3,487        0      259            0       92.57%
vote_count          3,487        0      247            0       92.92%
retorno_inversion   3,487        0    2,638          356       14.14%
```

**Hallazgos críticos:**

*  **Budget y Revenue:** 75% de valores en cero (datos faltantes críticos)
*  **ROI:** Solo 14.14% de datos válidos (depende de budget/revenue)
*  **Popularity:** 99.89% de completitud (excelente calidad)
*  **Runtime:** 96.76% de datos válidos
*  **Vote Average/Count:** ~7-8% de registros sin votos

#### Campos de Texto Analizados
```
Campo                Total    Vacíos    % Válido
title               3,487         0      100.00%
original_title      3,487         0      100.00%
overview            3,487        75       97.85%
genres              3,487         0      100.00%
status              3,487         5       99.86%
original_language   3,487         1       99.97%
```

---

### SECCIÓN 2: Detección de Valores Atípicos

Se utilizan **dos métodos estadísticos** para identificar outliers:

#### Método IQR (Rango Intercuartílico)

Calcula límites basados en Q1, Q3 y el rango intercuartílico (IQR = Q3 - Q1).

**Fórmula:** 
- Límite inferior = Q1 - 1.5 × IQR
- Límite superior = Q3 + 1.5 × IQR
```
Budget:         Rango [0.00 - 0.00]       | Outliers: 849 altos (24.35%)
Revenue:        Rango [0.00 - 0.00]       | Outliers: 795 altos (22.80%)
Popularity:     Rango [0.00 - 13.45]      | Outliers: 172 altos (4.93%)
Runtime:        Rango [50.50 - 142.50]    | Outliers: 261 bajos, 165 altos (12.22%)
Vote Average:   Rango [2.00 - 10.00]      | Outliers: 283 bajos (8.12%)
Vote Count:     Rango [0.00 - 142.00]     | Outliers: 623 altos (17.87%)
ROI:            Rango [0.00 - 0.00]       | Outliers: 356 bajos, 493 altos (24.35%)
```

**Interpretación:**

* **Budget/Revenue:** El rango [0-0] indica que la mayoría de valores son cero, por lo que los valores reales son considerados outliers
* **Popularity:** 4.93% de películas con popularidad excepcionalmente alta
* **Runtime:** 12.22% de películas demasiado cortas o largas
* **Vote Count:** 17.87% de películas con cantidad anómala de votos

#### Método Z-Score (Desviación Estándar)

Identifica valores a más de 3 desviaciones estándar de la media.

**Fórmula:** |Z| = |(x - μ) / σ| > 3
```
Budget:         88 outliers (2.52%)
Revenue:        67 outliers (1.92%)
Popularity:     39 outliers (1.12%)
Runtime:        22 outliers (0.63%)
Vote Average:    0 outliers (0.00%)
```

**Comparación de métodos:**

* Z-Score es más conservador (detecta menos outliers)
* IQR es más sensible a valores extremos
* Vote Average no tiene outliers extremos por Z-Score (distribución normal)

---

### SECCIÓN 3: Pipeline de Limpieza Gradual

El proceso de depuración se realiza en **3 fases** progresivas:

#### Fase 1: Eliminación de Registros Inválidos

**Criterios aplicados:**
- ID > 0
- Budget > 0
- Revenue > 0
- Runtime > 0
- Popularity > 0
- Vote Count > 0
- Título no vacío

**Resultado:**
```
Registros originales:      3,487
Tras eliminar inválidos:     624  (2,863 removidos - 82.1%)
```

**Impacto:** Se eliminó el **82.1%** del dataset, principalmente por valores cero en budget/revenue.

#### Fase 2: Validación de Dominios

**Criterios aplicados:**
- Año entre 1888-2025 (era del cine)
- Mes entre 1-12
- Día entre 1-31
- Runtime < 500 minutos
- Vote Average entre 0-10
- ROI >= -100%

**Resultado:**
```
Tras validar dominios:       624  (0 removidos)
```

**Impacto:** Todos los registros restantes cumplen restricciones de dominio.

#### Fase 3: Filtrado de Outliers

Se aplican dos estrategias:

**a) Filtrado Estricto (IQR):**
- Elimina registros con outliers en budget, revenue O popularity
```
Filtrado estricto (IQR):     542  (82 removidos - 13.1%)
```

**b) Filtrado Flexible:**
- Permite hasta 1 outlier por registro en 4 campos (budget, revenue, popularity, ROI)
```
Filtrado flexible:           573  (51 removidos - 8.2%)
```

**Resultado Final:**
```
Retención final:     16.43%
Descarte total:      83.57%
```

---

### SECCIÓN 4: Estadísticas Descriptivas del Dataset Limpio

Resumen de los datos **después de la limpieza** usando el método flexible:

#### Presupuesto
```
Min: $1  |  Q1: $6.5M  |  Mediana: $19M  |  Media: $30.5M  |  Q3: $42M  |  Max: $190M
Desviación estándar: $32.1M
```

**Interpretación:** Gran variabilidad en presupuestos. El 50% de películas tiene entre $6.5M y $42M.

#### Ingresos
```
Min: $5  |  Q1: $11.7M  |  Mediana: $45.4M  |  Media: $82.2M  |  Q3: $120M  |  Max: $850M
Desviación estándar: $98.2M
```

**Interpretación:** Alta dispersión. Algunas películas generan ingresos masivos (Max: $850M).

#### Popularidad
```
Min: 0.08  |  Q1: 7.05  |  Mediana: 9.80  |  Media: 10.17  |  Q3: 12.68  |  Max: 51.65
Desviación estándar: 4.85
```

**Interpretación:** Distribución concentrada, con pocas películas extremadamente populares.

#### Duración (Runtime)
```
Min: 57 min  |  Q1: 98 min  |  Mediana: 110 min  |  Media: 112 min  |  Q3: 124 min  |  Max: 208 min
```

**Interpretación:** La mayoría dura entre 1h 38min y 2h 4min. Pocas películas exceden las 3 horas.

#### Promedio de Votos
```
Min: 2.30  |  Q1: 6.00  |  Mediana: 6.60  |  Media: 6.53  |  Q3: 7.10  |  Max: 8.50
```

**Interpretación:** Concentración alta entre 6.0 y 7.1. Pocas películas superan 8.0.

#### Cantidad de Votos
```
Min: 1  |  Q1: 175  |  Mediana: 419  |  Media: 805  |  Q3: 1,042  |  Max: 8,358
```

**Interpretación:** Sesgo positivo. Pocas películas tienen miles de votos.

#### Año de Estreno
```
Min: 1927  |  Q1: 1991  |  Mediana: 2001  |  Media: 1997  |  Q3: 2006  |  Max: 2016
```

**Interpretación:** Dataset concentrado en películas de 1991-2016 (era moderna del cine).

#### ROI (Retorno de Inversión)
```
Min: -100%  |  Q1: 9%  |  Mediana: 138%  |  Media: 1,120%  |  Q3: 378%  |  Max: 413,233%
Desviación estándar: 17,281%
```

**Interpretación:** 
- 50% de películas duplican o más su inversión
- Outliers extremos (Max: 413,233% = 4,132x retorno)
- Alta varianza indica riesgo significativo en la industria

---

## Tecnologías y Métodos

* **Lenguaje:** Scala 3 con programación funcional
* **Procesamiento:** fs2 streams para manejo eficiente de datos
* **Métodos estadísticos:** IQR, Z-Score
* **Validación:** Múltiples capas de filtros progresivos

---

# Procesamiento de Datos de Crew - Pipeline ETL en Scala

## Descripción General

Este proyecto implementa un **pipeline de extracción, transformación y carga (ETL)** para procesar datos de equipos de producción cinematográfica (crew) contenidos en un archivo CSV. El sistema está diseñado para manejar eficientemente grandes volúmenes de datos mediante procesamiento fila por fila, evitando la carga completa del archivo en memoria.

---

## Tecnologías Utilizadas

| Tecnología | Propósito |
|------------|-----------|
| **Scala** | Lenguaje de programación funcional |
| **Circe** | Biblioteca para parsing y serialización JSON |
| **scala.io.Source** | Lectura eficiente de archivos |

---

## Arquitectura del Pipeline

El procesamiento sigue un flujo secuencial de cinco etapas:
```
1. Lectura incremental → 2. Parsing CSV → 3. Limpieza JSON → 4. Decodificación → 5. Normalización
```

---

## Modelo de Datos

Se define una case class `Crew` que representa a cada miembro del equipo de producción:
```scala
case class Crew(
  credit_id: Option[String],
  department: Option[String],
  gender: Option[Int],
  id: Option[Int],
  job: Option[String],
  name: Option[String],
  profile_path: Option[String]
)
```

> El uso de `Option[T]` permite manejar valores nulos o ausentes de forma segura, siguiendo los principios de programación funcional.

---

## Funciones Principales

### Limpieza de JSON

La función `cleanCrewJson` transforma el formato Python-like del CSV a JSON válido:
- Reemplaza comillas simples (`'`) por dobles (`"`)
- Convierte `None` → `null`
- Convierte `True`/`False` → `true`/`false`

### Normalización de Datos

| Tipo | Regla Aplicada |
|------|----------------|
| **Texto** | Elimina espacios redundantes, retorna `None` para cadenas vacías |
| **Enteros** | Aplica valor absoluto para garantizar valores positivos |
| **Crew** | Aplica las reglas anteriores a todos los campos del objeto |

### Parsing CSV Personalizado

La función `parseCSVLine` implementa un parser que respeta las comillas, permitiendo manejar correctamente campos que contienen el delimitador (`;`) dentro de su contenido.

---

## Características Técnicas

- **Procesamiento lazy**: Utiliza iteradores para evitar cargar todo el archivo en memoria
- **Tolerancia a errores**: Los registros con JSON malformado se omiten sin interrumpir el proceso
- **Inmutabilidad**: Todas las transformaciones generan nuevos objetos sin modificar los originales
- **Composición funcional**: Uso extensivo de `flatMap`, `map` y pattern matching

---

## Salida del Sistema

El pipeline genera:

1. Conteo total de registros procesados
2. Ranking de los 5 departamentos más frecuentes
3. Muestra del primer registro en formato JSON formateado

---

## Ejecución

### Requisitos previos
- Scala 2.13+
- SBT (Scala Build Tool)
- Dependencias de Circe

### Archivo de entrada
El archivo debe cumplir con:
- Formato: **CSV**
- Codificación: **UTF-8**
- Separador: **`;`**
- Columna requerida: **`crew`** (con datos JSON embebido)

# Limpieza de Datos de Películas - Pipeline Streaming con Cats Effect y FS2

## Descripción General

Este proyecto implementa un **pipeline de limpieza y transformación de datos** para procesar información cinematográfica contenida en un archivo CSV. El sistema utiliza **procesamiento en streaming** mediante las bibliotecas Cats Effect y FS2, permitiendo manejar grandes volúmenes de datos de manera eficiente y con control total sobre los efectos secundarios.

---

## Tecnologías Utilizadas

| Tecnología | Propósito |
|------------|-----------|
| **Scala** | Lenguaje de programación funcional |
| **Cats Effect** | Manejo de efectos e IO asíncrono |
| **FS2** | Procesamiento de streams funcional |
| **fs2-data-csv** | Parsing de archivos CSV en streaming |

---

## Arquitectura del Pipeline

El procesamiento sigue un flujo de **dos pasadas**:
```
PASADA 1: Lectura → Parsing CSV → Transformación → Limpieza Básica (nulos y rangos)
PASADA 2: Cálculo de límites IQR → Filtrado de Outliers → Resultados Finales
```

---

## Modelos de Datos

### Modelo Crudo (MovieRaw)

Representa la estructura original del CSV con 25 campos:
```scala
case class MovieRaw(
  adult: String,
  belongs_to_collection: String,
  budget: Double,
  genres: String,
  homepage: String,
  id: Double,
  imdb_id: String,
  original_language: String,
  original_title: String,
  overview: String,
  popularity: Double,
  poster_path: String,
  production_companies: String,
  production_countries: String,
  release_date: String,
  revenue: Double,
  runtime: Double,
  spoken_languages: String,
  status: String,
  tagline: String,
  title: String,
  video: String,
  vote_average: Double,
  vote_count: Double,
  crew: String
)
```

### Modelo Procesado (Movie)

Incluye campos derivados calculados durante la transformación:
```scala
case class Movie(
  // ... campos originales ...
  release_year: Double,
  release_month: Double,
  release_day: Double,
  `return`: Double  // ROI calculado
)
```

---

## Funciones Principales

### Transformación de Datos

| Función | Descripción |
|---------|-------------|
| `parsearFecha` | Extrae año, mes y día de una fecha en formato "YYYY-MM-DD" |
| `calcularReturn` | Calcula el ROI como `(revenue - budget) / budget` |
| `transformar` | Convierte `MovieRaw` a `Movie` aplicando las transformaciones |

### Validaciones de Limpieza

| Función | Criterios |
|---------|-----------|
| `tieneValoresValidos` | Verifica que campos críticos no sean nulos, vacíos o cero |
| `tieneRangosValidos` | Valida rangos lógicos (año 1888-2025, mes 1-12, rating 0-10, etc.) |
| `esOutlier` | Detecta valores atípicos usando el método IQR con factor 3.0 |

### Detección de Outliers (IQR)
```scala
def calcularLimitesIQR(datos: List[Double]): (Double, Double) =
  val ordenados = datos.sorted
  val q1 = ordenados((ordenados.size * 0.25).toInt)
  val q3 = ordenados((ordenados.size * 0.75).toInt)
  val iqr = q3 - q1
  (math.max(0, q1 - 3.0 * iqr), q3 + 3.0 * iqr)
```

> Se utiliza un factor de 3.0 (en lugar del tradicional 1.5) para ser menos restrictivo con los datos.

---

## Estadísticas de Limpieza

El sistema rastrea métricas detalladas mediante una estructura inmutable:
```scala
case class EstadisticasLimpieza(
  totalLeidos: Int,
  erroresLectura: Int,
  descartadosNulos: Int,
  descartadosRangos: Int,
  descartadosOutliers: Int,
  conservados: Int,
  totalCeldas: Long
)
```

---

## Características Técnicas

- **Procesamiento en streaming**: Utiliza FS2 para procesar datos sin cargar todo el archivo en memoria
- **Efectos controlados**: Cats Effect IO garantiza pureza funcional y manejo seguro de efectos
- **Estado inmutable**: Uso de `Ref` para mantener contadores de forma thread-safe
- **Tolerancia a errores**: Las filas malformadas se ignoran silenciosamente sin interrumpir el proceso
- **Derivación automática**: Uso de `deriveCsvRowDecoder` para generar decodificadores CSV automáticamente
- **Feedback en tiempo real**: Muestra progreso cada N registros procesados

---

## Flujo de Ejecución

1. **Conteo de líneas**: Primera lectura para obtener el total de líneas del archivo
2. **Parsing CSV**: Decodificación de registros válidos usando el separador `;`
3. **Transformación**: Conversión de `MovieRaw` a `Movie` con campos derivados
4. **Limpieza básica**: Filtrado por valores nulos y rangos inválidos
5. **Cálculo IQR**: Determinación de límites para budget, revenue y popularity
6. **Filtrado de outliers**: Eliminación de registros fuera de los límites IQR
7. **Generación de reporte**: Estadísticas finales y análisis de datos

---

## Salida del Sistema

El pipeline genera un reporte completo que incluye:

1. Estadísticas de lectura y limpieza (registros leídos, errores, descartados por categoría)
2. Distribución de películas por año (últimos 10 años)
3. Estadísticas básicas (promedios de budget, revenue, runtime y return)
4. Porcentaje de datos conservados

---

## Ejecución

### Requisitos previos
- Scala 3.x
- SBT (Scala Build Tool)
- Dependencias:
  - cats-effect
  - fs2-core
  - fs2-io
  - fs2-data-csv

### Archivo de entrada
El archivo debe cumplir con:
- Formato: **CSV**
- Codificación: **UTF-8**
- Separador: **`;`**
- Estructura: **25 columnas** según el modelo `MovieRaw`

### Ejecutar el proyecto
```bash
sbt run
```
