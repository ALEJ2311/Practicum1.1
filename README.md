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
