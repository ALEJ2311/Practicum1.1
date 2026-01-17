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
