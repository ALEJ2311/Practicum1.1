# Análsis y Limpieza de Dataset Movies

Utilizando **Scala** junto con **Cats Effect** y **FS2**, este sistema orquesta un flujo de trabajo completo que incluye:
* **Ingesta de Datos:** Procesamiento eficiente de archivos CSV.
* **Saneamiento:** Limpieza y normalización de registros.
* **Análisis:** Generación de métricas estadísticas y procesamiento de texto.

## 1. AnalisisEstadisticoSimple.scala - Reporte de Métricas Descriptivas

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
