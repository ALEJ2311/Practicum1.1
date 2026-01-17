import cats.effect.{IO, IOApp, Ref}
import fs2.text
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*

// ========================================
// MODELO DE DATOS CRUDO DEL CSV
// ========================================

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

// ========================================
// MODELO DE DATOS PROCESADO
// ========================================

case class Movie(
                  adult: String,
                  belongs_to_collection: String,
                  budget: Double,
                  homepage: String,
                  id: Double,
                  imdb_id: String,
                  original_language: String,
                  original_title: String,
                  overview: String,
                  popularity: Double,
                  poster_path: String,
                  release_date: String,
                  revenue: Double,
                  runtime: Double,
                  status: String,
                  tagline: String,
                  title: String,
                  video: String,
                  vote_average: Double,
                  vote_count: Double,
                  release_year: Double,
                  release_month: Double,
                  release_day: Double,
                  `return`: Double
                )

given CsvRowDecoder[MovieRaw, String] = deriveCsvRowDecoder[MovieRaw]

// ========================================
// ESTADÍSTICAS DE LIMPIEZA
// ========================================

case class EstadisticasLimpieza(
                                 totalLeidos: Int,
                                 erroresLectura: Int,
                                 descartadosNulos: Int,
                                 descartadosRangos: Int,
                                 descartadosOutliers: Int,
                                 conservados: Int,
                                 totalCeldas: Long
                               )

// ========================================
// PROCESADOR Y LIMPIADOR EN UNA SOLA PASADA
// ========================================

object ProcesadorStreaming:

  // Parsear fecha
  def parsearFecha(fecha: String): (Double, Double, Double) =
    try {
      val partes = fecha.split("-")
      if (partes.length == 3) {
        val year = partes(0).toDouble
        val month = partes(1).toDouble
        val day = partes(2).toDouble
        (year, month, day)
      } else (0.0, 0.0, 0.0)
    } catch {
      case _: Exception => (0.0, 0.0, 0.0)
    }

  // Calcular return
  def calcularReturn(budget: Double, revenue: Double): Double =
    if (budget > 0) (revenue - budget) / budget else 0.0

  // Convertir MovieRaw a Movie
  def transformar(raw: MovieRaw): Movie =
    val (year, month, day) = parsearFecha(raw.release_date)
    val roi = calcularReturn(raw.budget, raw.revenue)

    Movie(
      adult = raw.adult,
      belongs_to_collection = raw.belongs_to_collection,
      budget = raw.budget,
      homepage = raw.homepage,
      id = raw.id,
      imdb_id = raw.imdb_id,
      original_language = raw.original_language,
      original_title = raw.original_title,
      overview = raw.overview,
      popularity = raw.popularity,
      poster_path = raw.poster_path,
      release_date = raw.release_date,
      revenue = raw.revenue,
      runtime = raw.runtime,
      status = raw.status,
      tagline = raw.tagline,
      title = raw.title,
      video = raw.video,
      vote_average = raw.vote_average,
      vote_count = raw.vote_count,
      release_year = year,
      release_month = month,
      release_day = day,
      `return` = roi
    )

  // Validar valores nulos/vacíos
  def tieneValoresValidos(m: Movie): Boolean =
    m.id > 0 &&
      m.budget > 0 &&
      m.revenue > 0 &&
      m.runtime > 0 &&
      m.popularity > 0 &&
      m.vote_count > 0 &&
      !m.title.trim.isEmpty &&
      !m.original_title.trim.isEmpty

  // Validar rangos
  def tieneRangosValidos(m: Movie): Boolean =
    m.release_year >= 1888 && m.release_year <= 2025 &&
      m.release_month >= 1 && m.release_month <= 12 &&
      m.release_day >= 1 && m.release_day <= 31 &&
      m.runtime > 0 && m.runtime < 500 &&
      m.vote_average >= 0 && m.vote_average <= 10 &&
      m.`return` >= -1.0

  // Detectar outliers usando IQR (necesita dos pasadas mínimo)
  // Usando 3.0 en lugar de 1.5 para ser menos restrictivo
  def calcularLimitesIQR(datos: List[Double]): (Double, Double) =
    if (datos.size < 4) return (0.0, Double.MaxValue)
    val ordenados = datos.sorted
    val q1 = ordenados((ordenados.size * 0.25).toInt)
    val q3 = ordenados((ordenados.size * 0.75).toInt)
    val iqr = q3 - q1
    (math.max(0, q1 - 3.0 * iqr), q3 + 3.0 * iqr)

  def esOutlier(m: Movie, limites: (Double, Double, Double, Double, Double, Double)): Boolean =
    val (bLow, bHigh, rLow, rHigh, pLow, pHigh) = limites
    m.budget < bLow || m.budget > bHigh ||
      m.revenue < rLow || m.revenue > rHigh ||
      m.popularity < pLow || m.popularity > pHigh

// ========================================
// APLICACIÓN PRINCIPAL
// ========================================

object LimpiezaSinJSON extends IOApp.Simple:
  val filePath = Path("src/main/resources/data/pi_movies_complete.csv")

  def run: IO[Unit] = for {
    // Contador simple para líneas totales del archivo
    contadorLineas <- Ref.of[IO, Int](0)

    // PASO 0: Contar todas las líneas del archivo (sin parsear)
    _ <- IO.println("=" * 90)
    _ <- IO.println("PASO 1: Contando total de líneas en el archivo CSV...")
    _ <- IO.println("=" * 90)

    totalLineas <- Files[IO]
      .readAll(filePath)
      .through(text.utf8.decode)
      .through(text.lines)
      .evalMap { linea =>
        contadorLineas.update(_ + 1) >>
          contadorLineas.get.flatMap { count =>
            if (count % 500 == 0) {
              IO.println(s"  → Líneas contadas: ${count}")
            } else IO.unit
          }
      }
      .compile
      .drain >> contadorLineas.get

    _ <- IO.println("")
    _ <- IO.println("=" * 90)
    _ <- IO.println(s"✓ TOTAL DE LÍNEAS EN EL ARCHIVO: ${totalLineas}")
    _ <- IO.println(s"  (Incluyendo encabezado, filas válidas y filas con errores)")
    _ <- IO.println("=" * 90)
    _ <- IO.println("")

    // Referencias mutables para estadísticas de procesamiento
    statsRef <- Ref.of[IO, EstadisticasLimpieza](
      EstadisticasLimpieza(0, 0, 0, 0, 0, 0, 0)
    )

    // PASO 1: Leer TODO el CSV (solo las filas que se pueden parsear)
    _ <- IO.println("PASO 2: Parseando registros del CSV...")
    _ <- IO.println("=" * 90)

    // Número de columnas por registro (según el modelo MovieRaw)
    numColumnas = 25  // MovieRaw tiene 25 campos

    todasLasPeliculas <- Files[IO]
      .readAll(filePath)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[MovieRaw](';'))
      .attempt
      .evalMap {
        case Right(raw) =>
          val celdasLeidas = numColumnas.toLong
          statsRef.update(s => s.copy(
            totalLeidos = s.totalLeidos + 1,
            totalCeldas = s.totalCeldas + celdasLeidas
          )) >>
            statsRef.get.flatMap { s =>
              if (s.totalLeidos % 100 == 0) {
                IO.println(s"  → Registro ${s.totalLeidos} parseado: ${raw.title}")
              } else IO.unit
            } >>
            IO.pure(Some(ProcesadorStreaming.transformar(raw)))
        case Left(e) =>
          statsRef.update(s => s.copy(erroresLectura = s.erroresLectura + 1)) >>
            IO.pure(None)  // Ignorar silenciosamente las filas malformadas
      }
      .collect { case Some(movie) => movie }
      .compile
      .toList

    stats <- statsRef.get

    _ <- IO.println("")
    _ <- IO.println("=" * 90)
    _ <- IO.println(s"✓ RESUMEN DE LECTURA:")
    _ <- IO.println(s"  - Total de líneas en archivo:     ${totalLineas}")
    _ <- IO.println(s"  - Registros parseados con éxito:  ${stats.totalLeidos}")
    _ <- IO.println(s"  - Filas con errores (ignoradas):  ${stats.erroresLectura}")
    _ <- IO.println(s"  - Línea de encabezado:             1")
    _ <- IO.println("=" * 90)
    _ <- IO.println("")
    _ <- IO.println("PASO 3: Iniciando proceso de limpieza de datos...")
    _ <- IO.println("")

    // APLICAR LIMPIEZA: filtrar nulos y rangos
    peliculasLimpiasPreliminar <- fs2.Stream.emits(todasLasPeliculas)
      .evalMap { movie =>
        if (!ProcesadorStreaming.tieneValoresValidos(movie)) {
          statsRef.update(s => s.copy(descartadosNulos = s.descartadosNulos + 1)) >>
            IO.pure(None)
        } else if (!ProcesadorStreaming.tieneRangosValidos(movie)) {
          statsRef.update(s => s.copy(descartadosRangos = s.descartadosRangos + 1)) >>
            IO.pure(None)
        } else {
          IO.pure(Some(movie))
        }
      }
      .collect { case Some(movie) => movie }
      .compile
      .toList

    _ <- IO.println(s"✓ Después de limpieza básica: ${peliculasLimpiasPreliminar.length} películas válidas")
    _ <- IO.println("Calculando límites IQR y filtrando outliers...")
    _ <- IO.println("")

    limitesBudget = ProcesadorStreaming.calcularLimitesIQR(peliculasLimpiasPreliminar.map(_.budget))
    limitesRevenue = ProcesadorStreaming.calcularLimitesIQR(peliculasLimpiasPreliminar.map(_.revenue))
    limitesPopularity = ProcesadorStreaming.calcularLimitesIQR(peliculasLimpiasPreliminar.map(_.popularity))

    limites = (limitesBudget._1, limitesBudget._2,
      limitesRevenue._1, limitesRevenue._2,
      limitesPopularity._1, limitesPopularity._2)

    peliculasFinales <- fs2.Stream.emits(peliculasLimpiasPreliminar)
      .evalMap { movie =>
        if (ProcesadorStreaming.esOutlier(movie, limites)) {
          statsRef.update(s => s.copy(descartadosOutliers = s.descartadosOutliers + 1)) >>
            IO.pure(None)
        } else {
          statsRef.update(s => s.copy(conservados = s.conservados + 1)) >>
            IO.pure(Some(movie))
        }
      }
      .collect { case Some(movie) => movie }
      .compile
      .toList

    // Obtener estadísticas finales
    statsFinales <- statsRef.get

    // Análisis adicional
    peliculasPorAnio = peliculasFinales
      .groupBy(_.release_year)
      .view.mapValues(_.size)
      .toMap
      .toList
      .sortBy(_._1)
      .takeRight(10)

    budgets = peliculasFinales.map(_.budget)
    revenues = peliculasFinales.map(_.revenue)
    runtimes = peliculasFinales.map(_.runtime)
    returns = peliculasFinales.map(_.`return`)

    // Reporte final
    _ <- IO.println("")
    _ <- IO.println("=" * 90)
    _ <- IO.println("     REPORTE DE LIMPIEZA DE DATOS (PROCESAMIENTO EN STREAMING)")
    _ <- IO.println("=" * 90)
    _ <- IO.println("")
    _ <- IO.println("1. ESTADÍSTICAS DE LECTURA Y LIMPIEZA")
    _ <- IO.println("-" * 90)
    _ <- IO.println(f"Registros leídos correctamente:      ${statsFinales.totalLeidos}%,10d filas")
    _ <- IO.println(f"Errores de lectura (filas ignoradas): ${statsFinales.erroresLectura}%,10d")
    _ <- IO.println(f"Descartados por valores nulos:       ${statsFinales.descartadosNulos}%,10d")
    _ <- IO.println(f"Descartados por rangos inválidos:    ${statsFinales.descartadosRangos}%,10d")
    _ <- IO.println(f"Descartados por outliers (IQR):      ${statsFinales.descartadosOutliers}%,10d")
    _ <- IO.println(f"Registros conservados:               ${statsFinales.conservados}%,10d")
    _ <- IO.println(f"Porcentaje conservado:               ${statsFinales.conservados.toDouble / statsFinales.totalLeidos * 100}%18.2f%%")
    _ <- IO.println("")
    _ <- IO.println("2. PELÍCULAS POR AÑO (Últimos 10 años)")
    _ <- IO.println("-" * 90)
    _ <- peliculasPorAnio.foldLeft(IO.unit) { case (acc, (year, count)) =>
      acc >> IO.println(f"  ${year}%.0f: ${count}%,d películas")
    }
    _ <- IO.println("")
    _ <- IO.println("3. ESTADÍSTICAS BÁSICAS")
    _ <- IO.println("-" * 90)
    _ <- IO.println(f"Budget promedio:                     $$${budgets.sum / budgets.size}%,.2f")
    _ <- IO.println(f"Revenue promedio:                    $$${revenues.sum / revenues.size}%,.2f")
    _ <- IO.println(f"Runtime promedio:                    ${runtimes.sum / runtimes.size}%.2f minutos")
    _ <- IO.println(f"Return promedio:                     ${(returns.sum / returns.size) * 100}%.2f%%")
    _ <- IO.println("")
    _ <- IO.println("=" * 90)
    _ <- IO.println("✓ Limpieza completada en modo streaming (2 pasadas)")
    _ <- IO.println("  - Pasada 1: Lectura + transformación + limpieza básica")
    _ <- IO.println("  - Pasada 2: Cálculo de IQR + filtrado de outliers")
    _ <- IO.println("=" * 90)
  } yield ()