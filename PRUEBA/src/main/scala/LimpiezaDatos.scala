import cats.effect.{IO, IOApp}
import fs2.text
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*

// Registro sin transformaciones
case class DatosCrudos(
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
                        vote_count: Double
                      )

// Registro con atributos calculados
case class DatosEnriquecidos(
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
                              anio_estreno: Double,
                              mes_estreno: Double,
                              dia_estreno: Double,
                              retorno_inversion: Double
                            )

given CsvRowDecoder[DatosCrudos, String] = deriveCsvRowDecoder[DatosCrudos]
given CsvRowDecoder[DatosEnriquecidos, String] = deriveCsvRowDecoder[DatosEnriquecidos]

// Estadísticas descriptivas
case class ResumenEstadistico(
                               campo: String,
                               minimo: Double,
                               q1: Double,
                               mediana: Double,
                               media: Double,
                               q3: Double,
                               maximo: Double,
                               desviacion: Double
                             )

// Análisis de calidad
case class AnalisisCalidad(
                            campo: String,
                            total: Int,
                            nulos: Int,
                            ceros: Int,
                            negativos: Int,
                            vacios: Int,
                            validez: Double
                          )

object UtilFechas:
  def descomponerFecha(fecha: String): (Double, Double, Double) =
    try {
      val partes = fecha.split("-")
      if (partes.length == 3)
        (partes(0).toDouble, partes(1).toDouble, partes(2).toDouble)
      else (0.0, 0.0, 0.0)
    } catch {
      case _: Exception => (0.0, 0.0, 0.0)
    }

object CalculosFinancieros:
  def roi(costo: Double, ganancia: Double): Double =
    if (costo > 0) (ganancia - costo) / costo else 0.0

object Transformador:
  def transformar(crudo: DatosCrudos): DatosEnriquecidos =
    val (a, m, d) = UtilFechas.descomponerFecha(crudo.release_date)
    val rendimiento = CalculosFinancieros.roi(crudo.budget, crudo.revenue)

    DatosEnriquecidos(
      adult = crudo.adult,
      belongs_to_collection = crudo.belongs_to_collection,
      budget = crudo.budget,
      genres = crudo.genres,
      homepage = crudo.homepage,
      id = crudo.id,
      imdb_id = crudo.imdb_id,
      original_language = crudo.original_language,
      original_title = crudo.original_title,
      overview = crudo.overview,
      popularity = crudo.popularity,
      poster_path = crudo.poster_path,
      production_companies = crudo.production_companies,
      production_countries = crudo.production_countries,
      release_date = crudo.release_date,
      revenue = crudo.revenue,
      runtime = crudo.runtime,
      spoken_languages = crudo.spoken_languages,
      status = crudo.status,
      tagline = crudo.tagline,
      title = crudo.title,
      video = crudo.video,
      vote_average = crudo.vote_average,
      vote_count = crudo.vote_count,
      anio_estreno = a,
      mes_estreno = m,
      dia_estreno = d,
      retorno_inversion = rendimiento
    )

object ValidadorCalidad:
  def validarNumerico(campo: String, datos: List[Double], n: Int): AnalisisCalidad =
    val nul = datos.count(x => x.isNaN || x.isInfinite)
    val zer = datos.count(_ == 0.0)
    val neg = datos.count(_ < 0.0)
    val ok = n - nul - zer - neg

    AnalisisCalidad(campo, n, nul, zer, neg, 0, if (n > 0) (ok.toDouble / n * 100) else 0.0)

  def validarTexto(campo: String, datos: List[String], n: Int): AnalisisCalidad =
    val vac = datos.count(x => x == null || x.trim.isEmpty)
    val ok = n - vac

    AnalisisCalidad(campo, n, 0, 0, 0, vac, if (n > 0) (ok.toDouble / n * 100) else 0.0)

object AnalizadorOutliers:
  def percentil(sortedData: List[Double], p: Double): Double =
    if (sortedData.isEmpty) return 0.0
    val idx = p * (sortedData.size - 1)
    val lo = sortedData(idx.toInt)
    val hi = if (idx.toInt + 1 < sortedData.size) sortedData(idx.toInt + 1) else lo
    val frac = idx - idx.toInt
    lo + frac * (hi - lo)

  def iqrLimites(datos: List[Double]): (Double, Double, Int, Int) =
    if (datos.size < 4) return (0.0, 0.0, 0, 0)

    val sorted = datos.sorted
    val q1 = percentil(sorted, 0.25)
    val q3 = percentil(sorted, 0.75)
    val iqr = q3 - q1

    val lower = math.max(0, q1 - 1.5 * iqr)
    val upper = q3 + 1.5 * iqr

    val outlowCount = datos.count(_ < lower)
    val outhighCount = datos.count(_ > upper)

    (lower, upper, outlowCount, outhighCount)

  def zscoreOutliers(datos: List[Double], threshold: Double = 3.0): Int =
    if (datos.isEmpty) return 0

    val avg = datos.sum / datos.size
    val variance = datos.map(x => math.pow(x - avg, 2)).sum / datos.size
    val std = math.sqrt(variance)

    if (std == 0.0) 0
    else datos.count(x => math.abs((x - avg) / std) > threshold)

object Filtros:
  def eliminarInvalidos(datos: List[DatosEnriquecidos]): List[DatosEnriquecidos] =
    datos.filter { d =>
      d.id > 0 && d.budget > 0 && d.revenue > 0 && d.runtime > 0 &&
        d.popularity > 0 && d.vote_count > 0 &&
        !d.title.trim.isEmpty && !d.original_title.trim.isEmpty
    }

  def validarDominios(datos: List[DatosEnriquecidos]): List[DatosEnriquecidos] =
    datos.filter { d =>
      d.anio_estreno >= 1888 && d.anio_estreno <= 2025 &&
        d.mes_estreno >= 1 && d.mes_estreno <= 12 &&
        d.dia_estreno >= 1 && d.dia_estreno <= 31 &&
        d.runtime < 500 &&
        d.vote_average >= 0 && d.vote_average <= 10 &&
        d.retorno_inversion >= -1.0
    }

  def filtrarOutliersEstricto(datos: List[DatosEnriquecidos]): List[DatosEnriquecidos] =
    if (datos.isEmpty) return Nil

    val (bMin, bMax, _, _) = AnalizadorOutliers.iqrLimites(datos.map(_.budget))
    val (rMin, rMax, _, _) = AnalizadorOutliers.iqrLimites(datos.map(_.revenue))
    val (pMin, pMax, _, _) = AnalizadorOutliers.iqrLimites(datos.map(_.popularity))

    datos.filter { d =>
      d.budget >= bMin && d.budget <= bMax &&
        d.revenue >= rMin && d.revenue <= rMax &&
        d.popularity >= pMin && d.popularity <= pMax
    }

  def filtrarOutliersFlexible(datos: List[DatosEnriquecidos]): List[DatosEnriquecidos] =
    if (datos.isEmpty) return Nil

    val (bMin, bMax, _, _) = AnalizadorOutliers.iqrLimites(datos.map(_.budget))
    val (rMin, rMax, _, _) = AnalizadorOutliers.iqrLimites(datos.map(_.revenue))
    val (pMin, pMax, _, _) = AnalizadorOutliers.iqrLimites(datos.map(_.popularity))
    val (roiMin, roiMax, _, _) = AnalizadorOutliers.iqrLimites(datos.map(_.retorno_inversion))

    datos.filter { d =>
      val outliers = Seq(
        d.budget < bMin || d.budget > bMax,
        d.revenue < rMin || d.revenue > rMax,
        d.popularity < pMin || d.popularity > pMax,
        d.retorno_inversion < roiMin || d.retorno_inversion > roiMax
      ).count(identity)

      outliers < 2
    }

object EstadisticasDescriptivas:
  def calcular(datos: List[Double]): Map[String, Double] =
    if (datos.isEmpty) return Map.empty

    val sorted = datos.sorted
    val n = sorted.size
    val avg = datos.sum / n
    val variance = datos.map(x => math.pow(x - avg, 2)).sum / n
    val med = if (n % 2 == 1) sorted(n / 2) else (sorted(n / 2 - 1) + sorted(n / 2)) / 2.0

    Map(
      "min" -> sorted.head,
      "max" -> sorted.last,
      "avg" -> avg,
      "med" -> med,
      "std" -> math.sqrt(variance),
      "q1" -> AnalizadorOutliers.percentil(sorted, 0.25),
      "q3" -> AnalizadorOutliers.percentil(sorted, 0.75)
    )

  def resumen(campo: String, datos: List[Double]): ResumenEstadistico =
    val stats = calcular(datos)
    ResumenEstadistico(
      campo,
      stats.getOrElse("min", 0.0),
      stats.getOrElse("q1", 0.0),
      stats.getOrElse("med", 0.0),
      stats.getOrElse("avg", 0.0),
      stats.getOrElse("q3", 0.0),
      stats.getOrElse("max", 0.0),
      stats.getOrElse("std", 0.0)
    )

object LimpiezaDatos extends IOApp.Simple:

  def run: IO[Unit] =
    val archivo = Path("C:\\Users\\anapa\\IdeaProjects\\b2-proyecto-integrador-a-proyecto_integrador_radiant_developers\\src\\main\\resources\\pi-movies-complete-2025-12-04.csv")
    val cargar: IO[List[DatosEnriquecidos]] = Files[IO]
      .readAll(archivo)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[DatosCrudos](';'))
      .map(Transformador.transformar)
      .compile
      .toList
      .handleErrorWith { e =>
        IO.println(s"Error carga: ${e.getMessage}") >> IO.pure(Nil)
      }

    cargar.flatMap { dataset =>
      val N = dataset.length

      // CALIDAD DE DATOS - Todos los campos numéricos
      val calidadBudget = ValidadorCalidad.validarNumerico("budget", dataset.map(_.budget), N)
      val calidadRevenue = ValidadorCalidad.validarNumerico("revenue", dataset.map(_.revenue), N)
      val calidadPopularity = ValidadorCalidad.validarNumerico("popularity", dataset.map(_.popularity), N)
      val calidadRuntime = ValidadorCalidad.validarNumerico("runtime", dataset.map(_.runtime), N)
      val calidadVoteAvg = ValidadorCalidad.validarNumerico("vote_average", dataset.map(_.vote_average), N)
      val calidadVoteCount = ValidadorCalidad.validarNumerico("vote_count", dataset.map(_.vote_count), N)
      val calidadId = ValidadorCalidad.validarNumerico("id", dataset.map(_.id), N)
      val calidadYear = ValidadorCalidad.validarNumerico("anio_estreno", dataset.map(_.anio_estreno), N)
      val calidadMonth = ValidadorCalidad.validarNumerico("mes_estreno", dataset.map(_.mes_estreno), N)
      val calidadDay = ValidadorCalidad.validarNumerico("dia_estreno", dataset.map(_.dia_estreno), N)
      val calidadROI = ValidadorCalidad.validarNumerico("retorno_inversion", dataset.map(_.retorno_inversion), N)

      // CALIDAD DE DATOS - Campos de texto
      val calidadTitle = ValidadorCalidad.validarTexto("title", dataset.map(_.title), N)
      val calidadOrigTitle = ValidadorCalidad.validarTexto("original_title", dataset.map(_.original_title), N)
      val calidadOverview = ValidadorCalidad.validarTexto("overview", dataset.map(_.overview), N)
      val calidadGenres = ValidadorCalidad.validarTexto("genres", dataset.map(_.genres), N)
      val calidadStatus = ValidadorCalidad.validarTexto("status", dataset.map(_.status), N)
      val calidadLanguage = ValidadorCalidad.validarTexto("original_language", dataset.map(_.original_language), N)

      // OUTLIERS - Campos principales
      val (budgetLo, budgetHi, budgetOutLo, budgetOutHi) = AnalizadorOutliers.iqrLimites(dataset.map(_.budget))
      val (revenueLo, revenueHi, revenueOutLo, revenueOutHi) = AnalizadorOutliers.iqrLimites(dataset.map(_.revenue))
      val (popLo, popHi, popOutLo, popOutHi) = AnalizadorOutliers.iqrLimites(dataset.map(_.popularity))
      val (runLo, runHi, runOutLo, runOutHi) = AnalizadorOutliers.iqrLimites(dataset.map(_.runtime))
      val (voteLo, voteHi, voteOutLo, voteOutHi) = AnalizadorOutliers.iqrLimites(dataset.map(_.vote_average))
      val (countLo, countHi, countOutLo, countOutHi) = AnalizadorOutliers.iqrLimites(dataset.map(_.vote_count))
      val (roiLo, roiHi, roiOutLo, roiOutHi) = AnalizadorOutliers.iqrLimites(dataset.map(_.retorno_inversion))

      val budgetZ = AnalizadorOutliers.zscoreOutliers(dataset.map(_.budget))
      val revenueZ = AnalizadorOutliers.zscoreOutliers(dataset.map(_.revenue))
      val popularityZ = AnalizadorOutliers.zscoreOutliers(dataset.map(_.popularity))
      val runtimeZ = AnalizadorOutliers.zscoreOutliers(dataset.map(_.runtime))
      val voteAvgZ = AnalizadorOutliers.zscoreOutliers(dataset.map(_.vote_average))

      // LIMPIEZA
      val paso1 = Filtros.eliminarInvalidos(dataset)
      val paso2 = Filtros.validarDominios(paso1)
      val paso3a = Filtros.filtrarOutliersEstricto(paso2)
      val paso3b = Filtros.filtrarOutliersFlexible(paso2)

      // ESTADÍSTICAS FINALES - Todos los campos numéricos
      val statsBudget = EstadisticasDescriptivas.resumen("Presupuesto", paso3b.map(_.budget))
      val statsRevenue = EstadisticasDescriptivas.resumen("Ingresos", paso3b.map(_.revenue))
      val statsPopularity = EstadisticasDescriptivas.resumen("Popularidad", paso3b.map(_.popularity))
      val statsRuntime = EstadisticasDescriptivas.resumen("Duración", paso3b.map(_.runtime))
      val statsVoteAvg = EstadisticasDescriptivas.resumen("Promedio votos", paso3b.map(_.vote_average))
      val statsVoteCount = EstadisticasDescriptivas.resumen("Cantidad votos", paso3b.map(_.vote_count))
      val statsYear = EstadisticasDescriptivas.resumen("Año", paso3b.map(_.anio_estreno))
      val statsROI = EstadisticasDescriptivas.resumen("ROI", paso3b.map(_.retorno_inversion))

      val todasCalidadNum = List(calidadBudget, calidadRevenue, calidadPopularity, calidadRuntime,
        calidadVoteAvg, calidadVoteCount, calidadId, calidadYear, calidadMonth, calidadDay, calidadROI)
      val todasCalidadTxt = List(calidadTitle, calidadOrigTitle, calidadOverview, calidadGenres,
        calidadStatus, calidadLanguage)
      val todasEstadisticas = List(statsBudget, statsRevenue, statsPopularity, statsRuntime,
        statsVoteAvg, statsVoteCount, statsYear, statsROI)

      // REPORTE
      IO.println("=" * 100) >>
        IO.println("                    ANÁLISIS DE CALIDAD Y LIMPIEZA - PELÍCULAS") >>
        IO.println("=" * 100) >>
        IO.println("") >>
        IO.println("SECCIÓN 1: EVALUACIÓN DE INTEGRIDAD DE DATOS") >>
        IO.println("-" * 100) >>
        IO.println("CAMPOS NUMÉRICOS:") >>
        IO.println(f"${"Campo"}%-20s ${"Total"}%8s ${"Nulos"}%8s ${"Ceros"}%8s ${"Negativos"}%10s ${"% Válido"}%10s") >>
        todasCalidadNum.map { c =>
          IO.println(f"${c.campo}%-20s ${c.total}%,8d ${c.nulos}%,8d ${c.ceros}%,8d ${c.negativos}%,10d ${c.validez}%9.2f%%")
        }.sequence.void >>
        IO.println("") >>
        IO.println("CAMPOS DE TEXTO:") >>
        IO.println(f"${"Campo"}%-20s ${"Total"}%8s ${"Vacíos"}%8s ${"% Válido"}%10s") >>
        todasCalidadTxt.map { c =>
          IO.println(f"${c.campo}%-20s ${c.total}%,8d ${c.vacios}%,8d ${c.validez}%9.2f%%")
        }.sequence.void >>
        IO.println("") >>
        IO.println("SECCIÓN 2: DETECCIÓN DE VALORES ATÍPICOS") >>
        IO.println("-" * 100) >>
        IO.println("Método IQR (Rango Intercuartílico):") >>
        IO.println("") >>
        IO.println(f"Budget:         Rango [$budgetLo%,.2f - $budgetHi%,.2f] | Outliers: ${budgetOutLo}%,d bajos, ${budgetOutHi}%,d altos (${(budgetOutLo + budgetOutHi).toDouble / N * 100}%.2f%%)") >>
        IO.println(f"Revenue:        Rango [$revenueLo%,.2f - $revenueHi%,.2f] | Outliers: ${revenueOutLo}%,d bajos, ${revenueOutHi}%,d altos (${(revenueOutLo + revenueOutHi).toDouble / N * 100}%.2f%%)") >>
        IO.println(f"Popularity:     Rango [$popLo%,.2f - $popHi%,.2f] | Outliers: ${popOutLo}%,d bajos, ${popOutHi}%,d altos (${(popOutLo + popOutHi).toDouble / N * 100}%.2f%%)") >>
        IO.println(f"Runtime:        Rango [$runLo%,.2f - $runHi%,.2f] | Outliers: ${runOutLo}%,d bajos, ${runOutHi}%,d altos (${(runOutLo + runOutHi).toDouble / N * 100}%.2f%%)") >>
        IO.println(f"Vote Average:   Rango [$voteLo%,.2f - $voteHi%,.2f] | Outliers: ${voteOutLo}%,d bajos, ${voteOutHi}%,d altos (${(voteOutLo + voteOutHi).toDouble / N * 100}%.2f%%)") >>
        IO.println(f"Vote Count:     Rango [$countLo%,.2f - $countHi%,.2f] | Outliers: ${countOutLo}%,d bajos, ${countOutHi}%,d altos (${(countOutLo + countOutHi).toDouble / N * 100}%.2f%%)") >>
        IO.println(f"ROI:            Rango [$roiLo%,.2f - $roiHi%,.2f] | Outliers: ${roiOutLo}%,d bajos, ${roiOutHi}%,d altos (${(roiOutLo + roiOutHi).toDouble / N * 100}%.2f%%)") >>
        IO.println("") >>
        IO.println("Método Z-Score (umbral = 3):") >>
        IO.println(f"Budget:         ${budgetZ}%,d outliers (${budgetZ.toDouble / N * 100}%.2f%%)") >>
        IO.println(f"Revenue:        ${revenueZ}%,d outliers (${revenueZ.toDouble / N * 100}%.2f%%)") >>
        IO.println(f"Popularity:     ${popularityZ}%,d outliers (${popularityZ.toDouble / N * 100}%.2f%%)") >>
        IO.println(f"Runtime:        ${runtimeZ}%,d outliers (${runtimeZ.toDouble / N * 100}%.2f%%)") >>
        IO.println(f"Vote Average:   ${voteAvgZ}%,d outliers (${voteAvgZ.toDouble / N * 100}%.2f%%)") >>
        IO.println("") >>
        IO.println("SECCIÓN 3: PIPELINE DE LIMPIEZA") >>
        IO.println("-" * 100) >>
        IO.println(f"Registros originales:            ${dataset.length}%,10d") >>
        IO.println(f"Tras eliminar inválidos:         ${paso1.length}%,10d  (${N - paso1.length}%,d removidos)") >>
        IO.println(f"Tras validar dominios:           ${paso2.length}%,10d  (${paso1.length - paso2.length}%,d removidos)") >>
        IO.println(f"Filtrado estricto (IQR):         ${paso3a.length}%,10d  (${paso2.length - paso3a.length}%,d removidos)") >>
        IO.println(f"Filtrado flexible (1 outlier):   ${paso3b.length}%,10d  (${paso2.length - paso3b.length}%,d removidos)") >>
        IO.println("") >>
        IO.println(f"Retención final:                 ${paso3b.length.toDouble / N * 100}%9.2f%%") >>
        IO.println(f"Descarte total:                  ${(N - paso3b.length).toDouble / N * 100}%9.2f%%") >>
        IO.println("") >>
        IO.println("SECCIÓN 4: ESTADÍSTICAS DESCRIPTIVAS (Dataset Limpio)") >>
        IO.println("-" * 100) >>
        todasEstadisticas.map { s =>
          IO.println(f"${s.campo}:") >>
            IO.println(f"  Min: ${s.minimo}%,.2f  |  Q1: ${s.q1}%,.2f  |  Med: ${s.mediana}%,.2f  |  Avg: ${s.media}%,.2f  |  Q3: ${s.q3}%,.2f  |  Max: ${s.maximo}%,.2f  |  Std: ${s.desviacion}%,.2f") >>
            IO.println("")
        }.sequence.void >>
        IO.println("=" * 100) >>
        IO.println("✓ Análisis completado") >>
        IO.println("✓ Dataset optimizado para modelado") >>
        IO.println("=" * 100)
    }