import cats.effect.{IO, IOApp}
import fs2.text
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*

// ===============================
// MODELO DE DATOS (28 ATRIBUTOS)
// ===============================
case class Movie(
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
                  release_year: Double,
                  release_month: Double,
                  release_day: Double,
                  `return`: Double
                )

given CsvRowDecoder[Movie, String] = deriveCsvRowDecoder[Movie]

// ===============================
// MÉTRICAS DE CALIDAD
// ===============================
case class MetricasColumna(
                            atributo: String,
                            totalRegistros: Int,
                            valoresInvalidos: Int,
                            valoresCero: Int,
                            valoresNegativos: Int,
                            valoresVacios: Int,
                            porcentajeCorrectos: Double
                          )

// ===============================
// EVALUACIÓN DE DATOS
// ===============================
object EvaluadorDatos {

  def evaluarNumerico(nombre: String, datos: List[Double], total: Int): MetricasColumna = {
    val invalidos = datos.count(d => d.isNaN || d.isInfinite)
    val ceros = datos.count(_ == 0.0)
    val negativos = datos.count(_ < 0.0)
    val validos = total - invalidos - ceros - negativos

    MetricasColumna(
      atributo = nombre,
      totalRegistros = total,
      valoresInvalidos = invalidos,
      valoresCero = ceros,
      valoresNegativos = negativos,
      valoresVacios = 0,
      porcentajeCorrectos = if (total > 0) validos.toDouble / total * 100 else 0.0
    )
  }

  def evaluarTexto(nombre: String, datos: List[String], total: Int): MetricasColumna = {
    val vacios = datos.count(s => s == null || s.trim.isEmpty)
    val validos = total - vacios

    MetricasColumna(
      atributo = nombre,
      totalRegistros = total,
      valoresInvalidos = 0,
      valoresCero = 0,
      valoresNegativos = 0,
      valoresVacios = vacios,
      porcentajeCorrectos = if (total > 0) validos.toDouble / total * 100 else 0.0
    )
  }
}

// ===============================
// ANÁLISIS DE VALORES ATÍPICOS
// ===============================
object AnalisisAtipicos {

  def cuartil(datos: List[Double], p: Double): Double = {
    if (datos.isEmpty) 0.0
    else {
      val ordenados = datos.sorted
      val pos = p * (ordenados.size - 1)
      val base = ordenados(pos.toInt)
      val siguiente =
        if (pos.toInt + 1 < ordenados.size) ordenados(pos.toInt + 1)
        else base
      base + (pos - pos.toInt) * (siguiente - base)
    }
  }

  def detectarIQR(datos: List[Double]): (Double, Double, Int, Int) = {
    if (datos.size < 4) return (0.0, 0.0, 0, 0)

    val q1 = cuartil(datos, 0.25)
    val q3 = cuartil(datos, 0.75)
    val iqr = q3 - q1

    val min = math.max(0, q1 - 1.5 * iqr)
    val max = q3 + 1.5 * iqr

    val bajos = datos.count(_ < min)
    val altos = datos.count(_ > max)

    (min, max, bajos, altos)
  }
}

// ===============================
// PROCESO DE DEPURACIÓN
// ===============================
object ProcesoDepuracion {

  def filtroBasico(pelis: List[Movie]): List[Movie] =
    pelis.filter(m =>
      m.id > 0 &&
        m.budget > 0 &&
        m.revenue > 0 &&
        m.runtime > 0 &&
        m.popularity > 0 &&
        m.vote_count > 0 &&
        m.title.nonEmpty &&
        m.original_title.nonEmpty
    )

  def validarRangos(pelis: List[Movie]): List[Movie] =
    pelis.filter(m =>
      m.release_year >= 1888 && m.release_year <= 2025 &&
        m.release_month >= 1 && m.release_month <= 12 &&
        m.release_day >= 1 && m.release_day <= 31 &&
        m.runtime < 500 &&
        m.vote_average >= 0 && m.vote_average <= 10 &&
        m.`return` >= -1
    )

  def filtrarAtipicosFlexible(pelis: List[Movie]): List[Movie] = {
    if (pelis.isEmpty) return Nil

    val (bMin, bMax, _, _) = AnalisisAtipicos.detectarIQR(pelis.map(_.budget))
    val (rMin, rMax, _, _) = AnalisisAtipicos.detectarIQR(pelis.map(_.revenue))
    val (pMin, pMax, _, _) = AnalisisAtipicos.detectarIQR(pelis.map(_.popularity))

    pelis.filter { m =>
      val fuera = Seq(
        m.budget < bMin || m.budget > bMax,
        m.revenue < rMin || m.revenue > rMax,
        m.popularity < pMin || m.popularity > pMax
      ).count(identity)

      fuera < 2
    }
  }
}

// ===============================
// ESTADÍSTICAS DESCRIPTIVAS
// ===============================
object ResumenEstadistico {

  def calcular(datos: List[Double]): Map[String, Double] = {
    if (datos.isEmpty) return Map.empty

    val orden = datos.sorted
    val n = orden.size
    val media = datos.sum / n
    val varianza = datos.map(x => math.pow(x - media, 2)).sum / n
    val mediana =
      if (n % 2 == 1) orden(n / 2)
      else (orden(n / 2 - 1) + orden(n / 2)) / 2

    Map(
      "min" -> orden.head,
      "max" -> orden.last,
      "media" -> media,
      "mediana" -> mediana,
      "desv" -> math.sqrt(varianza)
    )
  }
}

// ===============================
// PROGRAMA PRINCIPAL
// ===============================
object LimpiezaYAnalisis extends IOApp.Simple {

  val path = Path("src/main/resources/data/pi_movies_complete.csv")

  def run: IO[Unit] =
    Files[IO]
      .readAll(path)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[Movie](';'))
      .compile
      .toList
      .flatMap { datasetInicial =>

        val total = datasetInicial.size

        val calidadBudget =
          EvaluadorDatos.evaluarNumerico("budget", datasetInicial.map(_.budget), total)

        val postFiltroBasico =
          ProcesoDepuracion.filtroBasico(datasetInicial)

        val postValidacionRangos =
          ProcesoDepuracion.validarRangos(postFiltroBasico)

        val datasetFinal =
          ProcesoDepuracion.filtrarAtipicosFlexible(postValidacionRangos)

        val statsBudget =
          ResumenEstadistico.calcular(datasetFinal.map(_.budget))

        (
          IO.println("=" * 95) >>
            IO.println("   INFORME DE EVALUACIÓN Y DEPURACIÓN DEL CONJUNTO DE DATOS CINEMATOGRÁFICO") >>
            IO.println("=" * 95) >>
            IO.println("") >>

            IO.println("1) REVISIÓN GENERAL DE CONSISTENCIA") >>
            IO.println("-" * 95) >>
            IO.println(s"Atributo evaluado: ${calidadBudget.atributo}") >>
            IO.println(s"Total de registros: ${calidadBudget.totalRegistros}") >>
            IO.println(s"Valores inválidos: ${calidadBudget.valoresInvalidos}") >>
            IO.println(s"Valores cero: ${calidadBudget.valoresCero}") >>
            IO.println(s"Valores negativos: ${calidadBudget.valoresNegativos}") >>
            IO.println(f"Porcentaje válido: ${calidadBudget.porcentajeCorrectos}%.2f%%") >>
            IO.println("") >>

            IO.println("2) PROCESO DE DEPURACIÓN APLICADO") >>
            IO.println("-" * 95) >>
            IO.println(s"Registros iniciales:        $total") >>
            IO.println(s"Tras filtros básicos:      ${postFiltroBasico.size}") >>
            IO.println(s"Tras validación de rangos: ${postValidacionRangos.size}") >>
            IO.println(s"Dataset final depurado:    ${datasetFinal.size}") >>
            IO.println("") >>

            IO.println("3) RESUMEN ESTADÍSTICO DEL DATASET FINAL") >>
            IO.println("-" * 95) >>
            IO.println(f"Presupuesto promedio: ${statsBudget("media")}%.2f") >>
            IO.println(f"Presupuesto mínimo:   ${statsBudget("min")}%.2f") >>
            IO.println(f"Presupuesto máximo:   ${statsBudget("max")}%.2f") >>
            IO.println(f"Desviación estándar:  ${statsBudget("desv")}%.2f") >>
            IO.println("") >>

            IO.println("=" * 95) >>
            IO.println("✔ Evaluación y depuración completadas correctamente") >>
            IO.println("✔ Dataset preparado para análisis exploratorio") >>
            IO.println("=" * 95)    
          )
      }
}
