import cats.effect.{IO, IOApp}
import cats.syntax.all.*
import fs2.io.file.{Files, Path}
import fs2.text
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*

// ============================================================================
// MODELO PROCESADO
// ============================================================================

case class MovieProcesada(
                           id: Int,
                           budget: Option[Double],
                           revenue: Option[Double],
                           runtime: Option[Int],
                           popularity: Double,
                           vote_average: Double,
                           vote_count: Int,
                           release_year: Option[Int]
                         )

// ============================================================================
// MODELO CSV
// ============================================================================

case class MovieNumeric(
                         id: String,
                         budget: String,
                         revenue: String,
                         runtime: String,
                         popularity: String,
                         vote_average: String,
                         vote_count: String,
                         release_date: String
                       )

given CsvRowDecoder[MovieNumeric, String] =
  deriveCsvRowDecoder[MovieNumeric]

// ============================================================================
// PARSER FUNCIONAL
// ============================================================================

object ParserFuncional {

  def parseDoubleSeguro(valor: String): Option[Double] = {
    val limpio = valor.trim
    if (limpio.isEmpty || limpio == "0") None
    else
      try Some(limpio.toDouble)
      catch case _: Exception => None
  }

  def parseIntSeguro(valor: String): Option[Int] = {
    val limpio = valor.trim
    if (limpio.isEmpty) None
    else
      try Some(limpio.toInt)
      catch case _: Exception => None
  }

  def extraerAnio(fecha: String): Option[Int] =
    if (fecha.length >= 4)
      parseIntSeguro(fecha.substring(0, 4))
    else
      None

  def procesarMovie(movie: MovieNumeric): MovieProcesada =
    MovieProcesada(
      id = parseIntSeguro(movie.id).getOrElse(0),
      budget = parseDoubleSeguro(movie.budget),
      revenue = parseDoubleSeguro(movie.revenue),
      runtime = parseIntSeguro(movie.runtime),
      popularity = parseDoubleSeguro(movie.popularity).getOrElse(0.0),
      vote_average = parseDoubleSeguro(movie.vote_average).getOrElse(0.0),
      vote_count = parseIntSeguro(movie.vote_count).getOrElse(0),
      release_year = extraerAnio(movie.release_date)
    )
}

// ============================================================================
// MAIN
// ============================================================================

object LecturaColumnasNumericas extends IOApp.Simple {

  // 🔴 ÚNICO CAMBIO: ruta absoluta al archivo
  val filePath =
    Path("C:\\Users\\Luis\\Desktop\\PRUEBA\\src\\main\\resources\\data\\pi_movies_complete.csv")

  override def run: IO[Unit] = {

    val lecturaCSV: IO[List[MovieNumeric]] =
      Files[IO]
        .readAll(filePath)
        .through(text.utf8.decode)
        .through(decodeUsingHeaders[MovieNumeric](','))
        .compile
        .toList

    lecturaCSV.flatMap { moviesRaw =>

      val moviesProcesadas =
        moviesRaw.map(ParserFuncional.procesarMovie)

      val budgets       = moviesProcesadas.flatMap(_.budget)
      val revenues      = moviesProcesadas.flatMap(_.revenue)
      val runtimes      = moviesProcesadas.flatMap(_.runtime)
      val popularities  = moviesProcesadas.map(_.popularity)
      val voteAverages  = moviesProcesadas.map(_.vote_average)
      val voteCounts    = moviesProcesadas.map(_.vote_count)
      val releaseYears  = moviesProcesadas.flatMap(_.release_year)

      (
        IO.println("=" * 80) >>
          IO.println("5.2 LECTURA DE COLUMNAS NUMÉRICAS") >>
          IO.println("=" * 80) >>
          IO.println("") >>

          IO.println("📊 REPORTE DE LECTURA") >>
          IO.println("-" * 40) >>
          IO.println(s"Total de películas leídas: ${moviesRaw.length}") >>
          IO.println(s"Películas procesadas: ${moviesProcesadas.length}") >>
          IO.println("") >>

          IO.println("📈 VALORES VÁLIDOS ENCONTRADOS") >>
          IO.println("-" * 40) >>
          IO.println(s"• Budget: ${budgets.length}") >>
          IO.println(s"• Revenue: ${revenues.length}") >>
          IO.println(s"• Runtime: ${runtimes.length}") >>
          IO.println(s"• Popularidad: ${popularities.length}") >>
          IO.println(s"• Vote Average: ${voteAverages.length}") >>
          IO.println(s"• Vote Count: ${voteCounts.length}") >>
          IO.println(s"• Release Year: ${releaseYears.length}") >>
          IO.println("") >>

          IO.println("📝 EJEMPLOS (primeras 3 películas)") >>
          IO.println("-" * 40) >>

          moviesProcesadas.take(3).zipWithIndex.traverse_ {
            case (movie, idx) =>
              IO.println(s"Película ${idx + 1}:") >>
                IO.println(s"  ID: ${movie.id}") >>
                movie.budget.traverse_(b => IO.println(f"  Budget: $$$b%.0f")) >>
                movie.revenue.traverse_(r => IO.println(f"  Revenue: $$$r%.0f")) >>
                movie.runtime.traverse_(rt => IO.println(s"  Runtime: ${rt} min")) >>
                IO.println(f"  Popularity: ${movie.popularity}%.2f") >>
                IO.println(f"  Vote Average: ${movie.vote_average}%.1f / 10") >>
                IO.println(s"  Vote Count: ${movie.vote_count}") >>
                movie.release_year.traverse_(y => IO.println(s"  Year: $y")) >>
                IO.println("")
          } >>

          IO.println("✓ Lectura completada correctamente")
        )
    }
  }
}
