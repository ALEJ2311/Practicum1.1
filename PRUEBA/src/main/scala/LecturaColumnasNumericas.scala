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
                           vote_count: Int
                           // release_year ELIMINADO
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
                         vote_count: String
                         // release_date ELIMINADO
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

  // extraerAnio ELIMINADO

  def procesarMovie(movie: MovieNumeric): MovieProcesada =
    MovieProcesada(
      id = parseIntSeguro(movie.id).getOrElse(0),
      budget = parseDoubleSeguro(movie.budget),
      revenue = parseDoubleSeguro(movie.revenue),
      runtime = parseIntSeguro(movie.runtime),
      popularity = parseDoubleSeguro(movie.popularity).getOrElse(0.0),
      vote_average = parseDoubleSeguro(movie.vote_average).getOrElse(0.0),
      vote_count = parseIntSeguro(movie.vote_count).getOrElse(0)
      // release_year asignación ELIMINADA
    )
}

// ============================================================================
// MAIN
// ============================================================================

object LecturaColumnasNumericas extends IOApp.Simple {

  // Ruta intacta
  val filePath =
    Path("C:\\Users\\Luis\\Desktop\\PRUEBA\\src\\main\\resources\\data\\pi_movies_complete.csv")

  override def run: IO[Unit] = {

    val lecturaCSV: IO[List[MovieNumeric]] =
      Files[IO]
        .readAll(filePath)
        .through(text.utf8.decode)
        .through(decodeUsingHeaders[MovieNumeric](';'))
        .compile
        .toList

    lecturaCSV.flatMap { moviesRaw =>

      val moviesProcesadas =
        moviesRaw
          .map(ParserFuncional.procesarMovie)
          // Filtro intacto
          .filter(_.id != 0)

      val budgets       = moviesProcesadas.flatMap(_.budget)
      val revenues      = moviesProcesadas.flatMap(_.revenue)
      val runtimes      = moviesProcesadas.flatMap(_.runtime)
      val popularities  = moviesProcesadas.map(_.popularity)
      val voteAverages  = moviesProcesadas.map(_.vote_average)
      val voteCounts    = moviesProcesadas.map(_.vote_count)
      // releaseYears cálculo ELIMINADO

      (
        IO.println("=" * 80) >>
          IO.println("5.2 LECTURA DE COLUMNAS NUMÉRICAS") >>
          IO.println("=" * 80) >>
          IO.println("") >>

          IO.println("📊 REPORTE DE LECTURA") >>
          IO.println("-" * 40) >>
          IO.println(s"Total de filas leídas (raw): ${moviesRaw.length}") >>
          IO.println(s"Películas válidas procesadas: ${moviesProcesadas.length}") >>
          IO.println("") >>

          IO.println("📈 VALORES VÁLIDOS ENCONTRADOS") >>
          IO.println("-" * 40) >>
          IO.println(s"• Budget: ${budgets.length}") >>
          IO.println(s"• Revenue: ${revenues.length}") >>
          IO.println(s"• Runtime: ${runtimes.length}") >>
          IO.println(s"• Popularidad: ${popularities.length}") >>
          IO.println(s"• Vote Average: ${voteAverages.length}") >>
          IO.println(s"• Vote Count: ${voteCounts.length}")
        // Print de Release Year ELIMINADO

        )
    }
  }
}