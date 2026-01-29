package analysis

import cats.effect.{IO, IOApp}
import cats.syntax.all._
import fs2.io.file.{Files, Path}
import fs2.text
import fs2.data.csv._
import models.MovieInput
import analysis.AnalysisUtils._

object AnalysisRunner extends IOApp.Simple {

  val inputPath = Path("src/main/resources/data/pi_movies_complete.csv")

  def imprimirTitulo(titulo: String): IO[Unit] = {
    IO.println("") >>
      IO.println(titulo) >>
      IO.println("-" * 100)
  }

  val run: IO[Unit] = {

    val readStream = Files[IO].readAll(inputPath)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[MovieInput](';'))
      .attempt
      .collect { case Right(m) => m }
      .compile
      .toList

    for {
      _ <- IO.println("=" * 100)
      _ <- IO.println("ANÁLISIS EXPLORATORIO DE DATOS (EDA)")
      _ <- IO.println("=" * 100)

      movies <- readStream
      _ <- IO.println(f"Total registros procesados: ${movies.size}%,d")


      statsNumericos = List(
        calcularStatsNumericos("Budget",       movies.map(_.budget)),
        calcularStatsNumericos("ID",           movies.map(_.id)),
        calcularStatsNumericos("Popularity",   movies.map(_.popularity)),
        calcularStatsNumericos("Revenue",      movies.map(_.revenue)),
        calcularStatsNumericos("Runtime",      movies.flatMap(_.runtime.toList)), // Filtra los Nones antes de calcular
        calcularStatsNumericos("Vote Average", movies.map(_.vote_average)),
        calcularStatsNumericos("Vote Count",   movies.map(_.vote_count))
      )

      statsTexto = List(
        calcularStatsTexto("Homepage",      movies.map(_.homepage)),
        calcularStatsTexto("IMDB ID",       movies.map(_.imdb_id)),
        calcularStatsTexto("Orig. Language",movies.map(m => Some(m.original_language))),
        calcularStatsTexto("Orig. Title",   movies.map(m => Some(m.original_title))),
        calcularStatsTexto("Overview",      movies.map(_.overview)),
        calcularStatsTexto("Poster Path",   movies.map(_.poster_path)),
        calcularStatsTexto("Status",        movies.map(_.status)),
        calcularStatsTexto("Tagline",       movies.map(_.tagline)),
        calcularStatsTexto("Title",         movies.map(m => Some(m.title)))
      )
      
      _ <- imprimirTitulo("ESTADÍSTICAS VARIABLES NUMÉRICAS")
      _ <- statsNumericos.traverse(s => IO.println(formatearNumericos(s)))

      _ <- imprimirTitulo("ANÁLISIS DE FRECUENCIA - VARIABLES DE TEXTO (TOP 5)")
      _ <- statsTexto.traverse(s => IO.println(formatearTexto(s)))
      
    } yield ()
  }
}