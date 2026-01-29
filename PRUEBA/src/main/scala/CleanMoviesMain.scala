import cats.effect.{IO, IOApp}
import fs2.Stream
import fs2.io.file.{Files, Path}
import fs2.text
import fs2.data.csv._
import fs2.data.csv.lenient._
import models.{MovieInput, MovieClean, CollectionJson}
import utilities.{JsonCleaner, CleaningRules}

object CleanMoviesMain extends IOApp.Simple {

  val inputPath = Path("src/main/resources/data/pi_movies_complete.csv")

  val outputPath = Path("src/main/resources/data/normalized/clean_movies.csv")

  def transformar(raw: MovieInput): Option[MovieClean] = {

    // valida id
    val idValido = CleaningRules.cleanId(raw.id)

    idValido.map { id =>
      val collId = raw.belongs_to_collection.flatMap { json =>
        JsonCleaner.parseObject[CollectionJson](json).map(_.id)
      }

      // contruir objetos
      MovieClean(
        adult             = CleaningRules.cleanAdult(raw.adult),
        budget            = CleaningRules.cleanDouble(raw.budget),
        homepage          = CleaningRules.cleanUrl(raw.homepage),
        id                = id,
        imdb_id           = CleaningRules.cleanImdb(raw.imdb_id),
        original_language = CleaningRules.cleanLang(raw.original_language),

        // cleanText evita que los enter
        original_title    = CleaningRules.cleanText(if (raw.original_title.trim.nonEmpty) raw.original_title else "Untitled"),
        overview          = raw.overview.map(CleaningRules.cleanText).filter(_.nonEmpty),

        popularity        = CleaningRules.cleanDouble(raw.popularity),
        poster_path       = CleaningRules.cleanPath(raw.poster_path),
        release_date      = CleaningRules.cleanDate(raw.release_date),
        revenue           = CleaningRules.cleanDouble(raw.revenue),
        runtime           = raw.runtime.flatMap(CleaningRules.cleanDouble),
        status            = CleaningRules.cleanStatus(raw.status),

        tagline           = raw.tagline.map(CleaningRules.cleanText).filter(_.nonEmpty),
        title             = CleaningRules.cleanText(if (raw.title.trim.nonEmpty) raw.title else "Untitled"),

        video             = CleaningRules.cleanAdult(raw.video),
        vote_average      = CleaningRules.cleanDouble(raw.vote_average),
        vote_count        = CleaningRules.cleanDouble(raw.vote_count),
        collection_id     = collId
      )
    }
  }

  val run: IO[Unit] = {
    println("=== INICIANDO LIMPIEZA DE TEXTO ===")

    val readStream = Files[IO].readAll(inputPath)
      .through(text.utf8.decode)
      .through(attemptDecodeUsingHeaders[MovieInput](';'))
      .collect { case Right(m) => m }

    readStream.compile.toList.flatMap { rawList =>
      println(s"- Leídas: ${rawList.size}")

      val validMovies = rawList.flatMap(transformar)

      // evitamos duplicados por id se toma la primera aparición
      val uniqueMovies = validMovies
        .groupBy(_.id)
        .map { case (_, movies) => movies.head }
        .toList
        .sortBy(_.id)

      println(s"- Películas Únicas: ${uniqueMovies.size}")

      if (uniqueMovies.nonEmpty) {
        Stream.emits(uniqueMovies)
          .covary[IO]
          .through(encodeUsingFirstHeaders(fullRows = true))
          .through(text.utf8.encode)
          .through(Files[IO].writeAll(outputPath))
          .compile.drain *> IO.println(s"- Archivo guardado correctamente en: $outputPath")
      } else {
        IO.println("- ERROR: Lista vacía.")
      }
    }
  }
}