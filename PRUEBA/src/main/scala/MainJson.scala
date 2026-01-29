import cats.effect.{IO, IOApp}
import fs2.Stream
import fs2.io.file.{Files, Path}
import fs2.text
import fs2.data.csv._
import fs2.data.csv.lenient._
import models._
import utilities.JsonCleaner

object MainJson extends IOApp.Simple {

  val inputPath =
    Path("src/main/resources/data/pi_movies_complete.csv")
  val outputDir = "src/main/resources/data/normalized/"

  def escribir[T](datos: List[T], nombreArchivo: String)
                 (implicit enc: CsvRowEncoder[T, String]): IO[Unit] = {
    if (datos.isEmpty) IO.println(s"- $nombreArchivo está vacío")
    else {
      val fullPath = Path(outputDir) / nombreArchivo

      Stream.emits(datos)
        .covary[IO]
        .through(encodeUsingFirstHeaders(fullRows = true))
        .through(text.utf8.encode)
        .through(Files[IO].writeAll(fullPath))
        .compile.drain *> IO.println(s"- Generado: $nombreArchivo (${datos.size} filas)")
    }
  }


  // eivtamos fuplicados tomando la primera aparición de cada id y devuelven lista ordenada
  private def dedupByIntId[T](items: List[T])(idFn: T => Int): List[T] =
    items.groupBy(idFn).map { case (_, vs) => vs.head }.toList.sortBy(idFn)

  private def dedupByStringId[T](items: List[T])(idFn: T => String): List[T] =
    items.groupBy(idFn).map { case (_, vs) => vs.head }.toList.sortBy(idFn)

  // main
  val run: IO[Unit] = {
    println("=== INICIANDO EXTRACCIÓN NORMALIZADA (ENTIDADES Y RELACIONES) ===")

    val readStream =
      Files[IO].readAll(inputPath)
        .through(text.utf8.decode)
        .through(attemptDecodeUsingHeaders[MovieInput](';'))
        .collect { case Right(m) => m }

    readStream.compile.toList.flatMap { rawMovies =>

      // filtramos solo películas con id valido
      val validMovies = rawMovies.filter(m => m.id.trim.toIntOption.exists(_ > 0))

      // evitamos duplicados por id
      val movies = validMovies
        .groupBy(_.id.trim.toInt)
        .map { case (_, duplicates) => duplicates.head }
        .toList
        .sortBy(_.id.trim.toInt)

      println(s"- Películas únicas procesadas: ${movies.size} (de ${rawMovies.size} totales)")
      println("==️ Procesando entidades y relaciones ==")

      // 1. GENRES
      val allGenresData = movies.flatMap { m =>
        val movieId = m.id.trim.toInt
        JsonCleaner.parseList[GenreJson](m.genres).map { g =>
          (GenreEntity(g.id, g.name), MovieGenreLink(movieId, g.id))
        }
      }
      // dedup por genre_id (primera aparición)
      val uniqueGenresRaw = allGenresData.map(_._1)
      val uniqueGenres = dedupByIntId(uniqueGenresRaw)(_.genre_id)
      val genreLinks   = allGenresData.map(_._2)

      // 2. COMPANIES
      val allCompanyData = movies.flatMap { m =>
        val movieId = m.id.trim.toInt
        JsonCleaner.parseList[CompanyJson](m.production_companies).map { c =>
          (CompanyEntity(c.id, c.name), MovieCompanyLink(movieId, c.id))
        }
      }
      val uniqueCompaniesRaw = allCompanyData.map(_._1)
      val uniqueCompanies = dedupByIntId(uniqueCompaniesRaw)(_.company_id)
      val companyLinks    = allCompanyData.map(_._2)

      // 3. COUNTRIES
      val allCountryData = movies.flatMap { m =>
        val movieId = m.id.trim.toInt
        JsonCleaner.parseList[CountryJson](m.production_countries).map { c =>
          (CountryEntity(c.iso_3166_1, c.name), MovieCountryLink(movieId, c.iso_3166_1))
        }
      }
      val uniqueCountriesRaw = allCountryData.map(_._1)
      val uniqueCountries = dedupByStringId(uniqueCountriesRaw)(_.iso_3166_1)
      val countryLinks    = allCountryData.map(_._2)

      // 4. LANGUAGES
      val allLangData = movies.flatMap { m =>
        val movieId = m.id.trim.toInt
        JsonCleaner.parseList[LanguageJson](m.spoken_languages).map { l =>
          (LanguageEntity(l.iso_639_1, l.name), MovieLanguageLink(movieId, l.iso_639_1))
        }
      }
      val uniqueLanguagesRaw = allLangData.map(_._1)
      val uniqueLanguages = dedupByStringId(uniqueLanguagesRaw)(_.iso_639_1)
      val langLinks       = allLangData.map(_._2)

      // 5. KEYWORDS
      val allKeyData = movies.flatMap { m =>
        val movieId = m.id.trim.toInt
        JsonCleaner.parseList[KeywordJson](m.keywords).map { k =>
          (KeywordEntity(k.id, k.name), MovieKeywordLink(movieId, k.id))
        }
      }
      val uniqueKeywordsRaw = allKeyData.map(_._1)
      val uniqueKeywords = dedupByIntId(uniqueKeywordsRaw)(_.keyword_id)
      val keywordLinks   = allKeyData.map(_._2)

      // 6. COLLECTIONS
      val allCollectionsRaw = movies.flatMap { m =>
        m.belongs_to_collection.flatMap(raw => JsonCleaner.parseObject[CollectionJson](raw)).map { c =>
          CollectionEntity(c.id, c.name, c.poster_path.getOrElse(""), c.backdrop_path.getOrElse(""))
        }
      }
      val uniqueCollections = dedupByIntId(allCollectionsRaw)(_.collection_id)

      // 7. PEOPLE (CAST & CREW)
      val castData = movies.flatMap { m =>
        val movieId = m.id.trim.toInt
        JsonCleaner.parseList[CastJson](m.cast).map { c =>
          (
            PersonEntity(
              c.id,
              c.name,
              c.gender.getOrElse(0),
              c.profile_path.getOrElse("")
            ),
            MovieCastLink(movieId, c.id, c.character, c.order, c.credit_id, c.cast_id)
          )
        }
      }

      val crewData = movies.flatMap { m =>
        val movieId = m.id.trim.toInt
        JsonCleaner.parseList[CrewJson](m.crew).map { c =>
          (
            PersonEntity(
              c.id,
              c.name,
              c.gender.getOrElse(0),
              c.profile_path.getOrElse("")
            ),
            MovieCrewLink(movieId, c.id, c.department, c.job, c.credit_id)
          )
        }
      }

      val allPeople = castData.map(_._1) ++ crewData.map(_._1)

      val uniquePeople = allPeople
        .groupBy(_.person_id)
        .map { case (_, people) => people.head }
        .toList
        .sortBy(_.person_id)

      val castLinks = castData.map(_._2)
      val crewLinks = crewData.map(_._2)

      // 8. USERS & RATINGS
      val ratingsData = movies.flatMap { m =>
        val movieId = m.id.trim.toInt
        JsonCleaner.parseList[RatingJson](m.ratings).map { r =>
          (UserEntity(r.userId), RatingLink(movieId, r.userId, r.rating, r.timestamp))
        }
      }
      val uniqueUsers = ratingsData.map(_._1).distinct
      val ratingLinks = ratingsData.map(_._2)

      // ESCRITURA
      println(s"=== Guardando archivos en: $outputDir ===")

      for {
        // entidades
        _ <- escribir(uniqueCollections, "Entity_Collections.csv")
        _ <- escribir(uniqueCompanies,   "Entity_Companies.csv")
        _ <- escribir(uniqueCountries,   "Entity_Countries.csv")
        _ <- escribir(uniqueGenres,      "Entity_Genres.csv")
        _ <- escribir(uniqueKeywords,    "Entity_Keywords.csv")
        _ <- escribir(uniqueLanguages,   "Entity_Languages.csv")
        _ <- escribir(uniquePeople,      "Entity_People.csv")
        _ <- escribir(uniqueUsers,       "Entity_Users.csv")

        // relaciones
        _ <- escribir(castLinks,    "Link_Movie_Cast.csv")
        _ <- escribir(companyLinks, "Link_Movie_Companies.csv")
        _ <- escribir(countryLinks, "Link_Movie_Countries.csv")
        _ <- escribir(crewLinks,    "Link_Movie_Crew.csv")
        _ <- escribir(genreLinks,   "Link_Movie_Genres.csv")
        _ <- escribir(keywordLinks, "Link_Movie_Keywords.csv")
        _ <- escribir(langLinks,    "Link_Movie_Languages.csv")
        _ <- escribir(ratingLinks,  "Link_Movie_Ratings.csv")

        _ <- IO.println("\n === PROCESO TERMINADO: Archivos generados correctamente ===")
      } yield ()
    }
  }
}
