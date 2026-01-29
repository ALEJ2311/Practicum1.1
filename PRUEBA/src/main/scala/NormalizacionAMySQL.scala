import cats.effect.{IO, IOApp}
import cats.syntax.all.*
import fs2.text
import fs2.Stream
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*
import doobie.*
import doobie.implicits.*
import models._
import utilities.CleaningRules.parseDate
import scala.util.Try

object NormalizacionAMySQL extends IOApp.Simple {

  private val xa = Transactor.fromDriverManager[IO](
    driver = "com.mysql.cj.jdbc.Driver",
    url = "jdbc:mysql://localhost:3306/",
    user = "root",
    password = "datosutpl",
    logHandler = None
  )

  val appxa = Transactor.fromDriverManager[IO](
    driver = "com.mysql.cj.jdbc.Driver",
    url = "jdbc:mysql://localhost:3306/MoviesDatabase",
    user = "root",
    password = "datosutpl",
    logHandler = None
  )

  // creacion de tablas

  private def crearTablas(): ConnectionIO[Unit] = {
    val queries = List(
      // PRE
      sql"""SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0""".update.run,
      sql"""SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0""".update.run,
      sql"""SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'""".update.run,

      // SCHEMA + USE
      sql"""CREATE SCHEMA IF NOT EXISTS MoviesDatabase DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci""".update.run,
      sql"""USE MoviesDatabase""".update.run,

      // Collection
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.Collection (
        collection_id INT NOT NULL,
        name VARCHAR(255) NOT NULL,
        poster_path VARCHAR(255) NOT NULL,
        backdrop_path VARCHAR(255) NOT NULL,
        PRIMARY KEY (collection_id)
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,
      sql"""CREATE UNIQUE INDEX name_Collection_UNIQUE ON MoviesDatabase.Collection (name ASC) VISIBLE""".update.run,

      // Movie
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.Movie (
        movie_id INT NOT NULL,
        title VARCHAR(255) NULL,
        original_title VARCHAR(255) NOT NULL,
        original_language VARCHAR(10) NULL DEFAULT NULL,
        overview TEXT NULL DEFAULT NULL,
        release_date DATE NULL DEFAULT NULL,
        runtime INT NOT NULL,
        budget DECIMAL(15,2) NULL DEFAULT 0,
        revenue DECIMAL(15,2) NULL DEFAULT 0,
        popularity DECIMAL(10,2) NULL DEFAULT 0.0,
        vote_average DECIMAL(3,1) NULL DEFAULT 0,
        vote_count INT NULL DEFAULT NULL,
        adult TINYINT(1) NOT NULL,
        video TINYINT(1) NULL DEFAULT NULL,
        status VARCHAR(50) NULL DEFAULT NULL,
        homepage VARCHAR(255) NULL DEFAULT NULL,
        imdb_id VARCHAR(20) NULL,
        poster_path VARCHAR(255) NULL DEFAULT NULL,
        tagline VARCHAR(255) NULL DEFAULT NULL,
        collection_id INT NULL,
        PRIMARY KEY (movie_id),
        CONSTRAINT fk_movie_collection1
          FOREIGN KEY (collection_id)
          REFERENCES MoviesDatabase.Collection (collection_id)
          ON DELETE NO ACTION
          ON UPDATE NO ACTION
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,
      sql"""CREATE INDEX fk_movie_collection1_idx ON MoviesDatabase.Movie (collection_id ASC) VISIBLE""".update.run,
      sql"""CREATE UNIQUE INDEX imdb_id_UNIQUE ON MoviesDatabase.Movie (imdb_id ASC) VISIBLE""".update.run,

      // Person
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.Person (
        person_id INT NOT NULL,
        name VARCHAR(255) NOT NULL,
        gender INT NOT NULL,
        profile_path VARCHAR(255) NULL DEFAULT NULL,
        PRIMARY KEY (person_id)
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,

      // Cast
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.Cast (
        movie_id INT NOT NULL,
        person_id INT NOT NULL,
        character_name VARCHAR(255) NOT NULL,
        cast_order INT NOT NULL,
        credit_id VARCHAR(50) NOT NULL,
        cast_id INT NOT NULL,
        PRIMARY KEY (movie_id, person_id, cast_id),
        CONSTRAINT cast_ibfk_1
          FOREIGN KEY (movie_id)
          REFERENCES MoviesDatabase.Movie (movie_id),
        CONSTRAINT cast_ibfk_2
          FOREIGN KEY (person_id)
          REFERENCES MoviesDatabase.Person (person_id)
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,
      sql"""CREATE INDEX person_id ON MoviesDatabase.Cast (person_id ASC) VISIBLE""".update.run,

      // Crew
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.Crew (
        movie_id INT NOT NULL,
        person_id INT NOT NULL,
        department VARCHAR(100) NOT NULL,
        job VARCHAR(100) NOT NULL,
        credit_id VARCHAR(50) NOT NULL,
        PRIMARY KEY (movie_id, person_id, job),
        CONSTRAINT crew_ibfk_1
          FOREIGN KEY (movie_id)
          REFERENCES MoviesDatabase.Movie (movie_id),
        CONSTRAINT crew_ibfk_2
          FOREIGN KEY (person_id)
          REFERENCES MoviesDatabase.Person (person_id)
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,
      sql"""CREATE INDEX person_id ON MoviesDatabase.Crew (person_id ASC) VISIBLE""".update.run,

      // Genre
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.Genre (
        genre_id INT NOT NULL,
        name VARCHAR(50) NOT NULL,
        PRIMARY KEY (genre_id)
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,
      sql"""CREATE UNIQUE INDEX name_Genre_UNIQUE ON MoviesDatabase.Genre (name ASC) VISIBLE""".update.run,

      // Keyword
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.Keyword (
        keyword_id INT NOT NULL,
        name VARCHAR(100) NOT NULL,
        PRIMARY KEY (keyword_id)
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,
      sql"""CREATE UNIQUE INDEX name_Keyword_UNIQUE ON MoviesDatabase.Keyword (name ASC) VISIBLE""".update.run,

      // SpokenLanguage
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.SpokenLanguage (
        iso_639_1 VARCHAR(10) NOT NULL,
        name VARCHAR(100) NOT NULL,
        PRIMARY KEY (iso_639_1)
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,

      // Movie_Genre
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.Movie_Genre (
        movie_id INT NOT NULL,
        genre_id INT NOT NULL,
        PRIMARY KEY (movie_id, genre_id),
        CONSTRAINT movie_genre_ibfk_1
          FOREIGN KEY (movie_id)
          REFERENCES MoviesDatabase.Movie (movie_id),
        CONSTRAINT movie_genre_ibfk_2
          FOREIGN KEY (genre_id)
          REFERENCES MoviesDatabase.Genre (genre_id)
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,
      sql"""CREATE INDEX genre_id ON MoviesDatabase.Movie_Genre (genre_id ASC) VISIBLE""".update.run,

      // Movie_Keyword
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.Movie_Keyword (
        movie_id INT NOT NULL,
        keyword_id INT NOT NULL,
        PRIMARY KEY (movie_id, keyword_id),
        CONSTRAINT movie_keyword_ibfk_1
          FOREIGN KEY (movie_id)
          REFERENCES MoviesDatabase.Movie (movie_id),
        CONSTRAINT movie_keyword_ibfk_2
          FOREIGN KEY (keyword_id)
          REFERENCES MoviesDatabase.Keyword (keyword_id)
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,
      sql"""CREATE INDEX keyword_id ON MoviesDatabase.Movie_Keyword (keyword_id ASC) VISIBLE""".update.run,

      // Movie_SpokenLanguage
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.Movie_SpokenLanguage (
        movie_id INT NOT NULL,
        language_code VARCHAR(10) NOT NULL,
        PRIMARY KEY (movie_id, language_code),
        CONSTRAINT movie_language_ibfk_1
          FOREIGN KEY (movie_id)
          REFERENCES MoviesDatabase.Movie (movie_id),
        CONSTRAINT movie_language_ibfk_2
          FOREIGN KEY (language_code)
          REFERENCES MoviesDatabase.SpokenLanguage (iso_639_1)
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,
      sql"""CREATE INDEX language_code ON MoviesDatabase.Movie_SpokenLanguage (language_code ASC) VISIBLE""".update.run,

      // ProductionCompany
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.ProductionCompany (
        company_id INT NOT NULL,
        name VARCHAR(255) NOT NULL,
        PRIMARY KEY (company_id)
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,

      // Movie_ProductionCompany
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.Movie_ProductionCompany (
        movie_id INT NOT NULL,
        company_id INT NOT NULL,
        PRIMARY KEY (movie_id, company_id),
        CONSTRAINT movie_productioncompany_ibfk_1
          FOREIGN KEY (movie_id)
          REFERENCES MoviesDatabase.Movie (movie_id),
        CONSTRAINT movie_productioncompany_ibfk_2
          FOREIGN KEY (company_id)
          REFERENCES MoviesDatabase.ProductionCompany (company_id)
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,
      sql"""CREATE INDEX company_id ON MoviesDatabase.Movie_ProductionCompany (company_id ASC) VISIBLE""".update.run,

      // ProductionCountry
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.ProductionCountry (
        iso_3166_1 CHAR(2) NOT NULL,
        name VARCHAR(100) NOT NULL,
        PRIMARY KEY (iso_3166_1)
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,
      sql"""CREATE UNIQUE INDEX name_ProductionCountry_UNIQUE ON MoviesDatabase.ProductionCountry (name ASC) VISIBLE""".update.run,

      // Movie_ProductionCountry
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.Movie_ProductionCountry (
        movie_id INT NOT NULL,
        country_code CHAR(2) NOT NULL,
        PRIMARY KEY (movie_id, country_code),
        CONSTRAINT movie_productioncountry_ibfk_1
          FOREIGN KEY (movie_id)
          REFERENCES MoviesDatabase.Movie (movie_id),
        CONSTRAINT movie_productioncountry_ibfk_2
          FOREIGN KEY (country_code)
          REFERENCES MoviesDatabase.ProductionCountry (iso_3166_1)
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,
      sql"""CREATE INDEX country_code ON MoviesDatabase.Movie_ProductionCountry (country_code ASC) VISIBLE""".update.run,

      // User
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.User (
        user_id INT NOT NULL,
        PRIMARY KEY (user_id)
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,

      // Rating
      sql"""
      CREATE TABLE IF NOT EXISTS MoviesDatabase.Rating (
        user_id INT NOT NULL,
        movie_id INT NOT NULL,
        rating DECIMAL(2,1) NOT NULL,
        timestamp BIGINT NOT NULL,
        PRIMARY KEY (user_id, movie_id),
        CONSTRAINT rating_ibfk_1
          FOREIGN KEY (user_id)
          REFERENCES MoviesDatabase.User (user_id),
        CONSTRAINT rating_ibfk_2
          FOREIGN KEY (movie_id)
          REFERENCES MoviesDatabase.Movie (movie_id)
      ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
      """.update.run,
      sql"""CREATE INDEX movie_id ON MoviesDatabase.Rating (movie_id ASC) VISIBLE""".update.run,

      // POST
      sql"""SET SQL_MODE=@OLD_SQL_MODE""".update.run,
      sql"""SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS""".update.run,
      sql"""SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS""".update.run
    )

    queries.traverse_(identity)
  }

  // insert por fila

  private def insertMovie(m: MovieClean): ConnectionIO[Int] =
    sql"""
      INSERT INTO Movie (
        movie_id,
        title,
        original_title,
        original_language,
        overview,
        release_date,
        runtime,
        budget,
        revenue,
        popularity,
        vote_average,
        vote_count,
        adult,
        video,
        status,
        homepage,
        imdb_id,
        poster_path,
        tagline,
        collection_id
      )
      VALUES (
        ${m.id},
        ${m.title},
        ${m.original_title},
        ${Option(m.original_language).filterNot(_.isEmpty)},
        ${m.overview},
        ${m.release_date.map(d => java.sql.Date.valueOf(d))},
        ${m.runtime.map(_.toInt).getOrElse(0)},
        ${m.budget},
        ${m.revenue},
        ${m.popularity},
        ${m.vote_average},
        ${m.vote_count},
        ${m.adult},
        ${m.video},
        ${m.status},
        ${m.homepage},
        ${m.imdb_id},
        ${m.poster_path},
        ${m.tagline},
        ${m.collection_id}
      )
    """.update.run

  private def insertGenre(g: GenreEntity): ConnectionIO[Int] =
    sql"INSERT INTO Genre (genre_id, name) VALUES (${g.genre_id}, ${g.name})".update.run

  private def insertCompany(c: CompanyEntity): ConnectionIO[Int] =
    sql"INSERT INTO ProductionCompany (company_id, name) VALUES (${c.company_id}, ${c.name})".update.run

  private def insertCountry(c: CountryEntity): ConnectionIO[Int] =
    sql"INSERT INTO ProductionCountry (iso_3166_1, name) VALUES (${c.iso_3166_1}, ${c.name})".update.run

  private def insertLanguage(l: LanguageEntity): ConnectionIO[Int] =
    sql"INSERT INTO SpokenLanguage (iso_639_1, name) VALUES (${l.iso_639_1}, ${l.name})".update.run

  private def insertKeyword(k: KeywordEntity): ConnectionIO[Int] =
    sql"INSERT INTO Keyword (keyword_id, name) VALUES (${k.keyword_id}, ${k.name})".update.run

  private def insertCollection(c: CollectionEntity): ConnectionIO[Int] =
    sql"INSERT INTO Collection (collection_id, name, poster_path, backdrop_path) VALUES (${c.collection_id}, ${c.name}, ${c.poster}, ${c.backdrop})".update.run

  private def insertPerson(p: PersonEntity): ConnectionIO[Int] =
    sql"INSERT INTO Person (person_id, name, gender, profile_path) VALUES (${p.person_id}, ${p.name}, ${p.gender}, ${p.profile_path})".update.run

  private def insertUser(u: UserEntity): ConnectionIO[Int] =
    sql"INSERT INTO User (user_id) VALUES (${u.user_id})".update.run

  // Links
  private def insertMovieGenre(l: MovieGenreLink): ConnectionIO[Int] =
    sql"INSERT INTO Movie_Genre (movie_id, genre_id) VALUES (${l.movie_id}, ${l.genre_id})".update.run

  private def insertMovieCompany(l: MovieCompanyLink): ConnectionIO[Int] =
    sql"INSERT INTO Movie_ProductionCompany (movie_id, company_id) VALUES (${l.movie_id}, ${l.company_id})".update.run

  private def insertMovieCountry(l: MovieCountryLink): ConnectionIO[Int] =
    sql"INSERT INTO Movie_ProductionCountry (movie_id, country_code) VALUES (${l.movie_id}, ${l.iso_3166_1})".update.run

  private def insertMovieLanguage(l: MovieLanguageLink): ConnectionIO[Int] =
    sql"INSERT INTO Movie_SpokenLanguage (movie_id, language_code) VALUES (${l.movie_id}, ${l.iso_639_1})".update.run

  private def insertMovieKeyword(l: MovieKeywordLink): ConnectionIO[Int] =
    sql"INSERT INTO Movie_Keyword (movie_id, keyword_id) VALUES (${l.movie_id}, ${l.keyword_id})".update.run

  private def insertMovieCast(l: MovieCastLink): ConnectionIO[Int] =
    sql"INSERT INTO Cast (movie_id, person_id, character_name, cast_order, credit_id, cast_id) VALUES (${l.movie_id}, ${l.person_id}, ${l.character_name}, ${l.order_cast}, ${l.credit_id}, ${l.cast_id})".update.run

  private def insertMovieCrew(l: MovieCrewLink): ConnectionIO[Int] =
    sql"INSERT INTO Crew (movie_id, person_id, department, job, credit_id) VALUES (${l.movie_id}, ${l.person_id}, ${l.department}, ${l.job}, ${l.credit_id})".update.run

  private def insertRating(r: RatingLink): ConnectionIO[Int] =
    sql"INSERT INTO Rating (user_id, movie_id, rating, timestamp) VALUES (${r.user_id}, ${r.movie_id}, ${r.rating}, ${r.timestamp})".update.run

  // funciones batch q ejecutan n inserts en una sola transacción
  // cada una recibe una lista y hace .traverse(insert...)

  private def insertMoviesChunk(ms: List[MovieClean]): ConnectionIO[Int] =
    ms.traverse(insertMovie).map(_.sum)

  private def insertGenresChunk(xs: List[GenreEntity]): ConnectionIO[Int] =
    xs.traverse(insertGenre).map(_.sum)

  private def insertCompaniesChunk(xs: List[CompanyEntity]): ConnectionIO[Int] =
    xs.traverse(insertCompany).map(_.sum)

  private def insertCountriesChunk(xs: List[CountryEntity]): ConnectionIO[Int] =
    xs.traverse(insertCountry).map(_.sum)

  private def insertLanguagesChunk(xs: List[LanguageEntity]): ConnectionIO[Int] =
    xs.traverse(insertLanguage).map(_.sum)

  private def insertKeywordsChunk(xs: List[KeywordEntity]): ConnectionIO[Int] =
    xs.traverse(insertKeyword).map(_.sum)

  private def insertCollectionsChunk(xs: List[CollectionEntity]): ConnectionIO[Int] =
    xs.traverse(insertCollection).map(_.sum)

  private def insertPeopleChunk(xs: List[PersonEntity]): ConnectionIO[Int] =
    xs.traverse(insertPerson).map(_.sum)

  private def insertUsersChunk(xs: List[UserEntity]): ConnectionIO[Int] =
    xs.traverse(insertUser).map(_.sum)

  // Links chunk
  private def insertMovieGenreChunk(xs: List[MovieGenreLink]): ConnectionIO[Int] =
    xs.traverse(insertMovieGenre).map(_.sum)

  private def insertMovieCompanyChunk(xs: List[MovieCompanyLink]): ConnectionIO[Int] =
    xs.traverse(insertMovieCompany).map(_.sum)

  private def insertMovieCountryChunk(xs: List[MovieCountryLink]): ConnectionIO[Int] =
    xs.traverse(insertMovieCountry).map(_.sum)

  private def insertMovieLanguageChunk(xs: List[MovieLanguageLink]): ConnectionIO[Int] =
    xs.traverse(insertMovieLanguage).map(_.sum)

  private def insertMovieKeywordChunk(xs: List[MovieKeywordLink]): ConnectionIO[Int] =
    xs.traverse(insertMovieKeyword).map(_.sum)

  private def insertMovieCastChunk(xs: List[MovieCastLink]): ConnectionIO[Int] =
    xs.traverse(insertMovieCast).map(_.sum)

  private def insertMovieCrewChunk(xs: List[MovieCrewLink]): ConnectionIO[Int] =
    xs.traverse(insertMovieCrew).map(_.sum)

  private def insertRatingChunk(xs: List[RatingLink]): ConnectionIO[Int] =
    xs.traverse(insertRating).map(_.sum)

  // funciones de lectura csv

  def leerCSV[T](archivo: String)(using decoder: CsvRowDecoder[T, String]): IO[List[T]] = {
    val ruta = Path(s"src/main/resources/data/normalized/$archivo")

    Files[IO]
      .readAll(ruta)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[T](','))
      .compile
      .toList
  }

  // main
  override def run: IO[Unit] = for {
    _ <- IO.println("=" * 100)
    _ <- IO.println("              CARGA DE DATOS NORMALIZADOS A MYSQL")
    _ <- IO.println("=" * 100)
    _ <- IO.println("")

    // Crear tablas
    _ <- IO.println("1. Creando tablas en MySQL")
    _ <- crearTablas().transact(xa)
    _ <- IO.println("   - Tablas creadas correctamente")
    _ <- IO.println("")

    // entidades por chunk
    _ <- IO.println("2. Cargando ENTIDADES (por lotes/chunks)")
    _ <- IO.println("-" * 100)

    // Genres (chunk)
    _ <- IO.println("   Cargando Genres")
    genres <- leerCSV[GenreEntity]("Entity_Genres.csv")
    _ <- Stream.emits(genres).covary[IO].chunkN(500).evalMap(chunk => insertGenresChunk(chunk.toList).transact(appxa)).compile.drain
    _ <- IO.println(s"   - ${genres.size} géneros insertados")

    // Companies (chunk)
    _ <- IO.println("   Cargando Companies")
    companies <- leerCSV[CompanyEntity]("Entity_Companies.csv")
    _ <- Stream.emits(companies).covary[IO].chunkN(500).evalMap(chunk => insertCompaniesChunk(chunk.toList).transact(appxa)).compile.drain
    _ <- IO.println(s"   - ${companies.size} compañías insertadas")

    // Countries (chunk)
    _ <- IO.println("   Cargando Countries")
    countries <- leerCSV[CountryEntity]("Entity_Countries.csv")
    _ <- Stream.emits(countries).covary[IO].chunkN(500).evalMap(chunk => insertCountriesChunk(chunk.toList).transact(appxa)).compile.drain
    _ <- IO.println(s"   - ${countries.size} países insertados")

    // Languages (chunk)
    _ <- IO.println("   Cargando Languages")
    languages <- leerCSV[LanguageEntity]("Entity_Languages.csv")
    _ <- Stream.emits(languages).covary[IO].chunkN(500).evalMap(chunk => insertLanguagesChunk(chunk.toList).transact(appxa)).compile.drain
    _ <- IO.println(s"   - ${languages.size} idiomas insertados")

    // Keywords (chunk)
    _ <- IO.println("   Cargando Keywords")
    keywords <- leerCSV[KeywordEntity]("Entity_Keywords.csv")
    _ <- Stream.emits(keywords).covary[IO].chunkN(500).evalMap(chunk => insertKeywordsChunk(chunk.toList).transact(appxa)).compile.drain
    _ <- IO.println(s"   - ${keywords.size} keywords insertadas")

    // Collections (chunk)
    _ <- IO.println("   Cargando Collections")
    collections <- leerCSV[CollectionEntity]("Entity_Collections.csv")
    _ <- Stream.emits(collections).covary[IO].chunkN(500).evalMap(chunk => insertCollectionsChunk(chunk.toList).transact(appxa)).compile.drain
    _ <- IO.println(s"   - ${collections.size} colecciones insertadas")

    // People (chunk)
    _ <- IO.println("   Cargando People")
    people <- leerCSV[PersonEntity]("Entity_People.csv")
    _ <- Stream.emits(people).covary[IO].chunkN(500).evalMap(chunk => insertPeopleChunk(chunk.toList).transact(appxa)).compile.drain
    _ <- IO.println(s"   - ${people.size} personas insertadas")

    // Users (chunk)
    _ <- IO.println("   Cargando Users")
    users <- leerCSV[UserEntity]("Entity_Users.csv")
    _ <- Stream.emits(users).covary[IO].chunkN(500).evalMap(chunk => insertUsersChunk(chunk.toList).transact(appxa)).compile.drain
    _ <- IO.println(s"   - ${users.size} usuarios insertados")

    _ <- IO.println("")

    // movies por chunk
    _ <- IO.println("   Cargando Movies (en lotes)")
    movies <- leerCSV[MovieClean]("clean_movies.csv")

    // Inserción por chunks
    _ <- Stream.emits(movies).covary[IO].chunkN(500).evalMap { chunk =>
      val list = chunk.toList
      insertMoviesChunk(list).transact(appxa)
    }.compile.drain
    _ <- IO.println(s"   - ${movies.size} películas insertadas")

    // links por chunk
    _ <- IO.println("3. Cargando TABLAS DE ENLACE")
    _ <- IO.println("-" * 100)

    // Movie-Genres
    _ <- IO.println("   Cargando Movie-Genres")
    movieGenres <- leerCSV[MovieGenreLink]("Link_Movie_Genres.csv")
    _ <- Stream.emits(movieGenres).covary[IO].chunkN(500).evalMap(chunk => insertMovieGenreChunk(chunk.toList).transact(appxa)).compile.drain
    _ <- IO.println(s"   - ${movieGenres.size} enlaces movie-genre insertados")

    // Movie-Companies
    _ <- IO.println("   Cargando Movie-Companies")
    movieCompanies <- leerCSV[MovieCompanyLink]("Link_Movie_Companies.csv")
    _ <- Stream.emits(movieCompanies).covary[IO].chunkN(500).evalMap(chunk => insertMovieCompanyChunk(chunk.toList).transact(appxa)).compile.drain
    _ <- IO.println(s"   - ${movieCompanies.size} enlaces movie-company insertados")

    // Movie-Countries
    _ <- IO.println("   Cargando Movie-Countries")
    movieCountries <- leerCSV[MovieCountryLink]("Link_Movie_Countries.csv")
    _ <- Stream.emits(movieCountries).covary[IO].chunkN(500).evalMap(chunk => insertMovieCountryChunk(chunk.toList).transact(appxa)).compile.drain
    _ <- IO.println(s"   - ${movieCountries.size} enlaces movie-country insertados")

    // Movie-Languages
    _ <- IO.println("   Cargando Movie-Languages")
    movieLanguages <- leerCSV[MovieLanguageLink]("Link_Movie_Languages.csv")
    _ <- Stream.emits(movieLanguages).covary[IO].chunkN(500).evalMap(chunk => insertMovieLanguageChunk(chunk.toList).transact(appxa)).compile.drain
    _ <- IO.println(s"   - ${movieLanguages.size} enlaces movie-language insertados")

    // Movie-Keywords
    _ <- IO.println("   Cargando Movie-Keywords")
    movieKeywords <- leerCSV[MovieKeywordLink]("Link_Movie_Keywords.csv")
    _ <- Stream.emits(movieKeywords).covary[IO].chunkN(500).evalMap(chunk => insertMovieKeywordChunk(chunk.toList).transact(appxa)).compile.drain
    _ <- IO.println(s"   - ${movieKeywords.size} enlaces movie-keyword insertados")

    // Movie-Cast
    _ <- IO.println("   Cargando Movie-Cast")
    movieCast <- leerCSV[MovieCastLink]("Link_Movie_Cast.csv")
    _ <- Stream.emits(movieCast).covary[IO].chunkN(500).evalMap(chunk => insertMovieCastChunk(chunk.toList).transact(appxa)).compile.drain
    _ <- IO.println(s"   - ${movieCast.size} enlaces movie-cast insertados")

    // Movie-Crew
    _ <- IO.println("   Cargando Movie-Crew")
    movieCrew <- leerCSV[MovieCrewLink]("Link_Movie_Crew.csv")
    _ <- Stream.emits(movieCrew).covary[IO].chunkN(500).evalMap(chunk => insertMovieCrewChunk(chunk.toList).transact(appxa)).compile.drain
    _ <- IO.println(s"   - ${movieCrew.size} enlaces movie-crew insertados")

    // Ratings
    _ <- IO.println("   Cargando Ratings")
    ratings <- leerCSV[RatingLink]("Link_Movie_Ratings.csv")
    _ <- Stream.emits(ratings).covary[IO].chunkN(500).evalMap(chunk => insertRatingChunk(chunk.toList).transact(appxa)).compile.drain
    _ <- IO.println(s"   - ${ratings.size} ratings insertados")

    _ <- IO.println("")
    _ <- IO.println("=" * 100)
    _ <- IO.println("- CARGA COMPLETADA EXITOSAMENTE")
    _ <- IO.println("=" * 100)
  } yield ()
}
