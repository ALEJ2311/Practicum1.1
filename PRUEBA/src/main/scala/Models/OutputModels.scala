package models

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import fs2.data.csv.CsvRowEncoder
import fs2.data.csv.generic.semiauto._
import fs2.data.csv._
import fs2.data.csv.generic.semiauto.deriveCsvRowEncoder


// estructuras para leer json

case class GenreJson(id: Int, name: String)
object GenreJson { given Decoder[GenreJson] = deriveDecoder }

case class CompanyJson(id: Int, name: String)
object CompanyJson { given Decoder[CompanyJson] = deriveDecoder }

case class CountryJson(iso_3166_1: String, name: String)
object CountryJson { given Decoder[CountryJson] = deriveDecoder }

case class LanguageJson(iso_639_1: String, name: String)
object LanguageJson { given Decoder[LanguageJson] = deriveDecoder }

case class KeywordJson(id: Int, name: String)
object KeywordJson { given Decoder[KeywordJson] = deriveDecoder }

case class CollectionJson(id: Int, name: String, poster_path: Option[String], backdrop_path: Option[String])
object CollectionJson { given Decoder[CollectionJson] = deriveDecoder }

case class CastJson(cast_id: Int, character: String, credit_id: String, gender: Option[Int], id: Int, name: String, order: Int, profile_path: Option[String])
object CastJson { given Decoder[CastJson] = deriveDecoder }

case class CrewJson(credit_id: String, department: String, gender: Option[Int], id: Int, job: String, name: String, profile_path: Option[String])
object CrewJson { given Decoder[CrewJson] = deriveDecoder }

case class RatingJson(userId: Long, rating: Double, timestamp: Long)
object RatingJson { given Decoder[RatingJson] = deriveDecoder }


// entidades unicas

case class GenreEntity(genre_id: Int, name: String)
object GenreEntity {
  given CsvRowEncoder[GenreEntity, String] = deriveCsvRowEncoder
  given CsvRowDecoder[GenreEntity, String] = deriveCsvRowDecoder
}

case class CompanyEntity(company_id: Int, name: String)
object CompanyEntity {
  given CsvRowEncoder[CompanyEntity, String] = deriveCsvRowEncoder
  given CsvRowDecoder[CompanyEntity, String] = deriveCsvRowDecoder
}

case class CountryEntity(iso_3166_1: String, name: String)
object CountryEntity {
  given CsvRowEncoder[CountryEntity, String] = deriveCsvRowEncoder
  given CsvRowDecoder[CountryEntity, String] = deriveCsvRowDecoder
}

case class LanguageEntity(iso_639_1: String, name: String)
object LanguageEntity {
  given CsvRowEncoder[LanguageEntity, String] = deriveCsvRowEncoder
  given CsvRowDecoder[LanguageEntity, String] = deriveCsvRowDecoder
}

case class KeywordEntity(keyword_id: Int, name: String)
object KeywordEntity {
  given CsvRowEncoder[KeywordEntity, String] = deriveCsvRowEncoder
  given CsvRowDecoder[KeywordEntity, String] = deriveCsvRowDecoder
}

case class CollectionEntity(collection_id: Int, name: String, poster: String, backdrop: String)
object CollectionEntity {
  given CsvRowEncoder[CollectionEntity, String] = deriveCsvRowEncoder
  given CsvRowDecoder[CollectionEntity, String] = deriveCsvRowDecoder
}

case class PersonEntity(person_id: Int, name: String, gender: Int, profile_path: String)
object PersonEntity {
  given CsvRowEncoder[PersonEntity, String] = deriveCsvRowEncoder
  given CsvRowDecoder[PersonEntity, String] = deriveCsvRowDecoder
}

case class UserEntity(user_id: Long)
object UserEntity {
  given CsvRowEncoder[UserEntity, String] = deriveCsvRowEncoder
  given CsvRowDecoder[UserEntity, String] = deriveCsvRowDecoder
}

// tablas de enlace

case class MovieGenreLink(movie_id: Int, genre_id: Int)
object MovieGenreLink {
  given CsvRowEncoder[MovieGenreLink, String] = deriveCsvRowEncoder
  given CsvRowDecoder[MovieGenreLink, String] = deriveCsvRowDecoder
}

case class MovieCompanyLink(movie_id: Int, company_id: Int)
object MovieCompanyLink {
  given CsvRowEncoder[MovieCompanyLink, String] = deriveCsvRowEncoder
  given CsvRowDecoder[MovieCompanyLink, String] = deriveCsvRowDecoder
}

case class MovieCountryLink(movie_id: Int, iso_3166_1: String)
object MovieCountryLink {
  given CsvRowEncoder[MovieCountryLink, String] = deriveCsvRowEncoder
  given CsvRowDecoder[MovieCountryLink, String] = deriveCsvRowDecoder
}

case class MovieLanguageLink(movie_id: Int, iso_639_1: String)
object MovieLanguageLink {
  given CsvRowEncoder[MovieLanguageLink, String] = deriveCsvRowEncoder
  given CsvRowDecoder[MovieLanguageLink, String] = deriveCsvRowDecoder
}

case class MovieKeywordLink(movie_id: Int, keyword_id: Int)
object MovieKeywordLink {
  given CsvRowEncoder[MovieKeywordLink, String] = deriveCsvRowEncoder
  given CsvRowDecoder[MovieKeywordLink, String] = deriveCsvRowDecoder
}

case class MovieCastLink(movie_id: Int, person_id: Int, character_name: String, order_cast: Int, credit_id: String, cast_id: Int)
object MovieCastLink {
  given CsvRowEncoder[MovieCastLink, String] = deriveCsvRowEncoder
  given CsvRowDecoder[MovieCastLink, String] = deriveCsvRowDecoder
}

case class MovieCrewLink(movie_id: Int, person_id: Int, department: String, job: String, credit_id: String)
object MovieCrewLink {
  given CsvRowEncoder[MovieCrewLink, String] = deriveCsvRowEncoder
  given CsvRowDecoder[MovieCrewLink, String] = deriveCsvRowDecoder
}

case class RatingLink(movie_id: Int, user_id: Long, rating: Double, timestamp: Long)
object RatingLink {
  given CsvRowEncoder[RatingLink, String] = deriveCsvRowEncoder
  given CsvRowDecoder[RatingLink, String] = deriveCsvRowDecoder
}