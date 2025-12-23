import cats.effect.{IO, IOApp}
import cats.syntax.all.*
import fs2.io.file.{Files, Path}
import fs2.text
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*

// ============================================================================
// MODELO CSV (COLUMNAS JSON)
// ============================================================================

case class MovieJson(
                      belongs_to_collection: String,
                      genres: String,
                      production_companies: String,
                      production_countries: String,
                      spoken_languages: String,
                      keywords: String,
                      cast: String,
                      crew: String,
                      ratings: String
                    )

given CsvRowDecoder[MovieJson, String] =
  deriveCsvRowDecoder[MovieJson]

// ============================================================================
// UTILIDADES DE ANÁLISIS JSON
// ============================================================================

object AnalisisJson {

  case class ResumenPresencia(
                               total: Int,
                               nulos: Int,
                               vacios: Int,
                               jsonVacios: Int,
                               conContenido: Int
                             )

  case class ResumenLongitud(
                              promedio: Double,
                              maximo: Int
                            )

  def esNulo(s: String): Boolean =
    s == null || s.trim.equalsIgnoreCase("null")

  def esVacio(s: String): Boolean =
    s != null && s.trim.isEmpty

  def esJsonVacio(s: String): Boolean =
    s != null && {
      val t = s.trim
      t == "[]" || t == "{}"
    }

  def analizarPresencia(col: List[String]): ResumenPresencia = {
    val total = col.size
    val nulos = col.count(esNulo)
    val vacios = col.count(esVacio)
    val jsonVacios = col.count(esJsonVacio)
    val conContenido = total - nulos - vacios - jsonVacios

    ResumenPresencia(total, nulos, vacios, jsonVacios, conContenido)
  }

  def analizarLongitud(col: List[String]): ResumenLongitud = {
    val validos =
      col.filter(s => s != null && s.trim.nonEmpty && !esJsonVacio(s))

    if (validos.isEmpty)
      ResumenLongitud(0.0, 0)
    else
      ResumenLongitud(
        promedio = validos.map(_.length).sum.toDouble / validos.size,
        maximo = validos.map(_.length).max
      )
  }
}

// ============================================================================
// MAIN
// ============================================================================

object AnalisisJsonFrecuencia extends IOApp.Simple {

  val filePath =
    Path("C:\\Users\\Luis\\Desktop\\PRUEBA\\src\\main\\resources\\data\\pi_movies_complete.csv")

  override def run: IO[Unit] = {

    val lecturaCSV: IO[List[MovieJson]] =
      Files[IO]
        .readAll(filePath)
        .through(text.utf8.decode)
        .through(decodeUsingHeaders[MovieJson](';'))
        .compile
        .toList

    lecturaCSV.flatMap { movies =>

      val total = movies.length

      val columnas = List(
        "belongs_to_collection" -> movies.map(_.belongs_to_collection),
        "genres" -> movies.map(_.genres),
        "production_companies" -> movies.map(_.production_companies),
        "production_countries" -> movies.map(_.production_countries),
        "spoken_languages" -> movies.map(_.spoken_languages),
        "keywords" -> movies.map(_.keywords),
        "cast" -> movies.map(_.cast),
        "crew" -> movies.map(_.crew),
        "ratings" -> movies.map(_.ratings)
      )

      val filasSinInfoJson =
        movies.count { m =>
          List(
            m.belongs_to_collection,
            m.genres,
            m.production_companies,
            m.production_countries,
            m.spoken_languages,
            m.keywords,
            m.cast,
            m.crew,
            m.ratings
          ).forall(s =>
            AnalisisJson.esNulo(s) ||
              AnalisisJson.esVacio(s) ||
              AnalisisJson.esJsonVacio(s)
          )
        }

      IO.println("=" * 90) >>
        IO.println("5.4 ANÁLISIS EXPLORATORIO DE COLUMNAS CON ESTRUCTURA JSON") >>
        IO.println("=" * 90) >>
        IO.println("") 

      IO.println("INFORMACIÓN GENERAL") >>
        IO.println("-" * 45) >>
        IO.println(s"Total de registros analizados: $total") >>
        IO.println("") 

      columnas.traverse_ { case (nombre, datos) =>
        val p = AnalisisJson.analizarPresencia(datos)
        val l = AnalisisJson.analizarLongitud(datos)

        IO.println(s"Columna: $nombre") >>
          IO.println(s"  • Registros totales      : ${p.total}") >>
          IO.println(s"  • Valores nulos          : ${p.nulos} (${p.nulos.toDouble / p.total * 100}%.2f%%)") >>
          IO.println(s"  • Valores vacíos         : ${p.vacios} (${p.vacios.toDouble / p.total * 100}%.2f%%)") >>
          IO.println(s"  • JSON vacíos ([] / {})  : ${p.jsonVacios} (${p.jsonVacios.toDouble / p.total * 100}%.2f%%)") >>
          IO.println(s"  • Con contenido          : ${p.conContenido} (${p.conContenido.toDouble / p.total * 100}%.2f%%)") >>
          IO.println(s"  • Longitud promedio      : ${l.promedio}%.1f caracteres") >>
          IO.println(s"  • Longitud máxima        : ${l.maximo} caracteres") >>
          IO.println("")
      } 

      IO.println("RESUMEN GLOBAL") >>
        IO.println("-" * 45) >>
        IO.println(s"Filas sin información JSON en ninguna columna: $filasSinInfoJson") >>
        IO.println(s"Porcentaje sobre el total: ${filasSinInfoJson.toDouble / total * 100}%.2f%%") >>
        IO.println("") 

      IO.println("=" * 90) >>
        IO.println("✓ Análisis exploratorio de columnas JSON completado") >>
        IO.println("✓ Resultados listos para justificar el proceso de limpieza (5.5)") >>
        IO.println("=" * 90)
    }
  }
}