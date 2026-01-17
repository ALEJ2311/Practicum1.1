import cats.effect.{IO, IOApp}
import fs2.io.file.{Files, Path}
import fs2.text
import fs2.data.csv._
import fs2.data.csv.generic.semiauto._

// 1. Modelo de datos para las columnas de texto
case class MovieText(
                      belongs_to_collection: String,
                      original_language: String,
                      status: String,
                      tagline: String,
                      title: String
                    )

object MovieText {
  // Decoder automático para mapear los encabezados del CSV a la Case Class
  given CsvRowDecoder[MovieText, String] = deriveCsvRowDecoder[MovieText]
}

// 2. Objeto de ejecución principal
object AnalisisTexto extends IOApp.Simple {

  val filePath = Path("C:\\Users\\anapa\\IdeaProjects\\b2-proyecto-integrador-a-proyecto_integrador_radiant_developers\\src\\main\\resources\\pi-movies-complete-2025-12-04.csv")

  /**
   * Calcula la distribución de frecuencia (Top 10)
   */
  def obtenerDistribucionFrecuencia(datos: List[String]): List[(String, Int)] = {
    datos
      .groupBy(identity)
      .map { case (valor, lista) => (valor, lista.size) }
      .toList
      .sortBy(-_._2) // Ordenar de mayor a menor frecuencia
      .take(10)      // Tomar los 10 más comunes
  }

  /**
   * Imprime los resultados de frecuencia en consola con formato
   */
  def imprimirFrecuencias(nombreColumna: String, datos: List[String]): IO[Unit] = {
    val topFrecuencias = obtenerDistribucionFrecuencia(datos)

    for {
      _ <- IO.println(f"\n--- Top Frecuencias: $nombreColumna ---")
      _ <- IO {
        topFrecuencias.zipWithIndex.foreach { case ((valor, cuenta), i) =>
          val valorLimpio = if (valor == null || valor.trim.isEmpty) "[Sin Datos/Vacío]" else valor
          // Usamos un ancho de 40 caracteres para el texto y alineación a la derecha para la cuenta
          println(f"  ${i + 1}. $valorLimpio%-40s | Apariciones: $cuenta")
        }
      }
    } yield ()
  }

  /**
   * Stream para cargar y procesar el CSV
   */
  def cargarPeliculas(path: Path): IO[List[MovieText]] = {
    Files[IO]
      .readAll(path)
      .through(text.utf8.decode)
      // Decodificación usando el separador ';' y los encabezados del CSV
      .through(decodeUsingHeaders[MovieText](';'))
      .attempt // Captura errores de formato (como filas con tamaños de columna inconsistentes)
      .collect {
        case Right(pelicula) => pelicula // Solo dejamos pasar los registros procesados correctamente
      }
      .compile
      .toList
  }

  /**
   * Lógica principal de ejecución
   */
  val run: IO[Unit] = for {
    _ <- IO.println("Iniciando carga de datos...")
    peliculas <- cargarPeliculas(filePath)

    _ <- IO.println("=" * 80)
    _ <- IO.println("            ANÁLISIS DE DISTRIBUCIÓN DE FRECUENCIA (TEXTO)")
    _ <- IO.println("=" * 80)

    // Análisis de cada columna solicitada
    _ <- imprimirFrecuencias("Idioma Original", peliculas.map(_.original_language))
    _ <- imprimirFrecuencias("Estado (Status)", peliculas.map(_.status))
    _ <- imprimirFrecuencias("Colección", peliculas.map(_.belongs_to_collection))
    _ <- imprimirFrecuencias("Tagline (Eslogan)", peliculas.map(_.tagline))
    _ <- imprimirFrecuencias("Título de la Película", peliculas.map(_.title))

    _ <- IO.println("\n" + "=" * 80)
    _ <- IO.println(s"  Total registros analizados correctamente: ${peliculas.length}")
    _ <- IO.println("=" * 80)
  } yield ()
}