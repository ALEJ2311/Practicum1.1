import cats.effect.{IO, IOApp}
import fs2.text
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*


case class Movie(
                  id: Double,
                  budget: Double,
                  revenue: Double,
                  popularity: Double,
                  runtime: Double,
                  vote_average: Double,
                  vote_count: Double
                )

given CsvRowDecoder[Movie, String] = deriveCsvRowDecoder[Movie]

object columnasNumericas extends IOApp.Simple:

  val filePath = Path("C:\\Users\\Luis\\Desktop\\Nueva carpeta\\Practicum1.1\\PRUEBA\\src\\main\\resources\\data\\pi-movies-complete-2025-12-04.csv")

  def imprimirMetricas(nombre: String, datos: List[Double]): IO[Unit] =
    val avg = Estadisticos.promedio(datos)
    val med = Estadisticos.mediana(datos)
    val min = Estadisticos.min(datos)
    val max = Estadisticos.max(datos)
    val std = Estadisticos.desviacionEstandar(datos)

    IO.println(f"| $nombre%-12s | Prom: $avg%10.2f | Med: $med%10.2f | Min: $min%10.0f | Max: $max%12.0f | Desv: $std%10.2f |")

  val run: IO[Unit] =
    val lecturaCSV: IO[List[Movie]] = Files[IO]
      .readAll(filePath)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[Movie](';'))
      .compile
      .toList

    lecturaCSV.flatMap { peliculas =>
      val peliculasValidas = peliculas.filter(_.id != 0)

      IO.println("=" * 115)
      IO.println("                               REPORTE ESTADÍSTICO DE PELÍCULAS")
      IO.println("=" * 115) >>
        imprimirMetricas("Budget",      peliculasValidas.map(_.budget)) >>
        imprimirMetricas("Revenue",     peliculasValidas.map(_.revenue)) >>
        imprimirMetricas("Popularity",  peliculasValidas.map(_.popularity)) >>
        imprimirMetricas("Runtime",     peliculasValidas.map(_.runtime)) >>
        imprimirMetricas("Vote Avg",    peliculasValidas.map(_.vote_average)) >>
        imprimirMetricas("Vote Count",  peliculasValidas.map(_.vote_count)) >>
        IO.println("-" * 115) >>
        IO.println(s" Total registros analizados: ${peliculasValidas.length}") >>
        IO.println("=" * 115)
    }

object Estadisticos:
  def promedio(datos: List[Double]): Double =
    if datos.isEmpty then 0.0 else datos.sum / datos.length

  def sumaTotal(datos: List[Double]): Double = datos.sum

  def min(datos: List[Double]): Double =
    if datos.isEmpty then 0.0 else datos.min

  def max(datos: List[Double]): Double =
    if datos.isEmpty then 0.0 else datos.max

  def mediana(datos: List[Double]): Double =
    if datos.isEmpty then 0.0
    else
      val sorted = datos.sorted
      val n = sorted.length
      if (n % 2 == 1) sorted(n / 2)
      else (sorted((n / 2) - 1) + sorted(n / 2)) / 2.0

  def desviacionEstandar(datos: List[Double]): Double =
    if datos.isEmpty then 0.0
    else
      val media = promedio(datos)
      val varianza = datos.map(x => Math.pow(x - media, 2)).sum / datos.length
      Math.sqrt(varianza)