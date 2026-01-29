package analysis

object AnalysisUtils {

  // --- MODELOS ---
  case class StatsNumericos(
                             campo: String,
                             total: Int,
                             nulos: Int,
                             min: Double,
                             max: Double,
                             promedio: Double
                           )

  case class StatsTexto(
                         campo: String,
                         total: Int,
                         vacios: Int,
                         topFrecuencias: List[(String, Int)]
                       )

  def calcularStatsNumericos(nombre: String, datosRaw: List[String]): StatsNumericos = {
    val validos = datosRaw
      .map(s => scala.util.Try(s.trim.toDouble).toOption)
      .flatten
      .filter(_ > 0)

    val n = validos.size
    val total = datosRaw.size

    if (n == 0) StatsNumericos(nombre, total, total, 0, 0, 0)
    else {
      val sorted = validos.sorted
      val sum = validos.sum
      StatsNumericos(nombre, total, total - n, sorted.head, sorted.last, sum / n)
    }
  }

  def calcularStatsTexto(nombre: String, datosRaw: List[Option[String]]): StatsTexto = {
    val textos = datosRaw.flatten.map(_.trim).filter(_.nonEmpty)
    val total = datosRaw.size

    val frecuencias = textos
      .groupBy(identity)
      .map { case (k, v) => (k, v.size) }
      .toList
      .sortBy(-_._2)
      .take(5)

    StatsTexto(nombre, total, total - textos.size, frecuencias)
  }


  def formatearNumericos(s: StatsNumericos): String = {
    val validos = s.total - s.nulos
    f"${s.campo}%-20s  Total: ${s.total}%,8d  Válidos: $validos%,8d  Min: ${s.min}%12.2f  Max: ${s.max}%12.2f  Prom: ${s.promedio}%12.2f"
  }

  def formatearTexto(s: StatsTexto): String = {
    val header = f"${s.campo}%-20s  Total: ${s.total}%,8d  Vacíos: ${s.vacios}%,8d"
    val top = s.topFrecuencias.zipWithIndex.map { case ((txt, count), i) =>
      f"   ${i+1}. $txt%-50s : $count%,d"
    }.mkString("\n")

    s"$header\n$top\n${"-" * 100}"
  }
}