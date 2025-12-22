import cats.effect.{IO, IOApp}
import cats.syntax.all.*
import fs2.io.file.{Files, Path}
import fs2.text
import fs2.data.csv.*
// Importante: No necesitamos definir de nuevo MovieNumeric, Scala usará la del otro archivo.

// ============================================================================
// 1. MODELO DE ESTADÍSTICAS (Esto sí es nuevo, se queda)
// ============================================================================

case class EstadisticasColumna(
                                nombre: String,
                                totalValores: Int,
                                valoresValidos: Int,
                                media: Option[Double],
                                mediana: Option[Double],
                                moda: Option[Double],
                                desviacionEstandar: Option[Double],
                                varianza: Option[Double],
                                minimo: Option[Double],
                                maximo: Option[Double],
                                rango: Option[Double],
                                suma: Option[Double],
                                cuartil1: Option[Double],
                                cuartil3: Option[Double],
                                iqr: Option[Double],
                                outliers: Int
                              )

// ============================================================================
// 2. FUNCIONES MATEMÁTICAS (LÓGICA PURA)
// ============================================================================

object AnalisisEstadistico {

  def calcularMedia(datos: List[Double]): Option[Double] =
    if (datos.isEmpty) None else Some(datos.sum / datos.length)

  def calcularMediana(datos: List[Double]): Option[Double] = {
    if (datos.isEmpty) return None
    val sorted = datos.sorted
    val n = sorted.length
    if (n % 2 == 1) Some(sorted(n / 2))
    else Some((sorted((n - 1) / 2) + sorted(n / 2)) / 2.0)
  }

  def calcularModa(datos: List[Double]): Option[Double] = {
    if (datos.isEmpty) return None
    val frecuencia = datos.groupBy(identity).mapValues(_.length).toList
    if (frecuencia.isEmpty) None else Some(frecuencia.maxBy(_._2)._1)
  }

  def calcularVarianza(datos: List[Double]): Option[Double] = {
    calcularMedia(datos).flatMap { media =>
      if (datos.length <= 1) None
      else Some(datos.map(x => math.pow(x - media, 2)).sum / (datos.length - 1))
    }
  }

  def calcularDesviacionEstandar(datos: List[Double]): Option[Double] =
    calcularVarianza(datos).map(math.sqrt)

  def calcularCuartil(datos: List[Double], percentil: Double): Option[Double] = {
    if (datos.isEmpty) return None
    val sorted = datos.sorted
    val k = math.ceil((datos.length - 1) * percentil).toInt
    Some(sorted(math.min(k, datos.length - 1)))
  }

  def detectarOutliers(datos: List[Double]): Int = {
    (for {
      q1 <- calcularCuartil(datos, 0.25)
      q3 <- calcularCuartil(datos, 0.75)
    } yield {
      val iqr = q3 - q1
      val lower = q1 - 1.5 * iqr
      val upper = q3 + 1.5 * iqr
      datos.count(x => x < lower || x > upper)
    }).getOrElse(0)
  }

  def analizarColumna(nombre: String, datos: List[Double], totalFilas: Int): EstadisticasColumna = {
    val datosValidos = datos.filterNot(d => d.isNaN || d.isInfinite)

    EstadisticasColumna(
      nombre = nombre,
      totalValores = totalFilas,
      valoresValidos = datosValidos.length,
      media = calcularMedia(datosValidos),
      mediana = calcularMediana(datosValidos),
      moda = calcularModa(datosValidos),
      desviacionEstandar = calcularDesviacionEstandar(datosValidos),
      varianza = calcularVarianza(datosValidos),
      minimo = if (datosValidos.nonEmpty) Some(datosValidos.min) else None,
      maximo = if (datosValidos.nonEmpty) Some(datosValidos.max) else None,
      rango = if (datosValidos.nonEmpty) Some(datosValidos.max - datosValidos.min) else None,
      suma = if (datosValidos.nonEmpty) Some(datosValidos.sum) else None,
      cuartil1 = calcularCuartil(datosValidos, 0.25),
      cuartil3 = calcularCuartil(datosValidos, 0.75),
      iqr = for { q1 <- calcularCuartil(datosValidos, 0.25); q3 <- calcularCuartil(datosValidos, 0.75) } yield q3 - q1,
      outliers = detectarOutliers(datosValidos)
    )
  }

  def formatearDinero(monto: Double): String = {
    if (monto >= 1_000_000_000) f"$$${monto / 1_000_000_000}%.1fB"
    else if (monto >= 1_000_000) f"$$${monto / 1_000_000}%.1fM"
    else if (monto >= 1_000) f"$$${monto / 1_000}%.1fK"
    else f"$$$monto%.0f"
  }
}

// ============================================================================
// 3. MAIN (EJECUCIÓN)
// ============================================================================

object AnalisisRealConLectura extends IOApp.Simple {

  val filePath = Path("C:\\Users\\Luis\\Desktop\\PRUEBA\\src\\main\\resources\\data\\pi_movies_complete.csv")

  // Función auxiliar para imprimir bonito
  def imprimirEstadisticas(estadisticas: EstadisticasColumna): IO[Unit] = {
    val fmt = (v: Option[Double]) => v.map(d => f"$d%.2f").getOrElse("N/A")
    val fmtMoney = (v: Option[Double]) => v.map(AnalisisEstadistico.formatearDinero).getOrElse("N/A")

    IO.println(s"📊 COLUMNA: ${estadisticas.nombre.toUpperCase}") >>
      IO.println("-" * 40) >>
      IO.println(s"  Total registros procesados: ${estadisticas.totalValores}") >>
      IO.println(s"  Valores válidos (no nulos/cero): ${estadisticas.valoresValidos}") >>
      IO.println(s"  Cobertura: ${(estadisticas.valoresValidos.toDouble / estadisticas.totalValores * 100).formatted("%.1f")}%") >>
      IO.println("") >>
      (if (estadisticas.nombre == "budget" || estadisticas.nombre == "revenue") {
        IO.println(s"  Media:   ${fmtMoney(estadisticas.media)}") >>
          IO.println(s"  Mediana: ${fmtMoney(estadisticas.mediana)}") >>
          IO.println(s"  Suma:    ${fmtMoney(estadisticas.suma)}")
      } else {
        IO.println(s"  Media:   ${fmt(estadisticas.media)}") >>
          IO.println(s"  Mediana: ${fmt(estadisticas.mediana)}") >>
          IO.println(s"  Moda:    ${fmt(estadisticas.moda)}")
      }) >>
      IO.println(s"  Mínimo:  ${fmt(estadisticas.minimo)}") >>
      IO.println(s"  Máximo:  ${fmt(estadisticas.maximo)}") >>
      IO.println("") >>
      IO.println(s"  Desv. Std: ${fmt(estadisticas.desviacionEstandar)}") >>
      IO.println(s"  Outliers:  ${estadisticas.outliers}") >>
      IO.println("")
  }

  override def run: IO[Unit] = {

    // 1. LEER EL CSV REAL
    val lecturaIO = Files[IO]
      .readAll(filePath)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[MovieNumeric](';')) // Usa el decoder del otro archivo automáticamente
      .compile
      .toList

    lecturaIO.flatMap { moviesRaw =>
      // 2. PROCESAR DATOS
      // Usamos ParserFuncional que ya está definido en el otro archivo
      val moviesValidas = moviesRaw
        .map(ParserFuncional.procesarMovie)
        .filter(_.id != 0) // Quitar basura

      val total = moviesValidas.length

      // 3. EXTRAER LISTAS NUMÉRICAS REALES
      val budgets      = moviesValidas.flatMap(_.budget)
      val revenues     = moviesValidas.flatMap(_.revenue)
      val runtimes     = moviesValidas.flatMap(_.runtime.map(_.toDouble))
      val popularities = moviesValidas.map(_.popularity)
      val votes        = moviesValidas.map(_.vote_average)
      val voteCounts   = moviesValidas.map(_.vote_count.toDouble)

      // 4. EJECUTAR ANÁLISIS
      val statsBudget = AnalisisEstadistico.analizarColumna("budget", budgets, total)
      val statsRevenue = AnalisisEstadistico.analizarColumna("revenue", revenues, total)
      val statsRuntime = AnalisisEstadistico.analizarColumna("runtime", runtimes, total)
      val statsPop = AnalisisEstadistico.analizarColumna("popularity", popularities, total)
      val statsVote = AnalisisEstadistico.analizarColumna("vote_average", votes, total)
      val statsVoteCount = AnalisisEstadistico.analizarColumna("vote_count", voteCounts, total)

      // 5. IMPRIMIR RESULTADOS
      IO.println("=" * 80) >>
        IO.println("5.3 ANÁLISIS ESTADÍSTICO DE DATOS REALES (CSV)") >>
        IO.println("=" * 80) >>
        IO.println("") >>
        imprimirEstadisticas(statsBudget) >>
        imprimirEstadisticas(statsRevenue) >>
        imprimirEstadisticas(statsRuntime) >>
        imprimirEstadisticas(statsPop) >>
        imprimirEstadisticas(statsVote) >>
        imprimirEstadisticas(statsVoteCount) >>
        IO.println("✅ Análisis completado con datos reales.")
    }
  }
}