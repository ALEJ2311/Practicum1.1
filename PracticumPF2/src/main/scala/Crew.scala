import scala.io.Source
import io.circe._
import io.circe.parser._
import io.circe.generic.auto._
import io.circe.syntax._

/**
 * 1. DEFINICIÓN DEL MODELO (CASE CLASS)
 * Se define fuera del objeto para que sea accesible globalmente.
 */
case class Crew(
                 credit_id: Option[String],
                 department: Option[String],
                 gender: Option[Int],
                 id: Option[Int],
                 job: Option[String],
                 name: Option[String],
                 profile_path: Option[String]
               )

/**
 * 2. OBJETO PRINCIPAL DE EJECUCIÓN
 */
object ProcesamientoCrewFilaPorFila extends App {

  // Ajusta esta ruta a tu archivo real
  val csvPath = "C:\\Users\\Usuario iTC\\Desktop\\Projectos PV\\PracticumPF2\\src\\main\\resources\\data\\pi_movies_complete.csv"

  // ----------------------------------------------------------------
  // A. FUNCIONES DE LIMPIEZA Y NORMALIZACIÓN (TUS FUNCIONES)
  // ----------------------------------------------------------------

  // Limpia el String sucio que parece JSON
  def cleanCrewJson(json: String): String =
    json.trim
      .replaceAll("'", "\"")
      .replaceAll("None", "null")
      .replaceAll("True", "true")
      .replaceAll("False", "false")
      .replaceAll("""\\""", "")

  // Normaliza texto (quita espacios extra)
  def normalizarTexto(texto: String): Option[String] = {
    val limpio = texto.trim.replaceAll("\\s+", " ")
    if (limpio.isEmpty) None else Some(limpio)
  }

  // Normaliza enteros (valor absoluto)
  def normalizarEntero(valor: Int): Option[Int] =
    Some(Math.abs(valor))

  // Normaliza un objeto Crew completo (aplica reglas a sus campos)
  def normalizarCrew(c: Crew): Crew =
    c.copy(
      credit_id = c.credit_id.flatMap(normalizarTexto),
      department = c.department.flatMap(normalizarTexto),
      gender = c.gender.flatMap(normalizarEntero),
      id = c.id.flatMap(normalizarEntero),
      job = c.job.flatMap(normalizarTexto),
      name = c.name.flatMap(normalizarTexto),
      profile_path = c.profile_path.flatMap(normalizarTexto)
    )

  // Parseo manual de línea CSV respetando comillas
  def parseCSVLine(line: String): Array[String] = {
    val (fields, lastBuilder, _) =
      line.foldLeft((Vector.empty[String], new StringBuilder, false)) {
        case ((fields, current, inQuotes), char) => char match {
          case '"' => (fields, current, !inQuotes)
          case ';' if !inQuotes => (fields :+ current.toString, new StringBuilder, false)
          case _ => current.append(char); (fields, current, inQuotes)
        }
      }
    (fields :+ lastBuilder.toString).toArray
  }

  // ----------------------------------------------------------------
  // B. PIPELINE PRINCIPAL (LOGICA FILA POR FILA DEL PROFESOR)
  // ----------------------------------------------------------------

  println("Iniciando lectura y procesamiento...")

  val source = Source.fromFile(csvPath, "UTF-8")

  // Usamos un iterador para recorrer el archivo línea por línea sin cargarlo todo
  val linesIterator = source.getLines()

  if (linesIterator.hasNext) {
    // Extraemos headers y buscamos el índice
    val headers = linesIterator.next().split(";").map(_.trim)
    val crewIndex = headers.indexOf("crew")

    if (crewIndex != -1) {

      // AQUÍ OCURRE EL PROCESAMIENTO
      // flatMap recorre línea a línea, transforma y acumula solo los resultados válidos
      val crewProcesado: List[Crew] = linesIterator.flatMap { line =>

        // 1. Parsear la línea CSV
        val cols = parseCSVLine(line)

        // 2. Verificar si la columna existe y tiene datos
        if (cols.length > crewIndex) {
          val rawJson = cols(crewIndex)

          // 3. Pre-validación rápida
          if (rawJson.nonEmpty && rawJson.startsWith("[") && rawJson != "[]") {

            // 4. Limpieza del String
            val jsonLimpio = cleanCrewJson(rawJson)

            // 5. Decodificación y Normalización Inmediata
            decode[List[Crew]](jsonLimpio) match {
              case Right(lista) =>
                // Aquí aplicamos la lógica de negocio a la fila antes de seguir
                lista.map(normalizarCrew)
              case Left(_) =>
                List.empty // Si falla el JSON, ignoramos
            }
          } else {
            List.empty
          }
        } else {
          List.empty
        }
      }.toList // Materializamos la lista final al terminar el archivo

      source.close()

      // ----------------------------------------------------------------
      // C. RESULTADOS Y ESTADÍSTICAS
      // ----------------------------------------------------------------

      println("=" * 60)
      println(f"PROCESAMIENTO TERMINADO")
      println(f"Total registros Crew válidos: ${crewProcesado.size}%,d")
      println("=" * 60)

      // Top 5 Departamentos
      println("\nTOP 5 DEPARTAMENTOS:")
      crewProcesado.flatMap(_.department).groupBy(identity).view.mapValues(_.size).toSeq.sortBy(-_._2).take(5).foreach { case (d, c) => println(f" - $d%-20s : $c") }

      // Muestra de datos limpios
      println("\nEJEMPLO (PRIMER REGISTRO):")
      crewProcesado.headOption.foreach(c => println(c.asJson.spaces2))

    } else {
      println("❌ Error: No se encontró la columna 'crew'.")
      source.close()
    }
  } else {
    println("❌ Error: El archivo está vacío.")
    source.close()
  }
}