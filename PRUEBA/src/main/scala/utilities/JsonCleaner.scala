package utilities

import io.circe.parser._
import io.circe.Decoder

object JsonCleaner {

  def parseList[T](raw: String)(implicit decoder: Decoder[List[T]]): List[T] = {
    val clean = fixJsonFormat(raw, isList = true)
    parse(clean).flatMap(_.as[List[T]]).getOrElse(Nil)
  }

  def parseObject[T](raw: String)(implicit decoder: Decoder[T]): Option[T] = {
    val clean = fixJsonFormat(raw, isList = false)
    parse(clean).flatMap(_.as[T]).toOption
  }

  private def fixJsonFormat(raw: String, isList: Boolean): String = {
    var text = raw.trim
    if (text.isEmpty || text == "[]" || text == "{}") return if (isList) "[]" else "{}"

    text = text
      .replaceAll("None", "null")
      .replaceAll("True", "true")
      .replaceAll("False", "false")
      .replace("'", "\"")

    if (isList && text.startsWith("[") && !text.endsWith("]")) {
      val lastBrace = text.lastIndexOf("}")
      if (lastBrace != -1) {
        text = text.substring(0, lastBrace + 1) + "]"
      } else {
        text = "[]"
      }
    }
    text
  }
}