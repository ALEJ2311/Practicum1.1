package utilities

import java.time.LocalDate
import scala.util.Try

object CleaningRules {

  def cleanText(s: String): String = {
    if (s == null) ""
    else s.trim
      .replaceAll("[\r\n]+", " ")
      .replaceAll("\"", "'")
      .trim
  }

  def cleanAdult(s: String): Option[Boolean] = {
    s.trim.toLowerCase match {
      case "true"  => Some(true)
      case "false" => Some(false)
      case _       => None
    }
  }

  def cleanDouble(s: String): Option[Double] = {
    s.trim.toDoubleOption.filter(_ >= 0)
  }

  def cleanUrl(opt: Option[String]): Option[String] = {
    opt.map(_.trim).filter(_.startsWith("http"))
  }

  def cleanId(s: String): Option[Int] = {
    val clean = s.trim.takeWhile(_ != '.')
    clean.toIntOption.filter(_ > 0)
  }

  def cleanImdb(opt: Option[String]): Option[String] = {
    opt.map(_.trim).filter(_.startsWith("tt"))
  }

  def cleanLang(s: String): Option[String] = {
    val trimmed = s.trim
    if (trimmed.matches("^[a-zA-Z]{2}$")) Some(trimmed) else None
  }

  def cleanStatus(opt: Option[String]): Option[String] = {
    val permitidos = Set("In Production", "Post Production", "Released", "Rumored")
    opt.map(_.trim).flatMap { s =>
      if (permitidos.contains(s)) Some(s)
      else if (s == "In Produ") Some("In Production")
      else if (s == "Post Pro") Some("Post Production")
      else None
    }
  }

  def cleanPath(opt: Option[String]): Option[String] = {
    opt.map(_.trim).filter(_.startsWith("/"))
  }

  def cleanDate(opt: Option[String]): Option[String] = {
    opt.map(_.trim).filter(_.matches("^\\d{4}-\\d{2}-\\d{2}$"))
  }

  def parseDate(s: Option[String]): Option[LocalDate] =
    s.flatMap { str =>
      val clean = str.trim
      if (clean.isEmpty || clean.equalsIgnoreCase("null")) None
      else Try(LocalDate.parse(clean)).toOption
    }
}