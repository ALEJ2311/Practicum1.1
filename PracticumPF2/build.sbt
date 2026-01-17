ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.7"

val circeVersion = "0.14.10"

lazy val root = (project in file("."))
  .settings(
    name := "Avance2",
    libraryDependencies ++= Seq(
      // Cats Effect y FS2 (Motor de la app)
      "org.typelevel" %% "cats-effect" % "3.5.4",
      "co.fs2"        %% "fs2-core"    % "3.12.2",
      "co.fs2"        %% "fs2-io"      % "3.12.2",

      // FS2 Data (Para leer CSV)
      "org.gnieh"     %% "fs2-data-csv"         % "1.11.1",
      "org.gnieh"     %% "fs2-data-csv-generic" % "1.11.1",

      // Circe (Para leer los campos JSON dentro del CSV)
      "io.circe"      %% "circe-core"    % circeVersion,
      "io.circe"      %% "circe-generic" % circeVersion,
      "io.circe"      %% "circe-parser"  % circeVersion
    )
  )