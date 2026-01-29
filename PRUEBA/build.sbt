ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.7"

val circeVersion = "0.14.10"

lazy val root = (project in file("."))
  .settings(
    name := "Clase",

    scalacOptions ++= Seq("-Xmax-inlines", "100"),

    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.5.4",
      "co.fs2"        %% "fs2-core"    % "3.12.2",
      "co.fs2"        %% "fs2-io"      % "3.12.2",
      "org.gnieh"     %% "fs2-data-csv"         % "1.11.1",
      "org.gnieh"     %% "fs2-data-csv-generic" % "1.11.1",
      "io.circe"      %% "circe-core"    % circeVersion,
      "io.circe"      %% "circe-generic" % circeVersion,
      "io.circe"      %% "circe-parser"  % circeVersion,

      "org.tpolecat" %% "doobie-core"   % "1.0.0-RC11",

      "org.tpolecat" %% "doobie-hikari" % "1.0.0-RC11",

      "com.mysql" % "mysql-connector-j" % "9.1.0",

      "com.typesafe" % "config"         % "1.4.2",

      "org.slf4j" % "slf4j-simple"       % "2.0.16"
    )
  )