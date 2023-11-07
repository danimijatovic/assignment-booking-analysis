ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "assignment-booking-analysis"
  )

val versions: Map[String, String] =
  Map(
    "scala" -> "2.12.17",
    "spark" -> "3.4.0",
    "hadoop" -> "3.3.1",
    "slf4j" -> "1.7.30",
    "pureconfig" -> "0.12.2",
    "typesafe" -> "1.4.1"

  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % versions("spark"),
  "org.apache.spark" %% "spark-sql" % versions("spark"),
  "org.apache.hadoop" % "hadoop-client" % versions("hadoop"),
  "com.github.pureconfig" %% "pureconfig" % versions("pureconfig"),
  "com.github.pureconfig" %% "pureconfig-joda" % versions("pureconfig"),
  "com.typesafe" % "config" % versions("typesafe"),
  "org.slf4j" % "slf4j-api" %  versions("slf4j"),
  "org.slf4j" % "jul-to-slf4j" %  versions("slf4j"),
  "org.slf4j" % "slf4j-log4j12" %  versions("slf4j"),
  "org.slf4j" % "jcl-over-slf4j" %  versions("slf4j")

)

