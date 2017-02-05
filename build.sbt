name := "candleProxy"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("com.typesafe.akka" %% "akka-actor" % "2.4.16")

val circeVersion = "0.7.0"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)
    