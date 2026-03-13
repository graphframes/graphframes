// https://github.com/scala/bug/issues/12632
// https://github.com/scoverage/sbt-scoverage/issues/475
// Workaround:
ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.4.4")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.6")

// Protobuf things needed for the Spark Connect
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.8")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.10.10"

// Scalafix
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.5")

// SBT CI Release
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.11.2")

// JMH & benchmarking
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.8")

// Laika
addSbtPlugin("org.typelevel" % "laika-sbt" % "1.3.2")
addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.8.2")

// Typelevel helper
addSbtPlugin("org.typelevel" % "sbt-tpolecat" % "0.5.2")

// JSONs processing for benchmarks
val circeVersion = "0.13.0"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)
