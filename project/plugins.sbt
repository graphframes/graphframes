// https://github.com/scala/bug/issues/12632
// https://github.com/scoverage/sbt-scoverage/issues/475
// Workaround:
ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.10")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.4")

// Protobuf things needed for the Spark Connect
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.10.10"

// Scalafix
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.3")

// SBT CI Release
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.11.1")

// JMH & benchmarking
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.7")

// Laika
addSbtPlugin("org.typelevel" % "laika-sbt" % "1.3.2")
addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.7.2")

// Typelevel helper
addSbtPlugin("org.typelevel" % "sbt-tpolecat" % "0.5.2")
