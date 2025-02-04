// https://github.com/scala/bug/issues/12632
// https://github.com/scoverage/sbt-scoverage/issues/475
// Workaround:
ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.10")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.4.0")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")
