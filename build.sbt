import ReleaseTransformations._

lazy val sparkVer = sys.props.getOrElse("spark.version", "3.5.4")
lazy val sparkBranch = sparkVer.substring(0, 3)
lazy val defaultScalaVer = sparkBranch match {
  case "3.5" => "2.12.18"
  case _ => throw new IllegalArgumentException(s"Unsupported Spark version: $sparkVer.")
}
lazy val scalaVer = sys.props.getOrElse("scala.version", defaultScalaVer)
lazy val defaultScalaTestVer = scalaVer match {
  case s if s.startsWith("2.12") || s.startsWith("2.13") => "3.0.8"
}

ThisBuild / version := {
  val baseVersion = (ThisBuild / version).value
  s"${baseVersion}-spark${sparkBranch}"
}

ThisBuild / scalaVersion := scalaVer
ThisBuild / organization := "org.graphframes"
ThisBuild / crossScalaVersions := Seq("2.12.18", "2.13.8")

lazy val root = (project in file("."))
  .settings(
    name := "graphframes",

    // Replace spark-packages plugin functionality with explicit dependencies
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-graphx" % sparkVer % "provided" cross CrossVersion.for3Use2_13,
      "org.apache.spark" %% "spark-sql" % sparkVer % "provided" cross CrossVersion.for3Use2_13,
      "org.apache.spark" %% "spark-mllib" % sparkVer % "provided" cross CrossVersion.for3Use2_13,
      "org.slf4j" % "slf4j-api" % "1.7.16",
      "org.scalatest" %% "scalatest" % defaultScalaTestVer % Test,
      "com.github.zafarkhaja" % "java-semver" % "0.9.0" % Test
    ),

    licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")),

    // Modern way to set Scala options
    Compile / scalacOptions ++= Seq("-deprecation", "-feature"),

    Compile / doc / scalacOptions ++= Seq(
      "-groups",
      "-implicits",
      "-skip-packages", Seq("org.apache.spark").mkString(":")
    ),

    Test / doc / scalacOptions ++= Seq("-groups", "-implicits"),

    // Test settings
    Test / fork := true,
    Test / parallelExecution := false,

    Test / javaOptions ++= Seq(
      "-XX:+IgnoreUnrecognizedVMOptions",
      "-Xmx2048m",
      "-XX:ReservedCodeCacheSize=384m",
      "-XX:MaxMetaspaceSize=384m",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED"
    ),

    // Global settings
    Global / concurrentRestrictions := Seq(
      Tags.limitAll(1)
    ),

    autoAPIMappings := true,

    coverageHighlighting := false,

    // Release settings
    releaseProcess := Seq[ReleaseStep](
      inquireVersions,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      setNextVersion,
      commitNextVersion
    ),

    // Assembly settings
    assembly / test := {}, // No tests in assembly
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },

    credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
  )