import ReleaseTransformations.*
import sbt.Credentials
import sbt.Keys.credentials

lazy val sparkVer = sys.props.getOrElse("spark.version", "3.5.5")
lazy val sparkBranch = sparkVer.substring(0, 3)
lazy val defaultScalaVer = sparkBranch match {
  case "4.0" => "2.13.16"
  case "3.5" => "2.12.18"
  case "3.4" => "2.12.17"
  case "3.3" => "2.12.15"
  case _ => throw new IllegalArgumentException(s"Unsupported Spark version: $sparkVer.")
}
lazy val scalaVer = sys.props.getOrElse("scala.version", defaultScalaVer)
lazy val defaultScalaTestVer = "3.0.8"

ThisBuild / version := {
  val baseVersion = (ThisBuild / version).value
  s"${baseVersion}-spark${sparkBranch}"
}

ThisBuild / scalaVersion := scalaVer
ThisBuild / organization := "org.graphframes"
ThisBuild / crossScalaVersions := Seq("2.12.18", "2.13.8")

def sparkVersionSettings(): Seq[Setting[_]] = {
  if (sparkVer.startsWith("4")) {
    Seq(
      resolvers += "Spark 4.0.0 RC3" at "https://repository.apache.org/content/repositories/orgapachespark-1479/",
      Compile / unmanagedSourceDirectories += (Compile / baseDirectory).value / "src" / "main" / "scala-spark-4",
      Test / unmanagedSourceDirectories += (Test / baseDirectory).value / "src" / "test" / "scala-spark-4",
    )
  } else {
    Seq(
      Compile / unmanagedSourceDirectories += (Compile / baseDirectory).value / "src" / "main" / "scala-spark-3",
      Test / unmanagedSourceDirectories += (Test / baseDirectory).value / "src" / "test" / "scala-spark-3",
    )
  }
}

lazy val commonSetting = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-graphx" % sparkVer % "provided" cross CrossVersion.for3Use2_13,
    "org.apache.spark" %% "spark-sql" % sparkVer % "provided" cross CrossVersion.for3Use2_13,
    "org.apache.spark" %% "spark-mllib" % sparkVer % "provided" cross CrossVersion.for3Use2_13,
    "org.slf4j" % "slf4j-api" % "2.0.16",
    "org.scalatest" %% "scalatest" % defaultScalaTestVer % Test,
    "com.github.zafarkhaja" % "java-semver" % "0.10.2" % Test),
  credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials"),
  licenses := Seq("Apache-2.0" -> url("https://opensource.org/licenses/Apache-2.0")),
  Compile / scalacOptions ++= Seq("-deprecation", "-feature"),
  Compile / doc / scalacOptions ++= Seq(
    "-groups",
    "-implicits",
    "-skip-packages",
    Seq("org.apache.spark").mkString(":")),
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
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED"),
  credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials"))

lazy val root = (project in file("."))
  .settings(
    commonSetting,
    sparkVersionSettings(),
    name := "graphframes",
    Compile / scalacOptions ++= Seq("-deprecation", "-feature"),

    // Global settings
    Global / concurrentRestrictions := Seq(Tags.limitAll(1)),
    autoAPIMappings := true,
    coverageHighlighting := false,

    // Release settings
    releaseProcess := Seq[ReleaseStep](
      inquireVersions,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      setNextVersion,
      commitNextVersion),

    // Assembly settings
    assembly / test := {}, // No tests in assembly
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    })

lazy val connect = (project in file("graphframes-connect"))
  .dependsOn(root)
  .settings(
    commonSetting,
    sparkVersionSettings(),
    name := "graphframes-connect",
    Compile / PB.targets := Seq(PB.gens.java -> (Compile / sourceManaged).value),
    Compile / PB.includePaths ++= Seq(file("src/main/protobuf")),
    PB.protocVersion := "3.23.4", // Spark 3.5 branch
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-connect" % sparkVer % "provided" cross CrossVersion.for3Use2_13),

    // Assembly and shading
    assembly / test := {},
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("com.google.protobuf.**" -> "org.sparkproject.connect.protobuf.@1").inAll),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )
