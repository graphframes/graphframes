import xerial.sbt.Sonatype.sonatypeCentralHost

lazy val sparkVer = sys.props.getOrElse("spark.version", "3.5.4")
lazy val sparkBranch = sparkVer.substring(0, 3)
lazy val defaultScalaVer = sparkBranch match {
  case "3.5" => "2.12.18"
  case "3.4" => "2.12.17"
  case _ => throw new IllegalArgumentException(s"Unsupported Spark version: $sparkVer.")
}
lazy val scalaVer = sys.props.getOrElse("scala.version", defaultScalaVer)
lazy val defaultScalaTestVer = scalaVer match {
  case s if s.startsWith("2.12") || s.startsWith("2.13") => "3.0.8"
}

ThisBuild / scalaVersion := scalaVer
ThisBuild / organization := "org.graphframes"
ThisBuild / homepage := Some(url("https://graphframes.io/"))
ThisBuild / licenses := Seq("Apache-2.0" -> url("https://opensource.org/licenses/Apache-2.0"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/graphframes/graphframes"),
    "scm:git@github.com:graphframes/graphframes.git"))
ThisBuild / developers := List(
  Developer(
    id = "rjurney",
    name = "Russell Jurney",
    email = "russell.jurney@gmail.com",
    url = url("https://github.com/rjurney")),
  Developer(
    id = "SemyonSinchenko",
    name = "Sem",
    email = "ssinchenko@apache.org",
    url = url("https://github.com/SemyonSinchenko")))
ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"
ThisBuild / sonatypeRepository := "https://s01.oss.sonatype.org/service/local"
ThisBuild / sonatypeProfileName := "io.graphframes"
ThisBuild / crossScalaVersions := Seq("2.12.18", "2.13.8")

// Scalafix configuration
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := "4.8.10" // The maximal version that supports both 2.13.8 and 2.12.18

lazy val commonSetting = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-graphx" % sparkVer % "provided" cross CrossVersion.for3Use2_13,
    "org.apache.spark" %% "spark-sql" % sparkVer % "provided" cross CrossVersion.for3Use2_13,
    "org.apache.spark" %% "spark-mllib" % sparkVer % "provided" cross CrossVersion.for3Use2_13,
    "org.slf4j" % "slf4j-api" % "2.0.16",
    "org.scalatest" %% "scalatest" % defaultScalaTestVer % Test,
    "com.github.zafarkhaja" % "java-semver" % "0.10.2" % Test),
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

  // Scalafix
  scalacOptions ++= Seq(
    "-Xlint", // to enforce code quality checks
    if (scalaVersion.value.startsWith("2.12")) {
      // fail on warning
      "-Xfatal-warnings"
    } else {
      "-Werror" // the same but in 2.13
    },
    // scalastyle related things
    if (scalaVersion.value.startsWith("2.12"))
      "-Ywarn-unused-import"
    else
      "-Wunused:imports"))

lazy val root = (project in file("."))
  .settings(
    commonSetting,
    name := "graphframes",
    moduleName := s"${name.value}-spark${sparkBranch}",

    // Global settings
    Global / concurrentRestrictions := Seq(Tags.limitAll(1)),
    autoAPIMappings := true,
    coverageHighlighting := false,

    // Assembly settings
    assembly / test := {}, // No tests in assembly
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    Test / packageBin / publishArtifact := false,
    Test / packageDoc / publishArtifact := false,
    Test / packageSrc / publishArtifact := false,
    Compile / packageBin / publishArtifact := true,
    Compile / packageDoc / publishArtifact := true,
    Compile / packageSrc / publishArtifact := true)

lazy val connect = (project in file("graphframes-connect"))
  .dependsOn(root)
  .settings(
    commonSetting,
    name := "graphframes-connect",
    moduleName := s"${name.value}-spark${sparkBranch}",
    Compile / PB.targets := Seq(PB.gens.java -> (Compile / sourceManaged).value),
    Compile / PB.includePaths ++= Seq(file("src/main/protobuf")),
    PB.protocVersion := "3.23.4", // Spark 3.5 branch
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-connect" % sparkVer % "provided" cross CrossVersion.for3Use2_13,
      "com.google.protobuf" % "protobuf-java" % PB.protocVersion.value % "provided"),

    // Assembly and shading
    assembly / test := {},
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("com.google.protobuf.**" -> "org.sparkproject.connect.protobuf.@1").inAll),
    assembly / assemblyMergeStrategy := {
      case PathList("google", "protobuf", xs @ _*) => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    assembly / assemblyExcludedJars := (Compile / fullClasspath).value.filter { className =>
      className.data
        .getName()
        .contains("scala-library-") || className.data
        .getName()
        .contains("slf4j-api-")
    },
    Compile / packageBin := assembly.value,
    Test / packageBin / publishArtifact := false,
    Test / packageDoc / publishArtifact := false,
    Test / packageSrc / publishArtifact := false,
    Compile / packageBin / publishArtifact := true,
    Compile / packageDoc / publishArtifact := false,
    Compile / packageSrc / publishArtifact := false)

// The same connect but with a different shading rules
lazy val databrickConnect = (project in file("graphframes-connect"))
  .dependsOn(root)
  .settings(
    commonSetting,
    name := "graphframes-databricks-connect",
    moduleName := s"${name.value}-spark${sparkBranch}",
    Compile / PB.targets := Seq(PB.gens.java -> (Compile / sourceManaged).value),
    Compile / PB.includePaths ++= Seq(file("src/main/protobuf")),
    target := file("graphframes-connect-databricks"),
    PB.protocVersion := "3.23.4", // Spark 3.5 branch
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-connect" % sparkVer % "provided" cross CrossVersion.for3Use2_13),

    // Assembly and shading
    assembly / test := {},
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("com.google.protobuf.**" -> "grpc_shaded.com.google.protobuf.@1").inAll),
    assembly / assemblyMergeStrategy := {
      case PathList("google", "protobuf", xs @ _*) => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    assembly / assemblyExcludedJars := (Compile / fullClasspath).value.filter { className =>
      className.data
        .getName()
        .contains("scala-library-") || className.data
        .getName()
        .contains("slf4j-api-")
    },
    Compile / packageBin := assembly.value,
    Test / packageBin / publishArtifact := false,
    Test / packageDoc / publishArtifact := false,
    Test / packageSrc / publishArtifact := false,
    Compile / packageBin / publishArtifact := true,
    Compile / packageDoc / publishArtifact := false,
    Compile / packageSrc / publishArtifact := false)
