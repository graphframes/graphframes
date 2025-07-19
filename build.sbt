lazy val sparkVer = sys.props.getOrElse("spark.version", "3.5.5")
lazy val sparkMajorVer = sparkVer.substring(0, 1)
lazy val sparkBranch = sparkVer.substring(0, 3)
lazy val scalaVersions = sparkMajorVer match {
  case "4" => Seq("2.13.12")
  case "3" => Seq("2.12.18", "2.13.12")
  case _ => throw new IllegalArgumentException(s"Unsupported Spark version: $sparkVer.")
}
lazy val scalaVer = sys.props.getOrElse("scala.version", scalaVersions(0))
lazy val defaultScalaTestVer = "3.0.8"

// Some vendors are using an own shading rule for protobuf
lazy val protobufShadingPattern = sys.props.getOrElse("vendor.name", "oss") match {
  case "oss" => "org.sparkproject.connect.protobuf.@1"
  case "dbx" => "grpc_shaded.com.google.protobuf.@1"
  case s: String =>
    throw new IllegalArgumentException(s"Unsupported vendor name: $s; supported: 'oss', 'dbx'")
}

lazy val protocVersion = sparkMajorVer match {
  case "4" => "4.29.3"
  case "3" => "3.23.4"
  case _ => throw new IllegalArgumentException(s"Unsupported Spark version: $sparkVer.")
}

ThisBuild / scalaVersion := scalaVer
ThisBuild / organization := "io.graphframes"
ThisBuild / homepage := Some(url("https://graphframes.io/"))
ThisBuild / licenses := Seq("Apache-2.0" -> url("https://opensource.org/licenses/Apache-2.0"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/graphframes/graphframes"),
    "scm:git@github.com:graphframes/graphframes.git"))
// The list of active maintainers with Write/Maintain/Admin access
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
    url = url("https://github.com/SemyonSinchenko")),
  Developer(
    id = "james-willis",
    name = "James Willis",
    email = "jimwillis95@gmail.com",
    url = url("https://github.com/james-willis"))
)
ThisBuild / crossScalaVersions := scalaVersions

// Scalafix configuration
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := "4.8.10" // The maximal version that supports both 2.13.8 and 2.12.18

lazy val commonSetting = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-graphx" % sparkVer % "provided" cross CrossVersion.for3Use2_13,
    "org.apache.spark" %% "spark-sql" % sparkVer % "provided" cross CrossVersion.for3Use2_13,
    "org.apache.spark" %% "spark-mllib" % sparkVer % "provided" cross CrossVersion.for3Use2_13,
    "org.slf4j" % "slf4j-api" % "2.0.16" % "provided",
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

lazy val core = (project in file("core"))
  .settings(
    commonSetting,
    name := "graphframes",
    moduleName := s"${name.value}-spark$sparkMajorVer",
    // Export the JAR so that this can be excluded from shading in connect
    exportJars := true,

    // Global settings
    Global / concurrentRestrictions := Seq(Tags.limitAll(1)),
    autoAPIMappings := true,
    coverageHighlighting := false,

    Compile / unmanagedSourceDirectories += (Compile / baseDirectory).value / "src" / "main" / s"scala-spark-$sparkMajorVer",

    Test / packageBin / publishArtifact := false,
    Test / packageDoc / publishArtifact := false,
    Test / packageSrc / publishArtifact := false,
    Compile / packageBin / publishArtifact := true,
    Compile / packageDoc / publishArtifact := true,
    Compile / packageSrc / publishArtifact := true)

lazy val connect = (project in file("connect"))
  .dependsOn(core)
  .settings(
    name := s"graphframes-connect",
    moduleName := s"${name.value}-spark${sparkMajorVer}",
    commonSetting,
    Compile / unmanagedSourceDirectories += (Compile / baseDirectory).value / "src" / "main" / s"scala-spark-$sparkMajorVer",
    Compile / PB.targets := Seq(PB.gens.java -> (Compile / sourceManaged).value),
    Compile / PB.includePaths ++= Seq(file("src/main/protobuf")),
    PB.protocVersion := protocVersion,
    PB.additionalDependencies := Nil,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-connect" % sparkVer % "provided" cross CrossVersion.for3Use2_13),

    // Assembly and shading
    assembly / assemblyJarName := s"${moduleName.value}_${(scalaBinaryVersion).value}-${version.value}.jar",
    assembly / test := {},
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("com.google.protobuf.**" -> protobufShadingPattern).inAll),
    // Don't actually shade anything, we just need to rename the protobuf packages to what's bundled with Spark
    assembly / assemblyExcludedJars := (assembly / fullClasspath).value,
    Compile / packageBin := assembly.value,
    Test / packageBin / publishArtifact := false,
    Test / packageDoc / publishArtifact := false,
    Test / packageSrc / publishArtifact := false,
    Compile / packageBin / publishArtifact := true,
    Compile / packageDoc / publishArtifact := true,
    Compile / packageSrc / publishArtifact := true
  )
