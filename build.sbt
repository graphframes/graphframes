import laika.config.LaikaKeys
import laika.config.SyntaxHighlighting
import laika.format.Markdown.GitHubFlavor
import org.typelevel.scalacoptions.ScalacOptions

lazy val sparkVer = sys.props.getOrElse("spark.version", "3.5.5")
lazy val sparkMajorVer = sparkVer.substring(0, 1)
lazy val sparkBranch = sparkVer.substring(0, 3)
lazy val scalaVersions = sparkMajorVer match {
  case "4" => Seq("2.13.16")
  case "3" => Seq("2.12.18", "2.13.16")
  case _ => throw new IllegalArgumentException(s"Unsupported Spark version: $sparkVer.")
}
lazy val scalaVer = sys.props.getOrElse("scala.version", scalaVersions.head)
lazy val defaultScalaTestVer = "3.2.19"
lazy val jmhVersion = "1.37"

// Some vendors are using an own shading rule for protobuf
lazy val protobufShadingPattern = sys.props.getOrElse("vendor.name", "oss") match {
  case "oss" => "org.sparkproject.connect.protobuf.@1"
  case "dbx" => "grpc_shaded.com.google.protobuf.@1"
  case s: String =>
    throw new IllegalArgumentException(s"Unsupported vendor name: $s; supported: 'oss', 'dbx'")
}

// Docs deployment URI
lazy val siteBaseUri = sys.props.getOrElse("docs.mode", "preview") match {
  case "production" => "https://graphframes.io"
  case "preview" => "localhost:4242"
  case s: String => s"https://$s"
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
    url = url("https://github.com/james-willis")))
ThisBuild / crossScalaVersions := scalaVersions

// Scalafix configuration
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := "4.12.3" // The maximal version that supports both 2.13.12 and 2.12.18

// Don't publish the root project
publishArtifact := false

lazy val commonSetting = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % sparkVer % "provided" cross CrossVersion.for3Use2_13,
    "org.apache.spark" %% "spark-mllib" % sparkVer % "provided" cross CrossVersion.for3Use2_13,
    "org.slf4j" % "slf4j-api" % "2.0.17" % "provided",
    "org.scalatest" %% "scalatest" % defaultScalaTestVer % Test,
    "com.github.zafarkhaja" % "java-semver" % "0.10.2" % Test),
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

  // Scalac options
  tpolecatScalacOptions ++= Set(
    ScalacOptions.lint,
    ScalacOptions.deprecation,
    ScalacOptions.warnDeadCode,
    ScalacOptions.warnUnusedExplicits,
    ScalacOptions.warnUnusedImplicits,
    ScalacOptions.warnUnusedImports,
    ScalacOptions.warnUnusedParams,
    ScalacOptions.warnUnusedPrivates,
    ScalacOptions.warnUnusedNoWarn,
    ScalacOptions.source3,
    ScalacOptions.fatalWarnings),
  tpolecatExcludeOptions ++= Set(ScalacOptions.warnNonUnitStatement),
  Test / tpolecatExcludeOptions ++= Set(
    ScalacOptions.warnValueDiscard,
    ScalacOptions.warnUnusedLocals,
    ScalacOptions.warnUnusedExplicits,
    ScalacOptions.warnUnusedImplicits,
    ScalacOptions.warnUnusedParams,
    ScalacOptions.warnUnusedPrivates,
    ScalacOptions.warnNumericWiden,
    ScalacOptions.privateWarnNumericWiden,
  ))

lazy val graphx = (project in file("graphx"))
  .settings(
    commonSetting,
    name := "graphframes-graphx",
    moduleName := s"${name.value}-spark$sparkMajorVer",
    // Export the JAR so that this can be excluded from shading in connect
    exportJars := true,

    // for scala 2.13 we should mark "unused" class tags by @nowarn,
    // for scala 2.12 we shouldn't
    // the only way at the moment is to not check unused @nowarn for GraphX
    tpolecatExcludeOptions ++= Set(ScalacOptions.warnUnusedNoWarn, ScalacOptions.privateWarnUnusedNoWarn),

    // Global settings
    Global / concurrentRestrictions := Seq(Tags.limitAll(1)),
    autoAPIMappings := true,
    coverageHighlighting := false,
    Test / packageBin / publishArtifact := false,
    Test / packageDoc / publishArtifact := false,
    Test / packageSrc / publishArtifact := false,
    Compile / packageBin / publishArtifact := true,
    Compile / packageDoc / publishArtifact := true,
    Compile / packageSrc / publishArtifact := true)

lazy val core = (project in file("core"))
  .dependsOn(graphx)
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
    // Don't shade anything, we just need to rename the protobuf packages to what's bundled with Spark
    assembly / assemblyExcludedJars := (assembly / fullClasspath).value,
    Compile / packageBin := assembly.value,
    Test / packageBin / publishArtifact := false,
    Test / packageDoc / publishArtifact := false,
    Test / packageSrc / publishArtifact := false,
    Compile / packageBin / publishArtifact := true,
    Compile / packageDoc / publishArtifact := true,
    Compile / packageSrc / publishArtifact := true)

lazy val benchmarks = (project in file("benchmarks"))
  .dependsOn(core)
  .settings(
    commonSetting,
    coverageEnabled := false,
    name := "graphframes-benchmarks",
    publish / skip := true,
    publishArtifact := false,
    libraryDependencies ++= Seq(
      // required for jmh IDEA plugin.
      "org.openjdk.jmh" % "jmh-generator-annprocess" % jmhVersion,
      // for benchmarks the scope should be runtime
      "org.apache.spark" %% "spark-graphx" % sparkVer % "runtime" cross CrossVersion.for3Use2_13,
      "org.apache.spark" %% "spark-sql" % sparkVer % "runtime" cross CrossVersion.for3Use2_13,
      "org.apache.spark" %% "spark-mllib" % sparkVer % "runtime" cross CrossVersion.for3Use2_13))
  .enablePlugins(JmhPlugin)

// Laika things
lazy val buildAndCopyScalaDoc = taskKey[Unit]("Build and copy ScalaDoc to docs/api")
lazy val buildAndCopyPythonDoc = taskKey[Unit]("Build and copy PythonDoc to docs/api")
lazy val generateAtomFeed = taskKey[Unit]("Generate Atom feed")

lazy val docs = (project in file("docs"))
  .dependsOn(core)
  .enablePlugins(LaikaPlugin)
  .enablePlugins(MdocPlugin)
  .settings(
    commonSetting,
    coverageEnabled := false,
    name := "docs",
    publish / skip := true,
    publishArtifact := false,
    mdocVariables := Map("VERSION" -> (version.value match {
      case LaikaCustoms.thisVersionShortRegex(v, _) => v
      case v => v
    })),
    mdocIn := baseDirectory.value / "mdoc",
    mdocOut := baseDirectory.value / "src" / "02-quick-start",
    mdocExtraArguments := Seq("--no-link-hygiene"),
    buildAndCopyPythonDoc := LaikaCustoms.copyAll(
      baseDirectory.value.toPath.resolve("src/api/python"),
      baseDirectory.value.toPath.getParent.resolve("python/docs/_build/html")),
    buildAndCopyScalaDoc := LaikaCustoms.copyAll(
      baseDirectory.value.toPath.resolve("src/api/scaladoc"),
      (core / Compile / doc).value.toPath),
    generateAtomFeed := LaikaCustoms
      .generateAtomFeed(baseDirectory.value.toPath.resolve("src/05-blog"), siteBaseUri),
    laikaConfig := LaikaCustoms
      .laikaConfig((benchmarks / baseDirectory).value.toPath.resolve("jmh-result.json"))
      .withConfigValue(LaikaKeys.siteBaseURL, siteBaseUri)
      .withConfigValue("pydoc.baseUri", s"$siteBaseUri/api/python")
      .withConfigValue("scaladoc.baseUri", s"$siteBaseUri/api/scaladoc")
      .withConfigValue("spark.version", sparkVer)
      .withConfigValue("scala.version", scalaVer),
    laikaExtensions := Seq(GitHubFlavor, SyntaxHighlighting, LaikaCustomDirectives),
    laikaHTML := (laikaHTML dependsOn mdoc.toTask(
      "") dependsOn generateAtomFeed dependsOn buildAndCopyScalaDoc dependsOn buildAndCopyPythonDoc dependsOn (core / Compile / doc)).value,
    laikaPreview := (laikaPreview dependsOn mdoc.toTask(
      "") dependsOn generateAtomFeed dependsOn buildAndCopyScalaDoc dependsOn buildAndCopyPythonDoc dependsOn (core / Compile / doc)).value,
    laikaTheme := LaikaCustoms.heliumTheme(version.value),
    Laika / sourceDirectories := Seq((ThisBuild / baseDirectory).value / "docs" / "src"))
