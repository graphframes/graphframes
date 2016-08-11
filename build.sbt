// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

val sparkVer = sys.props.getOrElse("spark.version", "2.0.0")
val sparkBranch = sparkVer.substring(0, 3)
val defaultScalaVer = sparkBranch match {
  case "1.4" => "2.10.4"
  case "1.5" => "2.10.4"
  case "1.6" => "2.10.5"
  case "2.0" => "2.11.7"
}
val scalaVer = sys.props.getOrElse("scala.version", defaultScalaVer)
val defaultScalaTestVer = scalaVer match {
  case "2.10.4" => "2.0"
  case "2.10.5" => "2.0"
  case "2.11.7" => "2.2.6" // scalatest_2.11 does not have 2.0 published
}

sparkVersion := sparkVer

scalaVersion := scalaVer

spName := "graphframes/graphframes"

// Don't forget to set the version
version := s"0.1.0-spark$sparkBranch-SNAPSHOT"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))


// Add Spark components this package depends on, e.g, "mllib", ....
sparkComponents ++= Seq("graphx", "sql")

// uncomment and change the value below to change the directory where your zip artifact will be created
// spDistDirectory := target.value

// add any Spark Package dependencies using spDependencies.
// e.g. spDependencies += "databricks/spark-avro:0.1"

libraryDependencies += "org.scalatest" %% "scalatest" % defaultScalaTestVer % "test"

// These versions are ancient, but they cross-compile around scala 2.10 and 2.11.
// Update them when dropping support for scala 2.10
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-api" % "2.1.2"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

parallelExecution := false

unmanagedSourceDirectories in Compile ++=
  Seq(baseDirectory.value / "src" / "main" /
    (if (sparkBranch == "1.4") "spark-1.4"
     else if (sparkBranch == "1.5" || sparkBranch == "1.6") "spark-1.x"
     else "spark-2.x"))

scalacOptions in (Compile, doc) ++= Seq(
  "-groups",
  "-implicits",
  "-skip-packages", Seq("org.apache.spark").mkString(":"))

scalacOptions in (Test, doc) ++= Seq("-groups", "-implicits")

// This fixes a class loader problem with scala.Tuple2 class, scala-2.11, Spark 2.x
// "scala.ScalaReflectionException: class scala.Tuple2 in JavaMirror with sbt.classpath.ClasspathFilter@61de7710 of type class sbt.classpath.ClasspathFilter with classpath [<unknown>] and parent being sbt.classpath.ClasspathUtilities$$anon$1@4b18bc2d of type class sbt.classpath.ClasspathUtilities$$anon$1 with classpath"
fork in Test := true
//  (if (sparkBranch.startsWith("2.") && scalaVer.startsWith("2.11.")) true else false)

// This and the next line fix a problem with forked run: https://github.com/scalatest/scalatest/issues/770
javaOptions in Test ++= Seq("-Xmx2048m", "-XX:ReservedCodeCacheSize=384m", "-XX:MaxPermSize=384m")

concurrentRestrictions in Global := Seq(
  Tags.limitAll(1))

autoAPIMappings := true
