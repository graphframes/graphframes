// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

val sparkVer = sys.props.getOrElse("spark.version", "1.4.1")
val sparkBranch = sparkVer.substring(0, 3)
val defaultScalaVer = sparkBranch match {
  case "1.4" => "2.10.4"
  case "1.5" => "2.10.4"
  case "1.6" => "2.10.5"
  case "2.0" => "2.11.7"
}
val defaultScalaTestVer = sparkBranch match {
  case "1.4" => "2.0"
  case "1.5" => "2.0"
  case "1.6" => "2.0"
  case "2.0" => "2.2.6" // scalatest_2.11 does not have 2.0 published
}
val scalaVer = sys.props.getOrElse("scala.version", defaultScalaVer)

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

parallelExecution := false

unmanagedSourceDirectories in Compile ++=
  Seq(baseDirectory.value / "src" / "main" / (if (sparkBranch == "1.4") "spark-1.4" else "spark-x"))

scalacOptions in (Compile, doc) ++= Seq(
  "-groups",
  "-implicits",
  "-skip-packages", Seq("org.apache.spark").mkString(":"))

scalacOptions in (Test, doc) ++= Seq("-groups", "-implicits")

autoAPIMappings := true
