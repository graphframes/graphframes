name := "graphframes"

version := "0.1-SNAPSHOT"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.11.7"

licenses += "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0-SNAPSHOT"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.0-SNAPSHOT"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfuture",
  "-Ywarn-unused-import"
)

scalacOptions in (Compile, console) := Seq()

// Run tests with more memory
javaOptions in test += "-Xmx2G"

fork := true
