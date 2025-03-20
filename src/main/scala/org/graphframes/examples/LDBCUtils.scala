package org.graphframes.examples

import java.net.URL
import java.nio.file._
import java.util.Properties

import scala.sys.process._

import org.graphframes.GraphFrame

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.types.LongType

object LDBCUtils {
  // TODO: This object can be actually a class, parametrized by the id of LDBC graph (kgs, etc.)
  private val caseName = "kgs" // XS, 65.7Mb
  private val LDBCUrl = new URL(
    s"https://datasets.ldbcouncil.org/graphalytics/${caseName}.tar.zst")
  private val bufferSize = 8192 // 8Kb
  private val resourcesDir: Path = sys.env.get("LDBC_TEST_ROOT") match {
    case Some(s) => Paths.get(s).resolve(caseName)
    case None => throw new RuntimeException("LDBC_TEST_ROOT is not set!")
  }
  private val archivePath: Path = resourcesDir.resolve(s"${caseName}.tar.zst")
  //  Unreachable vertices should be given the value infinity (represented as 9223372036854775807).
  val UNREACHABLE_ID_VALUE = 9223372036854775807L

  private def checkZSTD(): Unit = {
    try {
      s"zstd --version".!
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          "zstd is not available or not found. Please install zstd and try again.",
          e)
    }
  }
  def downloadLDBCIfNotExists(): Unit = {
    if (Files.notExists(resourcesDir) || (Files.list(resourcesDir).count() == 0L)) {
      println("LDBC data not found. Downloading...")
      checkZSTD()
      if (Files.notExists(resourcesDir)) {
        Files.createDirectory(resourcesDir)
      }
      val connection = LDBCUrl.openConnection()
      val inputStream = connection.getInputStream
      val outputStream = Files.newOutputStream(archivePath)
      val buffer = new Array[Byte](bufferSize)
      var bytesRead = 0
      while ({ bytesRead = inputStream.read(buffer); bytesRead } != -1) {
        outputStream.write(buffer, 0, bytesRead)
      }
      inputStream.close()
      outputStream.close()
      println(s"Uncompressing ${archivePath.toString} to ${resourcesDir.toString}...")
      s"zstd -d ${archivePath.toString} -o ${archivePath.toString.replace(".zst", "")}".!
      s"tar -xf ${archivePath.toString.replace(".zst", "")} -C ${resourcesDir.toString}".!

      // Clean up
      Files.delete(archivePath)
      Files.delete(Paths.get(archivePath.toString.replace(".zst", "")))
    }
  }

  private def getProps: Properties = {
    downloadLDBCIfNotExists()
    val propsFile = resourcesDir.resolve(s"${caseName}.properties")
    val inputStream = Files.newInputStream(propsFile)
    val properties = new Properties()
    properties.load(inputStream)
    inputStream.close()
    properties
  }

  def getLDBCGraph(spark: SparkSession, makeUndirected: Boolean = true): GraphFrame = {
    val properties = getProps
    val vertices = spark.read
      .text(
        resourcesDir.resolve(properties.getProperty(s"graph.${caseName}.vertex-file")).toString)
      .select(col("value").cast(LongType).alias(GraphFrame.ID))

    val edges = spark.read
      .text(resourcesDir.resolve(properties.getProperty(s"graph.${caseName}.edge-file")).toString)
      .withColumn("split", split(col("value"), " "))
      .select(
        col("split").getItem(0).cast(LongType).alias(GraphFrame.SRC),
        col("split").getItem(1).cast(LongType).alias(GraphFrame.DST))

    if (makeUndirected) {
      GraphFrame(
        vertices,
        edges.union(
          edges.select(
            col(GraphFrame.DST).alias(GraphFrame.SRC),
            col(GraphFrame.SRC).alias(GraphFrame.DST))))
    } else {
      GraphFrame(vertices, edges)
    }
  }

  def getBFSExpectedResults(spark: SparkSession): DataFrame = {
    spark.read
      .text(resourcesDir.resolve(s"${caseName}-BFS").toString)
      .withColumn("split", split(col("value"), " "))
      .select(
        col("split").getItem(0).cast(LongType).alias(GraphFrame.ID),
        col("split").getItem(1).cast(LongType).alias("expectedDistance"))
  }

  def getCDLPExpectedResults(spark: SparkSession): DataFrame = {
    spark.read
      .text(resourcesDir.resolve(s"${caseName}-CDLP").toString)
      .withColumn("split", split(col("value"), " "))
      .select(
        col("split").getItem(0).cast(LongType).alias(GraphFrame.ID),
        col("split").getItem(1).cast(LongType).alias("expectedLabel"))
  }

  def getBFSTarget: Long = getProps.getProperty(s"graph.${caseName}.bfs.source-vertex").toLong
  def getCDLPMaxIter: Int = getProps.getProperty(s"graph.${caseName}.cdlp.max-iterations").toInt
}
