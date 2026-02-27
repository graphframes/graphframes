package org.graphframes.benchmarks

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame
import org.openjdk.jmh.annotations.*

import java.io.File
import java.nio.file.Path

trait LDBCBenchmarkBase {
  @Param(Array("wiki-Talk"))
  var graphName: String = _

  @Param(Array("true"))
  var useLocalCheckpoints: String = _

  var spark: SparkSession = _
  var graph: GraphFrame = _

  protected def cacheDir: Path = Path.of(new File("target").toURI).resolve("ldbc-cache")

  @Setup(Level.Trial)
  def setup(): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("GraphFramesBenchmarks")
      .set("spark.sql.shuffle.partitions", s"${Runtime.getRuntime.availableProcessors() * 2}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    spark = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setCheckpointDir("/tmp/graphframes-checkpoints")

    loadGraph(graphName)
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  def loadGraph(name: String): Unit = {
    val loader = new ParquetDataLoader(cacheDir)
    val graphDir = cacheDir.resolve(name)

    loader.downloadParquetIfNotExists(name)

    val isWeighted = ParquetDataLoader.weightedGraphs.contains(name)

    val edges = if (isWeighted) {
      spark.read
        .parquet(graphDir.resolve(s"${name}-e.parquet").toString)
        .withColumnRenamed("source", "src")
        .withColumnRenamed("target", "dst")
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    } else {
      spark.read
        .parquet(graphDir.resolve(s"${name}-e.parquet").toString)
        .withColumnRenamed("source", "src")
        .withColumnRenamed("target", "dst")
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
    println(s"Read edges: ${edges.count()}")

    val vertices = spark.read
      .parquet(graphDir.resolve(s"${name}-v.parquet").toString)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(s"Read vertices: ${vertices.count()}")

    graph = GraphFrame(vertices, edges)
  }
}
