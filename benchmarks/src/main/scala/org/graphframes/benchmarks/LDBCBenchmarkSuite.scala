package org.graphframes.benchmarks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.graphframes.GraphFrame
import org.graphframes.examples.LDBCUtils
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Warmup
import org.openjdk.jmh.infra.Blackhole

import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties
import java.util.concurrent.TimeUnit

class LDBCBenchmarkSuite {
  private def spark: SparkSession = {
    SparkSession.getActiveSession match {
      case Some(session) => session
      case None =>
        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("GraphFramesBenchmarks")
          .config("spark.sql.shuffle.partitions", s"${Runtime.getRuntime.availableProcessors()}")
          .config("spark.driver.memory", "8g")
          .config("spark.executor.memory", "8g")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        spark
    }
  }

  private def resourcesPath = Path.of(new File("target").toURI())
  private def unreachableID = 9223372036854775807L

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 1)
  @Measurement(iterations = 2)
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def benchmarkSP(blackhole: Blackhole): Unit = {
    LDBCUtils.downloadLDBCIfNotExists(resourcesPath, LDBCUtils.GRAPH500_22)
    val caseRoot = resourcesPath.resolve(LDBCUtils.GRAPH500_22)

    val expectedResults = spark.read
      .format("csv")
      .option("header", "false")
      .schema(StructType(Seq(StructField("id", LongType), StructField("distance", LongType))))
      .csv(caseRoot.resolve(s"${LDBCUtils.GRAPH500_22}-BFS").toString)

    val props = new Properties()
    val stream = Files.newInputStream(caseRoot.resolve(s"${LDBCUtils.GRAPH500_22}.properties"))
    props.load(stream)
    stream.close()

    val sourceVertex =
      props.getProperty(s"graph.${LDBCUtils.GRAPH500_22}.bfs.source-vertex").toLong

    val edges = spark.read
      .format("csv")
      .option("header", "false")
      .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
      .load(caseRoot.resolve(s"${LDBCUtils.GRAPH500_22}.e").toString)
    val vertices = spark.read
      .format("csv")
      .option("header", "false")
      .schema(StructType(Seq(StructField("id", LongType))))
      .load(caseRoot.resolve(s"${LDBCUtils.GRAPH500_22}.v").toString)
    val graph = GraphFrame(vertices, edges)

    val spResults = graph.shortestPaths
      .setUseLocalCheckpoints(true)
      .landmarks(Seq(sourceVertex))
      .setCheckpointInterval(1)
      .setAlgorithm("graphframes")
      .run()
      .select(
        col(GraphFrame.ID),
        col("distances").getItem(sourceVertex).cast(LongType).alias("got_distance"))
      .na
      .fill(Map("got_distance" -> unreachableID))

    val cntOfMismatches = spResults
      .join(expectedResults, Seq("id"))
      .filter(col("got_distance") =!= col("distance"))
      .count()
    blackhole.consume(assert(cntOfMismatches == 0))
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 1)
  @Measurement(iterations = 2)
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def benchmarkCC(blackhole: Blackhole): Unit = {
    LDBCUtils.downloadLDBCIfNotExists(resourcesPath, LDBCUtils.GRAPH500_22)
    val caseRoot = resourcesPath.resolve(LDBCUtils.GRAPH500_22)

    val expectedResults = spark.read
      .format("csv")
      .option("header", "false")
      .schema(StructType(Seq(StructField("id", LongType), StructField("wcomp", LongType))))
      .csv(caseRoot.resolve(s"${LDBCUtils.GRAPH500_22}-WCC").toString)

    val edges = spark.read
      .format("csv")
      .option("header", "false")
      .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
      .load(caseRoot.resolve(s"${LDBCUtils.GRAPH500_22}.e").toString)
    val vertices = spark.read
      .format("csv")
      .option("header", "false")
      .schema(StructType(Seq(StructField("id", LongType))))
      .load(caseRoot.resolve(s"${LDBCUtils.GRAPH500_22}.v").toString)
    val graph = GraphFrame(vertices, edges)

    val ccResults =
      graph.connectedComponents.setUseLocalCheckpoints(true).setAlgorithm("graphframes").run()
    val cntOfMismatches = ccResults
      .join(expectedResults, Seq("id"))
      .filter(col("wcomp") =!= col("component"))
      .count()
    blackhole.consume(assert(cntOfMismatches == 0))
  }
}
