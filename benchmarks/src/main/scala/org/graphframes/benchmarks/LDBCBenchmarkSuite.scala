package org.graphframes.benchmarks

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame
import org.graphframes.examples.LDBCUtils
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(
  value = 1,
  jvmArgs = Array(
    "-Xmx10g",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED"))
class LDBCBenchmarkSuite {
  val benchmarkGraphName: String = LDBCUtils.WIKI_TALKS
  var graph: GraphFrame = _
  var props: Properties = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("GraphFramesBenchmarks")
      .set("spark.sql.shuffle.partitions", s"${Runtime.getRuntime.availableProcessors() * 2}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val context = spark.sparkContext
    context.setLogLevel("ERROR")
    context.setCheckpointDir("/tmp/graphframes-checkpoints")

    LDBCUtils.downloadLDBCIfNotExists(resourcesPath, benchmarkGraphName)

    val edges = spark.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", " ")
      .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
      .load(caseRoot.resolve(s"${benchmarkGraphName}.e").toString)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    println()
    println(s"Read edges: ${edges.count()}")

    val vertices = spark.read
      .format("csv")
      .option("header", "false")
      .schema(StructType(Seq(StructField("id", LongType))))
      .load(caseRoot.resolve(s"${benchmarkGraphName}.v").toString)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(s"Read vertices: ${vertices.count()}")

    graph = GraphFrame(vertices, edges)
    props = new Properties()
    val stream = Files.newInputStream(caseRoot.resolve(s"${benchmarkGraphName}.properties"))
    props.load(stream)
    stream.close()
  }

  private def caseRoot: Path = resourcesPath.resolve(benchmarkGraphName)

  private def resourcesPath = Path.of(new File("target").toURI)

  @Benchmark
  def benchmarkSP(blackhole: Blackhole): Unit = {
    val sourceVertex =
      props.getProperty(s"graph.${benchmarkGraphName}.bfs.source-vertex").toLong

    val spResults = graph.shortestPaths
      .setAlgorithm("graphframes")
      .landmarks(Seq(sourceVertex))
      .run()

    val res: Unit = spResults.write.format("noop").mode("overwrite").save()
    blackhole.consume(res)
  }

  @Benchmark
  def benchmarkSPlocalCheckpoints(blackhole: Blackhole): Unit = {
    val sourceVertex =
      props.getProperty(s"graph.${benchmarkGraphName}.bfs.source-vertex").toLong

    val spResults = graph.shortestPaths
      .setUseLocalCheckpoints(true)
      .landmarks(Seq(sourceVertex))
      .setCheckpointInterval(1)
      .setAlgorithm("graphframes")
      .run()

    val res: Unit = spResults.write.format("noop").mode("overwrite").save()
    blackhole.consume(res)
  }

  @Benchmark
  def benchmarkSPGraphX(blackhole: Blackhole): Unit = {
    val sourceVertex =
      props.getProperty(s"graph.${benchmarkGraphName}.bfs.source-vertex").toLong

    val spResults = graph.shortestPaths.setAlgorithm("graphx").landmarks(Seq(sourceVertex)).run()
    val res: Unit = spResults.write.format("noop").mode("overwrite").save()
    blackhole.consume(res)
  }

  @Benchmark
  def benchmarkCC(blackhole: Blackhole): Unit = {
    val ccResults =
      graph.connectedComponents.setUseLocalCheckpoints(true).setAlgorithm("graphframes").run()
    val res: Unit = ccResults.write.format("noop").mode("overwrite").save()
    blackhole.consume(res)
  }

  @Benchmark
  def benchmarkCCGraphX(blackhole: Blackhole): Unit = {
    val ccResults = graph.connectedComponents.setAlgorithm("graphx").run()
    val res: Unit = ccResults.write.format("noop").mode("overwrite").save()
    blackhole.consume(res)
  }

  @Benchmark
  def benchmarkCDLP(blackhole: Blackhole): Unit = {
    val cdlpResults = graph.labelPropagation
      .setAlgorithm("graphframes")
      .maxIter(10)
      .setUseLocalCheckpoints(true)
      .run()
    val res: Unit = cdlpResults.write.format("noop").mode("overwrite").save()
    blackhole.consume(res)
  }
}
