package org.graphframes.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame

import java.nio.file.*
import java.util.Properties

object ConnectedComponentsLDBC {
  def main(args: Array[String]): Unit = {
    val benchmarkGraphName = args.headOption.getOrElse("kgs")
    val resourcesPath = Paths.get(args.lift(1).getOrElse("/tmp/ldbc_graphalitics_datesets"))
    val caseRoot: Path = resourcesPath.resolve(benchmarkGraphName)

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
      .load(caseRoot.resolve(s"$benchmarkGraphName.e").toString)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    println()
    println(s"Read edges: ${edges.count()}")

    val vertices = spark.read
      .format("csv")
      .option("header", "false")
      .schema(StructType(Seq(StructField("id", LongType))))
      .load(caseRoot.resolve(s"$benchmarkGraphName.v").toString)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(s"Read vertices: ${vertices.count()}")

    val graph = GraphFrame(vertices, edges)
    val props = new Properties()
    val stream = Files.newInputStream(caseRoot.resolve(s"$benchmarkGraphName.properties"))
    props.load(stream)
    stream.close()

    val expectedPath = caseRoot.resolve(s"$benchmarkGraphName-WCC")

    val expectedComponents = spark.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", " ")
      .schema(StructType(Seq(StructField("id", LongType), StructField("wcomp", LongType))))
      .load(expectedPath.toString)
      .toDF("id", "wcomp")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    println(s"Expected components: ${expectedComponents.count()}")

    val start = System.currentTimeMillis()
    val results = graph.connectedComponents
      .setAlgorithm("graphframes")
      .setBroadcastThreshold(-1)
      .setUseLocalCheckpoints(true)
      .run()

    println(s"Connected components: ${results.count()}")

    val combined = results.join(expectedComponents, Seq("id"), "left")
    combined.show(10)

    val notMatchedRows = combined.filter(col("wcomp") =!= col("component"))
    println(s"Not matched rows count: ${notMatchedRows.count()}")
    notMatchedRows.show(20)

    val end = System.currentTimeMillis()
    println(s"Total time in seconds: ${(end - start) / 1000.0}")

    spark.stop()
  }
}
