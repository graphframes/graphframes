package org.graphframes.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame
import org.graphframes.rw.RandomWalkWithRestart

import java.nio.file.*

object RWExample {
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
    val rwBuilder =
      new RandomWalkWithRestart()
        .onGraph(graph)
        .setRestartProbability(0.2)
        .setGlobalSeed(42)
        .setTemporaryPrefix("rw-test-data")
    val walks = rwBuilder.run()

    walks.write.mode("overwrite").format("parquet").save("rw-test")
  }
}
