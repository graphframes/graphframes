package org.graphframes.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame
import org.graphframes.embeddings.Hash2Vec
import org.graphframes.rw.RandomWalkWithRestart

import java.nio.file.*

object EmbeddingsExample {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new RuntimeException("expected one arg")
    }

    val filePath = Paths.get(args(0))
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("GraphFramesBenchmarks")
      .set("spark.sql.shuffle.partitions", s"${Runtime.getRuntime.availableProcessors() * 2}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val context = spark.sparkContext
    context.setLogLevel("ERROR")
    context.setCheckpointDir("/tmp/graphframes-checkpoints")

    val edges = spark.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", " ")
      .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
      .load(filePath.toString())
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    println()
    println(s"Read edges: ${edges.count()}")

    val vertices =
      edges
        .select(col("src").alias("id"))
        .union(edges.select(col("dst").alias("id")))
        .distinct()
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(s"Read vertices: ${vertices.count()}")

    println("Run random walks...")
    val graph = GraphFrame(vertices, edges)
    val rwBuilder =
      new RandomWalkWithRestart()
        .onGraph(graph)
        .setRestartProbability(0.2)
        .setGlobalSeed(42)
        .setTemporaryPrefix("rw-test-data")
    val walks = rwBuilder.run().persist(StorageLevel.MEMORY_AND_DISK_SER)

    println(s"Generated ${walks.count()} random walks")
    println("Checkpointing walks")

    // manual checkpointing
    walks.write.mode("overwrite").format("parquet").save("rw-test")
    val checkpointedWalks = spark.read.parquet("rw-test")

    println("Learn embeddings")

    val embeddings = new Hash2Vec().setEmbeddingsDim(512).run(checkpointedWalks)
    embeddings.write.mode("overwrite").format("parquet").save("embeddings")
  }
}
