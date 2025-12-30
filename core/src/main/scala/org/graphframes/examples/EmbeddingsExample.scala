package org.graphframes.examples

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame
import org.graphframes.embeddings.Hash2Vec
import org.graphframes.embeddings.RandomWalkEmbeddings
import org.graphframes.rw.RandomWalkWithRestart

import java.nio.file.*

object EmbeddingsExample {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      throw new RuntimeException("expected three arg: input path, model type, output path")
    }

    val filePath = Paths.get(args(0).strip())
    val modelType = args(1).strip()
    if (!Seq("word2vec", "hash2vec").contains(modelType)) {
      throw new RuntimeException("supported models are word2vec and hash2vec")
    }
    val outputPath = Paths.get(args(2).strip())
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
      .option("header", "true")
      .option("delimiter", ",")
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

    val graph = GraphFrame(vertices, edges)

    val model = modelType match {
      case "word2vec" => Left(new Word2Vec().setVectorSize(128))
      case "hash2vec" =>
        Right(new Hash2Vec().setDoNormalization(true, true).setEmbeddingsDim(512))
    }

    val rwModel = new RandomWalkWithRestart()
      .setRestartProbability(0.2)
      .setGlobalSeed(42)
      .setTemporaryPrefix(outputPath.resolve("rw-temp-data").toAbsolutePath().toString())

    val embeddingsModel =
      new RandomWalkEmbeddings(graph).setSequenceModel(model).setRandomWalks(rwModel)

    val embeddings = embeddingsModel.run()
    embeddings.write
      .mode("overwrite")
      .format("parquet")
      .save(outputPath.resolve("embeddings").toAbsolutePath().toString())
  }
}
