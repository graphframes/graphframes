package org.graphframes.embeddings

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.transform
import org.apache.spark.sql.types.StringType
import org.graphframes.GraphFrame
import org.graphframes.Logging
import org.graphframes.WithIntermediateStorageLevel
import org.graphframes.WithLocalCheckpoints
import org.graphframes.convolutions.SamplingConvolution
import org.graphframes.rw.RandomWalkBase

import java.io.IOException

class RandomWalkEmbeddings private[graphframes] (private val graph: GraphFrame)
    extends Serializable
    with Logging
    with WithLocalCheckpoints
    with WithIntermediateStorageLevel {
  private var sequenceModel: Either[Word2Vec, Hash2Vec] = _
  private var randomWalks: RandomWalkBase = _
  private var aggregateNeighbors: Boolean = true
  private var useEdgeDirections: Boolean = false
  private var maxNbrs: Int = 50
  private var seed: Long = 42L

  def setSequenceModel(value: Either[Word2Vec, Hash2Vec]): this.type = {
    sequenceModel = value
    this
  }

  def setRandomWalks(value: RandomWalkBase): this.type = {
    randomWalks = value
    this
  }

  def setSeed(value: Long): this.type = {
    seed = value
    this
  }

  def setUseEdgeDirections(value: Boolean): this.type = {
    useEdgeDirections = value
    this
  }

  def setAggregateNeighbors(value: Boolean): this.type = {
    aggregateNeighbors = value
    this
  }

  def setMaxNbrs(value: Int): this.type = {
    maxNbrs = value
    this
  }

  def run(): DataFrame = {
    val spark = graph.vertices.sparkSession
    val sc = spark.sparkContext
    val walksGenerator = randomWalks.onGraph(graph).setUseEdgeDirection(useEdgeDirections)
    val walks = if (useLocalCheckpoints) {
      walksGenerator.run().localCheckpoint()
    } else {
      if (sc.getCheckpointDir.isEmpty) {
        val checkpointDir = spark.conf.getOption("spark.checkpoint.dir")
        if (checkpointDir.isEmpty) {
          throw new IOException(
            "Checkpoint directory is not set. Please set it first using sc.setCheckpointDir()" +
              "or by specifying the conf 'spark.checkpoint.dir'.")
        } else {
          sc.setCheckpointDir(checkpointDir.get)
        }
      }

      walksGenerator.run().checkpoint()
    }

    val embeddings = sequenceModel match {
      case Left(w2v) => {
        val model = w2v.setInputCol(RandomWalkBase.rwColName)
        val preProcessedSequences =
          if (graph.vertices.schema(GraphFrame.ID).dataType != StringType) {
            walks.withColumn(
              RandomWalkBase.rwColName,
              transform(col(RandomWalkBase.rwColName), (c: Column) => c.cast(StringType)))
          } else {
            walks
          }

        val fittedW2V = model.fit(preProcessedSequences)
        fittedW2V.getVectors.withColumnsRenamed(
          Map("word" -> GraphFrame.ID, "vector" -> RandomWalkEmbeddings.embeddingColName))
      }
      case Right(h2v) => {
        h2v.run(walks).withColumnRenamed("vector", RandomWalkEmbeddings.embeddingColName)
      }
    }

    // If requested, do the following:
    // - sample neighbors up to maxNbrs
    // - compute avg embedding of sample
    // - concatenate self and aggregated
    val aggregated = if (aggregateNeighbors) {
      new SamplingConvolution()
        .onGraph(GraphFrame(embeddings, graph.edges))
        .setFeaturesCol(RandomWalkEmbeddings.embeddingColName)
        .setMaxNbrs(maxNbrs)
        .setSeed(seed)
        .setUseEdgeDirections(useEdgeDirections)
        .setConcatEmbeddings(true)
        .run()
    } else {
      embeddings
    }

    val persistedDF = aggregated.persist(intermediateStorageLevel)

    // materialize
    persistedDF.count()
    resultIsPersistent()

    persistedDF
  }
}

object RandomWalkEmbeddings {
  val embeddingColName: String = "embedding"
}
