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
import org.graphframes.rw.RandomWalkBase

import java.io.IOException

class RandomWalkEmbeddings private[graphframes] (private val graph: GraphFrame)
    extends Serializable
    with Logging
    with WithLocalCheckpoints
    with WithIntermediateStorageLevel {
  private var sequenceModel: Either[Word2Vec, Hash2Vec] = _
  private var randomWalks: RandomWalkBase = _

  def setSequenceModel(value: Either[Word2Vec, Hash2Vec]): this.type = {
    sequenceModel = value
    this
  }

  def setRandomWalks(value: RandomWalkBase): this.type = {
    randomWalks = value
    this
  }

  def run(): DataFrame = {
    val spark = graph.vertices.sparkSession
    val sc = spark.sparkContext
    val walksGenerator = randomWalks.onGraph(graph)
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
        fittedW2V.getVectors.withColumnsRenamed(Map("word" -> "id", "vector" -> "embedding"))
      }
      case Right(h2v) => {
        h2v.run(walks)
      }
    }

    val persistedDF = embeddings.persist(intermediateStorageLevel)
    // materialize
    persistedDF.count()
    resultIsPersistent()

    persistedDF
  }
}
