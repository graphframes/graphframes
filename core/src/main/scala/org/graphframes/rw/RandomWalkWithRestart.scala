package org.graphframes.rw

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.*
import org.graphframes.GraphFrame

/**
 * An implementation of random walk with restart. At each step of the walk, there is a probability
 * (defined by restartProbability) to reset the walk to the original starting node, otherwise the
 * walk continues to a random neighbor.
 */
/**
 * An implementation of random walk with restart. At each step of the walk, there is a probability
 * (defined by restartProbability) to reset the walk to the original starting node, otherwise the
 * walk continues to a random neighbor.
 */
class RandomWalkWithRestart extends RandomWalkBase {

  /** The probability of restarting the walk at each step (resets to starting node). */
  private var restartProbability: Double = 0.1

  /**
   * Sets the restart probability for the random walk.
   *
   * @param value
   *   the probability value (between 0.0 and 1.0)
   * @return
   *   this RandomWalkWithRestart instance for chaining
   */
  def setRestartProbability(value: Double): this.type = {
    restartProbability = value
    this
  }

  override protected def runIter(
      graph: GraphFrame,
      prevIterationDF: Option[DataFrame],
      iterSeed: Long): DataFrame = {
    val neighbors = graph.vertices.select(col(GraphFrame.ID), col(RandomWalkBase.nbrsColName))
    var walks = if (prevIterationDF.isEmpty) {
      graph.vertices.select(
        col(GraphFrame.ID).alias("startingNode"),
        col(GraphFrame.ID).alias(RandomWalkBase.currVisitingVertexColName),
        explode(
          when(
            array_size(col(RandomWalkBase.nbrsColName)) > lit(0),
            array((0 until numWalksPerNode).map(_ => uuid()): _*)).otherwise(array()))
          .alias(RandomWalkBase.walkIdCol),
        array(col(GraphFrame.ID)).alias(RandomWalkBase.rwColName))
    } else {
      prevIterationDF.get.select(
        col("startingNode"),
        col(RandomWalkBase.currVisitingVertexColName),
        col(RandomWalkBase.walkIdCol),
        array(col(RandomWalkBase.currVisitingVertexColName)).alias(RandomWalkBase.rwColName))
    }

    for (_ <- (0 until batchSize)) {
      walks = walks
        .join(
          neighbors,
          col(GraphFrame.ID) === col(RandomWalkBase.currVisitingVertexColName),
          "left")
        .withColumn("doRestart", rand() <= lit(restartProbability))
        .withColumn(
          "nextNode",
          when(col("doRestart"), col("startingNode")).otherwise(
            element_at(shuffle(col(RandomWalkBase.nbrsColName)), 1)))
        .select(
          col("startingNode"),
          col("nextNode").alias(RandomWalkBase.currVisitingVertexColName),
          array_append(
            col(RandomWalkBase.rwColName),
            col(RandomWalkBase.currVisitingVertexColName)))
    }

    walks
  }
}
