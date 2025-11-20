package org.graphframes.rw

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.*
import org.graphframes.GraphFrame

class RandomWalkWithRestart extends RandomWalkBase {
  private var restartProbability: Double = 0.1

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
            element_at(shuffle(col(RandomWalkBase.nbrsColName)), 0)))
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
