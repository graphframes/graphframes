package org.graphframes.rw

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_set
import org.apache.spark.sql.functions.shuffle
import org.apache.spark.sql.functions.slice
import org.apache.spark.sql.functions.array_union
import org.graphframes.GraphFrame
import org.graphframes.Logging
import org.graphframes.WithIntermediateStorageLevel

import scala.util.Random

trait RandomWalkBase extends Serializable with Logging with WithIntermediateStorageLevel {
  protected var maxNbrs: Int = 50
  protected var graph: GraphFrame = null

  protected var numWalksPerNode: Int = 5
  protected var batchSize: Int = 10
  protected var numBatches: Int = 5

  protected var useEdgeDirection: Boolean = false

  protected var globalSeed: Long = 42L
  protected var temporaryPrefix: Option[String] = None
  protected var runID: String = ""

  def onGraph(graph: GraphFrame): this.type = {
    this.graph = graph
    this
  }

  def setTemporaryPrefix(value: String): this.type = {
    temporaryPrefix = Some(value)
    this
  }

  def setMaxNbrsPerVertex(value: Int): this.type = {
    maxNbrs = value
    this
  }

  def setNumWalksPerNode(value: Int): this.type = {
    numWalksPerNode = value
    this
  }

  def setBatchSize(value: Int): this.type = {
    batchSize = value
    this
  }

  def setNumBatches(value: Int): this.type = {
    numBatches = value
    this
  }

  def setUseEdgeDirection(value: Boolean): this.type = {
    useEdgeDirection = value
    this
  }

  def setGlobalSeed(value: Long): this.type = {
    globalSeed = value
    this
  }

  private def iterationTmpPath(iter: Int): String = if (temporaryPrefix.get.endsWith("/")) {
    s"${temporaryPrefix.get}${runID}_batch_${iter}"
  } else {
    s"${temporaryPrefix.get}/${runID}_batch_${iter}"
  }

  def run(): DataFrame = {
    if (graph == null) {
      throw new IllegalArgumentException("Graph is not set")
    }
    if (temporaryPrefix.isEmpty) {
      throw new IllegalArgumentException("Temporary prefix is required for random walks.")
    }
    runID = java.util.UUID.randomUUID().toString
    logInfo(s"Starting random walk with runID: $runID")
    val iterationsRng = new Random()
    iterationsRng.setSeed(globalSeed)
    val spark = graph.vertices.sparkSession

    for (i <- 1 to numBatches) {
      logInfo(s"Starting batch $i of $numBatches")
      val iterSeed = iterationsRng.nextLong()
      val preparedGraph = prepareGraph()
      val prevIterationDF = if (i == 1) { None }
      else {
        Some(spark.read.parquet(iterationTmpPath(i - 1)))
      }
      val iterationResult: DataFrame = runIter(preparedGraph, prevIterationDF, iterSeed)
      iterationResult.write.parquet(iterationTmpPath(i))
    }

    logInfo(s"Finished all batches, merging results.")
    var result = spark.read.parquet(iterationTmpPath(1))

    for (i <- 2 to numBatches) {
      val tmpDF = spark.read
        .parquet(iterationTmpPath(i))
        .withColumnRenamed(RandomWalkBase.rwColName, "toMerge")
      result = result
        .join(tmpDF, Seq(RandomWalkBase.walkIdCol))
        .select(
          col(RandomWalkBase.walkIdCol),
          array_union(col(RandomWalkBase.rwColName), col("toMerge"))
            .alias(RandomWalkBase.rwColName))
    }
    result = result.persist(intermediateStorageLevel)

    val cnt = result.count()
    resultIsPersistent()
    logInfo(s"$cnt random walks are returned")
    result
  }

  protected def prepareGraph(): GraphFrame = {
    // Would be nice to implement something like Reservoir Sampling here
    // but I have no idea at the moment how to do it in Spark effectively
    val vertices = (if (useEdgeDirection) {
                      graph.edges
                        .select(col(GraphFrame.SRC), col(GraphFrame.DST))
                        .groupBy(col(GraphFrame.SRC).alias(GraphFrame.ID))
                        .agg(collect_set(GraphFrame.DST).alias(RandomWalkBase.nbrsColName))
                    } else {
                      graph.edges
                        .select(GraphFrame.SRC, GraphFrame.DST)
                        .union(graph.edges.select(GraphFrame.DST, GraphFrame.SRC))
                        .distinct()
                        .groupBy(col(GraphFrame.SRC).alias(GraphFrame.ID))
                        .agg(collect_set(GraphFrame.DST).alias(RandomWalkBase.nbrsColName))
                    }).select(
      col(GraphFrame.ID),
      slice(shuffle(col(RandomWalkBase.nbrsColName)), 0, maxNbrs)
        .alias(RandomWalkBase.nbrsColName))

    val edges = graph.edges
      .select(GraphFrame.SRC, GraphFrame.DST)
      .join(vertices, col(GraphFrame.SRC) === col(GraphFrame.ID))
      .drop(GraphFrame.ID)
      .join(vertices, col(GraphFrame.DST) === col(GraphFrame.ID))
      .drop(GraphFrame.ID)

    GraphFrame(vertices, edges)
  }

  protected def runIter(
      graph: GraphFrame,
      prevIterationDF: Option[DataFrame],
      iterSeed: Long): DataFrame
}

object RandomWalkBase {
  val rwColName: String = "random_walk"
  val walkIdCol: String = "random_walk_uuid"
  val nbrsColName: String = "random_walk_nbrs"
  val currVisitingVertexColName: String = "random_walk_curr_vertex"
}
