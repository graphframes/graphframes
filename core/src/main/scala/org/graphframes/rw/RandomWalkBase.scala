package org.graphframes.rw

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.array_union
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_set
import org.apache.spark.sql.functions.shuffle
import org.apache.spark.sql.functions.slice
import org.graphframes.GraphFrame
import org.graphframes.Logging
import org.graphframes.WithIntermediateStorageLevel

import scala.util.Random

/**
 * Base trait for implementing random walk algorithms on graph data. Provides common functionality
 * for generating random walks across a graph structure.
 */
trait RandomWalkBase extends Serializable with Logging with WithIntermediateStorageLevel {

  /** Maximum number of neighbors to consider per vertex during random walks. */
  protected var maxNbrs: Int = 50

  /** GraphFrame on which random walks are performed. */
  protected var graph: GraphFrame = null

  /** Number of random walks to generate per node. */
  protected var numWalksPerNode: Int = 5

  /** Size of each batch in the random walk process. */
  protected var batchSize: Int = 10

  /** Number of batches to run in the random walk process. */
  protected var numBatches: Int = 5

  /** Whether to respect edge direction in the graph (true for directed graphs). */
  protected var useEdgeDirection: Boolean = false

  /** Global random seed for reproducibility. */
  protected var globalSeed: Long = 42L

  /** Optional prefix for temporary storage during random walks. */
  protected var temporaryPrefix: Option[String] = None

  /** Unique identifier for the current random walk run. */
  protected var runID: String = ""

  /**
   * Sets the graph to perform random walks on.
   *
   * @param graph
   *   the GraphFrame to run random walks on
   * @return
   *   this RandomWalkBase instance for chaining
   */
  def onGraph(graph: GraphFrame): this.type = {
    this.graph = graph
    this
  }

  /**
   * Sets the temporary prefix for storing intermediate results.
   *
   * @param value
   *   the prefix string
   * @return
   *   this RandomWalkBase instance for chaining
   */
  def setTemporaryPrefix(value: String): this.type = {
    temporaryPrefix = Some(value)
    this
  }

  /**
   * Sets the maximum number of neighbors per vertex.
   *
   * @param value
   *   the max number of neighbors
   * @return
   *   this RandomWalkBase instance for chaining
   */
  def setMaxNbrsPerVertex(value: Int): this.type = {
    maxNbrs = value
    this
  }

  /**
   * Sets the number of walks per node.
   *
   * @param value
   *   number of walks
   * @return
   *   this RandomWalkBase instance for chaining
   */
  def setNumWalksPerNode(value: Int): this.type = {
    numWalksPerNode = value
    this
  }

  /**
   * Sets the batch size.
   *
   * @param value
   *   batch size
   * @return
   *   this RandomWalkBase instance for chaining
   */
  def setBatchSize(value: Int): this.type = {
    batchSize = value
    this
  }

  /**
   * Sets the number of batches.
   *
   * @param value
   *   number of batches
   * @return
   *   this RandomWalkBase instance for chaining
   */
  def setNumBatches(value: Int): this.type = {
    numBatches = value
    this
  }

  /**
   * Sets whether to use edge direction.
   *
   * @param value
   *   true if the graph is directed
   * @return
   *   this RandomWalkBase instance for chaining
   */
  def setUseEdgeDirection(value: Boolean): this.type = {
    useEdgeDirection = value
    this
  }

  /**
   * Sets the global random seed.
   *
   * @param value
   *   the seed value
   * @return
   *   this RandomWalkBase instance for chaining
   */
  def setGlobalSeed(value: Long): this.type = {
    globalSeed = value
    this
  }

  /**
   * Generates a temporary path for a given iteration.
   *
   * @param iter
   *   iteration number
   * @return
   *   path string
   */
  private def iterationTmpPath(iter: Int): String = if (temporaryPrefix.get.endsWith("/")) {
    s"${temporaryPrefix.get}${runID}_batch_${iter}"
  } else {
    s"${temporaryPrefix.get}/${runID}_batch_${iter}"
  }

  /**
   * Executes the random walk algorithm on the set graph.
   *
   * @return
   *   DataFrame containing the random walks
   */
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

    logInfo("Finished all batches, merging results.")
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

  /**
   * Prepares the graph for random walk by limiting neighbors and handling direction.
   *
   * @return
   *   prepared GraphFrame
   */
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

  /**
   * Runs a single iteration of the random walk.
   *
   * @param graph
   *   prepared graph
   * @param prevIterationDF
   *   DataFrame from previous iteration (if any)
   * @param iterSeed
   *   seed for this iteration
   * @return
   *   DataFrame result of this iteration
   */
  protected def runIter(
      graph: GraphFrame,
      prevIterationDF: Option[DataFrame],
      iterSeed: Long): DataFrame
}

object RandomWalkBase {

  /** Column name for the random walk array. */
  val rwColName: String = "random_walk"

  /** Column name for the unique walk ID. */
  val walkIdCol: String = "random_walk_uuid"

  /** Column name for neighbors list. */
  val nbrsColName: String = "random_walk_nbrs"

  /** Column name for the current visiting vertex. */
  val currVisitingVertexColName: String = "random_walk_curr_vertex"
}
