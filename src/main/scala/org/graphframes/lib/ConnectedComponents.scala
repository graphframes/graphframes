/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.graphframes.lib

import java.io.IOException
import java.math.BigDecimal
import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

import org.graphframes.{GraphFrame, Logging}

/**
 * Connected components algorithm.
 *
 * Computes the connected component membership of each vertex and returns a DataFrame of vertex
 * information with each vertex assigned a component ID.
 *
 * The resulting DataFrame contains all the vertex information and one additional column:
 *  - component (`LongType`): unique ID for this component
 */
class ConnectedComponents private[graphframes] (
    private val graph: GraphFrame) extends Arguments with Logging {

  import org.graphframes.lib.ConnectedComponents._

  private var broadcastThreshold: Int = 1000000

  /**
   * Sets broadcast threshold in propagating component assignments (default: 1000000).
   * If a node degree is greater than this threshold at some iteration, its component assignment
   * will be collected and then broadcasted back to propagate the assignment to its neighbors.
   * Otherwise, the assignment propagation is done by a normal Spark join.
   * This parameter is only used when the algorithm is set to "graphframes".
   */
  def setBroadcastThreshold(value: Int): this.type = {
    require(value >= 0, s"Broadcast threshold must be non-negative but got $value.")
    broadcastThreshold = value
    this
  }

  // python-friendly setter
  private[graphframes] def setBroadcastThreshold(value: java.lang.Integer): this.type = {
    setBroadcastThreshold(value.toInt)
  }

  /**
   * Gets broadcast threshold in propagating component assignment.
   * @see [[org.graphframes.lib.ConnectedComponents.setBroadcastThreshold]]
   */
  def getBroadcastThreshold: Int = broadcastThreshold

  private var algorithm: String = ALGO_GRAPHFRAMES

  /**
   * Sets the connected components algorithm to use (default: "graphframes").
   * Supported algorithms are:
   *   - "graphframes": Uses alternating large star and small star iterations proposed in
   *     [[http://dx.doi.org/10.1145/2670979.2670997 Connected Components in MapReduce and Beyond]]
   *     with skewed join optimization.
   *   - "graphx": Converts the graph to a GraphX graph and then uses the connected components
   *     implementation in GraphX.
   * @see [[org.graphframes.lib.ConnectedComponents.supportedAlgorithms]]
   */
  def setAlgorithm(value: String): this.type = {
    require(supportedAlgorithms.contains(value),
      s"Supported algorithms are {${supportedAlgorithms.mkString(", ")}}, but got $value.")
    algorithm = value
    this
  }

  /**
   * Gets the connected component algorithm to use.
   * @see [[org.graphframes.lib.ConnectedComponents.setAlgorithm]].
   */
  def getAlgorithm: String = algorithm


  private var sparsityThreshold: Double = 2

  private var shrinkageThreshold: Double = 2


  private var optStartIter: Int = 2

  /*
  * Sets the iteration of trying pruning nodes optimization for the sparse graph (default: 2).
  * When the graph is sparse, we will try the pruning nodes optimization at $optStartIter iteration.
  * Typically it's better to try the optimization in the previous iterations (<= 3). 
  * This is because the algorithm converges quickly and we can have more performance gain if pruning 
  * nodes in the early step. 
  * Default value is good enough in most cases, and we do not recommend to change it. If you
  * do not want such optimization, set it to a large value (like 1000)
  */ 
  def setOptStartIter(value: Int): this.type = {
    if (value > 3) {
      logger.warn(
        s"Set optStartIter to $value. This would delay the pruning nodes optimization and may" +
          "damage the overall performance. The default value 2 is good enough in most cases")
    }
    optStartIter = value
    this
  }

  // python-friendly setter
  private[graphframes] def setOptStartIter(value: java.lang.Integer): this.type = {
    setOptStartIter(value.toInt)
  }

  /**
   * Gets starting iteration of the pruning nodes optimization.
   * @see [[org.graphframes.lib.ConnectedComponents.setOptStartIter]]
   */
  def getOptStartIter: Int = optStartIter


  
  private var checkpointInterval: Int = 2

  /**
   * Sets checkpoint interval in terms of number of iterations (default: 2).
   * Checkpointing regularly helps recover from failures, clean shuffle files, shorten the
   * lineage of the computation graph, and reduce the complexity of plan optimization.
   * As of Spark 2.0, the complexity of plan optimization would grow exponentially without
   * checkpointing.
   * Hence disabling or setting longer-than-default checkpoint intervals are not recommended.
   * Checkpoint data is saved under `org.apache.spark.SparkContext.getCheckpointDir` with
   * prefix "connected-components".
   * If the checkpoint directory is not set, this throws a `java.io.IOException`.
   * Set a nonpositive value to disable checkpointing.
   * This parameter is only used when the algorithm is set to "graphframes".
   * Its default value might change in the future.
   * @see `org.apache.spark.SparkContext.setCheckpointDir` in Spark API doc
   */
  def setCheckpointInterval(value: Int): this.type = {
    if (value <= 0 || value > 2) {
      logger.warn(
        s"Set checkpointInterval to $value. This would blow up the query plan and hang the " +
          "driver for large graphs.")
    }
    checkpointInterval = value
    this
  }

  // python-friendly setter
  private[graphframes] def setCheckpointInterval(value: java.lang.Integer): this.type = {
    setCheckpointInterval(value.toInt)
  }

  /**
   * Gets checkpoint interval.
   * @see [[org.graphframes.lib.ConnectedComponents.setCheckpointInterval]]
   */
  def getCheckpointInterval: Int = checkpointInterval


  private var intermediateStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK

  /**
   * Sets storage level for intermediate datasets that require multiple passes (default: ``MEMORY_AND_DISK``).
   */
  def setIntermediateStorageLevel(value: StorageLevel): this.type = {
    intermediateStorageLevel = value
    this
  }

  /**
   * Gets storage level for intermediate datasets that require multiple passes.
   */
  def getIntermediateStorageLevel: StorageLevel = intermediateStorageLevel

  /**
   * Runs the algorithm.
   */
  def run(): DataFrame = {
    ConnectedComponents.run(graph,
      algorithm = algorithm,
      broadcastThreshold = broadcastThreshold,
      checkpointInterval = checkpointInterval,
      intermediateStorageLevel = intermediateStorageLevel,
      optStartIter = optStartIter,
      sparsityThreshold = sparsityThreshold,
      shrinkageThreshold = shrinkageThreshold)
  }
}

object ConnectedComponents extends Logging {

  import org.graphframes.GraphFrame._

  private val COMPONENT = "component"
  private val ORIG_ID = "orig_id"
  private val MIN_NBR = "min_nbr"
  private val CNT = "cnt"
  private val CHECKPOINT_NAME_PREFIX = "connected-components"

  private val ALGO_GRAPHX = "graphx"
  private val ALGO_GRAPHFRAMES = "graphframes"

  /**
   * Supported algorithms in [[org.graphframes.lib.ConnectedComponents.setAlgorithm]]: "graphframes"
   * and "graphx".
   */
  val supportedAlgorithms: Array[String] = Array(ALGO_GRAPHX, ALGO_GRAPHFRAMES)

  /**
   * Returns the symmetric directed graph of the graph specified by input edges.
   * @param ee non-bidirectional edges
   */
  private def symmetrize(ee: DataFrame): DataFrame = {
    val EDGE = "_edge"
    ee.select(explode(array(
          struct(col(SRC), col(DST)),
          struct(col(DST).as(SRC), col(SRC).as(DST)))
        ).as(EDGE))
      .select(col(s"$EDGE.$SRC").as(SRC), col(s"$EDGE.$DST").as(DST))
  }

  /**
   * Prepares the input graph for computing connected components by:
   *   - de-duplicating vertices and assigning unique long IDs to each,
   *   - changing edge directions to have increasing long IDs from src to dst,
   *   - de-duplicating edges and removing self-loops.
   * In the returned GraphFrame, the vertex DataFrame has two columns:
   *   - column `id` stores a long ID assigned to the vertex,
   *   - column `attr` stores the original vertex attributes.
   * The edge DataFrame has two columns:
   *   - column `src` stores the long ID of the source vertex,
   *   - column `dst` stores the long ID of the destination vertex,
   * where we always have `src` < `dst`.
   */
  private def prepare(graph: GraphFrame): GraphFrame = {
    // TODO: This assignment job might fail if the graph is skewed.
    val vertices = graph.indexedVertices
      .select(col(LONG_ID).as(ID), col(ATTR))
      // TODO: confirm the contract for a graph and decide whether we need distinct here
      // .distinct()
    val edges = graph.indexedEdges
      .select(col(LONG_SRC).as(SRC), col(LONG_DST).as(DST))
    val orderedEdges = edges.filter(col(SRC) !== col(DST))
      .select(minValue(col(SRC), col(DST)).as(SRC), maxValue(col(SRC), col(DST)).as(DST))
      .distinct()
    GraphFrame(vertices, orderedEdges)
  }

  /**
   * Returns the min vertex among each vertex and its neighbors in a DataFrame with three columns:
   *   - `src`, the ID of the vertex
   *   - `min_nbr`, the min vertex ID among itself and its neighbors
   *   - `cnt`, the total number of neighbors
   */
  private def minNbrs(ee: DataFrame): DataFrame = {
    symmetrize(ee)
      .groupBy(SRC).agg(min(col(DST)).as(MIN_NBR), count("*").as(CNT))
      .withColumn(MIN_NBR, minValue(col(SRC), col(MIN_NBR)))
  }

  private def minValue(x: Column, y: Column): Column = {
    when(x < y, x).otherwise(y)
  }

  private def maxValue(x: Column, y: Column): Column = {
    when(x > y, x).otherwise(y)
  }

  /**
   * Performs a possibly skewed join between edges and current component assignments.
   * The skew join is done by broadcast join for frequent keys and normal join for the rest.
   */
  private def skewedJoin(
      edges: DataFrame,
      minNbrs: DataFrame,
      broadcastThreshold: Int,
      logPrefix: String): DataFrame = {
    import edges.sqlContext.implicits._
    val hubs = minNbrs.filter(col(CNT) > broadcastThreshold)
      .select(SRC)
      .as[Long]
      .collect()
      .toSet
    GraphFrame.skewedJoin(edges, minNbrs, SRC, hubs, logPrefix)
  }

  /*
  *  Leaf nodes are vertices with 0 out-degree and 1 in-degree. 
  *  After each big/small star join iteration, the number of leaf nodes increases a lot.
  *  In our optimization, we prune leaf nodes and get a shrinked graph to reduce the shuffle 
  *  size in the following iterations. Currently, we only perform such optimization one time.  
  */ 
  private def pruneLeafNodes(
    edges: DataFrame,
    intermediateStorageLevel: StorageLevel,
    numNodes: Long,
    shrinkageThreshold: Double) = {

    // vertices whose indegree > 1.
    val v1 = edges.groupBy(DST).agg(count("*").as(CNT))
      .filter(col(CNT) > 1).select(DST)
       
    // vertices whose outdegree > 0 or indegree > 1.
    val new_vv = edges.select(SRC).union(v1).distinct().withColumnRenamed(SRC, ID)
      .persist(intermediateStorageLevel)
    val new_vv_cnt = new_vv.count()

    // When performs the optimization, the function returns vertices and edges 
    // of the shrinked graph. Otherwise return null.
    var ret: (DataFrame, DataFrame, Long) = null

    // We perform such optimization only if the number of shrinked graph's vertices
    // is much smaller than the orignial one. The default condition is that #orignial
    // should be 2 times bigger than #shrinked. You may change this condition by setting
    // shrinkageThreshold (default is 2), but we do not recommend to change that.
    // If the condition is not satisfied, this graph converges very slowly and cannot benefit
    // from such optimization (like grid graphs). We do not try it anymore. 
    if(new_vv_cnt * shrinkageThreshold < numNodes) 
    {
      // Edges of the shrinked graph are no more than the orignial ones.
      val new_ee = edges.join(new_vv.withColumnRenamed(ID, DST), DST)
      ret = (new_vv, new_ee, new_vv_cnt)
    }
    else{
      new_vv.unpersist(false)
    }
    ret
  }


  /*
  *  Source nodes: edges.select(SRC).distinct()
  *  After each big/small star join iteration, the number of source nodes decreases a lot.
  *  In our optimization, we keep source nodes and prune other nodes, and get a shrinked 
  *  graph to reduce the shuffle size in the following iterations.
  *
  *  Compared to pruneLeafNodes() optimization, this method can prune more nodes. But the 
  *  edges of the shrinked graph is not bounded by the original graph (i.e. the shrinked
  *  graph may have more edges than the original graph). Although in most cases, it does not
  *  happen, we can always generate a special original graph to make shrinked graph has more 
  *  edges. Thus we need to estimate the edge size and the cost of constructing edges for the 
  *  shrinked graph. If it is too high, we do not perform the optimization. 
  */
  private def keepSrcNodes(
    edges: DataFrame,
    intermediateStorageLevel: StorageLevel,
    numNodes: Long,
    shrinkageThreshold: Double,
    edgeCnt: Long) = {

    var new_vv = edges.select(col(SRC)).distinct()
        .persist(intermediateStorageLevel)
    val new_vv_cnt = new_vv.count()

    // When performs the optimization, the function returns vertices and edges 
    // of the shrinked graph. Otherwise return null.
    var ret: (DataFrame, DataFrame, Long) = null

    if(new_vv_cnt * shrinkageThreshold < numNodes)
    {
      val s = edges.groupBy(DST).agg(count("*").as("count")).groupBy("count").agg(count("*").as("cnt"))
        .select(sum(col("count") * col("count") * col("cnt")).as(CNT)).agg(sum(CNT)).first().getLong(0)
      
      // s / 2 is the estimate of edge size, it should be no more than $shrinkageThreshold times
      // of the original edges 
      if(s < edgeCnt * shrinkageThreshold * 2)
      {
        val je = edges.union(new_vv.withColumn(DST, col(SRC))) 
        
        // edges set of the small graph
        val new_ee = je.as("l").join(je.as("r"), col(s"l.$DST") === col(s"r.$DST"))
              .select(col(s"l.$SRC").as(SRC), col(s"r.$SRC").as(DST))
              .filter(col(SRC) < col(DST))
              .distinct() // src < dst
        
        new_vv = new_vv.withColumnRenamed(SRC, ID)
        ret = (new_vv, new_ee, new_vv_cnt)
      }
    }
    ret 
  }

  /**
   * Runs connected components with default parameters.
   */
  def run(graph: GraphFrame): DataFrame = {
    new ConnectedComponents(graph).run()
  }

  private def runGraphX(graph: GraphFrame): DataFrame = {
    val components = org.apache.spark.graphx.lib.ConnectedComponents.run(graph.cachedTopologyGraphX)
    GraphXConversions.fromGraphX(graph, components, vertexNames = Seq(COMPONENT)).vertices
  }

  private def run(
      graph: GraphFrame,
      algorithm: String,
      broadcastThreshold: Int,
      checkpointInterval: Int,
      intermediateStorageLevel: StorageLevel,
      optStartIter: Int,
      sparsityThreshold: Double,
      shrinkageThreshold: Double): DataFrame = {
    require(supportedAlgorithms.contains(algorithm),
      s"Supported algorithms are {${supportedAlgorithms.mkString(", ")}}, but got $algorithm.")

    if (algorithm == ALGO_GRAPHX) {
      return runGraphX(graph)
    }

    val runId = UUID.randomUUID().toString.takeRight(8)
    val logPrefix = s"[CC $runId]"
    logger.info(s"$logPrefix Start connected components with run ID $runId.")

    val sqlContext = graph.sqlContext
    val sc = sqlContext.sparkContext


    val shouldCheckpoint = checkpointInterval > 0
    val checkpointDir: Option[String] = if (shouldCheckpoint) {
      val dir = sc.getCheckpointDir.map { d =>
        new Path(d, s"$CHECKPOINT_NAME_PREFIX-$runId").toString
      }.getOrElse {
        throw new IOException(
          "Checkpoint directory is not set. Please set it first using sc.setCheckpointDir().")
      }
      logger.info(s"$logPrefix Using $dir for checkpointing with interval $checkpointInterval.")
      Some(dir)
    } else {
      logger.info(
        s"$logPrefix Checkpointing is disabled because checkpointInterval=$checkpointInterval.")
      None
    }

    logger.info(s"$logPrefix Preparing the graph for connected component computation ...")
    val g = prepare(graph)
    var vv = g.vertices.persist(intermediateStorageLevel)
    var ee = g.edges.persist(intermediateStorageLevel) // src < dst
    var isOptimized = false
    var tryOptimize = false
    var shouldDeleteCheckpoint = true

    var numEdges = ee.count()
    var numNodes = vv.count()
    logger.info(s"$logPrefix Found $numNodes nodes after preparation.")
    logger.info(s"$logPrefix Found $numEdges edges after preparation.")

    var converged = false
    var iteration = 1
    var prevSum: BigDecimal = null
    var old_ee: DataFrame = null
    var new_vv: DataFrame = null

    while (!converged) {
      // large-star step
      // compute min neighbors (including self-min)
      val minNbrs1 = minNbrs(ee) // src >= min_nbr
        .persist(intermediateStorageLevel)
      // connect all strictly larger neighbors to the min neighbor (including self)
      val ee1 = skewedJoin(ee, minNbrs1, broadcastThreshold, logPrefix)
        .select(col(DST).as(SRC), col(MIN_NBR).as(DST)) // src > dst
        .distinct()
        .persist(intermediateStorageLevel)


      // small-star step
      // compute min neighbors (excluding self-min)
      val minNbrs2 = ee1.groupBy(col(SRC)).agg(min(col(DST)).as(MIN_NBR), count("*").as(CNT)) // src > min_nbr
        .persist(intermediateStorageLevel)
      // connect all smaller neighbors to the min neighbor
      ee = skewedJoin(ee1, minNbrs2, broadcastThreshold, logPrefix)
        .select(col(MIN_NBR).as(SRC), col(DST)) // src <= dst
        .filter(col(SRC) =!= col(DST)) // src < dst
      // connect self to the min neighbor
      ee = ee.union(minNbrs2.select(col(MIN_NBR).as(SRC), col(SRC).as(DST))) // src < dst
        .distinct()

      // checkpointing
      if (shouldCheckpoint && (iteration % checkpointInterval == 0)) {
        // TODO: remove this after DataFrame.checkpoint is implemented
        val out = s"${checkpointDir.get}/$iteration"
        ee.write.parquet(out)
        // may hit S3 eventually consistent issue
        ee = sqlContext.read.parquet(out)

        minNbrs1.unpersist(false)
        minNbrs2.unpersist(false)
        ee1.unpersist(false)

        // remove previous checkpoint
        if (iteration > checkpointInterval) {
          val path = new Path(s"${checkpointDir.get}/${iteration - checkpointInterval}")
          
          // remain the checkpoint file for old_ee
          if(shouldDeleteCheckpoint == true) {
            path.getFileSystem(sc.hadoopConfiguration).delete(path, true)
          }
          else
            shouldDeleteCheckpoint = true
        }

        System.gc() // hint Spark to clean shuffle directories
      }


      // test convergence

      // Taking the sum in DecimalType to preserve precision.
      // We use 20 digits for long values and Spark SQL will add 10 digits for the sum.
      // It should be able to handle 200 billion edges without overflow.
      val (currSum, edgeCnt) = ee.select(sum(col(SRC).cast(DecimalType(20, 0))), count("*")).rdd
        .map { r =>
          (r.getAs[BigDecimal](0), r.getLong(1))
        }.first()

      if (edgeCnt != 0L && currSum == null) {
        throw new ArithmeticException(
          s"""
             |The total sum of edge src IDs is used to determine convergence during iterations.
             |However, the total sum at iteration $iteration exceeded 30 digits (1e30),
             |which should happen only if the graph contains more than 200 billion edges.
             |If not, please file a bug report at https://github.com/graphframes/graphframes/issues.
            """.stripMargin)
      }
      
      ee.persist(intermediateStorageLevel)

      // Pruning Node Optimization: construct a new small graph with fewer nodes, 
      // and find connected components of the shrinked graph, then join back to get the 
      // connected components of the original graph. 

      // If the graph becomes sparse and current iteration >= $optStartIter, we start to
      // try such optmization. However, the optmization is only performed if the shrinked 
      // graph is much smaller than the original graph, otherwise we do not perform it (
      // in this case, the only additional cost is to determine the size of shrinked graph). 
      // In current implementation, we only try such optimization one time and it is 
      // performed at most one time. So the additional cost is bounded. 

      // According to such heuristic rule, we can determine when and whether we should 
      // perform the optmization. For the sparse graphs (defined by sparsityThreshold), 
      // we will try such optmization at the $optStartIter iteration (default is 2). 
      // Typically we want to try it in the previous iterations (<= 3). This is because 
      // the algorithm converges quickly and we can have more performance gain if pruning 
      // nodes in the early step. For the dense graph, its edges will be pruned at each 
      // large/small star join iteration, and we will try the optimization once the graph 
      // becomes sparse.
      
      // We do not recommend to change the parameters $sparsityThreshold, $optStartIter, 
      // and $shrinkageThreshold. Default values are good enough in most cases. If you do
      // not want such optimization, just set $optStartIter a big value (like 1000). 
   
      if((edgeCnt < sparsityThreshold * numNodes) && (iteration >= optStartIter)
        && (tryOptimize == false))
      {
        // if a graph does not have edges, we do not perform pruning node optimization.
        if(edgeCnt > 0) 
        {     
          old_ee = ee

          // Pruning Leaf Nodes Optimization
          val r = pruneLeafNodes(ee, intermediateStorageLevel, numNodes, shrinkageThreshold)
          
          // Keep Source Nodes, prune other nodes
          //val r = keepSrcNodes(ee, intermediateStorageLevel, numNodes, shrinkageThreshold, edgeCnt)

          // when r != null, the optimization is performed. Otherwise it is not performed, and
          // we will not try it anymore. 
          if(r != null)
          {
            new_vv = r._1
            ee = r._2
            numNodes = r._3  // number of nodes in the shrinked graph.
            ee.persist(intermediateStorageLevel)
            isOptimized = true
            shouldDeleteCheckpoint = false
          }
        }
        tryOptimize = true
      }

      logInfo(s"In iteration $iteration: edge cnt: $edgeCnt , node cnt: $numNodes")
      logInfo(s"$logPrefix Sum of assigned components in iteration $iteration: $currSum.")

      if (currSum == prevSum) {
        // This also covers the case when cnt = 0 and currSum is null, which means no edges.
        converged = true
      } else {
        prevSum = currSum
      }
      iteration += 1
    }
    
    // If we have performed pruning node optimization to shrink the graph, 
    // we need to get the results of the original graph from the shrinked one.
    if(isOptimized == true)
    {
      // connected components of the small graph
      val cc = new_vv.join(ee, new_vv(ID) === ee(DST), "left_outer")
        .select(when(ee(SRC).isNull, new_vv(ID)).otherwise(ee(SRC)).as(SRC), new_vv(ID).as(DST))
        .persist(intermediateStorageLevel)

      // join back to get results of the original graph
      ee = cc.join(old_ee, cc(DST) === old_ee(SRC))
        .select(cc(SRC), old_ee(DST))
        .union(cc)
        .distinct() // src <= dst

    }

    logger.info(s"$logPrefix Connected components converged in ${iteration - 1} iterations.")
    logger.info(s"$logPrefix Join and return component assignments with original vertex IDs.")
    vv.join(ee, vv(ID) === ee(DST), "left_outer")
      .select(vv(ATTR), when(ee(SRC).isNull, vv(ID)).otherwise(ee(SRC)).as(COMPONENT))
      .select(col(s"$ATTR.*"), col(COMPONENT))
      .persist(intermediateStorageLevel)
  }
}
