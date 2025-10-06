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

import org.apache.spark.graphframes.graphx
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.graphframes.GraphFramesConf
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame
import org.graphframes.Logging
import org.graphframes.WithAlgorithmChoice
import org.graphframes.WithBroadcastThreshold
import org.graphframes.WithCheckpointInterval
import org.graphframes.WithIntermediateStorageLevel
import org.graphframes.WithLocalCheckpoints
import org.graphframes.WithMaxIter
import org.graphframes.WithUseLabelsAsComponents

import java.io.IOException
import java.math.BigDecimal
import java.util.UUID

/**
 * Connected Components algorithm.
 *
 * Computes the connected component membership of each vertex and returns a DataFrame of vertex
 * information with each vertex assigned a component ID.
 *
 * The resulting DataFrame contains all the vertex information and one additional column:
 *   - component (`LongType`): unique ID for this component
 */
class ConnectedComponents private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with Logging
    with WithAlgorithmChoice
    with WithCheckpointInterval
    with WithBroadcastThreshold
    with WithIntermediateStorageLevel
    with WithUseLabelsAsComponents
    with WithMaxIter
    with WithLocalCheckpoints {

  setAlgorithm(GraphFramesConf.getConnectedComponentsAlgorithm.getOrElse(ALGO_GRAPHFRAMES))
  setCheckpointInterval(
    GraphFramesConf.getConnectedComponentsCheckpointInterval.getOrElse(checkpointInterval))
  setBroadcastThreshold(
    GraphFramesConf.getConnectedComponentsBroadcastThreshold.getOrElse(broadcastThreshold))
  setIntermediateStorageLevel(
    GraphFramesConf.getConnectedComponentsStorageLevel.getOrElse(intermediateStorageLevel))
  setUseLabelsAsComponents(
    GraphFramesConf.getUseLabelsAsComponents.getOrElse(useLabelsAsComponents))
  setUseLocalCheckpoints(GraphFramesConf.getUseLocalCheckpoints.getOrElse(useLocalCheckpoints))

  /**
   * Runs the algorithm.
   */
  def run(): DataFrame = {
    ConnectedComponents.run(
      graph,
      runInGraphX = algorithm == ALGO_GRAPHX,
      broadcastThreshold = broadcastThreshold,
      checkpointInterval = checkpointInterval,
      intermediateStorageLevel = intermediateStorageLevel,
      useLabelsAsComponents = useLabelsAsComponents,
      maxIter = maxIter,
      useLocalCheckpoints = useLocalCheckpoints)
  }
}

object ConnectedComponents extends Logging {

  import org.graphframes.GraphFrame.*

  private val COMPONENT = "component"
  private val ORIG_ID = "orig_id"
  private val MIN_NBR = "min_nbr"
  private val CNT = "cnt"

  /**
   * Returns the symmetric directed graph of the graph specified by input edges.
   * @param ee
   *   non-bidirectional edges
   */
  private def symmetrize(ee: DataFrame): DataFrame = {
    val EDGE = "_edge"
    ee.select(explode(
      array(struct(col(SRC), col(DST)), struct(col(DST).as(SRC), col(SRC).as(DST)))).as(EDGE))
      .select(col(s"$EDGE.$SRC").as(SRC), col(s"$EDGE.$DST").as(DST))
  }

  /**
   * Prepares the input graph for computing connected components by:
   *   - de-duplicating vertices and assigning unique long IDs to each,
   *   - changing edge directions to have increasing long IDs from src to dst,
   *   - de-duplicating edges and removing self-loops. In the returned GraphFrame, the vertex
   *     DataFrame has two columns:
   *   - column `id` stores a long ID assigned to the vertex,
   *   - column `attr` stores the original vertex attributes. The edge DataFrame has two columns:
   *   - column `src` stores the long ID of the source vertex,
   *   - column `dst` stores the long ID of the destination vertex, where we always have `src` <
   *     `dst`.
   */
  private def prepare(graph: GraphFrame): GraphFrame = {
    val vertices = graph.indexedVertices
      .select(col(LONG_ID).as(ID), col(ATTR))
    val edges = graph.indexedEdges
      .select(col(LONG_SRC).as(SRC), col(LONG_DST).as(DST))
    val orderedEdges = edges
      .filter(col(SRC) =!= col(DST))
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
  private def minNbrs(
      ee: DataFrame,
      computeCount: Boolean,
      includeSelf: Boolean,
      doSymmetrize: Boolean): DataFrame = {
    val ee2 = if (doSymmetrize) {
      symmetrize(ee)
    } else {
      ee
    }
    val res = if (computeCount) {
      ee2
        .groupBy(SRC)
        .agg(min(col(DST)).as(MIN_NBR), count("*").as(CNT))
    } else {
      ee2
        .groupBy(SRC)
        .agg(min(col(DST)).as(MIN_NBR))
    }
    if (includeSelf) {
      res.withColumn(MIN_NBR, minValue(col(SRC), col(MIN_NBR)))
    } else { res }
  }

  private def minValue(x: Column, y: Column): Column = {
    when(x < y, x).otherwise(y)
  }

  private def maxValue(x: Column, y: Column): Column =
    when(x > y, x).otherwise(y)

  /**
   * Performs a possibly skewed join between edges and current component assignments. The skew
   * join is done by broadcast join for frequent keys and normal join for the rest.
   */
  private def skewedJoin(
      edges: DataFrame,
      minNbrs: DataFrame,
      broadcastThreshold: Int,
      logPrefix: String): DataFrame = {
    import edges.sparkSession.implicits.*
    val hubs = minNbrs
      .filter(col(CNT) > broadcastThreshold)
      .select(SRC)
      .as[Long]
      .collect()
      .toSet
    GraphFrame.skewedJoin(edges, minNbrs, SRC, hubs, logPrefix)
  }

  /**
   * Runs connected components with default parameters.
   */
  def run(graph: GraphFrame): DataFrame = {
    new ConnectedComponents(graph).run()
  }

  private def runGraphX(graph: GraphFrame, maxIter: Int): DataFrame = {
    val components =
      graphx.lib.ConnectedComponents.run(graph.cachedTopologyGraphX, maxIter)
    val res =
      GraphXConversions.fromGraphX(graph, components, vertexNames = Seq(COMPONENT)).vertices
    res.persist(StorageLevel.MEMORY_AND_DISK_SER)
    res.count()
    components.unpersist()
    res
  }

  private def run(
      graph: GraphFrame,
      runInGraphX: Boolean,
      broadcastThreshold: Int,
      checkpointInterval: Int,
      intermediateStorageLevel: StorageLevel,
      useLabelsAsComponents: Boolean,
      maxIter: Option[Int],
      useLocalCheckpoints: Boolean): DataFrame = {
    if (runInGraphX) {
      return runGraphX(graph, maxIter.getOrElse(Int.MaxValue))
    }

    val spark = graph.spark
    // Store original AQE setting
    val originalAQE = spark.conf.get("spark.sql.adaptive.enabled")

    try {
      val shouldDoSkewedJoin = broadcastThreshold != -1

      if (shouldDoSkewedJoin) {
        spark.conf.set("spark.sql.adaptive.enabled", "false")
      }

      val runId = UUID.randomUUID().toString.takeRight(8)
      val logPrefix = s"[CC $runId]"
      logInfo(s"$logPrefix Start connected components with run ID $runId.")

      val shouldCheckpoint = checkpointInterval > 0
      if (shouldCheckpoint && !useLocalCheckpoints && spark.sparkContext.getCheckpointDir.isEmpty) {
        // Spark-Connect workaround
        spark.sparkContext.setCheckpointDir(spark.conf.getOption("spark.checkpoint.dir") match {
          case Some(d) => d
          case None =>
            throw new IOException(
              "Checkpoint directory is not set. Please set it first using sc.setCheckpointDir()" +
                "or by specifying the conf 'spark.checkpoint.dir'.")
        })
      }

      logInfo(s"$logPrefix Preparing the graph for connected component computation ...")
      val g = prepare(graph)
      val vv = g.vertices
      var ee = g.edges.persist(intermediateStorageLevel) // src < dst
      logInfo(s"$logPrefix Found ${ee.count()} edges after preparation.")

      var converged = false
      var iteration = 1

      def _calcMinNbrSum(minNbrsDF: DataFrame): BigDecimal = {
        if (shouldDoSkewedJoin) {
          minNbrsDF
            .select(sum(col(MIN_NBR).cast(DecimalType(38, 0))), count("*"))
            .first()
            .getDecimal(0)
        } else {
          minNbrsDF.select(sum(col(MIN_NBR).cast(DecimalType(38, 0)))).first().getDecimal(0)
        }
      }
      // compute min neighbors (including self-min)
      var minNbrs1: DataFrame =
        minNbrs(
          ee,
          computeCount = shouldDoSkewedJoin,
          includeSelf = true,
          doSymmetrize = true
        ) // src >= min_nbr
          .persist(intermediateStorageLevel)

      var prevSum: BigDecimal = _calcMinNbrSum(minNbrs1)

      var lastRoundPersistedDFs = Seq[DataFrame](ee, minNbrs1)
      while (!converged) {
        var currRoundPersistedDFs = Seq[DataFrame]()
        // large-star step
        // connect all strictly larger neighbors to the min neighbor (including self)
        ee = {
          if (shouldDoSkewedJoin) {
            skewedJoin(ee, minNbrs1, broadcastThreshold, logPrefix)

          } else {
            ee.join(minNbrs1, SRC)
          }
        }.select(col(DST).as(SRC), col(MIN_NBR).as(DST)) // src > dst
          .distinct()
          .persist(intermediateStorageLevel)

        currRoundPersistedDFs = currRoundPersistedDFs :+ ee

        // small-star step
        // compute min neighbors (excluding self-min)
        val minNbrs2 = minNbrs(
          ee,
          computeCount = shouldDoSkewedJoin,
          includeSelf = false,
          doSymmetrize = false)
          .persist(intermediateStorageLevel)
        currRoundPersistedDFs = currRoundPersistedDFs :+ minNbrs2

        // connect all smaller neighbors to the min neighbor
        ee = {
          if (shouldDoSkewedJoin) {
            skewedJoin(ee, minNbrs2, broadcastThreshold, logPrefix)
          } else {
            ee.join(minNbrs2, SRC)
          }
        }.select(col(MIN_NBR).as(SRC), col(DST)) // src <= dst
          .filter(col(SRC) =!= col(DST)) // src < dst
        // connect self to the min neighbor
        ee = ee
          .union(minNbrs2.select(col(MIN_NBR).as(SRC), col(SRC).as(DST))) // src < dst
          .distinct()

        // checkpointing
        if (shouldCheckpoint && (iteration % checkpointInterval == 0)) {
          if (useLocalCheckpoints) {
            ee = ee.localCheckpoint(eager = true)
          } else {
            ee = ee.checkpoint(eager = true)
          }
        }

        ee = ee.persist(intermediateStorageLevel)
        currRoundPersistedDFs = currRoundPersistedDFs :+ ee

        minNbrs1 = minNbrs(
          ee,
          computeCount = shouldDoSkewedJoin,
          includeSelf = true,
          doSymmetrize = true
        ) // src >= min_nbr
          .persist(intermediateStorageLevel)
        currRoundPersistedDFs = currRoundPersistedDFs :+ minNbrs1

        // test convergence
        val currSum = _calcMinNbrSum(minNbrs1)
        logInfo(s"$logPrefix Sum of assigned components in iteration $iteration: $currSum.")
        if (currSum == prevSum) {
          // This also covers the case when cnt = 0 and currSum is null, which means no edges.
          converged = true
        } else {
          prevSum = currSum
        }

        // clean up persisted DFs
        for (persisted_df <- lastRoundPersistedDFs) {
          persisted_df.unpersist()
        }
        lastRoundPersistedDFs = currRoundPersistedDFs
        iteration += 1
      }

      logInfo(s"$logPrefix Connected components converged in ${iteration - 1} iterations.")

      logInfo(s"$logPrefix Join and return component assignments with original vertex IDs.")
      val indexedLabel = vv
        .join(ee, vv(ID) === ee(DST), "left_outer")
        .select(
          vv(ATTR),
          when(ee(SRC).isNull, vv(ID)).otherwise(ee(SRC)).as(COMPONENT),
          col(ATTR + "." + ID).as(ID))
      val output = if (graph.hasIntegralIdType || !useLabelsAsComponents) {
        indexedLabel
          .select(col(s"$ATTR.*"), col(COMPONENT))
          .persist(intermediateStorageLevel)
      } else {
        indexedLabel
          .join(
            indexedLabel
              .groupBy(col(COMPONENT))
              .agg(min(col(ID)).as(ORIG_ID))
              .select(col(COMPONENT), col(ORIG_ID)),
            COMPONENT)
          .select(col(s"$ATTR.*"), col(ORIG_ID).as(COMPONENT))
          .persist(intermediateStorageLevel)
      }

      // An action must be performed on the DataFrame for the cache to load
      output.count()

      // clean up persisted DFs
      for (persisted_df <- lastRoundPersistedDFs) {
        persisted_df.unpersist()
      }

      resultIsPersistent()

      output
    } finally {
      // Restore original AQE setting
      spark.conf.set("spark.sql.adaptive.enabled", originalAQE)
    }
  }
}
