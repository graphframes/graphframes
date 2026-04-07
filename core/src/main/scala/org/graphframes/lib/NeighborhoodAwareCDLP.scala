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

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*
import org.graphframes.GraphFrame
import org.graphframes.GraphFrame.*
import org.graphframes.GraphFramesSparkVersionException
import org.graphframes.Logging
import org.graphframes.WithAlgorithmChoice
import org.graphframes.WithCheckpointInterval
import org.graphframes.WithDirection
import org.graphframes.WithIntermediateStorageLevel
import org.graphframes.WithLocalCheckpoints
import org.graphframes.WithMaxIter

/**
 * Neighborhood-aware community detection via weighted label propagation.
 *
 * This algorithm is a Label Propagation variant where each incoming label vote is weighted by a
 * combination of:
 *   - direct-link strength (`a`), and
 *   - neighborhood-overlap strength (`c * commonNeighbors`).
 *
 * Intuitively, labels from neighbors that are structurally similar to the destination (many
 * common neighbors) can be amplified, instead of treating all edges equally.
 *
 * At each iteration, every vertex aggregates weighted incoming votes by label and picks the label
 * with maximum total weight.
 *
 * Main hyperparameters:
 *   - `maxIter` (required): maximum number of propagation rounds.
 *   - `a` (default `1.0`): direct-link baseline weight.
 *   - `c` (default `0.5`): neighborhood-overlap weight.
 *
 * How to balance `a` and `c`:
 *   - `a = 1, c = 0`: recovers classical unweighted label propagation behavior (all incoming
 *     edges vote equally).
 *   - larger `a` / smaller `c`: rely more on direct connectivity, less on higher-order structure.
 *   - smaller `a` / larger `c`: rely more on structural similarity via shared neighborhoods.
 *   - `a = 0, c = 1`: pure structure propagation based only on common-neighbor overlap.
 *
 * This implementation is inspired by neighborhood-strength-driven label propagation ideas from:
 * Xie, Jierui, and Boleslaw K. Szymanski. "Community detection using a neighborhood strength
 * driven label propagation algorithm." 2011 IEEE Network Science Workshop. IEEE, 2011.
 *
 * Note: this implementation does not strictly reproduce the paper; it adopts the core idea of
 * modulating label votes with a common-neighbor term (`c`) within the GraphFrames/Pregel design.
 */
class NeighborhoodAwareCDLP private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with WithAlgorithmChoice
    with WithCheckpointInterval
    with WithMaxIter
    with WithLocalCheckpoints
    with WithIntermediateStorageLevel
    with WithDirection
    with Logging {

  private var c: Double = 0.5
  private var a: Double = 1.0
  private var initialLabelCol: Option[String] = None
  private var lgNomEntries: Int = 12

  import NeighborhoodAwareCDLP.*

  /**
   * Sets weight for the neighborhood-overlap signal (common neighbors).
   *
   * This is the `c` term in edge weighting: {{ edgeWeight(src, dst) = a + c *
   * commonNeighbors(src, dst) }} where `commonNeighbors(src, dst)` is the (approximate) number of
   * shared out-neighbors between source and destination.
   *
   * Higher `c` increases the impact of structural similarity: labels coming from vertices that
   * share many neighbors with the destination receive more voting mass.
   *
   * Key regimes:
   *   - `c = 0` (with `a = 1`) behaves like classical unweighted label propagation: each incoming
   *     edge contributes equal vote mass, independent of overlap.
   *   - `c = 1` and `a = 0` gives pure structure propagation: vote mass depends only on common
   *     neighbors, so direct links with no shared-neighborhood support contribute zero mass.
   *
   * Default: `0.5`.
   */
  def setC(value: Double): this.type = {
    require(value >= 0.0 && value <= 1.0, "c must be in [0,1]")
    c = value
    this
  }

  /**
   * Sets weight for the direct-link signal.
   *
   * This is the `a` base term in edge weighting: {{ edgeWeight(src, dst) = a + c *
   * commonNeighbors(src, dst) }}
   *
   * Higher `a` gives every existing edge a stronger uniform baseline vote, regardless of
   * structural overlap.
   *
   * Key regimes:
   *   - `a = 1` and `c = 0` recovers classical label propagation behavior where each incoming
   *     edge has equal unit influence.
   *   - `a = 0` and `c = 1` removes direct-link baseline influence and relies fully on common
   *     neighbors (pure structure propagation).
   *
   * Default: `1.0`.
   */
  def setA(value: Double): this.type = {
    require(value >= 0.0 && value <= 1.0, "a must be in [0,1]")
    a = value
    this
  }

  /**
   * Sets an explicit vertex column to use as initial labels.
   *
   * By default, each vertex starts with its own `id` as label. When this setter is used, the
   * algorithm initializes labels from the provided attribute column instead, enabling
   * attribute-guided label propagation (attribute propagation): labels can start from domain
   * values such as categories, types, or seeds and then propagate through the graph structure.
   *
   * The output `label` column keeps the data type of the provided column.
   */
  def setInitialLabelCol(col: String): this.type = {
    require(
      graph.vertices.columns.contains(col),
      s"Initial label column '$col' does not exist in vertex columns: ${graph.vertices.columns.mkString(", ")}")
    initialLabelCol = Some(col)
    this
  }

  /**
   * Sets sketch size parameter for approximate common-neighbor counting.
   *
   * This value controls Theta sketch capacity (`theta_sketch_agg`) used to estimate overlap
   * between neighborhood sets. It tunes the quality/resource tradeoff:
   *   - Larger values improve approximation quality/stability of common-neighbor estimates.
   *   - Smaller values reduce memory usage (and often compute overhead) but increase
   *     approximation error.
   *
   * Valid range is `[4, 24]`. Default: `12`.
   */
  def setLgNomEntries(value: Int): this.type = {
    require((value >= 4) && (value <= 24), "lgNomEntries must be between 4 and 24")
    lgNomEntries = value
    this
  }

  def run(): DataFrame = {
    // Validate parameters
    val maxIterChecked = check(maxIter, "maxIter")

    // Sketch-based features require Spark >= 4.1
    val sparkVersion = graph.vertices.sparkSession.version
    if (sparkVersion.substring(0, 3) < "4.1") {
      throw new GraphFramesSparkVersionException("4.1.0")
    }

    val edges = if (isDirected) {
      graph.edges.select(SRC, DST)
    } else {
      graph.edges
        .select(col(DST).alias(SRC), col(SRC).alias(DST))
        .union(graph.edges.select(SRC, DST))
        .distinct()
    }

    // Compute approximate common neighbor counts on edges and materialize.
    val enrichedEdges =
      computeEdgeApproxCommonNeighbors(edges, lgNomEntries, c, a)

    val vertices = if (initialLabelCol.isDefined) {
      graph.vertices.select(col(GraphFrame.ID), col(initialLabelCol.get).alias(INITIAL_LABEL_COL))
    } else {
      graph.vertices.select(col(GraphFrame.ID), col(GraphFrame.ID).alias(INITIAL_LABEL_COL))
    }

    val preparedGraph = GraphFrame(vertices, enrichedEdges)

    val pregel = preparedGraph.pregel

    pregel
      .setMaxIter(maxIterChecked)
      .setCheckpointInterval(checkpointInterval)
      .setUseLocalCheckpoints(useLocalCheckpoints)
      .setIntermediateStorageLevel(intermediateStorageLevel)
      .sendMsgToDst(struct(Pregel.src(LABEL_COL), Pregel.edge(EDGE_WEIGHT_COL)))
      .aggMsgs(aggregateMessages(Pregel.msg, vertices.schema(INITIAL_LABEL_COL).dataType))
      .withVertexColumn(
        LABEL_COL,
        col(INITIAL_LABEL_COL),
        coalesce(keyWithMaxValue(Pregel.msg), col(LABEL_COL)))
      .setSkipMessagesFromNonActiveVertices(false)
      .setUpdateActiveVertexExpression(
        col(LABEL_COL) =!= coalesce(keyWithMaxValue(Pregel.msg), col(LABEL_COL)))
      .setStopIfAllNonActiveVertices(true)
      .setEarlyStopping(false)

    val result = pregel.run().drop(INITIAL_LABEL_COL)
    resultIsPersistent()

    result
  }
}

object NeighborhoodAwareCDLP extends Logging {

  val LABEL_COL = "label"
  private val INITIAL_LABEL_COL = "initial_label"
  private val EDGE_WEIGHT_COL = "edge_weight"

  private def aggregateMessages(msgCol: Column, idType: DataType): Column = reduce(
    collect_list(msgCol),
    map().cast(MapType(idType, DoubleType)),
    (acc, x) =>
      map_zip_with(
        acc,
        map(x.getField(LABEL_COL), x.getField(EDGE_WEIGHT_COL)),
        (_, left, right) => coalesce(left, lit(0.0)) + coalesce(right, lit(0.0))))

  private def keyWithMaxValue(column: Column): Column = array_max(
    transform(
      map_entries(column),
      x => struct(x.getField("value"), x.getField("key").alias("key"))))
    .getField("key")

  private def computeEdgeApproxCommonNeighbors(
      edges: DataFrame,
      lgNomEntries: Int,
      neighborsScale: Double,
      linkScale: Double): DataFrame = {
    val thetaSketchAggExpr = expr(s"theta_sketch_agg($DST, $lgNomEntries)")

    val vertexSketches = edges
      .groupBy(col(SRC).alias(ID))
      .agg(thetaSketchAggExpr.alias("nbr_theta_sketch"))

    val thetaSketchIntersect = (left: String, right: String) =>
      expr(s"theta_sketch_estimate(theta_intersection($left, $right))")

    // Prepare sketches for join (id, nbr_theta_sketch)
    val srcSketch = vertexSketches.select(
      col(ID).alias("sk_src_id"),
      col("nbr_theta_sketch").alias("src_nbr_sketch"))
    val dstSketch = vertexSketches.select(
      col(ID).alias("sk_dst_id"),
      col("nbr_theta_sketch").alias("dst_nbr_sketch"))

    // Join edges with sketches. Use left_outer so edges without sketches will have null sketches.
    val e = edges
      .select(SRC, DST)
      .join(srcSketch, col(SRC) === col("sk_src_id"), "left_outer")
      .join(dstSketch, col(DST) === col("sk_dst_id"), "left_outer")

    // Compute approximate intersection and coalesce nulls to 0.0
    val weightCol = lit(linkScale) + lit(neighborsScale) * coalesce(
      thetaSketchIntersect("src_nbr_sketch", "dst_nbr_sketch"),
      lit(0.0))
    val edgesWithOverlap = e
      .select(col(SRC), col(DST), weightCol.alias(EDGE_WEIGHT_COL))

    edgesWithOverlap
  }
}
