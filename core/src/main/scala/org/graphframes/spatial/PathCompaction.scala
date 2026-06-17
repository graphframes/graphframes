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

package org.graphframes.spatial

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.*
import org.graphframes.GraphFrame
import org.graphframes.Logging
import org.graphframes.WithCheckpointInterval
import org.graphframes.WithIntermediateStorageLevel
import org.graphframes.lib.ConnectedComponents

import scala.collection.mutable

/**
 * Compact non-branching paths in a distributed graph.
 *
 * A "non-branching path" is a maximal chain of vertices each having `inDegree == 1` and
 * `outDegree == 1`. Each such chain is collapsed into a single edge from the chain's unique
 * external predecessor to its unique external successor. The resulting edge's attribute columns
 * are produced by user-supplied aggregations evaluated over the chain's interior vertices.
 *
 * The algorithm proceeds as follows:
 *   1. Select vertices with `inDegree == outDegree == 1` (the candidates).
 *   2. Run connected components on the subgraph induced by the candidates.
 *   3. Each connected component is either a chain (to compact) or a cycle.
 *   4. Per the cycle policy (see [[setDropCompactedCycles]]), cycles are either compacted into a
 *      self-loop on the minimal interior id, or left untouched.
 *   5. For each chain, `src`/`dst` are inferred as the only vertices in `collect_set(orig_src)` /
 *      `collect_set(orig_dst)` that are not interior to the component. User aggregations are
 *      evaluated over the interior vertices.
 *   6. The result is the union of the compacted edges and every original edge that does not touch
 *      any compacted (interior) vertex, i.e. the compacted graph rather than just the new edges.
 *
 * A `count` aggregation (the number of interior vertices collapsed into each compacted edge) is
 * always present in the output; register more with [[withAggExpression]]. On edges that survive
 * compaction unchanged (i.e. are not the product of a compaction), all aggregation columns --
 * including `count` -- are `null`.
 *
 * Usage (Scala):
 * {{{
 *   import org.apache.spark.sql.functions.collect_set
 *
 *   new PathCompaction(graph)
 *     .setDropCompactedCycles(false)
 *     .withAggExpression("ids", collect_set("id"))
 *     .run()
 * }}}
 *
 * The returned [[DataFrame]] has columns `src`, `dst`, `count`, followed by one column per
 * additionally registered aggregation expression.
 */
class PathCompaction private[graphframes] (graph: GraphFrame)
    extends Logging
    with WithIntermediateStorageLevel
    with WithCheckpointInterval {

  private var dropCompactedCycles: Boolean = true
  private val aggExpressions: mutable.Map[String, Column] = mutable.Map(("count", count("*")))
  private var alwaysKeepVertices: Option[Column] = None
  private var alwaysDropVertices: Option[Column] = None
  private var clusteringAlgorithm: String = ConnectedComponents.ALGO_RANDOMIZED_CONTRACTION
  private var requiredVertexAttributes: Seq[String] = Seq.empty

  /**
   * Specifies vertex attribute columns (besides the ID) that must be available when evaluating
   * user-registered aggregation expressions (see [[withAggExpression]]). These columns are
   * projected from the original vertices and joined onto the annotated candidates so that
   * aggregations can reference them.
   *
   * The ID column is always included automatically and does not need to be specified here; if it
   * appears in `value` it is silently removed.
   *
   * @param value
   *   vertex attribute column names to carry into the aggregation input
   * @return
   *   this instance for method chaining
   */
  def requiredVertexAttributes(value: Seq[String]): this.type = {
    requiredVertexAttributes = value.filter(_ != GraphFrame.ID).toSeq
    this
  }

  /**
   * If `true` (default), cycles of candidates are not compacted: they are absent from the set of
   * compacted edges, so their original edges survive unchanged in the output. If `false`, each
   * cycle of candidates is compacted into a single self-loop edge on the minimal interior id (and
   * the cycle's original edges are dropped).
   */
  def setDropCompactedCycles(value: Boolean): this.type = {
    dropCompactedCycles = value
    this
  }

  /**
   * Register a named aggregation expression to be evaluated over the interior vertices of each
   * compacted path. The expression is applied inside a `groupBy(component).agg(...)`; it may
   * reference any vertex attribute column of the original graph as well as `orig_src` and
   * `orig_dst`.
   *
   * A default `count` aggregation is always present. Registering an expression with the name
   * `count` overrides it. Aggregation columns are `null` on edges that survive compaction
   * unchanged.
   */
  def withAggExpression(name: String, expr: Column): this.type = {
    val _ = aggExpressions.put(name, expr)
    this
  }

  /**
   * Vertices matching this condition are never compacted away: they are excluded from the
   * candidate set, so any chain passing through them is split into separate compacted edges.
   */
  def setAlwaysKeepVertices(value: Column): this.type = {
    alwaysKeepVertices = Some(value)
    this
  }

  /**
   * Vertices matching this condition are dropped from the graph before compaction. Chains passing
   * through a dropped vertex are likewise split.
   */
  def setAlwaysDropVertices(value: Column): this.type = {
    alwaysDropVertices = Some(value)
    this
  }

  /**
   * Connected-components backend used to group candidates into chains/cycles. Must be one of
   * `"two_phase"` or `"randomized_contraction"`. Default is `"randomized_contraction"`
   */
  def setClusteringAlgorithm(value: String): this.type = {
    require(
      PathCompaction.supportedAlgorithms.contains(value),
      s"algorithm $value is not supported; supported algorithms: $supportedAlgorithmsAsString")
    clusteringAlgorithm = value
    this
  }

  private def supportedAlgorithmsAsString: String =
    PathCompaction.supportedAlgorithms.mkString(", ")

  /**
   * Runs the algorithm and returns the compacted graph as a DataFrame of edges.
   *
   * The result is the union of:
   *   - one row per compacted path/cycle (an edge with its aggregation columns populated), and
   *   - every original edge whose endpoints are not interior to any compacted path, with all
   *     aggregation columns (including `count`) set to `null`.
   *
   * The schema is `src`, `dst`, `count`, followed by one column per additionally registered
   * aggregation expression.
   */
  def run(): DataFrame = {
    val vertexAttrs = requiredVertexAttributes :+ GraphFrame.ID

    // STEP 1 - honor alwaysDropVertices by pruning the graph up front.
    var baseSubgraph = (alwaysDropVertices match {
      case Some(filter) => graph.filterVertices(!filter)
      case None => graph
    })

    val persistedBaseVertices =
      baseSubgraph.vertices
        .select(vertexAttrs.map(col(_)).toSeq: _*)
        .persist(intermediateStorageLevel)
    val persistedBaseEdges =
      baseSubgraph.edges.select(GraphFrame.SRC, GraphFrame.DST).persist(intermediateStorageLevel)
    baseSubgraph = GraphFrame(persistedBaseVertices, persistedBaseEdges)

    // STEP 2 - candidates: inDegree == outDegree == 1, excluding always-keep vertices.
    val candidates = {
      val degreesFilter = baseSubgraph.inDegrees
        .join(baseSubgraph.outDegrees, Seq(GraphFrame.ID), "inner")
        .filter((col("inDegree") === lit(1)) && (col("outDegree") === lit(1)))
      alwaysKeepVertices match {
        case Some(keep) =>
          degreesFilter
            .join(baseSubgraph.vertices.filter(keep), Seq(GraphFrame.ID), "left_anti")
            .select(GraphFrame.ID)
        case None =>
          degreesFilter.select(GraphFrame.ID)
      }
    }

    // STEP 3 - annotate each candidate with its unique predecessor (orig_src) and successor
    //          (orig_dst), and carry original vertex attribute columns so that user
    //          aggregations can reference them. inDeg == outDeg == 1 guarantees exactly one
    //          match in each join.
    val annotated = ({
      val inner = candidates
        .join(
          persistedBaseEdges.select(col(GraphFrame.SRC).as("orig_src"), col(GraphFrame.DST)),
          col(GraphFrame.DST) === col(GraphFrame.ID),
          "inner")
        .join(
          persistedBaseEdges.select(col(GraphFrame.SRC), col(GraphFrame.DST).as("orig_dst")),
          col(GraphFrame.SRC) === col(GraphFrame.ID),
          "inner")

      if (requiredVertexAttributes.isEmpty) {
        inner
      } else {
        inner.join(persistedBaseVertices, Seq(GraphFrame.ID), "left")
      }
    }).persist(intermediateStorageLevel)

    val cntAnnotated = annotated.count()
    logInfo(s"$cntAnnotated vertices have been choosen for compaction")
    persistedBaseVertices.unpersist(blocking = true)

    // STEP 4 - induced subgraph (edges with both endpoints candidates) + connected components.
    val interiorEdges = baseSubgraph.edges
      .join(
        candidates.withColumnRenamed(GraphFrame.ID, "sid"),
        col(GraphFrame.SRC) === col("sid"),
        "inner")
      .join(
        candidates.withColumnRenamed(GraphFrame.ID, "did"),
        col(GraphFrame.DST) === col("did"),
        "inner")
      .select(col(GraphFrame.SRC), col(GraphFrame.DST))

    val components = GraphFrame(annotated, interiorEdges).connectedComponents
      .setAlgorithm(clusteringAlgorithm)
      .setCheckpointInterval(checkpointInterval)
      .setIntermediateStorageLevel(intermediateStorageLevel)
      .setBroadcastThreshold(-1)
      .run()

    val toAgg = if (clusteringAlgorithm == ConnectedComponents.ALGO_TWO_PHASE) {
      components
    } else {
      // randomized contraction does not preserve columns
      components.join(annotated, GraphFrame.ID, "inner")
    }

    // STEP 5 - group by component: collect inner ids and the candidate external endpoints,
    //          and evaluate user aggregations over the interior vertices.
    val aggColNames = aggExpressions.keys.toSeq
    val userAggCols = aggExpressions.toSeq.map { case (name, expr) => expr.as(name) }
    val leadAggCols = Seq(
      collect_set(GraphFrame.ID).as("inner_ids"),
      collect_set("orig_src").as("all_orig_src"),
      collect_set("orig_dst").as("all_orig_dst"))
    val grouped = toAgg
      .groupBy(col(ConnectedComponents.COMPONENT))
      .agg(leadAggCols.head, (leadAggCols.tail ++ userAggCols): _*)

    // STEP 6 - derive src/dst, detect cycles, filter, and project the final schema.
    //   - For a path: the external predecessor/successor are exactly the elements of
    //     collect_set(orig_src)/collect_set(orig_dst) that are NOT interior to the component.
    //     A non-branching chain has at most one of each, so array_except yields a singleton.
    //   - For a cycle: both arrays are empty.
    val withEndpoints = grouped
      .withColumn("compacted_src", array_except(col("all_orig_src"), col("inner_ids")))
      .withColumn("compacted_dst", array_except(col("all_orig_dst"), col("inner_ids")))
      .withColumn(
        "is_cycle",
        size(col("compacted_src")) === 0 && size(col("compacted_dst")) === 0)
      .withColumn(
        GraphFrame.SRC,
        when(col("is_cycle"), array_min(col("inner_ids")))
          .otherwise(element_at(col("compacted_src"), 1)))
      .withColumn(
        GraphFrame.DST,
        when(col("is_cycle"), array_min(col("inner_ids")))
          .otherwise(element_at(col("compacted_dst"), 1)))

    val filtered =
      if (dropCompactedCycles) withEndpoints.filter(!col("is_cycle")) else withEndpoints

    val result = filtered
      .select((Seq(col(GraphFrame.SRC), col(GraphFrame.DST)) ++ aggColNames.map(col)): _*)

    val outputEdgeColumns = result.schema.map(f => {
      if ((f.name == GraphFrame.SRC) || (f.name == GraphFrame.DST)) {
        col(f.name)
      } else {
        lit(null).cast(f.dataType).alias(f.name)
      }
    })

    // STEP 7 - keep every original edge whose endpoints are NOT interior to any compacted path.
    //   The boundary edges (entrance -> v_first, v_last -> exit) of each compacted path touch an
    //   interior vertex and must also be dropped, otherwise each compacted path would fan back
    //   out at its endpoints. Cycles that are not compacted (dropCompactedCycles = true) are
    //   absent from `filtered`, so their vertices are not interior ids and their edges survive
    //   unchanged; this is the "return all edges" semantics for dropped cycles.
    val compactedInteriorIds = filtered
      .select(explode(col("inner_ids")).as(GraphFrame.ID))
      .persist(intermediateStorageLevel)

    val survivingEdges = persistedBaseEdges
      .join(
        compactedInteriorIds,
        baseSubgraph.edges(GraphFrame.SRC) === compactedInteriorIds(GraphFrame.ID),
        "left_anti")
      .join(
        compactedInteriorIds,
        baseSubgraph.edges(GraphFrame.DST) === compactedInteriorIds(GraphFrame.ID),
        "left_anti")
      .select(outputEdgeColumns: _*)

    val allEdges = result
      .union(survivingEdges)
      .persist(intermediateStorageLevel)

    // materialize
    val totalCnt = allEdges.count()
    logInfo(s"new edges count: $totalCnt")

    // clean-up
    persistedBaseEdges.unpersist(blocking = true)
    annotated.unpersist(blocking = true)
    interiorEdges.unpersist(blocking = true)
    components.unpersist(blocking = true)
    compactedInteriorIds.unpersist(blocking = true)

    resultIsPersistent()
    allEdges
  }
}

private[graphframes] object PathCompaction extends Serializable {
  // It would be nice to add in the future a specialized version based on pointer jumping.
  private[graphframes] val supportedAlgorithms: Seq[String] =
    Seq(ConnectedComponents.ALGO_TWO_PHASE, ConnectedComponents.ALGO_RANDOMIZED_CONTRACTION)
}
