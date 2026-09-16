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
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame
import org.graphframes.Logging
import org.graphframes.WithCheckpointInterval
import org.graphframes.WithIntermediateStorageLevel
import org.graphframes.WithLocalCheckpoints
import org.graphframes.WithMaxIter

/**
 * PageRank algorithm implementation, version 2. It is a native DataFrame implementation built on
 * top of the [[org.graphframes.lib.Pregel Pregel]] engine and does not use GraphX.
 *
 * The algorithm is the delta-style (incremental) PageRank, similar to the dynamic PageRank of
 * GraphX. Every vertex carries the accumulated rank and the rank delta of the latest iteration:
 *   - every vertex sends its latest delta split across its outgoing edges (`delta / outDegree`)
 *     to the destinations;
 *   - the new delta is `(1 - resetProbability) * (sum of incoming messages)` and is added to the
 *     accumulated rank;
 *   - vertices with a delta not greater than the tolerance stop sending messages, so the amount
 *     of communications shrinks from iteration to iteration: only the active set of vertices
 *     participates in the message passing.
 *
 * The resulting DataFrame contains all the original vertex columns and one additional column:
 *   - pageranks (`DoubleType`): the PageRank of this vertex; the ranks are normalized to sum up
 *     to 1.0
 */
class PageRankV2 private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with Logging
    with WithCheckpointInterval
    with WithMaxIter
    with WithLocalCheckpoints
    with WithIntermediateStorageLevel {

  private var resetProb: Double = 0.15
  private var tol: Option[Double] = None
  private var srcId: Option[Any] = None

  /** Source vertex for a Personalized Page Rank (optional) */
  def sourceId(value: Any): this.type = {
    this.srcId = Some(value)
    this
  }

  /** Reset probability "alpha" */
  def resetProbability(value: Double): this.type = {
    resetProb = value
    this
  }

  /** Convergence tolerance. Cannot be used together with [[maxIter]]. */
  def tol(value: Double): this.type = {
    tol = Some(value)
    this
  }

  def run(): DataFrame = {
    require(
      resetProb >= 0.0 && resetProb <= 1.0,
      s"Random reset probability must belong to [0, 1], but got $resetProb")
    require(
      !graph.vertices.columns.contains(PageRankV2.PAGERANKS),
      s"The vertices DataFrame must not contain the '${PageRankV2.PAGERANKS}' column.")
    val res = tol match {
      case Some(t) =>
        assert(maxIter.isEmpty, "You cannot specify maxIter() and tol() at the same time.")
        // With the voting to halt there is no need for an iteration budget: vertices with a
        // small delta stop sending messages, so the run stops as soon as every vertex converges.
        PageRankV2.runWithPregel(
          graph,
          srcId,
          resetProb,
          Some(t),
          Int.MaxValue,
          checkpointInterval,
          useLocalCheckpoints,
          intermediateStorageLevel)
      case None =>
        PageRankV2.runWithPregel(
          graph,
          srcId,
          resetProb,
          None,
          check(maxIter, "maxIter"),
          checkpointInterval,
          useLocalCheckpoints,
          intermediateStorageLevel)
    }
    resultIsPersistent()
    res
  }
}

private object PageRankV2 {

  /** Default name for the pageranks column. */
  private[lib] val PAGERANKS = "pageranks"

  /** Internal name for the per-iteration rank delta column. */
  private val PAGERANKS_DELTA = "_pageranks_delta"

  /** Internal name for the out-degree column joined to the vertices. */
  private val OUT_DEGREE = "_out_degree"

  /**
   * Participation threshold for the fixed number of iterations mode. Vertices with a delta below
   * this value stop sending messages. It is small enough to keep the ranks numerically identical
   * to the untruncated run, while still shrinking the active set of vertices when the algorithm
   * approaches the fixpoint.
   */
  private val DELTA_EPS = 1e-8

  private def runWithPregel(
      graph: GraphFrame,
      srcId: Option[Any],
      resetProb: Double,
      tol: Option[Double],
      maxIter: Int,
      checkpointInterval: Int,
      useLocalCheckpoints: Boolean,
      intermediateStorageLevel: StorageLevel): DataFrame = {
    val damping = 1.0 - resetProb

    // Participation threshold: the user-defined tolerance in the convergence mode, a small
    // epsilon in the fixed number of iterations mode.
    val deltaThreshold = tol.getOrElse(DELTA_EPS)

    // PageRank needs the out-degree of each vertex to distribute its rank. The join is left:
    // sink vertices (out-degree = 0) and isolated vertices must stay in the vertex set because
    // they accumulate the incoming rank mass. They never appear on the source side of any
    // triplet, so their zero out-degree is never used in a division.
    val preparedGraph = GraphFrame(
      graph.vertices
        .join(
          graph.outDegrees.select(col(GraphFrame.ID), col("outDegree").alias(OUT_DEGREE)),
          Seq(GraphFrame.ID),
          "left_outer")
        .na
        .fill(Map(OUT_DEGREE -> 0)),
      graph.edges.select(GraphFrame.SRC, GraphFrame.DST))

    // The new per-iteration delta = damping * (sum of incoming messages). The same expression
    // updates the delta column and the active flag: a vertex is active while its latest delta
    // is greater than the threshold.
    val newDelta = lit(damping) * coalesce(Pregel.msg, lit(0.0))

    // Seeding. The uniform case seeds both the rank and the delta with the reset probability,
    // which reproduces the bootstrap of the GraphX dynamic PageRank (its initial message is
    // resetProb / damping). The personalized case seeds the rank and the delta of the source
    // vertex with 1.0, like the GraphX personalized vertex program; only the source vertex is
    // active initially.
    val (initRank, initDelta, initActive): (Column, Column, Column) = srcId match {
      case Some(src) =>
        val isSrc = col(GraphFrame.ID) === lit(src)
        (
          when(isSrc, lit(1.0)).otherwise(lit(0.0)),
          when(isSrc, lit(1.0)).otherwise(lit(0.0)),
          isSrc)
      case None => (lit(resetProb), lit(resetProb), lit(true))
    }

    var pregel = preparedGraph.pregel
      .withVertexColumn(PAGERANKS, initRank, col(PAGERANKS) + newDelta)
      .withVertexColumn(PAGERANKS_DELTA, initDelta, newDelta)
      // The message expression references only source columns, so the Pregel engine skips the
      // destination state (no second join) and prunes the non-active sources before joining
      // them with the edges.
      .requiredSrcColumns(PAGERANKS_DELTA, OUT_DEGREE)
      .sendMsgToDst(Pregel.src(PAGERANKS_DELTA) / Pregel.src(OUT_DEGREE))
      .aggMsgs(sum(Pregel.msg))
      .setMaxIter(maxIter)
      .setCheckpointInterval(checkpointInterval)
      .setSkipMessagesFromNonActiveVertices(true)
      .setInitialActiveVertexExpression(initActive)
      .setUpdateActiveVertexExpression(newDelta > lit(deltaThreshold))
      .setUseLocalCheckpoints(useLocalCheckpoints)
      .setIntermediateStorageLevel(intermediateStorageLevel)

    // In the convergence mode the vertices vote to halt: the run stops as soon as every vertex
    // delta is below the tolerance. In the fixed number of iterations mode the participation
    // filter is kept (it is a cheap tail optimization) but there is no early termination.
    if (tol.isDefined) {
      pregel = pregel.setStopIfAllNonActiveVertices(true)
    }

    val rawVertices = pregel.run()

    // Normalize the ranks so they sum up to 1.0 and project out all the internal columns
    // (out-degrees, deltas and the active flag).
    val rankSum = rawVertices.agg(coalesce(sum(col(PAGERANKS)), lit(1.0))).head().getDouble(0)
    val vertexCols = graph.vertices.columns.map(col).toSeq
    val res = rawVertices
      .select((vertexCols :+ (col(PAGERANKS) / lit(rankSum)).alias(PAGERANKS)): _*)
      .persist(intermediateStorageLevel)
    res.count()
    rawVertices.unpersist()
    res
  }
}
