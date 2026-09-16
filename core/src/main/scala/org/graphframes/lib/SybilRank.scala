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
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.NumericType
import org.graphframes.GraphFrame
import org.graphframes.Logging
import org.graphframes.WithCheckpointInterval
import org.graphframes.WithDirection
import org.graphframes.WithIntermediateStorageLevel
import org.graphframes.WithLocalCheckpoints
import org.graphframes.WithMaxIter

import java.util
import scala.jdk.CollectionConverters.*

/**
 * SybilRank algorithm for ranking the trustworthiness of accounts in social networks and
 * detecting sybils (fake accounts).
 *
 * The implementation follows the SybilRank algorithm published by Cao et al. at NSDI'12, with the
 * weighted-graph extension of Boshmaf et al. (the same variant that is used by the Okapi graph
 * processing library).
 *
 * The algorithm is a decaying power iteration:
 *   - Trusted ("non-sybil") vertices share an initial trust of `totalTrust / numTrusted` each
 *     (`totalTrust` defaults to the number of vertices, so every trusted vertex starts with rank
 *     1.0);
 *   - on every power iteration a vertex distributes its current rank among its neighbors
 *     proportionally to the edge weights, i.e. it sends `rank * weight / degree` to each
 *     neighbor, where the degree is the sum of the incident (or outgoing, for directed graphs)
 *     edge weights;
 *   - the rank of a vertex is updated to the sum of the incoming messages divided by its degree;
 *   - the number of power iterations defaults to `ceil(iterationMultiplier * log10(N))`, where
 *     `N` is the number of vertices and `iterationMultiplier` is 1 by default.
 *
 * Because every iteration multiplies the total rank by a factor that is at most one, the total
 * rank decays over iterations, and sybils far away from the trusted region end up with a rank
 * close to zero.
 *
 * By default the graph is treated as undirected: messages are sent along each edge in both
 * directions and the degree of a vertex is the sum of the weights of all the incident edges. Use
 * [[setIsDirected]] to follow the edge directions instead.
 *
 * The edges can be weighted with [[setWeightCol]]. Without a weight column all the edges are
 * treated as having weight 1.0.
 *
 * Vertices with zero degree (isolated vertices, or vertices connected only by zero-weight edges)
 * always end up with a zero rank.
 *
 * The resulting DataFrame contains the vertex ID and the rank:
 *   - id (`LongType` or whatever type the graph uses for IDs)
 *   - sybil_rank (`DoubleType`): the trust rank of the vertex
 *
 * Example:
 * {{{
 *   val ranks = graph.sybilRank
 *     .setTrustedVertices(1L, 2L)
 *     .run()
 * }}}
 *
 * @param graph
 *   the graph to run SybilRank on
 * @see
 *   <a href="https://doi.org/10.1145/1993077.1993083">Cao et al., "Aiding the Detection of Fake
 *   Accounts in Large Scale Social Online Services", NSDI'12</a>
 * @see
 *   <a href="https://doi.org/10.1145/2485885.2485888">Boshmaf et al., "Integro: Leveraging Victim
 *   Prediction for Robust and Automatic Fake Account Detection", CCS'15</a>
 */
class SybilRank private[graphframes] (private val graph: GraphFrame)
    extends WithCheckpointInterval
    with WithDirection
    with WithIntermediateStorageLevel
    with WithLocalCheckpoints
    with WithMaxIter
    with Logging {

  // Unlike the shared default, SybilRank is defined on undirected trust graphs.
  isDirected = false

  import SybilRank.*

  private var trustedVertexIds: Option[Seq[Any]] = None
  private var trustedVerticesColName: Option[String] = None
  private var weightColName: Option[String] = None
  private var totalTrust: Option[Double] = None
  private var iterationMultiplier: Double = 1.0

  /**
   * Sets the IDs of the trusted (non-sybil) vertices.
   *
   * This is an alternative to [[setTrustedVerticesCol]]; exactly one of the two must be set. All
   * the provided IDs must exist in the vertex set, otherwise the algorithm fast-fails.
   *
   * @param ids
   *   trusted vertex IDs
   */
  def setTrustedVertices(ids: Seq[Any]): this.type = {
    trustedVertexIds = Some(ids)
    this
  }

  // py4j-friendly overload
  def setTrustedVertices(ids: util.ArrayList[Any]): this.type = {
    setTrustedVertices(ids.asScala.toSeq)
  }

  /**
   * Sets the name of a boolean vertex column that marks the trusted (non-sybil) vertices.
   *
   * This is an alternative to `setTrustedVertices`; exactly one of the two must be set.
   *
   * @param colName
   *   name of a boolean vertex column
   */
  def setTrustedVerticesCol(colName: String): this.type = {
    trustedVerticesColName = Some(colName)
    this
  }

  /**
   * Sets the name of a numeric edge column with edge weights.
   *
   * If it is not set, all the edges are treated as having weight 1.0. The weighted degree of a
   * vertex is the sum of the weights of its incident edges (outgoing edges for directed graphs).
   *
   * @param colName
   *   name of a numeric edge column
   */
  def setWeightCol(colName: String): this.type = {
    weightColName = Some(colName)
    this
  }

  /**
   * Sets the total amount of trust distributed over the trusted vertices at the start (default:
   * the number of vertices, i.e. every trusted vertex starts with rank 1.0).
   *
   * @param value
   *   positive total trust
   */
  def setTotalTrust(value: Double): this.type = {
    require(value > 0.0, s"totalTrust must be positive, but got $value")
    totalTrust = Some(value)
    this
  }

  /**
   * Sets the iteration multiplier (default: 1.0).
   *
   * The number of power iterations is computed as `ceil(iterationMultiplier * log10(N))`, where
   * `N` is the number of vertices. An explicit `maxIter(...)` call takes precedence over the
   * multiplier.
   *
   * @param value
   *   positive iteration multiplier
   */
  def setIterationMultiplier(value: Double): this.type = {
    require(value > 0.0, s"iterationMultiplier must be positive, but got $value")
    iterationMultiplier = value
    this
  }

  /**
   * Runs the SybilRank algorithm.
   *
   * @return
   *   a DataFrame with the vertex `id` and the `sybil_rank` columns
   */
  def run(): DataFrame = {
    require(
      trustedVertexIds.isEmpty || trustedVerticesColName.isEmpty,
      "Either trusted vertices or a trusted vertices column should be set, but not both.")
    require(
      trustedVertexIds.isDefined || trustedVerticesColName.isDefined,
      "Trusted vertices should be set by setTrustedVertices or setTrustedVerticesCol.")

    val trustedColName = trustedVerticesColName
    require(
      trustedColName.isEmpty || graph.vertices.columns.contains(trustedColName.get),
      s"Trusted vertices column ${trustedColName.get} is not a vertex DataFrame column.")
    if (trustedColName.isDefined) {
      require(
        graph.vertices.schema(trustedColName.get).dataType == BooleanType,
        s"Trusted vertices column ${trustedColName.get} should be of boolean type.")
    }

    val weightCol = weightColName
    require(
      weightCol.isEmpty || graph.edges.columns.contains(weightCol.get),
      s"Weight column ${weightCol.get} is not an edge DataFrame column.")
    if (weightCol.isDefined) {
      require(
        graph.edges.schema(weightCol.get).dataType.isInstanceOf[NumericType],
        s"Weight column ${weightCol.get} should be of a numeric type.")
    }

    require(
      !graph.vertices.columns.contains(SYBIL_RANK_COL),
      s"Vertex DataFrame already contains a column named $SYBIL_RANK_COL.")
    require(
      !graph.vertices.columns.contains(DEGREE_COL),
      s"Vertex DataFrame already contains a column named $DEGREE_COL.")

    val numVertices = graph.vertices.count()
    require(numVertices > 0, "SybilRank cannot run on a graph without vertices.")

    val trustedExpr: Column = trustedColName match {
      case Some(cName) => col(cName)
      case None =>
        val ids = trustedVertexIds.get.distinct
        val numMatched = graph.vertices
          .filter(col(GraphFrame.ID).isin(ids: _*))
          .select(GraphFrame.ID)
          .distinct()
          .count()
        require(
          numMatched == ids.size,
          s"Only $numMatched of ${ids.size} trusted vertices are found in the graph; " +
            "SybilRank requires all the trusted vertices to exist.")
        col(GraphFrame.ID).isin(ids: _*)
    }

    val numTrusted: Long = trustedColName match {
      case Some(cName) => graph.vertices.filter(col(cName)).count()
      case None => trustedVertexIds.get.distinct.size.toLong
    }
    require(numTrusted > 0, "SybilRank requires at least one trusted vertex.")

    val iterations = maxIter.getOrElse {
      math.max(
        1,
        math.ceil(iterationMultiplier * math.log10(math.max(numVertices.toDouble, 1.0))).toInt)
    }

    val effectiveTotalTrust = totalTrust.getOrElse(numVertices.toDouble)

    val degrees = computeDegrees(weightCol)
    val preparedVertices = graph.vertices
      .join(degrees, Seq(GraphFrame.ID), "left")
      .withColumn(DEGREE_COL, coalesce(col(DEGREE_COL), lit(0.0)))
    val preparedGraph = GraphFrame(preparedVertices, graph.edges)

    val initRankExpr =
      when(trustedExpr, lit(effectiveTotalTrust / numTrusted.toDouble)).otherwise(lit(0.0))
    val updateRankExpr =
      when(col(DEGREE_COL) > lit(0.0), coalesce(Pregel.msg, lit(0.0)) / col(DEGREE_COL))
        .otherwise(lit(0.0))

    var pregel = preparedGraph.pregel
      .withVertexColumn(SYBIL_RANK_COL, initRankExpr, updateRankExpr)
      .sendMsgToDst(messageExpr(isSrc = true))
      .aggMsgs(sum(Pregel.msg))
      .setMaxIter(iterations)
      .setCheckpointInterval(checkpointInterval)
      .setUseLocalCheckpoints(useLocalCheckpoints)
      .setIntermediateStorageLevel(intermediateStorageLevel)
      .requiredSrcColumns(SYBIL_RANK_COL, DEGREE_COL)

    if (!isDirected) {
      pregel = pregel
        .sendMsgToSrc(messageExpr(isSrc = false))
        .requiredDstColumns(SYBIL_RANK_COL, DEGREE_COL)
    }

    if (weightCol.isDefined) {
      pregel = pregel.requiredEdgeColumns(weightCol.get)
    }

    val res = pregel.run().select(col(GraphFrame.ID), col(SYBIL_RANK_COL))
    resultIsPersistent()
    res
  }

  /**
   * Computes the weighted degree of every vertex from the edge list.
   *
   * For a directed graph it is the sum of the outgoing edge weights; for an undirected graph the
   * weights of all the incident edges are summed up. Vertices without edges are missing from the
   * result (they get a zero degree in [[run]]).
   */
  private def computeDegrees(weightCol: Option[String]): DataFrame = {
    val weightedEdges = graph.edges.select(
      col(GraphFrame.SRC),
      col(GraphFrame.DST),
      weightCol.map(cName => col(cName).cast(DoubleType)).getOrElse(lit(1.0)).as(EDGE_WEIGHT_COL))

    if (isDirected) {
      weightedEdges
        .groupBy(col(GraphFrame.SRC).as(GraphFrame.ID))
        .agg(sum(EDGE_WEIGHT_COL).as(DEGREE_COL))
    } else {
      weightedEdges
        .select(col(GraphFrame.SRC).as(GraphFrame.ID), col(EDGE_WEIGHT_COL))
        .union(weightedEdges.select(col(GraphFrame.DST).as(GraphFrame.ID), col(EDGE_WEIGHT_COL)))
        .groupBy(GraphFrame.ID)
        .agg(sum(EDGE_WEIGHT_COL).as(DEGREE_COL))
    }
  }

  /**
   * Expression of the rank that a vertex sends along an edge, normalized by its degree. The
   * degree is positive whenever a vertex appears on the sending side of a triplet; the guard
   * protects against zero-weight-only vertices producing NaNs.
   */
  private def messageExpr(isSrc: Boolean): Column = {
    val rank = if (isSrc) Pregel.src(SYBIL_RANK_COL) else Pregel.dst(SYBIL_RANK_COL)
    val degree = if (isSrc) Pregel.src(DEGREE_COL) else Pregel.dst(DEGREE_COL)
    val distributed =
      weightColName match {
        case Some(cName) => rank * (Pregel.edge(cName) / degree)
        case None => rank / degree
      }
    when(degree > lit(0.0), distributed).otherwise(lit(0.0))
  }
}

private object SybilRank {

  /** Name of the output rank column. */
  val SYBIL_RANK_COL = "sybil_rank"

  /** Internal name of the weighted degree vertex column. */
  val DEGREE_COL = "_sybil_degree"

  /** Internal name of the edge weight column used for degree computations. */
  val EDGE_WEIGHT_COL = "_sybil_edge_weight"
}
