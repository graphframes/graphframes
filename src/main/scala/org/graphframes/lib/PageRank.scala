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

import org.apache.spark.graphx.{lib => graphxlib}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.graphframes.GraphFrame

// TODO: srcID's type should be checked. The most futureproof check would be : Encoder, because it is compatible with
// Datasets after that.
private object PageRank {
  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * @param graph the graph on which to compute PageRank
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def run(
      graph: GraphFrame,
      numIter: Int,
      resetProb: Double = 0.15,
      srcId: Option[Any] = None): GraphFrame = {
    val longSrcId = srcId.map(integralId(graph, _))
    val gx = graphxlib.PageRank.runWithOptions(graph.cachedTopologyGraphX, numIter, resetProb, longSrcId)
    GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(WEIGHT), edgeNames = Seq(WEIGHT))
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   * @param srcId the source vertex for a Personalized Page Rank (optional)
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def runUntilConvergence(
      graph: GraphFrame,
      tol: Double,
      resetProb: Double = 0.15,
      srcId: Option[Any] = None): GraphFrame = {
    val longSrcId = srcId.map(integralId(graph, _))
    val gx = graphxlib.PageRank.runUntilConvergenceWithOptions(graph.cachedTopologyGraphX, tol, resetProb, longSrcId)
    GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(WEIGHT), edgeNames = Seq(WEIGHT))
  }


  /**
   * Default name for the weight column.
   */
  private val WEIGHT = "weight"

  /**
   * PageRank algorithm implementation. There are two implementations of PageRank implemented.
   *
   * The first implementation uses the standalone [[GraphFrame]] interface and runs PageRank
   * for a fixed number of iterations:
   * {{{
   * var PR = Array.fill(n)( 1.0 )
   * val oldPR = Array.fill(n)( 1.0 )
   * for( iter <- 0 until numIter ) {
   *   swap(oldPR, PR)
   *   for( i <- 0 until n ) {
   *     PR[i] = alpha + (1 - alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
   *   }
   * }
   * }}}
   *
   * The second implementation uses the [[org.apache.spark.graphx.Pregel]] interface and runs PageRank until
   * convergence:
   *
   * {{{
   * var PR = Array.fill(n)( 1.0 )
   * val oldPR = Array.fill(n)( 0.0 )
   * while( max(abs(PR - oldPr)) > tol ) {
   *   swap(oldPR, PR)
   *   for( i <- 0 until n if abs(PR[i] - oldPR[i]) > tol ) {
   *     PR[i] = alpha + (1 - \alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
   *   }
   * }
   * }}}
   *
   * `alpha` is the random reset probability (typically 0.15), `inNbrs[i]` is the set of
   * neighbors whick link to `i` and `outDeg[j]` is the out degree of vertex `j`.
   *
   * Note that this is not the "normalized" PageRank and as a consequence pages that have no
   * inlinks will have a PageRank of alpha.
   *
   * Result graphs:
   *
   * The result edges contain the following columns:
   *  - src: the id of the start vertex
   *  - dst: the id of the end vertex
   *  - weight: (double) the normalized weight of this edge after running PageRank
   * All the other columns are dropped.
   *
   * The result vertices contain the following columns:
   *  - id: the id of the vertex
   *  - weight (double): the normalized weight (page rank) of this vertex.
   * All the other columns are dropped.
   */
  class PageRankBuilder private[graphframes] (
      private val graph: GraphFrame) extends Arguments {

    private var tol: Option[Double] = None
    private var resetProb: Option[Double] = Some(0.15)
    private var numIters: Option[Int] = None
    private var srcId : Option[Any] = None

    def setSourceId[VertexId](srcId : VertexId): this.type = {
      this.srcId = Some(srcId)
      this
    }

    def setResetProbability(p: Double): this.type = {
      resetProb = Some(p)
      this
    }

    def untilConvergence(tolerance: Double): this.type = {
      tol = Some(tolerance)
      this
    }

    def fixedIterations(i: Int): this.type = {
      numIters = Some(i)
      this
    }

    def run(): GraphFrame = {
      tol match {
        case Some(t) =>
          PageRank.runUntilConvergence(graph, t, resetProb.get, srcId)
        case None =>
          PageRank.run(graph, check(numIters, "numIters"), resetProb.get, srcId)
      }
    }
  }


  /**
   * Given a graph and an object, attempts to get the the corresponding integral id in the
   * internal representation.
   *
   * @param graph
   * @param vertexId
   * @return
   */
  private[graphframes] def integralId(graph: GraphFrame, vertexId: Any): Long = {
    // Check if we can directly convert it
    vertexId match {
      case x: Int => return x.toLong
      case x: Long => return x.toLong
      case x: Short => return x.toLong
      case x: Byte => return x.toLong
      case _ =>
    }
    // It is a non-integral type such as a string, we need to use the translation table.
    // Check that the type is compatible.
    val tpe = graph.vertices.schema(GraphFrame.ID)
    vertexId match {
      case _: String => require(tpe.dataType == StringType, s"Type should be string, it is ${tpe.dataType}")
      case x => throw new Exception(s"Unknown type ${x.getClass}. The only accepted types are the raw SQL types")
    }
    val longIdRow = graph.indexedVertices
      .select(graph.vertices(GraphFrame.ID) === vertexId)
      .select(GraphFrame.LONG_ID)
    // TODO(tjh): could do more informative message
    val Seq(Row(long_id: Long)) = longIdRow.collect().toSeq
    long_id
  }
}
