package com.databricks.dfgraph.lib

import org.apache.spark.graphx.{lib => graphxlib, Graph}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, DoubleType, StructType}

import com.databricks.dfgraph.DFGraph

/**
 * PageRank algorithm implementation. There are two implementations of PageRank implemented.
 *
 * The first implementation uses the standalone [[DFGraph]] interface and runs PageRank
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
object PageRank {
  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * @param graph the graph on which to compute PageRank
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def run(
      graph: DFGraph,
      numIter: Int,
      resetProb: Double = 0.15,
      srcId: Any = null): DFGraph = {
    // TODO(tjh) figure out the srcId issues
    val gx = graphxlib.PageRank.runWithOptions(graph.cachedGraphX, numIter, resetProb, None)
    buildGraph(gx, graph)
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   * @param srcId the source vertex for a Personalized Page Rank (optional)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def runUntilConvergence(
      graph: DFGraph,
      tol: Double,
      resetProb: Double = 0.15, srcId: Any = null): DFGraph = {
    // TODO(tjh) figure out the srcId issues
    val gx = graphxlib.PageRank.runUntilConvergenceWithOptions(graph.cachedGraphX, tol, resetProb, None)
    buildGraph(gx, graph)
  }

  // Performs the graph conversion.
  private def buildGraph(gx: Graph[Double, Double], graph: DFGraph): DFGraph = {
    val fullGx = gx.mapVertices { case (vid, w) =>
      Row(vid, w)
    } .mapEdges( e => Row(e.srcId, e.dstId, e.attr))
    val vStruct = StructType(List(
      graph.vertices.schema(DFGraph.ID),
      field))
    val eStruct = StructType(List(
      graph.edges.schema(DFGraph.SRC),
      graph.edges.schema(DFGraph.DST),
      field))
    DFGraph.fromRowGraphX(fullGx, eStruct, vStruct)

  }

  /**
   * Default name for the weight column.
   */
  val WEIGHT = "weight"
  private val field = StructField(WEIGHT, DoubleType, nullable = false)
}
