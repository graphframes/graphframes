package com.databricks.dfgraph.lib

import com.databricks.dfgraph.DFGraph
import org.apache.spark.graphx.{Edge, lib => graphxlib}

/** Strongly connected components algorithm implementation. */
object StronglyConnectedComponents {
  /**
   * Compute the strongly connected component (SCC) of each vertex and return a graph with the
   * vertex value containing the lowest vertex id in the SCC containing that vertex.
   *
   * The edges have the same schema as the original edges.
   *
   * The resulting verticexs have the following columns:
   *  - id: the id of the vertex
   *  - weight (double): the normalized weight (page rank) of this vertex.
   *
   * @param graph the graph for which to compute the SCC
   *
   * @return a graph with vertex attributes containing the smallest vertex id in each SCC
   */
  def run(graph: DFGraph, numIters: Int): DFGraph = {
    GraphXConversions.checkVertexId(graph)
    val gx = graphxlib.StronglyConnectedComponents.run(graph.cachedGraphX, numIters)
    GraphXConversions.fromVertexGraphX(gx, graph, COMPONENT_ID)
  }

  private[dfgraph] val COMPONENT_ID = "component"

  class Builder private[dfgraph] (graph: DFGraph) extends Arguments {
    private var numIters: Option[Int] = None

    def setNumIterations(iterations: Int): this.type = {
      numIters = Some(iterations)
      this
    }

    def run(): DFGraph = {
      StronglyConnectedComponents.run(graph, check(numIters, "numIterations"))
    }
  }
}
