package com.databricks.dfgraph.lib

import org.apache.spark.graphx.{lib => graphxlib}

import com.databricks.dfgraph.DFGraph

/**
 * Connected components algorithm.
 */
object ConnectedComponents {

  /**
   * Compute the connected component membership of each vertex and return a graph with the vertex
   * value containing the lowest vertex id in the connected component containing that vertex.
   *
   * The resulting edges are the same as the original edges.
   *
   * The resulting vertices have two columns:
   *  - id: the id of the vertex
   *  - component: (same type as vertex id) the id of a vertex in the connected component, used as a unique identifier
   *    for this component.
   * All the other columns from the vertices are dropped.
   *
   * @param graph the graph for which to compute the connected components
   *
   * @return a graph with vertex attributes containing the smallest vertex in each
   *         connected component
   */
  def run(graph: DFGraph): DFGraph = {
    val gx = graphxlib.ConnectedComponents.run(graph.cachedTopologyGraphX)
    GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(COMPONENT_ID))
  }

  private val COMPONENT_ID = "component"

  class Builder private[dfgraph] (graph: DFGraph) extends Arguments {

    def run(): DFGraph = {
      ConnectedComponents.run(graph)
    }
  }

}
