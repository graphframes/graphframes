package com.databricks.dfgraph.lib

import org.apache.spark.graphx.{lib => graphxlib}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

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
   * @param g the graph for which to compute the connected components
   *
   * @return a graph with vertex attributes containing the smallest vertex in each
   *         connected component
   */
  def run(g: DFGraph): DFGraph = {
    val gx = graphxlib.ConnectedComponents.run(g.cachedGraphX).mapVertices { case (vid, componentId) =>
      Row(vid, componentId)
    }
    val s = g.vertices.schema(DFGraph.ID)
    val vStruct = StructType(List(s, s.copy(name = COMPONENT_ID)))
    DFGraph.fromRowGraphX(gx, g.edges.schema, vStruct)
  }

  val COMPONENT_ID = "component"
}
