package com.databricks.dfgraph.lib

import org.apache.spark.graphx.{lib => graphxlib}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import com.databricks.dfgraph.DFGraph

/**
 * Compute the number of triangles passing through each vertex.
 *
 * The algorithm is relatively straightforward and can be computed in three steps:
 *
 * <ul>
 * <li>Compute the set of neighbors for each vertex
 * <li>For each edge compute the intersection of the sets and send the count to both vertices.
 * <li> Compute the sum at each vertex and divide by two since each triangle is counted twice.
 * </ul>
 *
 * Note that the input graph should have its edges in canonical direction
 * (i.e. the `sourceId` less than `destId`).
 */
// TODO(tjh) graphX requires the graph to be partitioned.
object TriangleCount {

  /**
   * Computes the number of triangles passing through each vertex.
   *
   * The edges of the resulting graph have the same schema as the original edges.
   *
   * The vertices of the resulting graph have the following schema:
   *  - id: the id of the vertex
   *  - count (int): the count of triangles
   *
   * All other columns are dropped from the vertices.
   *
   * @param graph
   * @return
   */
  def run(graph: DFGraph): DFGraph = {
    GraphXConversions.checkVertexId(graph)
    val gx = graphxlib.TriangleCount.run(graph.cachedGraphX)
      .mapVertices { case (vid, vlabel) =>
        Row(vid, vlabel)
      }

    val s = graph.vertices.schema(DFGraph.ID)
    val vStruct = StructType(List(
      s,
      StructField(COUNT_ID, IntegerType, nullable = false)))
    GraphXConversions.fromRowGraphX(gx, graph.edges.schema, vStruct)
  }

  private val COUNT_ID = "count"
}
