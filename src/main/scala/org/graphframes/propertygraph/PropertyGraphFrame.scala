package org.graphframes.propertygraph

import org.apache.spark.sql.Column
import org.graphframes.GraphFrame
import org.graphframes.propertygraph.property.EdgePropertyGroup
import org.graphframes.propertygraph.property.VertexPropertyGroup

/**
 * A high-level abstraction for working with property graphs that simplifies interaction with the
 * GraphFrames library.
 *
 * PropertyGraphFrame serves as a logical structure that manages collections of vertex and edge
 * property groups, providing a user-friendly API for graph operations. It handles various
 * internal complexities such as:
 *   - ID conversion and collision prevention
 *   - Management of directed/undirected graph representations
 *   - Handling of weighted/unweighted edges
 *   - Data consistency across different property groups
 *
 * The class maintains separate collections for vertex and edge properties, allowing for flexible
 * graph construction while ensuring data integrity. Each property (vertex or edge) handles its
 * data internally, while this class provides a simplified interface for working with the
 * underlying GraphFrame structure.
 *
 * Example usage:
 * {{{
 *   val userVertices = VertexPropertyGroup("users", userDF, "userId")
 *   val productVertices = VertexPropertyGroup("products", productDF, "productId")
 *   val purchaseEdges = EdgePropertyGroup("purchases", purchaseDF, "userId", "productId")
 *
 *   val graph = PropertyGraphFrame(
 *     vertexPropertyGroups = Seq(userVertices, productVertices),
 *     edgesPropertyGroups = Seq(purchaseEdges)
 *   )
 * }}}
 *
 * @param vertexPropertyGroups
 *   Sequence of vertex property groups that define the graph's vertices
 * @param edgesPropertyGroups
 *   Sequence of edge property groups that define the graph's edges
 */
case class PropertyGraphFrame(
    vertexPropertyGroups: Seq[VertexPropertyGroup],
    edgesPropertyGroups: Seq[EdgePropertyGroup]) {
  lazy private val vertexGroups: Map[String, VertexPropertyGroup] =
    vertexPropertyGroups.map(pg => pg.name -> pg).toMap
  lazy private val edgeGroups: Map[String, EdgePropertyGroup] =
    edgesPropertyGroups.map(pg => pg.name -> pg).toMap

  /**
   * Converts a heterogeneous property graph into a unified GraphFrame representation.
   *
   * This method transforms a property graph that may contain multiple vertex types and both
   * directed and undirected edges into a single GraphFrame object where all vertices and edges
   * share the same schema. The conversion process handles:
   *
   *   - Internal ID generation and collision prevention by hashing vertex/edge IDs with their
   *     group names
   *   - Merging of different vertex types into a unified vertex DataFrame
   *   - Conversion of directed/undirected edge relationships into a consistent edge DataFrame
   *   - Filtering of vertices and edges based on provided predicates
   *
   * The method allows selecting a subset of property groups and applying filters to control which
   * data is included in the final GraphFrame.
   *
   * @param vertexPropertyGroups
   *   Sequence of vertex property group names to include in the GraphFrame
   * @param edgePropertyGroups
   *   Sequence of edge property group names to include in the GraphFrame
   * @param edgeGroupFilters
   *   Map of edge property group names to filter predicates (Column expressions)
   * @param vertexGroupFilters
   *   Map of vertex property group names to filter predicates (Column expressions)
   * @return
   *   A GraphFrame containing the unified representation of the selected and filtered property
   *   groups
   * @throws IllegalArgumentException
   *   if any specified property group name doesn't exist
   */
  def toGraphFrame(
      vertexPropertyGroups: Seq[String],
      edgePropertyGroups: Seq[String],
      edgeGroupFilters: Map[String, Column],
      vertexGroupFilters: Map[String, Column]): GraphFrame = {
    vertexPropertyGroups.foreach(name =>
      require(vertexGroups.contains(name), s"Vertex property group $name does not exist"))
    edgePropertyGroups.foreach(name =>
      require(edgeGroups.contains(name), s"Edge property group $name does not exist"))

    val vertices = vertexPropertyGroups
      .map(name => vertexGroups(name).getData(vertexGroupFilters(name)))
      .reduce(_ union _)

    val edges = edgePropertyGroups
      .map(name => edgeGroups(name).getData(edgeGroupFilters(name)))
      .reduce(_ union _)

    GraphFrame(vertices, edges)
  }
}
