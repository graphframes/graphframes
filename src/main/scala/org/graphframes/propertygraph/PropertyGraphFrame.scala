package org.graphframes.propertygraph
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
  vertexPropertyGroups.map(pg => pg.name -> pg).toMap
  edgesPropertyGroups.map(pg => pg.name -> pg).toMap

}
