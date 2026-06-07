package org.graphframes.propertygraph.internal

import org.graphframes.propertygraph.PropertyGraphFrame

private[propertygraph] final case class SchemaEdge(
    edgeGroupName: String,
    srcVertexGroupName: String,
    dstVertexGroupName: String,
    weight: Double = 1.0)

private[propertygraph] final case class SchemaPath(
    vertices: Vector[String],
    edges: Vector[SchemaEdge]) {
  require(vertices.size == edges.size + 1)
  def length: Int = edges.length
}

private[propertygraph] final case class SchemaGraphSnapshot(
    vertexGroupNames: Set[String],
    edges: Vector[SchemaEdge]) {
  lazy val outgoing: Map[String, Vector[SchemaEdge]] =
    edges.groupBy(_.srcVertexGroupName)

  lazy val incoming: Map[String, Vector[SchemaEdge]] =
    edges.groupBy(_.dstVertexGroupName)
}

private[propertygraph] object SchemaGraphSnapshot {

  def fromPropertyGraphFrame(pgf: PropertyGraphFrame): SchemaGraphSnapshot = {
    val vertexNames =
      pgf.vertexPropertyGroups.map(_.name).toSet

    val edges =
      pgf.edgesPropertyGroups.map { eg =>
        SchemaEdge(
          edgeGroupName = eg.name,
          srcVertexGroupName = eg.srcPropertyGroup.name,
          dstVertexGroupName = eg.dstPropertyGroup.name)
      }.toVector

    SchemaGraphSnapshot(vertexNames, edges)
  }

  def toDOT(snapshot: SchemaGraphSnapshot): String = {
    def q(value: String): String = {
      val escaped = value
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")

      val quote = "\"" // hack
      s"$quote$escaped$quote"
    }

    val sortedVertices = snapshot.vertexGroupNames.toVector.sorted
    val sortedEdges =
      snapshot.edges.sortBy(e => (e.srcVertexGroupName, e.dstVertexGroupName, e.edgeGroupName))

    val vertexLines = sortedVertices.map(v => s"  ${q(v)};")

    val edgeLines = sortedEdges.map { e =>
      s"  ${q(e.srcVertexGroupName)} -> ${q(e.dstVertexGroupName)} [label=${q(e.edgeGroupName)}];"
    }

    (Vector("digraph SchemaGraph {") ++ vertexLines ++ edgeLines ++ Vector("}"))
      .mkString("\n")
  }

  def toString(snapshot: SchemaGraphSnapshot): String = {
    val sortedVertices = snapshot.vertexGroupNames.toVector.sorted
    val sortedEdges =
      snapshot.edges.sortBy(e => (e.srcVertexGroupName, e.dstVertexGroupName, e.edgeGroupName))

    val vertexLines =
      if (sortedVertices.isEmpty) Vector("  (none)")
      else sortedVertices.map(v => s"  - $v")

    val edgeLines =
      if (sortedEdges.isEmpty) Vector("  (none)")
      else
        sortedEdges.map(e =>
          s"  - ${e.edgeGroupName}: ${e.srcVertexGroupName} -> ${e.dstVertexGroupName}")

    (Vector("Property graph schema:", s"Vertex property groups (${sortedVertices.size}):") ++
      vertexLines ++
      Vector(s"Edge property groups (${sortedEdges.size}):") ++
      edgeLines).mkString("\n")
  }
}
