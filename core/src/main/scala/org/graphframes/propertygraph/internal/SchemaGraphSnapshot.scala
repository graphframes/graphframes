package org.graphframes.propertygraph.internal

import org.graphframes.propertygraph.PropertyGraphFrame

/**
 * A directed edge in the schema graph, representing the pure topological relationship between two
 * vertex property groups via an edge property group.
 *
 * @param edgeGroupName
 *   name of the edge property group this schema edge corresponds to.
 * @param srcVertexGroupName
 *   name of the source vertex property group.
 * @param dstVertexGroupName
 *   name of the destination vertex property group.
 * @param isDirected
 *   is edge directed
 */
private[propertygraph] final case class SchemaEdge(
    edgeGroupName: String,
    srcVertexGroupName: String,
    dstVertexGroupName: String,
    isDirected: Boolean)

/**
 * One resolved vertex slot in a schema path.
 *
 * @param vertexGroupName
 *   concrete vertex property group this slot was resolved to during schema-graph enumeration.
 * @param variable
 *   the user-written pattern binding (e.g. `a`, `x`), or `None` for an anonymous `()` node.
 * @param scanFilter
 *   scan-local WHERE predicates (AST) that reference only this node's variable; lowered to a
 *   Spark `Column` at execution time by the (deferred) executor.
 */
private[propertygraph] final case class PathNode(
    vertexGroupName: String,
    variable: Option[String],
    scanFilter: Seq[Expression])

/**
 * One resolved edge hop in a schema path.
 *
 * @param edge
 *   the resolved edge group (pure topology).
 * @param traversedForward
 *   `true` when the pattern arrow agrees with the edge's `src -> dst` direction, `false` when the
 *   pattern arrow (`<-[e]-`) opposes it. Forward step joins `fromNode.id == edge.src` and
 *   `toNode.id == edge.dst`; backward step swaps src/dst. See design §7.
 * @param variable
 *   the edge binding, if any (`-[e:KNOWS]->`).
 */
private[propertygraph] final case class PathStep(
    edge: SchemaEdge,
    traversedForward: Boolean,
    variable: Option[String],
    scanFilter: Seq[Expression])

/**
 * A fully-resolved, concrete path through the schema graph: a linear chain of
 * `PathNode`-`PathStep`-`PathNode`-... produced by enumerating the schema graph against a user
 * pattern. Untyped pattern elements fan out into multiple `SchemaPath`s (one per candidate edge
 * group); a pattern that is disconnected in the schema graph yields no paths.
 */
private[propertygraph] final case class SchemaPath(
    nodes: Vector[PathNode],
    steps: Vector[PathStep]) {
  require(nodes.size == steps.size + 1)
  def length: Int = steps.length

  override def toString: String = {
    val sb = new StringBuilder("SchemaPath(")
    for (i <- nodes.indices) {
      if (i > 0) {
        val step = steps(i - 1)
        val arrow = if (step.traversedForward) "->" else "<-"
        val edgeLabel = step.variable match {
          case Some(v) => s"[$v:${step.edge.edgeGroupName}]"
          case None => s"[${step.edge.edgeGroupName}]"
        }
        sb.append(s"$arrow$edgeLabel$arrow")
      }
      val node = nodes(i)
      val nodeLabel = node.variable match {
        case Some(v) => s"($v:${node.vertexGroupName})"
        case None => s"(${node.vertexGroupName})"
      }
      sb.append(nodeLabel)
    }
    sb.append(")").toString()
  }
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
          dstVertexGroupName = eg.dstPropertyGroup.name,
          isDirected = eg.isDirected)
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
