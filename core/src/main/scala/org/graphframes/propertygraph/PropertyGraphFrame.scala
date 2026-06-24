package org.graphframes.propertygraph

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.graphframes.GraphFrame
import org.graphframes.propertygraph.internal.AstBuilder
import org.graphframes.propertygraph.internal.GqlExplain
import org.graphframes.propertygraph.internal.JoinOptimizer
import org.graphframes.propertygraph.internal.QueryExecutor
import org.graphframes.propertygraph.internal.ResolvedQuery
import org.graphframes.propertygraph.internal.Resolver
import org.graphframes.propertygraph.internal.SchemaGraphSnapshot
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
 * @param vertexPropertyGroups
 *   Sequence of vertex property groups that define the graph's vertices
 * @param edgesPropertyGroups
 *   Sequence of edge property groups that define the graph's edges
 */
case class PropertyGraphFrame(
    vertexPropertyGroups: Seq[VertexPropertyGroup],
    edgesPropertyGroups: Seq[EdgePropertyGroup]) {
  import PropertyGraphFrame._

  // Keys are lowercased so that lookups in toGraphFrame and projectionBy are case-insensitive.
  // It is an overall policy across all the LPG functionality.
  lazy private[propertygraph] val vertexGroups: Map[String, VertexPropertyGroup] =
    vertexPropertyGroups.map(pg => pg.name.toLowerCase -> pg).toMap
  lazy private[propertygraph] val edgeGroups: Map[String, EdgePropertyGroup] =
    edgesPropertyGroups.map(pg => pg.name.toLowerCase -> pg).toMap

  lazy private[propertygraph] val schemaGraphSnapshot: SchemaGraphSnapshot =
    SchemaGraphSnapshot.fromPropertyGraphFrame(this)

  /**
   * Returns a human-readable description of the property graph schema.
   *
   * The output lists all vertex property groups and edge property groups with their
   * source/destination connections, sorted alphabetically for determinism.
   *
   * @return
   *   a multi-line string describing the graph schema
   */
  def schemaString: String = SchemaGraphSnapshot.toString(schemaGraphSnapshot)

  /**
   * Returns the property graph schema in DOT (Graphviz) format.
   *
   * The output is a valid `digraph` that can be rendered by Graphviz tools. Vertex property
   * groups appear as nodes and edge property groups appear as directed edges labeled with the
   * group name.
   *
   * @return
   *   a DOT-format string representing the graph schema
   */
  def schemaStringDOT: String = SchemaGraphSnapshot.toDOT(schemaGraphSnapshot)

  /**
   * Executes a GQL `MATCH` query against this property graph and returns the matched paths as a
   * Spark DataFrame with a fixed output schema:
   *   - `start_id`, `start_property_group`, `end_id`, `end_property_group`,
   *     `edge_property_group`, and a
   *     `path: array<struct<edge_property_group, node_id, node_property_group>>` column for
   *     intermediate hops.
   *
   * This is a convenience overload equivalent to `query(gql, QueryOptions())`.
   *
   * @param gql
   *   a GQL `MATCH` statement in the supported subset.
   * @return
   *   a DataFrame over the fixed output schema.
   */
  def query(gql: String): DataFrame = query(gql, QueryOptions())

  /**
   * Executes a GQL `MATCH` query against this property graph and returns the matched paths as a
   * Spark DataFrame with a fixed output schema:
   *   - `start_id`, `start_property_group`, `end_id`, `end_property_group`,
   *     `edge_property_group`, and a
   *     `path: array<struct<edge_property_group, node_id, node_property_group>>` column for
   *     intermediate hops.
   *
   * The query is compiled through: ANTLR parse -> AST -> schema resolution -> join planning ->
   * DataFrame execution (per-path `UNION ALL`). Disconnected patterns (no schema path matches)
   * return an empty DataFrame without touching data. Bad syntax throws
   * [[org.graphframes.InvalidParseException]]; unknown labels throw
   * [[org.graphframes.InvalidPropertyGroupException]].
   *
   * @param gql
   *   a GQL `MATCH` statement in the supported subset.
   * @param options
   *   query options
   * @return
   *   a DataFrame over the fixed output schema.
   */
  def query(gql: String, options: QueryOptions): DataFrame = {
    val resolved =
      resolve(gql, options, enforceMaxSchemaPathLength = true, enforceMaxPathsCount = true)
    if (resolved.paths.isEmpty) {
      return QueryExecutor.execute(this, Seq.empty)
    }

    // Cost-based optimization and statistics will follow
    val _ = options.enableStatistics
    val plans = JoinOptimizer.plan(resolved, stats = None)
    QueryExecutor.execute(this, plans)
  }

  /**
   * Renders the logical (resolved) plan of `gql` without executing it.
   *
   * This is a convenience overload equivalent to `explain(gql, ExplainMode.Logical)`. To see the
   * per-path join plans (order + statistics basis).
   *
   * @param gql
   *   a GQL `MATCH` statement in the supported subset.
   * @return
   *   a string describing the resolved (logical) plan.
   */
  def explain(gql: String): String = explain(gql, ExplainMode.Logical)

  /**
   * Renders a plan of `gql` without executing it, using default query options.
   *
   * This is a convenience overload equivalent to `explain(gql, mode, QueryOptions())`. Pass
   * [[ExplainMode.Physical]] to see the per-path join plans (order + statistics basis);
   * [[ExplainMode.Logical]] shows the resolved (logical) plan.
   *
   * @param gql
   *   a GQL `MATCH` statement in the supported subset.
   * @param mode
   *   the explain mode: [[ExplainMode.Logical]] for the resolved plan or [[ExplainMode.Physical]]
   *   for the per-path join plans.
   * @return
   *   a string describing the requested plan.
   */
  def explain(gql: String, mode: ExplainMode): String = explain(gql, mode, QueryOptions())

  /**
   * Renders a plan of `gql` without executing it.
   *
   * Pass [[ExplainMode.Physical]] to see the per-path join plans (order + statistics basis);
   * [[ExplainMode.Logical]] shows the resolved (logical) plan.
   *
   * @param gql
   *   a GQL `MATCH` statement in the supported subset.
   * @param mode
   *   the explain mode: [[ExplainMode.Logical]] for the resolved plan or [[ExplainMode.Physical]]
   *   for the per-path join plans.
   * @param options
   *   query options.
   * @return
   *   a string describing the requested plan.
   */
  def explain(gql: String, mode: ExplainMode, options: QueryOptions): String = {
    // users should be able to see paths that exceed maxSchemaPathLength
    // even if it is not allowed for real queries.
    val resolved =
      resolve(gql, options, enforceMaxSchemaPathLength = false, enforceMaxPathsCount = false)
    mode match {
      case ExplainMode.Logical => GqlExplain.logical(resolved)
      case ExplainMode.Physical =>
        val plans = JoinOptimizer.plan(resolved, stats = None)
        GqlExplain.physical(plans)
    }
  }

  /**
   * Shared parse + resolve step for [[query]] and [[explain]]. Applies `maxSchemaPathLength` as a
   * guard against pathological enumeration depth when [[enforceMaxSchemaPathLength]] is true (the
   * [[query]] path). The [[explain]] path sets it to false so users can inspect the plan that
   * exceeds the cap and understand why [[query]] rejects it.
   */
  private def resolve(
      gql: String,
      options: QueryOptions,
      enforceMaxSchemaPathLength: Boolean,
      enforceMaxPathsCount: Boolean): ResolvedQuery = {
    require(
      options.maxSchemaPathLength > 0,
      s"maxSchemaPathLength must be positive, got ${options.maxSchemaPathLength}")
    val ast = AstBuilder.parse(gql)
    val resolved = Resolver.resolve(ast, schemaGraphSnapshot, options)
    if (enforceMaxSchemaPathLength) {
      resolved.paths.foreach { path =>
        require(
          path.length <= options.maxSchemaPathLength,
          s"Schema path length ${path.length} exceeds maxSchemaPathLength=${options.maxSchemaPathLength}: " +
            s"$path; try to rewrite the query and reduce a potential depth. Use `explain` to see the plan.")
      }
    }
    if (enforceMaxPathsCount) {
      require(
        resolved.paths.size <= options.maxEnumeratedPaths,
        s"An amount of paths in the resolved query exceeds ${options.maxEnumeratedPaths}: " + "either use `explain` and modify the pattern or increase the value in `QueryOptions`")
    }
    resolved
  }

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
   */
  def toGraphFrame(
      vertexPropertyGroups: Seq[String],
      edgePropertyGroups: Seq[String],
      edgeGroupFilters: Map[String, Column],
      vertexGroupFilters: Map[String, Column]): GraphFrame = {
    vertexPropertyGroups.foreach(name =>
      require(
        vertexGroups.contains(name.toLowerCase),
        s"Vertex property group $name does not exist"))
    edgePropertyGroups.foreach(name =>
      require(edgeGroups.contains(name.toLowerCase), s"Edge property group $name does not exist"))

    val vertices = vertexPropertyGroups
      .map(name => vertexGroups(name.toLowerCase).getData(vertexGroupFilters(name)))
      .reduce(_ union _)

    val edges = edgePropertyGroups
      .map(name => edgeGroups(name.toLowerCase).getData(edgeGroupFilters(name)))
      .reduce(_ union _)

    GraphFrame(vertices, edges)
  }

  /**
   * Projects a bipartite graph onto one of its parts, creating edges between vertices that share
   * neighbors in the other part. Drops the property group used for projection through and returns
   * a new property graph.
   *
   * @param leftBiGraphPart
   *   Name of the vertex property group to project onto
   * @param rightBiGraphPart
   *   Name of the vertex property group to project through
   * @param edgeGroup
   *   Name of the edge property group connecting the two parts
   * @param newEdgeWeight
   *   Optional function that takes two weight columns (Column objects) of edges as input and
   *   returns a new weight column. If None, a default weight of 1.0 is used for all projected
   *   edges.
   * @return
   *   A new PropertyGraphFrame containing the projected graph
   */
  def projectionBy(
      leftBiGraphPart: String,
      rightBiGraphPart: String,
      edgeGroup: String,
      newEdgeWeight: Option[(Column, Column) => Column] = None): PropertyGraphFrame = {
    // Hoisted before the require checks so the lowercased lookup is performed only once.
    val oldGroup = edgeGroups(edgeGroup.toLowerCase)
    require(
      oldGroup.srcPropertyGroup.name.equalsIgnoreCase(leftBiGraphPart),
      s"Edge Property Group should have $leftBiGraphPart source group but has ${oldGroup.srcPropertyGroup.name}")
    require(
      oldGroup.dstPropertyGroup.name.equalsIgnoreCase(rightBiGraphPart),
      s"Edge Property Group should have $rightBiGraphPart destination group but has ${oldGroup.dstPropertyGroup.name}")
    val keptVPropertyGroups =
      vertexPropertyGroups.filterNot(g => g.name.equalsIgnoreCase(rightBiGraphPart))
    val keptEPropertyGroups =
      edgesPropertyGroups.filterNot(g => g.name.equalsIgnoreCase(edgeGroup))
    val oldEdgesData = oldGroup.data

    // Create new edges by joining vertices through their common neighbors
    val projectedEdges = oldEdgesData
      .as("e1")
      .join(oldEdgesData.as("e2"), col("e1.dst") === col("e2.dst"))
      .where("e1.src < e2.src")
      .select(
        col("e1.src").alias(GraphFrame.SRC),
        col("e2.src").alias(GraphFrame.DST),
        newEdgeWeight match {
          case Some(newEdgeFunc) =>
            newEdgeFunc(
              col(s"e1.${oldGroup.weightColumnName}"),
              col(s"e2.${oldGroup.weightColumnName}")).alias(GraphFrame.WEIGHT)
          case None => lit(1.0).alias(GraphFrame.WEIGHT)
        })

    val newEdgeGroup = EdgePropertyGroup(
      name = s"projected_$edgeGroup",
      data = projectedEdges,
      srcPropertyGroup = vertexGroups(leftBiGraphPart.toLowerCase),
      dstPropertyGroup = vertexGroups(leftBiGraphPart.toLowerCase),
      isDirected = false,
      srcColumnName = GraphFrame.SRC,
      dstColumnName = GraphFrame.DST,
      weightColumnName = GraphFrame.WEIGHT)

    PropertyGraphFrame(keptVPropertyGroups, keptEPropertyGroups :+ newEdgeGroup)
  }

  /**
   * Joins the vertices data with the specified vertex property groups to produce a unified
   * DataFrame. Each vertex property group defines how the data should be structured and filtered.
   *
   * @param verticesData
   *   The DataFrame containing the vertices data to join. It must include vertex properties and
   *   the group identifiers to filter and map. It is expected to be an output of calling graph
   *   algorithms on GraphFrame, made by the method toGraphFrame.
   * @param vertexGroups
   *   A sequence of vertex group names that are to be joined. Each name must correspond to a
   *   valid vertex property group defined in the PropertyGraphFrame.
   * @return
   *   A DataFrame representing the unified vertices data where each group has been appropriately
   *   filtered, joined, and processed based on its configuration.
   */
  def joinVertices(verticesData: DataFrame, vertexGroups: Seq[String]): DataFrame = {
    require(vertexGroups.forall(this.vertexGroups.contains))
    vertexGroups
      .map { (vg: String) =>
        {
          val associatedGroup = this.vertexGroups(vg)
          val filteredForGroup = verticesData.filter(col(PROPERTY_GROUP_COL_NAME) === lit(vg))
          if (associatedGroup.applyMaskOnId) {
            associatedGroup.internalIdMapping
              .join(filteredForGroup, Seq(GraphFrame.ID), "left")
              .drop(GraphFrame.ID)
          } else {
            associatedGroup
              .getData()
              .join(filteredForGroup, GraphFrame.ID, "left")
              .withColumnRenamed(GraphFrame.ID, EXTERNAL_ID)
          }
        }
      }
      .reduce(_ union _)
  }
}

object PropertyGraphFrame {

  /**
   * A constant representing the column name used for property grouping. It is used within the
   * context of a property graph structure to manage or identify property group associations.
   */
  val PROPERTY_GROUP_COL_NAME = "property_group"

  /**
   * A constant representing the column name used for external identifiers. It serves as a key to
   * associate external data or entities within the context of a property graph structure.
   */
  val EXTERNAL_ID = "external_id"
}
