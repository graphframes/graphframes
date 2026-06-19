/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.graphframes.propertygraph.internal

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.graphframes.GraphFrame
import org.graphframes.propertygraph.PropertyGraphFrame

/**
 * Executes a sequence of [[JoinPlan]]s against a [[PropertyGraphFrame]] and returns a single
 * result DataFrame.
 *
 * Per plan: build a join tree following `plan.order` (each element scanned once and joined to the
 * growing frame on the masked id columns), apply join/post predicates, project to the output
 * schema. Across plans: `UNION ALL`. Reuses `PropertyGroup.getData(filter, requestedProperties)`,
 * which pushes the scan-local filter, applies id masking, and projects standardized columns --
 * edge `src`/`dst` are masked the same way as vertex `id`, so joins line up with no manual
 * casting.
 */
private[propertygraph] object QueryExecutor {

  // -------------------------------------------------------------------------
  // Output column-name constants. Package-private so test suites can reference
  // them when asserting against the query result schema/rows instead of
  // repeating the raw string literals.
  // -------------------------------------------------------------------------
  private[propertygraph] val EDGE_PROPERTY_GROUP: String = "edge_property_group"
  private[propertygraph] val NODE_ID: String = "node_id"
  private[propertygraph] val NODE_PROPERTY_GROUP: String = "node_property_group"
  private[propertygraph] val START_ID: String = "start_id"
  private[propertygraph] val START_PROPERTY_GROUP: String = "start_property_group"
  private[propertygraph] val END_ID: String = "end_id"
  private[propertygraph] val END_PROPERTY_GROUP: String = "end_property_group"
  private[propertygraph] val PATH: String = "path"

  /**
   * Execute all plans and `UNION ALL` them.
   *
   * Empty plans (disconnected pattern) -> an empty DataFrame with the fixed output schema (see
   * [[outputSchema]]). Empty result of a single plan is also a valid empty DataFrame.
   */
  def execute(pgf: PropertyGraphFrame, plans: Seq[JoinPlan]): DataFrame = {
    if (plans.isEmpty) {
      val spark = pgf.vertexPropertyGroups.headOption
        .map(_.data.sparkSession)
        .orElse(pgf.edgesPropertyGroups.headOption.map(_.data.sparkSession))
        .orElse(SparkSession.getActiveSession)
        .getOrElse(
          throw new IllegalStateException(
            "No active SparkSession and no property groups to derive one from; " +
              "PropertyGraphFrame.query must be called with an active session"))
      return spark.createDataFrame(
        spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
        outputSchema)
    }
    val perPlan = plans.map(executePlan(pgf, _))
    perPlan.reduce(_ unionByName _)
  }

  // -------------------------------------------------------------------------
  // Per-plan execution.
  // -------------------------------------------------------------------------

  private def executePlan(pgf: PropertyGraphFrame, plan: JoinPlan): DataFrame = {
    val path = plan.path
    val env = PrefixEnv(path)

    // Gather, per element, the property names that must be carried through the scan so that
    // WHERE/RETURN expressions referencing them can be lowered. (Join/post predicates, scan filters,
    // and RETURN items are all sources.)
    val requiredNodeProps: Map[Int, Set[String]] =
      collectRequiredProperties(path, plan, node = true)
    val requiredEdgeProps: Map[Int, Set[String]] =
      collectRequiredProperties(path, plan, node = false)

    // Scan each element once and alias its columns under the element's prefix.
    val nodeFrames: Map[Int, DataFrame] = path.nodes.indices.map { i =>
      i -> scanNode(pgf, path, i, env, requiredNodeProps.getOrElse(i, Set.empty))
    }.toMap
    val edgeFrames: Map[Int, DataFrame] = path.steps.indices.map { i =>
      i -> scanEdge(pgf, path, i, env, requiredEdgeProps.getOrElse(i, Set.empty))
    }.toMap

    // Walk the join order, joining each element onto the growing frame.
    var frame: DataFrame = null
    plan.order.foreach {
      case NodeRef(i) =>
        val f = nodeFrames(i)
        frame = if (frame == null) f else joinElement(frame, f, path, env)
      case EdgeRef(i) =>
        val f = edgeFrames(i)
        frame = if (frame == null) f else joinElement(frame, f, path, env)
    }
    if (frame == null) {
      // Degenerate: a path with a single node and no edges (MATCH (a:Person)).
      // Synthesize a trivially-valid frame from that node scan; nodeFrames is non-empty here.
      frame = nodeFrames(0)
    }

    // Apply WHERE predicates that span multiple elements.
    // P.S. These are only filters that can be applied after joins.
    // Everything else is pushed to the before join.
    val joinConds = plan.joinPredicates.map(expr => ExpressionLowering.lower(expr, env))
    if (joinConds.nonEmpty) frame = frame.where(joinConds.reduce(_ && _))
    plan.postFilters.foreach { expr =>
      frame = frame.filter(ExpressionLowering.lower(expr, env))
    }

    project(frame, plan)
  }

  // -------------------------------------------------------------------------
  // Scans + aliasing.
  // -------------------------------------------------------------------------

  private def scanNode(
      pgf: PropertyGraphFrame,
      path: SchemaPath,
      i: Int,
      env: PrefixEnv,
      requiredProps: Set[String]): DataFrame = {
    val node = path.nodes(i)
    val group = pgf.vertexGroups(node.vertexGroupName.toLowerCase)
    // Scan-local filters reference only this node's variable and are applied by `getData` against
    // the RAW group columns (before aliasing), so they are lowered with the empty (raw) prefix.
    val filterCol = lowerScanFilter(node.scanFilter)
    val scanned = group.getData(filterCol, requiredProps.toSeq)
    val prefix = env.nodePrefix(i)
    renameAll(scanned, prefix)
  }

  private def scanEdge(
      pgf: PropertyGraphFrame,
      path: SchemaPath,
      i: Int,
      env: PrefixEnv,
      requiredProps: Set[String]): DataFrame = {
    val step = path.steps(i)
    val group = pgf.edgeGroups(step.edge.edgeGroupName.toLowerCase)
    val scanned = group.getData(lit(true), requiredProps.toSeq)
    val prefix = env.edgePrefix(i)
    renameAll(scanned, prefix)
  }

  /**
   * Lower the scan-local predicates of a node into a single ANDed Column (lit(true) if none).
   * These reference the node's variable, but the resulting Column is applied by `getData` against
   * the RAW group columns (before aliasing), so the variable resolves to the empty prefix (raw
   * column names): e.g. `a.age > 30` -> `col("age") > 30`.
   */
  private def lowerScanFilter(filters: Seq[Expression]): Column =
    if (filters.isEmpty) lit(true)
    else filters.map(ExpressionLowering.lower(_, PrefixEnv.raw)).reduce(_ && _)

  /** Prefix every column of `df` with `prefix_` in place. */
  private def renameAll(df: DataFrame, prefix: String): DataFrame = {
    val mapping = df.columns.map(c => c -> s"${prefix}_$c").toMap
    val out = df.withColumnsRenamed(mapping)
    out
  }

  // -------------------------------------------------------------------------
  // Join tree.
  // -------------------------------------------------------------------------

  /**
   * Join `incoming` onto `frame`. The join condition is the conjunction of all adjacency edges
   * between the just-added element and already-present neighbors.
   *
   * For a step `k` connecting `node_k` and `node_{k+1}` through `edge_k`:
   *   - forward (`traversedForward = true`): `edge_k.src == node_k.id` and
   *     `edge_k.dst == node_{k+1}.id`;
   *   - backward: `edge_k.dst == node_k.id` and `edge_k.src == node_{k+1}.id` (src/dst swapped).
   */
  private def joinElement(
      frame: DataFrame,
      incoming: DataFrame,
      path: SchemaPath,
      env: PrefixEnv): DataFrame = {
    val presentCols = frame.columns.toSet
    val conditions = adjacencyConditions(incoming.columns.toSet, presentCols, path, env)
    require(
      conditions.nonEmpty,
      "Join order produced an element with no adjacency to the already-joined frame; " +
        "this indicates an invalid join order" + s" for path ${path.toString()}")
    frame.join(incoming, conditions.reduce(_ && _), "inner")
  }

  /**
   * Build the equi-join conditions between the newly-joined element's columns and the frame's
   * columns. An edge `k` is adjacent to its two endpoint nodes (`k`, `k+1`); a node `j` is
   * adjacent to the edges touching it (`j-1` and `j`).
   */
  private def adjacencyConditions(
      incoming: Set[String],
      present: Set[String],
      path: SchemaPath,
      env: PrefixEnv): Seq[Column] = {
    val conds = Seq.newBuilder[Column]

    def bothPresent(a: String, b: String): Option[Column] =
      if (incoming.contains(a) && present.contains(b)) Some(col(a) === col(b))
      else if (incoming.contains(b) && present.contains(a)) Some(col(b) === col(a))
      else None

    // Edge k <-> node k  and  edge k <-> node k+1.
    path.steps.indices.foreach { k =>
      val eSrc = env.edgeCol(k, GraphFrame.SRC)
      val eDst = env.edgeCol(k, GraphFrame.DST)
      val nKId = env.nodeCol(k, GraphFrame.ID)
      val nK1Id = env.nodeCol(k + 1, GraphFrame.ID)
      val forward = path.steps(k).traversedForward
      // forward: edge.src == node_k.id , edge.dst == node_{k+1}.id
      // backward: edge.dst == node_k.id , edge.src == node_{k+1}.id
      val (srcNode, dstNode) = if (forward) (nKId, nK1Id) else (nK1Id, nKId)
      bothPresent(eSrc, srcNode).foreach(conds += _)
      bothPresent(eDst, dstNode).foreach(conds += _)
    }
    conds.result()
  }

  /** The fixed output schema, regardless of pattern length or RETURN shape. */
  private[propertygraph] def outputSchema: StructType = {
    val pathElement = StructType(
      Seq(
        StructField(EDGE_PROPERTY_GROUP, StringType, nullable = true),
        StructField(NODE_ID, StringType, nullable = true),
        StructField(NODE_PROPERTY_GROUP, StringType, nullable = true)))
    StructType(
      Seq(
        StructField(START_ID, StringType, nullable = true),
        StructField(START_PROPERTY_GROUP, StringType, nullable = true),
        StructField(END_ID, StringType, nullable = true),
        StructField(END_PROPERTY_GROUP, StringType, nullable = true),
        StructField(EDGE_PROPERTY_GROUP, StringType, nullable = true),
        StructField(PATH, ArrayType(pathElement), nullable = true)))
  }

  private def project(frame: DataFrame, plan: JoinPlan): DataFrame = {
    val path = plan.path
    val env = PrefixEnv(path)
    plan.projection match {
      case Projection.Items(items) =>
        val cols = items.map { item =>
          val c = ExpressionLowering.lower(item.expression, env)
          item.alias match {
            case Some(a) => c.alias(a)
            case None =>
              item.expression match {
                case Variable(v) => c.alias(v)
                case PropertyAccess(_, p) => c.alias(p)
                case _ => c
              }
          }
        }
        frame.select(cols: _*)

      case Projection.Default | Projection.Star =>
        val namedIndices = path.nodes.indices.filter(i => path.nodes(i).variable.isDefined)
        require(
          namedIndices.nonEmpty,
          "RETURN (default/*) requires at least one named node variable in the pattern;" + s"path: ${plan.toString()}")
        val startIdx = namedIndices.head
        val endIdx = namedIndices.last

        val startId = col(env.nodeCol(startIdx, GraphFrame.ID))
        val startPg = lit(path.nodes(startIdx).vertexGroupName)
        val endId = col(env.nodeCol(endIdx, GraphFrame.ID))
        val endPg = lit(path.nodes(endIdx).vertexGroupName)

        if (path.steps.isEmpty) {
          // 0-hop: a single node. No edge, no path array.
          // edge_property_group is null; path is an empty array.
          frame.select(
            startId.alias(START_ID),
            startPg.alias(START_PROPERTY_GROUP),
            endId.alias(END_ID),
            endPg.alias(END_PROPERTY_GROUP),
            lit(null).cast(StringType).alias(EDGE_PROPERTY_GROUP),
            array().cast(outputSchema(PATH).dataType).alias(PATH))
        } else {
          val firstEdgeGroup =
            lit(path.steps.head.edge.edgeGroupName).alias(EDGE_PROPERTY_GROUP)
          // Intermediate hops: for step i (i in 1..k-1) we emit the edge group of step i and the
          // intermediate node i. The final step k carries its edge group but null node fields (the
          // end node is already in end_id).
          val pathStructs = path.steps.indices.flatMap { i =>
            val edgeGroup = lit(path.steps(i).edge.edgeGroupName)
            if (i == path.steps.length - 1) {
              // Last step: edge group only, null node fields.
              Seq(
                struct(
                  edgeGroup.alias(EDGE_PROPERTY_GROUP),
                  lit(null).cast(StringType).alias(NODE_ID),
                  lit(null).cast(StringType).alias(NODE_PROPERTY_GROUP)))
            } else {
              Seq(
                struct(
                  edgeGroup.alias(EDGE_PROPERTY_GROUP),
                  col(env.nodeCol(i + 1, GraphFrame.ID)).alias(NODE_ID),
                  lit(path.nodes(i + 1).vertexGroupName).alias(NODE_PROPERTY_GROUP)))
            }
          }
          val pathCol =
            if (pathStructs.isEmpty) array().cast(outputSchema(PATH).dataType)
            else array(pathStructs: _*)
          frame.select(
            startId.alias(START_ID),
            startPg.alias(START_PROPERTY_GROUP),
            endId.alias(END_ID),
            endPg.alias(END_PROPERTY_GROUP),
            firstEdgeGroup,
            pathCol.alias(PATH))
        }
    }
  }

  // -------------------------------------------------------------------------
  // Required-property collection.
  // -------------------------------------------------------------------------

  /**
   * Collect, for every element of the requested kind, the set of property names referenced in
   * WHERE / RETURN expressions that must be carried through the scan.
   */
  private def collectRequiredProperties(
      path: SchemaPath,
      plan: JoinPlan,
      node: Boolean): Map[Int, Set[String]] = {
    val nodeVarIndex: Map[String, Int] = path.nodes.zipWithIndex.collect {
      case (n, i) if n.variable.isDefined => n.variable.get -> i
    }.toMap
    val edgeVarIndex: Map[String, Int] = path.steps.zipWithIndex.collect {
      case (s, i) if s.variable.isDefined => s.variable.get -> i
    }.toMap

    // All expressions that could reference a property: scan-local filters, join/post predicates,
    // and (for Items projections) the RETURN items.
    val allExprs: Seq[Expression] = plan.projection match {
      case Projection.Items(items) =>
        path.nodes.flatMap(_.scanFilter) ++ plan.joinPredicates ++ plan.postFilters ++
          items.map(_.expression)
      case _ =>
        path.nodes.flatMap(_.scanFilter) ++ plan.joinPredicates ++ plan.postFilters
    }

    val varIndex = if (node) nodeVarIndex else edgeVarIndex
    collectProps(allExprs, varIndex)
  }

  /** Gather (elementIndex -> propertyNames) from the PropertyAccess nodes in `exprs`. */
  private def collectProps(
      exprs: Seq[Expression],
      varIndex: Map[String, Int]): Map[Int, Set[String]] = {
    val acc = scala.collection.mutable.Map.empty[Int, Set[String]].withDefaultValue(Set.empty)
    exprs.foreach { e =>
      propertyAccesses(e).foreach { case (v, p) =>
        varIndex.get(v).foreach { i => acc(i) = acc(i) + p }
      }
    }
    acc.toMap
  }

  /** Flatten all `(variable, property)` accesses appearing anywhere in `expr`. */
  private def propertyAccesses(expr: Expression): Seq[(String, String)] = expr match {
    case PropertyAccess(v, p) => Seq((v, p))
    case Comparison(l, _, r) => propertyAccesses(l) ++ propertyAccesses(r)
    case Arithmetic(l, _, r) => propertyAccesses(l) ++ propertyAccesses(r)
    case Not(e) => propertyAccesses(e)
    case And(l, r) => propertyAccesses(l) ++ propertyAccesses(r)
    case Or(l, r) => propertyAccesses(l) ++ propertyAccesses(r)
    case _ => Seq.empty
  }
}

// -----------------------------------------------------------------------------
// Prefix environment: maps each element to a unique column prefix so that joins
// never collide on the standardized column names (id/property_group/src/dst/weight).
// -----------------------------------------------------------------------------

private[propertygraph] final class PrefixEnv private (path: SchemaPath, val raw: Boolean) {

  /** Prefix for node `i`: the variable if named, else `node<i>`. Empty when `raw`. */
  def nodePrefix(i: Int): String =
    if (raw) "" else path.nodes(i).variable.getOrElse(s"node$i")

  /** Prefix for edge `i`: the variable if named, else `edge<i>`. Empty when `raw`. */
  def edgePrefix(i: Int): String =
    if (raw) "" else path.steps(i).variable.getOrElse(s"edge$i")

  /** Fully-qualified column name for a node's output column (e.g. the `id`). */
  def nodeCol(i: Int, colName: String): String = join(nodePrefix(i), colName)

  /** Fully-qualified column name for an edge's output column (`src`/`dst`/`weight`). */
  def edgeCol(i: Int, colName: String): String = join(edgePrefix(i), colName)

  /**
   * Join a prefix and a column name: `colName` when the prefix is empty, else `prefix_colName`.
   */
  def join(prefix: String, colName: String): String =
    if (prefix.isEmpty) colName else s"${prefix}_$colName"

  /** The prefix string for a variable, if that variable names a node or edge in this path. */
  def prefixFor(variable: String): Option[String] = {
    if (raw) Some("")
    else
      path.nodes.zipWithIndex
        .find(_._1.variable.contains(variable))
        .map { case (_, i) => nodePrefix(i) }
        .orElse(path.steps.zipWithIndex.find(_._1.variable.contains(variable)).map {
          case (_, i) =>
            edgePrefix(i)
        })
  }
}

private[propertygraph] object PrefixEnv {

  /** A real path environment. */
  def apply(path: SchemaPath): PrefixEnv = new PrefixEnv(path, raw = false)

  /**
   * A raw (empty-prefix) environment: every variable resolves to the empty prefix, so lowered
   * columns reference raw group column names. Used for scan-local filters, which are applied by
   * `getData` against the raw group data before aliasing.
   */
  val raw: PrefixEnv = new PrefixEnv(null, raw = true)
}

// -----------------------------------------------------------------------------
// Expression -> Column lowering.
// -----------------------------------------------------------------------------

private[propertygraph] object ExpressionLowering {

  /**
   * Lower a GQL AST [[Expression]] to a Spark [[Column]] in the context of `env` (variable ->
   * prefix mapping).
   *
   *   - `Variable(v)` -> `col("<prefix>_id")` (a bare variable reference resolves to the
   *     element's id column);
   *   - `PropertyAccess(v, p)` -> `col("<prefix>_<p>")`;
   *   - `Literal(value)` -> a typed `lit` (Long/Double/Boolean/String/null);
   *   - comparison/arithmetic/boolean combinators recurse and map to the corresponding Spark
   *     operators.
   */
  def lower(expr: Expression, env: PrefixEnv): Column = expr match {
    case Literal(value) => litLiteral(value)
    case Variable(name) =>
      val prefix = env
        .prefixFor(name)
        .getOrElse(
          throw new IllegalArgumentException(
            s"Variable '$name' is not bound to any element of the matched path"))
      col(env.join(prefix, GraphFrame.ID))
    case PropertyAccess(variable, property) =>
      val prefix = env
        .prefixFor(variable)
        .getOrElse(
          throw new IllegalArgumentException(
            s"Variable '$variable' is not bound to any element of the matched path"))
      col(env.join(prefix, property))
    case Comparison(left, op, right) => compOp(lower(left, env), op, lower(right, env))
    case Arithmetic(left, op, right) => arithOp(lower(left, env), op, lower(right, env))
    case Not(e) => !lower(e, env)
    case And(left, right) => lower(left, env) && lower(right, env)
    case Or(left, right) => lower(left, env) || lower(right, env)
  }

  private def compOp(l: Column, op: CompOp, r: Column): Column = op match {
    case Eq => l === r
    case Neq => l =!= r
    case Lt => l < r
    case Lte => l <= r
    case Gt => l > r
    case Gte => l >= r
  }

  private def arithOp(l: Column, op: AddOp, r: Column): Column = op match {
    case Plus => l + r
    case Minus => l - r
  }

  /** Narrow `Literal.value: Any` to the appropriate Spark-typed literal. */
  private def litLiteral(value: Any): Column = value match {
    case null => lit(null)
    case v: java.lang.Boolean => lit(v.booleanValue())
    case v: java.lang.Long => lit(v.longValue())
    case v: java.lang.Integer => lit(v.intValue())
    case v: java.lang.Double => lit(v.doubleValue())
    case v: java.lang.Float => lit(v.floatValue())
    case v: Long => lit(v)
    case v: Int => lit(v)
    case v: Double => lit(v)
    case v: Float => lit(v)
    case v: String => lit(v)
    case other =>
      throw new IllegalArgumentException(
        s"Unsupported literal value of type ${other.getClass.getName}: $other")
  }
}
