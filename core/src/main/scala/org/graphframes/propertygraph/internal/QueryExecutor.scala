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
   *
   * Scan sharing: a per-call memo (`scanMemo`) de-duplicates scans across the per-path fan-out.
   * It is keyed by a canonical scan signature ([[ScanKey]]) so every plan that references the
   * same group with the same scan-local filter and the same carried-column set pulls the *same*
   * `DataFrame` reference. This is the "floor" of scan reuse: never construct the same scan
   * twice.
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
    val scanMemo = scala.collection.mutable.Map.empty[ScanKey, DataFrame]
    val perPlan = plans.map(executePlan(pgf, _, scanMemo))
    perPlan.reduce(_ unionByName _)
  }

  /**
   * Test-only (debug) variant of [[execute]] that also returns the per-call scan memo, so tests
   * can assert the scan-reuse "floor" (a scan is never constructed twice for an equal
   * [[ScanKey]]) by reference-identity on the memo values. Package-private; not part of any
   * public contract.
   */
  private[propertygraph] def executeWithScanMemo(
      pgf: PropertyGraphFrame,
      plans: Seq[JoinPlan]): (DataFrame, Map[ScanKey, DataFrame]) = {
    val scanMemo = scala.collection.mutable.Map.empty[ScanKey, DataFrame]
    val result =
      if (plans.isEmpty) execute(pgf, plans)
      else {
        val perPlan = plans.map(executePlan(pgf, _, scanMemo))
        perPlan.reduce(_ unionByName _)
      }
    (result, scanMemo.toMap)
  }

  /**
   * Canonical signature of a scan. Two scans with equal keys share one `DataFrame` in the memo.
   * `Expression` AST nodes (including the scan-local filters) are case classes with structural
   * equality, so they key correctly without extra canonicalization. `carriedCols` is a `Set`, so
   * column order does not affect sharing.
   */
  private[propertygraph] final case class ScanKey(
      groupName: String,
      scanFilter: Seq[Expression],
      carriedCols: Set[String])

  // -------------------------------------------------------------------------
  // Per-plan execution.
  // -------------------------------------------------------------------------

  private def executePlan(
      pgf: PropertyGraphFrame,
      plan: JoinPlan,
      scanMemo: scala.collection.mutable.Map[ScanKey, DataFrame]): DataFrame = {
    val path = plan.path
    val env = PrefixEnv(path)

    // Classify, per element, which property columns are CARRIED through the scan (predicate /
    // filter-also-returned columns that influence join cardinality) vs which are OUTPUT-ONLY
    // (referenced solely by RETURN, deferred to a terminal join-back).
    val nodeProps: Map[Int, ElementProps] = classifyElementProps(path, plan, node = true)
    val edgeProps: Map[Int, ElementProps] = classifyElementProps(path, plan, node = false)

    // Scan each element once (shared via the memo below the rename) and alias its columns under the
    // element's prefix. The expensive shared node sits below a thin per-use Project (the rename).
    val nodeFrames: Map[Int, DataFrame] = path.nodes.indices.map { i =>
      val shared =
        sharedScanNode(pgf, path, i, nodeProps.getOrElse(i, ElementProps.Empty), scanMemo)
      i -> renameAll(shared, env.nodePrefix(i))
    }.toMap
    val edgeFrames: Map[Int, DataFrame] = path.steps.indices.map { i =>
      val shared =
        sharedScanEdge(pgf, path, i, edgeProps.getOrElse(i, ElementProps.Empty), scanMemo)
      i -> renameAll(shared, env.edgePrefix(i))
    }.toMap

    // Walk the join order, joining each element onto the growing frame, placing every multi-variable
    // predicate at the earliest element where all its operands are bound (replacing a blanket
    // post-tree `.where`).
    val allPredicates: Seq[Expression] = plan.joinPredicates ++ plan.postFilters
    val predicateVarSets: Seq[Set[String]] = allPredicates.map(GqlAst.referencedVariables)
    val placed = scala.collection.mutable.BitSet.empty
    var frame: DataFrame = null
    var boundVars: Set[String] = Set.empty
    def bindElement(i: Int, isNode: Boolean): Unit = {
      if (isNode) path.nodes(i).variable.foreach(v => boundVars += v)
      else path.steps(i).variable.foreach(v => boundVars += v)
    }
    plan.order.foreach {
      case NodeRef(i) =>
        val f = nodeFrames(i)
        bindElement(i, isNode = true)
        if (frame == null) {
          frame = f
        } else {
          val ready = readyPredicateIndices(predicateVarSets, placed, boundVars)
          val readyExprs = ready.map(allPredicates)
          frame = joinElement(frame, f, path, env, readyExprs)
          ready.foreach(placed.add)
        }
      case EdgeRef(i) =>
        val f = edgeFrames(i)
        bindElement(i, isNode = false)
        if (frame == null) {
          frame = f
        } else {
          val ready = readyPredicateIndices(predicateVarSets, placed, boundVars)
          val readyExprs = ready.map(allPredicates)
          frame = joinElement(frame, f, path, env, readyExprs)
          ready.foreach(placed.add)
        }
    }
    if (frame == null) {
      // Degenerate: a path with a single node and no edges (MATCH (a:Person)).
      // Synthesize a trivially-valid frame from that node scan; nodeFrames is non-empty here.
      frame = nodeFrames(0)
    }
    // Place any predicates not consumed during the join walk (e.g. a predicate whose variables were
    // all bound by the seed element, or a literal-only predicate) as a residual filter.
    val leftover = allPredicates.indices.filterNot(placed.contains).map(allPredicates)
    leftover.foreach { expr =>
      frame = frame.filter(ExpressionLowering.lower(expr, env))
    }

    // edgeProps is classified (and consumed by the shared edge scans above) but NOT join-backed
    // edge groups may be undirected (doubling rows), so edge RETURN-properties are CARRIED rather
    // than resolved by an id-keyed join-back.
    project(frame, plan, nodeProps, pgf)
  }

  /** Indices of predicates whose referenced variables are all bound and not yet placed. */
  private def readyPredicateIndices(
      varSets: Seq[Set[String]],
      placed: scala.collection.mutable.BitSet,
      boundVars: Set[String]): Seq[Int] =
    varSets.indices.collect {
      case k if !placed.contains(k) && varSets(k).subsetOf(boundVars) => k
    }

  // -------------------------------------------------------------------------
  // Scans + aliasing.
  // -------------------------------------------------------------------------

  /**
   * Return the shared (memoized), *un-prefixed* node scan for element `i`. The caller applies the
   * per-element rename on top. `ElementProps.carriedToScan` is what the scan requests;
   * output-only columns are NOT requested here (they are resolved by the terminal join-back in
   * `project`).
   */
  private def sharedScanNode(
      pgf: PropertyGraphFrame,
      path: SchemaPath,
      i: Int,
      props: ElementProps,
      scanMemo: scala.collection.mutable.Map[ScanKey, DataFrame]): DataFrame = {
    val node = path.nodes(i)
    val key = ScanKey(node.vertexGroupName.toLowerCase, node.scanFilter, props.carriedToScan)
    scanMemo.getOrElseUpdate(
      key, {
        val group = pgf.vertexGroups(node.vertexGroupName.toLowerCase)
        val filterCol = lowerScanFilter(node.scanFilter)
        group.getData(filterCol, props.carriedToScan.toSeq.sorted)
      })
  }

  /**
   * Return the shared (memoized), *un-prefixed* edge scan for element `i`.
   */
  private def sharedScanEdge(
      pgf: PropertyGraphFrame,
      path: SchemaPath,
      i: Int,
      props: ElementProps,
      scanMemo: scala.collection.mutable.Map[ScanKey, DataFrame]): DataFrame = {
    val step = path.steps(i)
    val key = ScanKey(step.edge.edgeGroupName.toLowerCase, Seq.empty, props.carriedToScan)
    scanMemo.getOrElseUpdate(
      key, {
        // lit(true) is a placeholder for the future edge-filters
        val group = pgf.edgeGroups(step.edge.edgeGroupName.toLowerCase)
        group.getData(lit(true), props.carriedToScan.toSeq.sorted)
      })
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
      env: PrefixEnv,
      residualPredicates: Seq[Expression]): DataFrame = {
    val presentCols = frame.columns.toSet
    val conditions = adjacencyConditions(incoming.columns.toSet, presentCols, path, env)
    require(
      conditions.nonEmpty,
      "Join order produced an element with no adjacency to the already-joined frame; " +
        "this indicates an invalid join order" + s" for path ${path.toString()}")
    // The residual predicates are multi-variable WHERE conjuncts whose operands are all bound at
    // this join.
    val residualCols = residualPredicates.map(ExpressionLowering.lower(_, env))
    val allConds = conditions ++ residualCols
    frame.join(incoming, allConds.reduce(_ && _), "inner")
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

  private def project(
      frame: DataFrame,
      plan: JoinPlan,
      nodeProps: Map[Int, ElementProps],
      pgf: PropertyGraphFrame): DataFrame = {
    val path = plan.path
    val env = PrefixEnv(path)
    plan.projection match {
      case Projection.Items(items) =>
        // For any node element with output-only (RETURN-only) properties, join the masked id back to
        // the group's properties so those columns are available for the RETURN projection. Carried
        // columns (predicate / filter-also-returned) are already on `frame` under the element
        // prefix; only output-only columns need the terminal join-back. Edges are NOT join-backed
        // and edge RETURN-properties are carried instead.
        //
        // The joined-back property columns are aliased to the element's PREFIXED names so that
        // `ExpressionLowering.lower(PropertyAccess(v, p))` -- which resolves to `<prefix>_p` --
        // finds them. (Carried columns already live under those prefixed names; this puts the
        // output-only ones under the same convention.)
        var withOutput = frame
        path.nodes.indices.foreach { i =>
          val props = nodeProps.getOrElse(i, ElementProps.Empty)
          if (props.outputOnly.nonEmpty) {
            val node = path.nodes(i)
            val group = pgf.vertexGroups(node.vertexGroupName.toLowerCase)
            val prefix = env.nodePrefix(i)
            // Build a narrow join-back frame: the masked `id` (join key) plus the output-only
            // property columns, renamed to the element's prefixed names so `ExpressionLowering`
            // resolves `PropertyAccess(v, p)` -> `<prefix>_p` against them. We drop
            // `property_group` (the group name is a constant the projection emits itself) and avoid
            // surfacing a second un-prefixed `id` column that would collide across multiple
            // join-backs; the masked `id` is kept only as the join key and dropped afterward.
            val carryCols = props.outputOnly.toSeq.sorted
            // `_gjid_<i>` is a throwaway join-key alias unique per element, so multiple join-backs
            // never collide on a raw `id` column.
            val joinKey = s"_gjid_$i"
            val groupId = group
              .getData(lit(true), carryCols)
              .select((col(GraphFrame.ID).alias(joinKey) +: carryCols
                .map(p => col(p).alias(env.join(prefix, p)))): _*)
            val idCol = env.nodeCol(i, GraphFrame.ID)
            withOutput = withOutput.join(groupId, withOutput(idCol) === groupId(joinKey), "left")
          }
        }
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
        withOutput.select(cols: _*)

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
  // Required-property classification (carry vs defer).
  //
  // Per scan-reuse: every required property is classified by the role that
  // references it:
  //   - scan-local filter columns  -> pushed into getData's filter, consumed before the shuffle;
  //   - join / post-filter columns -> CARRIED through the join tree to the predicate's evaluation
  //     point (they let the predicate cut the join's output cardinality);
  //   - output-only columns        -> DEFERRED to a terminal join-back (they never reduce
  //     cardinality, so carrying them only widens shuffles and fragments scan signatures).
  // A property referenced by BOTH a filter and RETURN is carried (the filter need dominates).
  // -------------------------------------------------------------------------

  /** Per-element property classification. */
  private[propertygraph] final case class ElementProps(
      carriedToScan: Set[String], // requested at the scan; rides the join tree to its consumers
      outputOnly: Set[String]
  ) // RETURN-only; resolved by the terminal join-back, never carried

  private[propertygraph] object ElementProps {
    val Empty: ElementProps = ElementProps(Set.empty, Set.empty)
  }

  /**
   * Classify the properties of every element of the requested kind (node or edge) into carried vs
   * output-only.
   *
   * For an element bound to variable `v`:
   *   - `scanFilterProps(v)` = props referenced in this element's scan-local filters;
   *   - `joinPostProps(v)` = props of `v` referenced in join/post predicates;
   *   - `returnProps(v)` = props of `v` referenced in RETURN items (empty for Default/Star);
   *   - `carry(v)` = `joinPostProps(v) ∪ (returnProps(v) ∩ scanFilterProps(v))`;
   *   - `outputOnly(v)` = `returnProps(v) − carry(v)`.
   */
  private def classifyElementProps(
      path: SchemaPath,
      plan: JoinPlan,
      node: Boolean): Map[Int, ElementProps] = {
    val nodeVarIndex: Map[String, Int] = path.nodes.zipWithIndex.collect {
      case (n, i) if n.variable.isDefined => n.variable.get -> i
    }.toMap
    val edgeVarIndex: Map[String, Int] = path.steps.zipWithIndex.collect {
      case (s, i) if s.variable.isDefined => s.variable.get -> i
    }.toMap
    val varIndex = if (node) nodeVarIndex else edgeVarIndex

    // Per-element scan-filter properties (these are applied at the scan; they do NOT by themselves
    // earn a carry -- they are consumed before the shuffle).
    val scanFilterExprs: Seq[Expression] =
      if (node) path.nodes.flatMap(_.scanFilter) else Seq.empty
    val scanFilterProps: Map[Int, Set[String]] = collectProps(scanFilterExprs, varIndex)

    // Join/post-predicate properties: these DO earn a carry (they ride to the predicate's point).
    val joinPostProps: Map[Int, Set[String]] =
      collectProps(plan.joinPredicates ++ plan.postFilters, varIndex)

    // RETURN properties (only for Items projections).
    val returnProps: Map[Int, Set[String]] = plan.projection match {
      case Projection.Items(items) => collectProps(items.map(_.expression), varIndex)
      case _ => Map.empty
    }

    val acc =
      scala.collection.mutable.Map.empty[Int, ElementProps].withDefaultValue(ElementProps.Empty)

    // For edges we should actually carry all the returnProps
    varIndex.values.foreach { idx =>
      val sf = scanFilterProps.getOrElse(idx, Set.empty)
      val jp = joinPostProps.getOrElse(idx, Set.empty)
      val rp = returnProps.getOrElse(idx, Set.empty)
      val carry = if (node) jp ++ rp.intersect(sf) else jp ++ rp
      // there is no join-back for edges and all the rp are already carried along the join-path
      val outputOnly = if (node) rp -- carry else Set.empty[String]
      acc(idx) = ElementProps(carry, outputOnly)
    }
    acc.toMap
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
