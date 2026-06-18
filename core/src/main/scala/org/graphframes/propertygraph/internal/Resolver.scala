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

import org.graphframes.InvalidPropertyGroupException

/**
 * Resolution: turns a `MatchStatement` AST plus a `SchemaGraphSnapshot` into a `ResolvedQuery`.
 * Think about it as about Catalyst' analysis phase: we take a raw path-pattern and try to match
 * it against the known LPG schema to determine all the possible join-chains. That is exactly why
 * we need the GraphSchema and that is the biggest difference from the existing Motifs Finding
 * API.
 *
 * As well we fail-fast in the case of syntax error in the query.
 *
 * Steps:
 *   1. Validate every typed label against the schema (unknown label =>
 *      `InvalidPropertyGroupException`).
 *   2. Enumerate concrete `SchemaPath`s by a bounded DFS over `outgoing`/`incoming`, fanning out
 *      over untyped nodes/edges. Disconnected patterns yield no paths (empty, not an error).
 *   3. Classify WHERE conjuncts into scan-local (attached to `PathNode`), join (two adjacent node
 *      vars), or post-join (everything else).
 *   4. Map the RETURN clause to a `Projection`.
 */
private[propertygraph] object Resolver {

  def resolve(ast: MatchStatement, schema: SchemaGraphSnapshot): ResolvedQuery = {
    val nodes = ast.pattern.elements.collect { case n: NodePattern => n }
    val edges = ast.pattern.elements.collect { case e: EdgePattern => e }
    // The grammar guarantees nodes.length == edges.length + 1; defend in depth.
    require(
      nodes.length == edges.length + 1,
      s"GQL pattern must alternate node/edge/node; got ${nodes.length} nodes, ${edges.length} edges")

    validateLabels(nodes, edges, schema)

    val paths = enumeratePaths(nodes, edges, schema)

    val (joinPredicates, postFilters, nodeScanFilters) = classifyWhere(ast.where, nodes, edges)

    // Attach scan-local predicates to the matching PathNode(s) in every enumerated path.
    val pathsWithFilters = paths.map(attachScanFilters(_, nodeScanFilters))

    val projection = ast.returnClause match {
      case Some(ReturnStar) => Projection.Star
      case Some(ReturnItems(items)) => Projection.Items(items)
      case None => Projection.Default
    }

    ResolvedQuery(pathsWithFilters, joinPredicates, postFilters, projection)
  }

  // ---------------------------------------------------------------------
  // Step 1: label validation.
  // ---------------------------------------------------------------------

  private def validateLabels(
      nodes: Seq[NodePattern],
      edges: Seq[EdgePattern],
      schema: SchemaGraphSnapshot): Unit = {
    val edgeGroupNames = schema.edges.map(_.edgeGroupName).toSet
    nodes.foreach { n =>
      n.label.foreach { label =>
        if (!schema.vertexGroupNames.exists(_.equalsIgnoreCase(label))) {
          throw new InvalidPropertyGroupException(
            s"Unknown vertex label '$label'; known vertex groups: " +
              schema.vertexGroupNames.toVector.sorted.mkString(", "))
        }
      }
    }
    edges.foreach { e =>
      e.label.foreach { label =>
        if (!edgeGroupNames.exists(_.equalsIgnoreCase(label))) {
          throw new InvalidPropertyGroupException(
            s"Unknown edge label '$label'; known edge groups: " +
              edgeGroupNames.toVector.sorted.mkString(", "))
        }
      }
    }
  }

  // ---------------------------------------------------------------------
  // Step 2: schema-graph path enumeration (bounded DFS).
  // ---------------------------------------------------------------------

  private[propertygraph] def enumeratePaths(
      nodes: Seq[NodePattern],
      edges: Seq[EdgePattern],
      schema: SchemaGraphSnapshot): Vector[SchemaPath] = {
    // Start vertex-group candidates for node 0.
    // Resolve the user-supplied label to its canonical-case name so that subsequent
    // outgoing/incoming map lookups (keyed by original case) still hit.
    val startGroups: Set[String] =
      nodes.head.label
        .map(label => schema.vertexGroupNames.find(_.equalsIgnoreCase(label)).toSet)
        .getOrElse(schema.vertexGroupNames)

    val results = scala.collection.mutable.ListBuffer.empty[SchemaPath]

    def dfs(
        nodeIndex: Int,
        currentGroup: String,
        accNodes: Vector[PathNode],
        accSteps: Vector[PathStep]): Unit = {
      val node = PathNode(currentGroup, nodes(nodeIndex).variable, scanFilter = Seq.empty)
      val nodesSoFar = accNodes :+ node

      if (nodeIndex == edges.length) {
        // Leaf: emit a complete path.
        results += SchemaPath(nodesSoFar, accSteps)
        return
      }

      val edgePat = edges(nodeIndex)
      val nextNodePat = nodes(nodeIndex + 1)

      // Candidate schema edges, depending on the arrow direction.
      val candidates: Vector[(SchemaEdge, String, Boolean)] = edgePat.direction match {
        case LeftToRight =>
          // Pattern arrow agrees with src->dst: enumerate edges whose src is the current group.
          schema.outgoing
            .getOrElse(currentGroup, Vector.empty)
            .map(e => (e, e.dstVertexGroupName, true))
        case RightToLeft =>
          // Pattern arrow opposes src->dst: enumerate edges whose dst is the current group, and the
          // next node becomes the edge's src.
          schema.incoming
            .getOrElse(currentGroup, Vector.empty)
            .map(e => (e, e.srcVertexGroupName, false))
      }

      candidates.foreach { case (edge, nextGroup, forward) =>
        // Filter by typed edge label, if any (case-insensitive).
        val edgeLabelOk = edgePat.label.forall(_.equalsIgnoreCase(edge.edgeGroupName))
        // Filter by typed next-node label, if any (case-insensitive).
        val nextLabelOk = nextNodePat.label.forall(_.equalsIgnoreCase(nextGroup))
        if (edgeLabelOk && nextLabelOk) {
          val step = PathStep(edge, forward, edgePat.variable)
          dfs(nodeIndex + 1, nextGroup, nodesSoFar, accSteps :+ step)
        }
      }
    }

    startGroups.foreach(g => dfs(nodeIndex = 0, currentGroup = g, Vector.empty, Vector.empty))
    results.toVector
  }

  // ---------------------------------------------------------------------
  // Step 3: WHERE classification.
  //
  // Returns (joinPredicates, postFilters, nodeScanFilters) where nodeScanFilters maps a node
  // variable to the predicates to attach to every PathNode bound to that variable.
  // ---------------------------------------------------------------------

  private def classifyWhere(
      whereOpt: Option[Expression],
      nodes: Seq[NodePattern],
      edges: Seq[EdgePattern])
      : (Seq[Expression], Seq[Expression], Map[String, Seq[Expression]]) = {
    // Variable -> node positions (0-based into `nodes`). The same variable may bind several
    // positions (e.g. a triangle pattern `(a)-[..]->(b)-[..]->(a)`).
    val nodeVarPositions: Map[String, Set[Int]] =
      nodes.zipWithIndex
        .flatMap { case (n, i) => n.variable.map(v => v -> i) }
        .groupBy(_._1)
        .map { case (v, pairs) =>
          v -> pairs.map(_._2).toSet
        }

    val edgeVarNames: Set[String] = edges.flatMap(_.variable).toSet

    val join = scala.collection.mutable.ListBuffer.empty[Expression]
    val post = scala.collection.mutable.ListBuffer.empty[Expression]
    val scan = scala.collection.mutable.Map.empty[String, Seq[Expression]]

    val conjuncts = whereOpt.map(GqlAst.flattenAnd).getOrElse(Seq.empty)
    conjuncts.foreach { conjunct =>
      val refs = GqlAst.referencedVariables(conjunct)
      val nodeRefs = refs.intersect(nodeVarPositions.keySet)
      val edgeRefs = refs.intersect(edgeVarNames)
      if (edgeRefs.isEmpty && nodeRefs.size == 1) {
        // Scan-local: a single node variable (possibly bound at several positions).
        val v = nodeRefs.head
        scan(v) = scan.getOrElse(v, Seq.empty) :+ conjunct
      } else if (edgeRefs.isEmpty && nodeRefs.size == 2) {
        val Seq(v1, v2) = nodeRefs.toSeq
        if (areAdjacent(nodeVarPositions(v1), nodeVarPositions(v2))) {
          join += conjunct
        } else {
          post += conjunct
        }
      } else {
        // 3+ vars, any edge var, or a literal-only conjunct: evaluate after the join tree.
        post += conjunct
      }
    }

    (join.toSeq, post.toSeq, scan.toMap)
  }

  /**
   * Two node-position sets are adjacent if any pair of positions differs by exactly 1. Positions
   * are indices into the nodes-only `Seq[NodePattern]` (edges already collected out), so a single
   * edge hop connects nodes at indices `i` and `i+1`.
   */
  private def areAdjacent(p1: Set[Int], p2: Set[Int]): Boolean =
    p1.exists(a => p2.exists(b => Math.abs(a - b) == 1))

  private def attachScanFilters(
      path: SchemaPath,
      nodeScanFilters: Map[String, Seq[Expression]]): SchemaPath = {
    if (nodeScanFilters.isEmpty) path
    else {
      val newNodes = path.nodes.map { n =>
        val extra = n.variable.flatMap(nodeScanFilters.get).getOrElse(Seq.empty)
        if (extra.isEmpty) n else n.copy(scanFilter = n.scanFilter ++ extra)
      }
      path.copy(nodes = newNodes)
    }
  }
}
