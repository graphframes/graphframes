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
import org.graphframes.SparkFunSuite
import org.graphframes.propertygraph.QueryOptions

/**
 * Pure-JVM tests for `Resolver.resolve`. Each test builds a `MatchStatement` AST by hand (or via
 * `AstBuilder.parse`) plus a small `SchemaGraphSnapshot`, and asserts on the resulting
 * `ResolvedQuery`. No SparkSession required — resolution is pure JVM.
 */
class ResolverSuite extends SparkFunSuite {

  // A small schema used across several tests:
  //   Person --KNOWS--> Person
  //   Person --WORKS_AT--> Company
  //   Company --LOCATED_IN--> City
  private val schema = SchemaGraphSnapshot(
    vertexGroupNames = Set("Person", "Company", "City"),
    edges = Vector(
      SchemaEdge("KNOWS", "Person", "Person", true),
      SchemaEdge("WORKS_AT", "Person", "Company", true),
      SchemaEdge("LOCATED_IN", "Company", "City", true)))

  private val options = QueryOptions()

  test("typed single-hop pattern resolves to exactly one path") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS]->(b:Person)")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.paths.length === 1)
    val path = rq.paths.head
    assert(path.length === 1)
    assert(path.nodes.map(_.vertexGroupName) === Vector("Person", "Person"))
    assert(path.steps.head.edge === SchemaEdge("KNOWS", "Person", "Person", true))
    assert(path.steps.head.traversedForward === true)
    assert(path.nodes.head.variable === Some("a"))
    assert(path.nodes(1).variable === Some("b"))
    assert(rq.projection === Projection.Default)
  }

  test("untyped middle node fans out over reachable schema paths") {
    // MATCH (a:Person)-[]->(x)-[]->(b:City): from Person, the only 2-hop chain landing on City is
    //   Person -WORKS_AT-> Company -LOCATED_IN-> City. (Person-KNOWS->Person has no onward edge to
    //   City.) So exactly one path survives, with the middle node resolved to Company.
    val ast = AstBuilder.parse("MATCH (a:Person)-[]->(x)-[]->(b:City)")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.paths.length === 1)
    val path = rq.paths.head
    assert(path.nodes.map(_.vertexGroupName) === Vector("Person", "Company", "City"))
    assert(path.steps(0).edge === SchemaEdge("WORKS_AT", "Person", "Company", true))
    assert(path.steps(1).edge === SchemaEdge("LOCATED_IN", "Company", "City", true))
    assert(path.steps.forall(_.traversedForward === true))
  }

  test("disconnected pattern yields no paths") {
    // No schema edge has City as src and Person as dst, and no 2-hop City->...->Person exists.
    val ast = AstBuilder.parse("MATCH (a:City)-[:KNOWS]->(b:Person)")
    val rq = Resolver.resolve(ast, schema, options)
    assert(rq.paths.isEmpty)
  }

  test("right-to-left arrow produces a backward step") {
    val ast = AstBuilder.parse("MATCH (a:Company)<-[:WORKS_AT]-(b:Person)")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.paths.length === 1)
    val step = rq.paths.head.steps.head
    // The edge group is WORKS_AT: Person->Company. Traversed right-to-left means the current node
    // is the edge's dst (Company) and the next node is the edge's src (Person).
    assert(step.edge === SchemaEdge("WORKS_AT", "Person", "Company", true))
    assert(step.traversedForward === false)
    assert(rq.paths.head.nodes.map(_.vertexGroupName) === Vector("Company", "Person"))
  }

  test("self-loop group is enumerated without special-casing") {
    // KNOWS: Person->Person is a self-loop. A typed pattern over it resolves to a single path.
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.paths.length === 1)
    assert(rq.paths.head.length === 2)
    assert(rq.paths.head.steps.forall(_.traversedForward === true))
  }

  test("untyped source node fans out over all vertex groups") {
    // MATCH (x)-[:LOCATED_IN]->(b:City): x must be a group that has an outgoing LOCATED_IN edge,
    // i.e. Company only. So one path.
    val ast = AstBuilder.parse("MATCH (x)-[:LOCATED_IN]->(b:City)")
    val rq = Resolver.resolve(ast, schema, options)
    assert(rq.paths.length === 1)
    assert(rq.paths.head.nodes.head.vertexGroupName === "Company")
  }

  test("untyped edge fans out over all candidate edge groups") {
    // MATCH (a:Person)-[]->(b:Person): Person->Person edges are KNOWS only. One path.
    // Person-WORKS_AT->Company does not land on Person. So one path.
    val ast = AstBuilder.parse("MATCH (a:Person)-[]->(b:Person)")
    val rq = Resolver.resolve(ast, schema, options)
    assert(rq.paths.length === 1)
    assert(rq.paths.head.steps.head.edge.edgeGroupName === "KNOWS")
  }

  test("unknown vertex label throws InvalidPropertyGroupException") {
    val ast = AstBuilder.parse("MATCH (a:Walrus)")
    intercept[InvalidPropertyGroupException] {
      Resolver.resolve(ast, schema, options)
    }
  }

  test("unknown edge label throws InvalidPropertyGroupException") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:HATES]->(b:Person)")
    intercept[InvalidPropertyGroupException] {
      Resolver.resolve(ast, schema, options)
    }
  }

  test("scan-local WHERE predicate is attached to the matching PathNode") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 30")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.joinPredicates === Nil)
    assert(rq.postFilters === Nil)
    val nodeA = rq.paths.head.nodes.head
    val nodeB = rq.paths.head.nodes(1)
    assert(nodeA.scanFilter.length === 1)
    assert(nodeA.scanFilter.head === Comparison(PropertyAccess("a", "age"), Gt, Literal(30L)))
    assert(nodeB.scanFilter === Nil)
  }

  test("two-variable adjacent WHERE predicate becomes a join predicate") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > b.age")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.joinPredicates.length === 1)
    assert(
      rq.joinPredicates.head === Comparison(
        PropertyAccess("a", "age"),
        Gt,
        PropertyAccess("b", "age")))
    assert(rq.paths.head.nodes.forall(_.scanFilter === Nil))
    assert(rq.postFilters === Nil)
  }

  test("three-variable WHERE predicate becomes a post-filter") {
    val ast = AstBuilder.parse(
      "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) WHERE a.age > c.age")
    val rq = Resolver.resolve(ast, schema, options)

    // a and c are non-adjacent (positions 0 and 4, differ by 4, not 2).
    assert(rq.joinPredicates === Nil)
    assert(rq.postFilters.length === 1)
    assert(
      rq.postFilters.head === Comparison(
        PropertyAccess("a", "age"),
        Gt,
        PropertyAccess("c", "age")))
  }

  test("AND is split so each conjunct is classified independently") {
    val ast = AstBuilder.parse(
      "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 30 AND a.age > b.age AND 1 = 1")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.paths.head.nodes.head.scanFilter.length === 1) // a.age > 30
    assert(rq.joinPredicates.length === 1) // a.age > b.age
    // The literal-only `1 = 1` references no node variable and falls into post-filters.
    assert(rq.postFilters.length === 1)
  }

  // -----------------------------------------------------------------------
  // Scalar function calls in WHERE: classification regression guards.
  // These guard the §4 traversal edits (`referencedVariables` + `propertyAccesses`):
  // a function call over a property must still contribute its variable so the
  // resolver can classify the predicate (scan-local vs join vs post).
  // -----------------------------------------------------------------------
  test("scan-local WHERE predicate inside a function call is attached to the matching node") {
    // `year(a.creationDate) = 2012` references only `a` -> scan-local on `a`.
    val ast =
      AstBuilder.parse("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE year(a.creationDate) = 2012")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.joinPredicates === Nil)
    assert(rq.postFilters === Nil)
    val nodeA = rq.paths.head.nodes.head
    val nodeB = rq.paths.head.nodes(1)
    assert(nodeA.scanFilter.length === 1)
    assert(
      nodeA.scanFilter.head === Comparison(
        FunctionCall("year", Seq(PropertyAccess("a", "creationDate"))),
        Eq,
        Literal(2012L)))
    assert(nodeB.scanFilter === Nil)
  }

  test("two-variable adjacent WHERE predicate inside a function call becomes a join predicate") {
    // `datediff(a.d, b.d) > 30` references `a` and `b` (adjacent) -> join predicate, with both
    // `a.d` and `b.d` carried (this is the silent-drop regression guard: if `propertyAccesses`
    // forgot the FunctionCall arm, these columns would not be classified as carry-to-scan).
    val ast =
      AstBuilder.parse("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE datediff(a.d, b.d) > 30")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.joinPredicates.length === 1)
    assert(
      rq.joinPredicates.head === Comparison(
        FunctionCall("datediff", Seq(PropertyAccess("a", "d"), PropertyAccess("b", "d"))),
        Gt,
        Literal(30L)))
    assert(rq.paths.head.nodes.forall(_.scanFilter === Nil))
    assert(rq.postFilters === Nil)
  }

  test(
    "hash-sampling WHERE predicate (pmod + hash) is classified scan-local on its only variable") {
    // `pmod(hash(a.id), 512) = 0` references only `a` -> scan-local on `a`. This is the
    // deterministic-sampling pushdown guard (spec §3): the predicate must reach the scan so the
    // ScanKey memo sees a reproducible filter, not a post-join filter.
    val ast =
      AstBuilder.parse("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE pmod(hash(a.id), 512) = 0")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.joinPredicates === Nil)
    assert(rq.postFilters === Nil)
    val nodeA = rq.paths.head.nodes.head
    val nodeB = rq.paths.head.nodes(1)
    assert(nodeA.scanFilter.length === 1)
    assert(
      nodeA.scanFilter.head === Comparison(
        FunctionCall(
          "pmod",
          Seq(FunctionCall("hash", Seq(PropertyAccess("a", "id"))), Literal(512L))),
        Eq,
        Literal(0L)))
    assert(nodeB.scanFilter === Nil)
  }

  test("multiplicative cross-variable WHERE predicate becomes a join predicate") {
    // `a.x * 2 > b.y` references `a` and `b` (adjacent) -> join predicate. Regression guard for
    // the widened ArithOp: the Arithmetic node must still contribute both variables through
    // referencedVariables / propertyAccesses (the node match is unchanged, only the op type
    // widened, but this pins the behaviour).
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.x * 2 > b.y")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.joinPredicates.length === 1)
    assert(
      rq.joinPredicates.head === Comparison(
        Arithmetic(PropertyAccess("a", "x"), Mult, Literal(2L)),
        Gt,
        PropertyAccess("b", "y")))
    assert(rq.paths.head.nodes.forall(_.scanFilter === Nil))
    assert(rq.postFilters === Nil)
  }

  test("projection default when RETURN omitted") {
    val ast = AstBuilder.parse("MATCH (a:Person)")
    assert(Resolver.resolve(ast, schema, options).projection === Projection.Default)
  }

  test("projection star") {
    val ast = AstBuilder.parse("MATCH (a:Person) RETURN *")
    assert(Resolver.resolve(ast, schema, options).projection === Projection.Star)
  }

  test("projection items") {
    val ast = AstBuilder.parse("MATCH (a:Person) RETURN a, a.name AS n")
    val Projection.Items(items) = Resolver.resolve(ast, schema, options).projection
    assert(items.length === 2)
    assert(items(1).alias === Some("n"))
  }

  test("fan-out produces multiple paths for a fully untyped 2-hop pattern") {
    // A dedicated schema with multiple 2-hop chains so fan-out is observable.
    val multi = SchemaGraphSnapshot(
      vertexGroupNames = Set("A", "B", "C", "D"),
      edges = Vector(
        SchemaEdge("e1", "A", "B", true),
        SchemaEdge("e2", "A", "C", true),
        SchemaEdge("e3", "B", "D", true),
        SchemaEdge("e4", "C", "D", true)))
    val ast = AstBuilder.parse("MATCH (x:A)-[]->()-[]->(y:D)")
    val rq = Resolver.resolve(ast, multi, options)

    // Two chains: A-e1->B-e3->D and A-e2->C-e4->D.
    assert(rq.paths.length === 2)
    val midGroups = rq.paths.map(_.nodes(1).vertexGroupName).toSet
    assert(midGroups === Set("B", "C"))
    rq.paths.foreach { p =>
      assert(p.steps.forall(_.traversedForward === true))
      assert(p.nodes.head.vertexGroupName === "A")
      assert(p.nodes.last.vertexGroupName === "D")
    }
  }

  // =========================================================================
  // Direct tests for `Resolver.enumeratePaths`.
  //
  // `enumeratePaths` is the bounded DFS that turns a linear node/edge pattern
  // plus a schema snapshot into 0..N concrete `SchemaPath`s. These tests call
  // it directly (bypassing label validation, WHERE classification, projection)
  // so each DFS branch and corner case is covered in isolation. Note that,
  // unlike `resolve`, `enumeratePaths` does NOT throw on unknown labels: it
  // simply yields no paths (the throw happens upstream in `validateLabels`).
  // =========================================================================

  /** Extract (nodes, edges) in user order from a parsed GQL pattern. */
  private def parsed(query: String): (Seq[NodePattern], Seq[EdgePattern]) = {
    val ast = AstBuilder.parse(query)
    val nodes = ast.pattern.elements.collect { case n: NodePattern => n }
    val edges = ast.pattern.elements.collect { case e: EdgePattern => e }
    (nodes, edges)
  }

  // --- Single-node patterns (zero edges / DFS leaf at root) ---------------

  test("enumeratePaths: typed single node yields one zero-step path") {
    val (nodes, edges) = parsed("MATCH (a:Person)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.length === 1)
    val p = paths.head
    assert(p.length === 0)
    assert(p.steps.isEmpty)
    assert(p.nodes.length === 1)
    assert(p.nodes.head.vertexGroupName === "Person")
    assert(p.nodes.head.variable === Some("a"))
    assert(p.nodes.head.scanFilter === Nil)
  }

  test("enumeratePaths: untyped single node fans out over every vertex group") {
    val (nodes, edges) = parsed("MATCH (x)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.length === 3)
    assert(paths.map(_.nodes.head.vertexGroupName).toSet === Set("Person", "Company", "City"))
    paths.foreach { p =>
      assert(p.length === 0)
      assert(p.steps.isEmpty)
      assert(p.nodes.head.variable === Some("x"))
    }
  }

  test("enumeratePaths: anonymous single node fans out with no variable") {
    val (nodes, edges) = parsed("MATCH ()")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.length === 3)
    paths.foreach { p =>
      assert(p.length === 0)
      assert(p.nodes.head.variable === None)
    }
  }

  test("enumeratePaths: label-only single node (no variable) resolves") {
    val (nodes, edges) = parsed("MATCH (:Company)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.length === 1)
    assert(paths.head.nodes.head.vertexGroupName === "Company")
    assert(paths.head.nodes.head.variable === None)
  }

  test("enumeratePaths: typed start label matches case-insensitively") {
    val (nodes, edges) = parsed("MATCH (a:PERSON)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.length === 1)
    // The resolved group preserves the schema's canonical casing, not the query's casing.
    assert(paths.head.nodes.head.vertexGroupName === "Person")
  }

  test("enumeratePaths: unknown start label yields no paths rather than throwing") {
    // `validateLabels` (upstream) is what throws; the DFS itself just sees an empty start set.
    val (nodes, edges) = parsed("MATCH (a:Walrus)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.isEmpty)
  }

  test("enumeratePaths: empty schema yields no paths for an untyped single node") {
    val empty = SchemaGraphSnapshot(vertexGroupNames = Set.empty, edges = Vector.empty)
    val (nodes, edges) = parsed("MATCH (x)")
    val paths = Resolver.enumeratePaths(nodes, edges, empty, options)
    assert(paths.isEmpty)
  }

  // --- Single-hop forward -------------------------------------------------

  test("enumeratePaths: typed forward single hop resolves to exactly one path") {
    val (nodes, edges) = parsed("MATCH (a:Person)-[:KNOWS]->(b:Person)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.length === 1)
    val p = paths.head
    assert(p.length === 1)
    assert(p.steps.head.edge === SchemaEdge("KNOWS", "Person", "Person", true))
    assert(p.steps.head.traversedForward === true)
    assert(p.nodes.map(_.vertexGroupName) === Vector("Person", "Person"))
    assert(p.nodes.map(_.variable) === Vector(Some("a"), Some("b")))
  }

  test("enumeratePaths: untyped edge from Person fans out over both outgoing groups") {
    // Person has two outgoing edges: KNOWS->Person and WORKS_AT->Company.
    val (nodes, edges) = parsed("MATCH (a:Person)-[]->(b)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.length === 2)
    val edgeNames = paths.map(_.steps.head.edge.edgeGroupName).toSet
    assert(edgeNames === Set("KNOWS", "WORKS_AT"))
    paths.foreach { p =>
      assert(p.steps.head.traversedForward === true)
      assert(p.nodes.head.vertexGroupName === "Person")
    }
  }

  test("enumeratePaths: typed edge label prunes unlabelled candidates") {
    // From Person the candidates are KNOWS->Person and WORKS_AT->Company; typing the edge as
    // WORKS_AT keeps only the latter.
    val (nodes, edges) = parsed("MATCH (a:Person)-[:WORKS_AT]->(b)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.length === 1)
    assert(paths.head.steps.head.edge.edgeGroupName === "WORKS_AT")
    assert(paths.head.nodes.map(_.vertexGroupName) === Vector("Person", "Company"))
  }

  test("enumeratePaths: typed next-node label prunes candidates") {
    // From Person (untyped edge), only WORKS_AT lands on Company.
    val (nodes, edges) = parsed("MATCH (a:Person)-[]->(b:Company)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.length === 1)
    assert(paths.head.steps.head.edge.edgeGroupName === "WORKS_AT")
    assert(paths.head.nodes.last.vertexGroupName === "Company")
  }

  test("enumeratePaths: edge label absent from outgoing candidates yields no paths") {
    // Company's only outgoing edge is LOCATED_IN->City; asking for KNOWS yields nothing.
    val (nodes, edges) = parsed("MATCH (a:Company)-[:KNOWS]->(b)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.isEmpty)
  }

  test("enumeratePaths: start group with no outgoing edges yields no forward paths") {
    // City is a pure sink in the schema.
    val (nodes, edges) = parsed("MATCH (a:City)-[]->(b)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.isEmpty)
  }

  test("enumeratePaths: next-node label that no candidate satisfies yields no paths") {
    // From Person, neither KNOWS->Person nor WORKS_AT->Company lands on City.
    val (nodes, edges) = parsed("MATCH (a:Person)-[]->(b:City)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.isEmpty)
  }

  test("enumeratePaths: edge label matches case-insensitively") {
    val (nodes, edges) = parsed("MATCH (a:Person)-[:knows]->(b:Person)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.length === 1)
    // The step carries the schema's canonical edge-group name.
    assert(paths.head.steps.head.edge.edgeGroupName === "KNOWS")
  }

  test("enumeratePaths: next-node label matches case-insensitively") {
    val (nodes, edges) = parsed("MATCH (a:Person)-[]->(b:COMPANY)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.length === 1)
    assert(paths.head.nodes.last.vertexGroupName === "Company")
  }

  test("enumeratePaths: edge variable is preserved on the resolved step") {
    val (nodes, edges) = parsed("MATCH (a:Person)-[e:KNOWS]->(b:Person)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.length === 1)
    assert(paths.head.steps.head.variable === Some("e"))
  }

  // --- Single-hop backward (right-to-left arrow) --------------------------

  test("enumeratePaths: typed backward single hop produces one backward step") {
    val (nodes, edges) = parsed("MATCH (a:Company)<-[:WORKS_AT]-(b:Person)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.length === 1)
    val p = paths.head
    assert(p.length === 1)
    assert(p.steps.head.edge === SchemaEdge("WORKS_AT", "Person", "Company", true))
    assert(p.steps.head.traversedForward === false)
    // Backward step: current node is the edge's dst (Company), next node is the edge's src (Person).
    assert(p.nodes.map(_.vertexGroupName) === Vector("Company", "Person"))
  }

  test("enumeratePaths: untyped backward edge fans out over all incoming edges") {
    // Untyped start + backward edge: each start group enumerates its incoming edges.
    val (nodes, edges) = parsed("MATCH (x)<-[]-(y)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    // Start candidates: Person, Company, City.
    //  - Person:  incoming = KNOWS from Person.   => Person<-KNOWS-Person.
    //  - Company: incoming = WORKS_AT from Person. => Company<-WORKS_AT-Person.
    //  - City:    incoming = LOCATED_IN from Company. => City<-LOCATED_IN-Company.
    assert(paths.length === 3)
    paths.foreach { p =>
      assert(p.length === 1)
      assert(p.steps.head.traversedForward === false)
      // For a backward step, the next node (y) is the edge's src.
      assert(p.nodes.head.vertexGroupName === p.steps.head.edge.dstVertexGroupName)
      assert(p.nodes.last.vertexGroupName === p.steps.head.edge.srcVertexGroupName)
    }
    val edgeNames = paths.map(_.steps.head.edge.edgeGroupName).toSet
    assert(edgeNames === Set("KNOWS", "WORKS_AT", "LOCATED_IN"))
  }

  test("enumeratePaths: sink group has no incoming edges and yields no backward paths") {
    // Person has no incoming-from-outside... actually Person has KNOWS from Person. Use a group
    // with no incoming at all. In this schema every group has an incoming edge, so build a
    // dedicated one.
    val src = SchemaGraphSnapshot(
      vertexGroupNames = Set("A", "B"),
      edges = Vector(SchemaEdge("ab", "A", "B", true)))
    val (nodes, edges) = parsed("MATCH (x:A)<-[]-(y)")
    val paths = Resolver.enumeratePaths(nodes, edges, src, options)
    assert(paths.isEmpty)
  }

  // --- Multi-hop patterns -------------------------------------------------

  test("enumeratePaths: long linear chain resolves to exactly one path") {
    val chain = SchemaGraphSnapshot(
      vertexGroupNames = Set("A", "B", "C", "D"),
      edges = Vector(
        SchemaEdge("e1", "A", "B", true),
        SchemaEdge("e2", "B", "C", true),
        SchemaEdge("e3", "C", "D", true)))
    val (nodes, edges) = parsed("MATCH (x:A)-[]->()-[]->()-[]->(y:D)")
    val paths = Resolver.enumeratePaths(nodes, edges, chain, options)
    assert(paths.length === 1)
    val p = paths.head
    assert(p.length === 3)
    assert(p.nodes.map(_.vertexGroupName) === Vector("A", "B", "C", "D"))
    assert(p.steps.map(_.edge.edgeGroupName) === Vector("e1", "e2", "e3"))
    assert(p.steps.forall(_.traversedForward === true))
  }

  test("enumeratePaths: diamond schema fans out into two reconverging paths") {
    val diamond = SchemaGraphSnapshot(
      vertexGroupNames = Set("A", "B", "C", "D"),
      edges = Vector(
        SchemaEdge("ab", "A", "B", true),
        SchemaEdge("ac", "A", "C", true),
        SchemaEdge("bd", "B", "D", true),
        SchemaEdge("cd", "C", "D", true)))
    val (nodes, edges) = parsed("MATCH (x:A)-[]->()-[]->(y:D)")
    val paths = Resolver.enumeratePaths(nodes, edges, diamond, options)
    assert(paths.length === 2)
    val midGroups = paths.map(_.nodes(1).vertexGroupName).toSet
    assert(midGroups === Set("B", "C"))
    paths.foreach { p =>
      assert(p.length === 2)
      assert(p.nodes.head.vertexGroupName === "A")
      assert(p.nodes.last.vertexGroupName === "D")
      assert(p.steps.forall(_.traversedForward === true))
    }
  }

  test("enumeratePaths: mid-path dead-end prunes an otherwise viable first hop") {
    // Pattern: Person -[]-> () -[]-> Person.
    // Hop-1 candidates from Person: KNOWS->Person, WORKS_AT->Company.
    //  - KNOWS->Person: from Person, KNOWS->Person survives the Person filter; WORKS_AT->Company
    //    is pruned. => one path Person-KNOWS->Person-KNOWS->Person.
    //  - WORKS_AT->Company: from Company, only LOCATED_IN->City, which fails the Person filter.
    //    => no paths.
    // Net: exactly one path survives, demonstrating the DFS prunes mid-chain, not just at the root.
    val (nodes, edges) = parsed("MATCH (a:Person)-[]->()-[]->(b:Person)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.length === 1)
    val p = paths.head
    assert(p.length === 2)
    assert(p.nodes.map(_.vertexGroupName) === Vector("Person", "Person", "Person"))
    assert(p.steps.map(_.edge.edgeGroupName) === Vector("KNOWS", "KNOWS"))
  }

  test("enumeratePaths: self-loop group enumerates a multi-hop chain without special-casing") {
    val (nodes, edges) = parsed("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.length === 1)
    val p = paths.head
    assert(p.length === 2)
    assert(p.steps.forall(_.edge.edgeGroupName === "KNOWS"))
    assert(p.steps.forall(_.traversedForward === true))
    assert(p.nodes.map(_.vertexGroupName) === Vector("Person", "Person", "Person"))
    assert(p.nodes.map(_.variable) === Vector(Some("a"), Some("b"), Some("c")))
  }

  test("enumeratePaths: mixed forward and backward arrows in one pattern") {
    // Two employees of the same company: Person -WORKS_AT-> Company <-WORKS_AT- Person.
    val (nodes, edges) =
      parsed("MATCH (a:Person)-[:WORKS_AT]->(c:Company)<-[:WORKS_AT]-(b:Person)")
    val paths = Resolver.enumeratePaths(nodes, edges, schema, options)
    assert(paths.length === 1)
    val p = paths.head
    assert(p.length === 2)
    assert(p.steps.map(_.traversedForward) === Vector(true, false))
    assert(p.steps.map(_.edge.edgeGroupName) === Vector("WORKS_AT", "WORKS_AT"))
    assert(p.nodes.map(_.vertexGroupName) === Vector("Person", "Company", "Person"))
  }

  // --- Parallel edges & exhaustive fan-out --------------------------------

  test("enumeratePaths: parallel edge groups between the same vertex pair each yield a path") {
    val parallel = SchemaGraphSnapshot(
      vertexGroupNames = Set("A", "B"),
      edges = Vector(SchemaEdge("e1", "A", "B", true), SchemaEdge("e2", "A", "B", true)))
    val (nodes, edges) = parsed("MATCH (a:A)-[]->(b:B)")
    val paths = Resolver.enumeratePaths(nodes, edges, parallel, options)
    assert(paths.length === 2)
    assert(paths.map(_.steps.head.edge.edgeGroupName).toSet === Set("e1", "e2"))
    paths.foreach { p =>
      assert(p.nodes.map(_.vertexGroupName) === Vector("A", "B"))
    }
  }

  test("enumeratePaths: fully untyped multi-hop enumerates every reachable chain") {
    // On the diamond schema, only A has 2-hop chains (A->B->D, A->C->D); B/C/D are dead-ends
    // after one hop. So even with every slot untyped, only 2 paths survive.
    val diamond = SchemaGraphSnapshot(
      vertexGroupNames = Set("A", "B", "C", "D"),
      edges = Vector(
        SchemaEdge("ab", "A", "B", true),
        SchemaEdge("ac", "A", "C", true),
        SchemaEdge("bd", "B", "D", true),
        SchemaEdge("cd", "C", "D", true)))
    val (nodes, edges) = parsed("MATCH (x)-[]->()-[]->(y)")
    val paths = Resolver.enumeratePaths(nodes, edges, diamond, options)
    assert(paths.length === 2)
    paths.foreach { p =>
      assert(p.length === 2)
      assert(p.nodes.head.vertexGroupName === "A")
      assert(p.nodes.last.vertexGroupName === "D")
    }
  }

  test("enumeratePaths: edge-less schema yields no paths for any multi-hop pattern") {
    val noEdges = SchemaGraphSnapshot(vertexGroupNames = Set("A", "B"), edges = Vector.empty)
    val (nodes, edges) = parsed("MATCH (a:A)-[]->(b:B)")
    val paths = Resolver.enumeratePaths(nodes, edges, noEdges, options)
    assert(paths.isEmpty)
  }

  // --- Undirected patterns -------------------------------------------------
  //
  //   KNOWS:    Person -> Person, UNDIRECTED (isDirected = false)
  //   FOLLOWS:  Person -> Person, directed
  //   WORKS_AT: Person -> Company, directed
  private val mixedSchema = SchemaGraphSnapshot(
    vertexGroupNames = Set("Person", "Company"),
    edges = Vector(
      SchemaEdge("KNOWS", "Person", "Person", isDirected = false),
      SchemaEdge("FOLLOWS", "Person", "Person", isDirected = true),
      SchemaEdge("WORKS_AT", "Person", "Company", isDirected = true)))

  test(
    "enumeratePaths: undirected pattern over a directed cross-group edge -> one forward path") {
    val (nodes, edges) = parsed("MATCH (a:Person)-[:WORKS_AT]-(b:Company)")
    val paths = Resolver.enumeratePaths(nodes, edges, mixedSchema, options)
    assert(paths.length === 1)
    assert(paths.head.steps.head.edge.edgeGroupName === "WORKS_AT")
    assert(paths.head.steps.head.traversedForward === true)
    assert(paths.head.nodes.map(_.vertexGroupName) === Vector("Person", "Company"))
  }

  test(
    "enumeratePaths: undirected pattern over a directed edge from the dst side -> one backward path") {
    val (nodes, edges) = parsed("MATCH (a:Company)-[:WORKS_AT]-(b:Person)")
    val paths = Resolver.enumeratePaths(nodes, edges, mixedSchema, options)
    assert(paths.length === 1)
    assert(paths.head.steps.head.traversedForward === false)
    assert(paths.head.nodes.map(_.vertexGroupName) === Vector("Company", "Person"))
  }

  test("enumeratePaths: undirected pattern over a DIRECTED self-loop -> both orientations") {
    // FOLLOWS is directed: (a follows b) and (b follows a) are distinct matches,
    // so an undirected match must surface BOTH as separate paths.
    val (nodes, edges) = parsed("MATCH (a:Person)-[:FOLLOWS]-(b:Person)")
    val paths = Resolver.enumeratePaths(nodes, edges, mixedSchema, options)
    assert(paths.length === 2)
    assert(paths.map(_.steps.head.traversedForward).toSet === Set(true, false))
    paths.foreach { p =>
      assert(p.steps.head.edge.edgeGroupName === "FOLLOWS")
      assert(p.nodes.map(_.vertexGroupName) === Vector("Person", "Person"))
    }
  }

  test(
    "enumeratePaths: undirected pattern over an UNDIRECTED self-loop is de-duplicated to one path") {
    // KNOWS is isDirected = false: getData already unions both orientations, so the
    // resolver must emit a SINGLE forward path -- a second path would double-count.
    val (nodes, edges) = parsed("MATCH (a:Person)-[:KNOWS]-(b:Person)")
    val paths = Resolver.enumeratePaths(nodes, edges, mixedSchema, options)
    assert(paths.length === 1)
    assert(paths.head.steps.head.edge.edgeGroupName === "KNOWS")
    assert(paths.head.steps.head.traversedForward === true)
  }

  test("enumeratePaths: untyped undirected edge from Person fans out, with self-loop dedup") {
    val (nodes, edges) = parsed("MATCH (a:Person)-[]-(b)")
    val paths = Resolver.enumeratePaths(nodes, edges, mixedSchema, options)
    // forward (outgoing): KNOWS->Person, FOLLOWS->Person, WORKS_AT->Company
    // backward (incoming, dst==Person): KNOWS (dropped: undirected self-loop), FOLLOWS (kept)
    val sig =
      paths.map(p => (p.steps.head.edge.edgeGroupName, p.steps.head.traversedForward)).toSet
    assert(
      sig === Set(("KNOWS", true), ("FOLLOWS", true), ("WORKS_AT", true), ("FOLLOWS", false)))
    assert(!sig.contains(("KNOWS", false))) // the undirected self-loop's backward copy is gone
    assert(paths.length === 4)
  }

  test("enumeratePaths: undirected disconnected pattern yields no paths") {
    val (nodes, edges) = parsed("MATCH (a:Company)-[:WORKS_AT]-(b:Company)")
    assert(Resolver.enumeratePaths(nodes, edges, mixedSchema, options).isEmpty)
  }

  // --- Variable-length patterns -------------------------------------------
  //
  // A bounded `*lo..hi` edge desugars into the union of fixed-length paths (one per length in
  // [lo, hi]) with anonymous intermediate nodes. These tests use the `schema` fixture, whose
  // KNOWS edge is a Person->Person self-loop, so a single var-length edge produces several
  // distinct path lengths over the same group.

  test("var-length *1..3 over a self-loop expands to one path per length (1, 2, 3)") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.paths.length === 3)
    assert(rq.paths.map(_.length).sorted === Vector(1, 2, 3))
    rq.paths.foreach { p =>
      assert(p.nodes.forall(_.vertexGroupName == "Person"))
      assert(p.steps.forall(s => s.edge.edgeGroupName == "KNOWS" && s.traversedForward))
    }
  }

  test("var-length keeps endpoint variables and makes intermediate nodes anonymous") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS*1..2]->(b:Person)")
    val rq = Resolver.resolve(ast, schema, options)

    // The 2-hop path is (a) - KNOWS -> (anon) - KNOWS -> (b).
    val twoHop = rq.paths.find(_.length == 2).getOrElse(fail("expected a 2-hop path"))
    assert(twoHop.nodes.head.variable === Some("a"))
    assert(twoHop.nodes.last.variable === Some("b"))
    assert(twoHop.nodes(1).variable === None)
    // The synthetic step carries no edge variable.
    assert(twoHop.steps.forall(_.variable.isEmpty))
  }

  test("var-length *2..2 resolves to exactly the two-hop path") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS*2..2]->(b:Person)")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.paths.length === 1)
    assert(rq.paths.head.length === 2)
    assert(rq.paths.head.nodes(1).variable === None)
  }

  test("var-length *N (exact) matches exactly N hops, not 1..N") {
    // `*3` means EXACTLY three hops -> a single 3-hop path, NOT the union of 1,2,3.
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS*3]->(b:Person)")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.paths.length === 1)
    assert(rq.paths.head.length === 3)
  }

  test("var-length endpoint label filters the final node group") {
    // KNOWS only ever lands on Person, so a Company endpoint yields no paths.
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS*1..2]->(b:Company)")
    val rq = Resolver.resolve(ast, schema, options)
    assert(rq.paths.isEmpty)
  }

  test("untyped var-length fans out over candidate edge groups at every hop") {
    // (a:Person)-[*1..2]->(x):
    //   length 1: Person-KNOWS->Person, Person-WORKS_AT->Company                      (2 paths)
    //   length 2: Person-KNOWS->Person-{KNOWS->Person, WORKS_AT->Company},
    //             Person-WORKS_AT->Company-LOCATED_IN->City                            (3 paths)
    val ast = AstBuilder.parse("MATCH (a:Person)-[*1..2]->(x)")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.paths.length === 5)
    assert(rq.paths.count(_.length == 1) === 2)
    assert(rq.paths.count(_.length == 2) === 3)
  }

  test("var-length spliced before a following fixed hop") {
    // (a:Person)-[:KNOWS*1..2]->(b:Person)-[:WORKS_AT]->(c:Company)
    //   length 2: KNOWS, WORKS_AT
    //   length 3: KNOWS, KNOWS, WORKS_AT
    val ast =
      AstBuilder.parse("MATCH (a:Person)-[:KNOWS*1..2]->(b:Person)-[:WORKS_AT]->(c:Company)")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.paths.length === 2)
    assert(rq.paths.map(_.length).sorted === Vector(2, 3))
    rq.paths.foreach { p =>
      assert(p.steps.last.edge.edgeGroupName === "WORKS_AT")
      assert(p.steps.init.forall(_.edge.edgeGroupName == "KNOWS"))
      assert(p.nodes.last.vertexGroupName === "Company")
    }
  }

  test("scan-local WHERE on a var-length endpoint is attached to that endpoint only") {
    val ast =
      AstBuilder.parse("MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) WHERE a.age > 30")
    val rq = Resolver.resolve(ast, schema, options)

    assert(rq.joinPredicates === Nil)
    assert(rq.postFilters === Nil)
    rq.paths.foreach { p =>
      // `a` is always node 0; the predicate rides there. Intermediates/`b` carry nothing.
      assert(p.nodes.head.scanFilter.length === 1)
      assert(p.nodes.tail.forall(_.scanFilter.isEmpty))
    }
  }

  test("var-length hi above maxVarLength is rejected") {
    // Intended type is a parse/validation error; assert rejection regardless of the exact type.
    intercept[Exception] {
      Resolver.resolve(
        AstBuilder.parse("MATCH (a:Person)-[:KNOWS*1..6]->(b:Person)"),
        schema,
        options
      ) // default maxVarLength = 5
    }
  }

  test("var-length hi above a custom maxVarLength is rejected (bound is on hi, not the span)") {
    // *1..3 has span 2 but max-hops 3; with maxVarLength = 2 it must be rejected.
    intercept[Exception] {
      Resolver.resolve(
        AstBuilder.parse("MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)"),
        schema,
        QueryOptions(maxVarLength = 2))
    }
  }

  test("var-length lo below 1 is rejected") {
    // `*0..2` must be rejected (no zero-length hop in v1), not produce a malformed path.
    intercept[Exception] {
      Resolver.resolve(
        AstBuilder.parse("MATCH (a:Person)-[:KNOWS*0..2]->(b:Person)"),
        schema,
        options)
    }
  }

  test("var-length lo greater than hi is rejected") {
    // `*3..1` is an empty range; it must fail rather than silently yield no paths.
    intercept[Exception] {
      Resolver.resolve(
        AstBuilder.parse("MATCH (a:Person)-[:KNOWS*3..1]->(b:Person)"),
        schema,
        options)
    }
  }
}
