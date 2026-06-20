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

package org.graphframes.propertygraph

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit
import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.InvalidParseException
import org.graphframes.InvalidPropertyGroupException
import org.graphframes.SparkFunSuite
import org.graphframes.propertygraph.property.EdgePropertyGroup
import org.graphframes.propertygraph.property.VertexPropertyGroup

import java.security.MessageDigest

/**
 * End-to-end tests for the public [[PropertyGraphFrame.query]] / [[PropertyGraphFrame.explain]]
 * API: parse -> resolve -> plan -> execute, exercising the full pipeline through the public
 * surface.
 */
class PropertyGraphFrameQuerySuite extends SparkFunSuite with GraphFrameTestSparkContext {

  import sqlImplicits._

  private var pgf: PropertyGraphFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val persons = Seq((1L, "Alice", 30), (2L, "Bob", 40)).toDF("id", "name", "age")
    val companies = Seq((10L, "Acme")).toDF("id", "name")
    val personGroup = VertexPropertyGroup("Person", persons, "id")
    val companyGroup = VertexPropertyGroup("Company", companies, "id")

    val knows = Seq((1L, 2L), (2L, 1L)).toDF("src", "dst")
    val worksAt = Seq((1L, 10L)).toDF("src", "dst")
    val knowsGroup = EdgePropertyGroup(
      "KNOWS",
      knows,
      personGroup,
      personGroup,
      isDirected = true,
      "src",
      "dst",
      lit(1.0))
    val worksAtGroup = EdgePropertyGroup(
      "WORKS_AT",
      worksAt,
      personGroup,
      companyGroup,
      isDirected = true,
      "src",
      "dst",
      lit(1.0))

    pgf = PropertyGraphFrame(Seq(personGroup, companyGroup), Seq(knowsGroup, worksAtGroup))
  }

  // -------------------------------------------------------------------------
  // Path-comparison helpers.
  //
  // The query output uses masked ids (`concat(groupName, sha2(id, 256))`) for every id column,
  // and a `path` array of (edge_property_group, node_id, node_property_group) structs whose final
  // entry carries only the edge group (the end node lives in `end_id`). These helpers let tests
  // express expectations in terms of *raw* ids + group names and assert equality of the full
  // result set, not just the row count.
  // -------------------------------------------------------------------------

  /** Mask an external id the same way [[VertexPropertyGroup]] does internally. */
  private def maskedId(id: Any, groupName: String): String = {
    val md = MessageDigest.getInstance("SHA-256")
    val hash = md.digest(id.toString.getBytes("UTF-8")).map("%02x".format(_)).mkString
    s"$groupName$hash"
  }

  /**
   * One entry of the `path` array. [[nodeId]]/[[nodeGroup]] are [[None]] for the trailing entry
   * of a multi-hop path (where the end node is already captured by `end_id`), and for the single
   * entry of a single-hop path.
   */
  case class ExpectedHop(
      edgeGroup: String,
      nodeId: Option[Long] = None,
      nodeGroup: Option[String] = None)

  object ExpectedHop {

    /** Intermediate hop carrying an intermediate node. */
    def mid(edgeGroup: String, nodeId: Long, nodeGroup: String): ExpectedHop =
      ExpectedHop(edgeGroup, Some(nodeId), Some(nodeGroup))

    /** Trailing hop: edge group only, no node (the end node lives in `end_id`). */
    def last(edgeGroup: String): ExpectedHop = ExpectedHop(edgeGroup, None, None)
  }

  /** A full expected output row, expressed in terms of raw ids and group names. */
  case class ExpectedPath(
      startId: Long,
      startGroup: String,
      endId: Long,
      endGroup: String,
      edgeGroup: String,
      hops: Seq[ExpectedHop])

  /** Normalized representation of an actual DataFrame row (masked ids inlined). */
  private case class ActualPath(
      startId: String,
      startGroup: String,
      endId: String,
      endGroup: String,
      edgeGroup: String,
      hops: Seq[(String, Option[String], Option[String])])

  /** Extract an [[ActualPath]] from a result row. */
  private def rowToActual(row: Row): ActualPath = {
    val pathArr = row
      .get(row.fieldIndex("path"))
      .asInstanceOf[scala.collection.Seq[Row]]
    val hops = pathArr.map { h =>
      val eg = h.getAs[String](0)
      val nodeId = if (h.isNullAt(1)) None else Some(h.getAs[String](1))
      val nodeGroup = if (h.isNullAt(2)) None else Some(h.getAs[String](2))
      (eg, nodeId, nodeGroup)
    }
    ActualPath(
      startId = row.getAs[String]("start_id"),
      startGroup = row.getAs[String]("start_property_group"),
      endId = row.getAs[String]("end_id"),
      endGroup = row.getAs[String]("end_property_group"),
      edgeGroup = row.getAs[String]("edge_property_group"),
      hops = hops.toSeq)
  }

  /** Convert an [[ExpectedPath]] (raw ids) to the comparable [[ActualPath]] (masked ids). */
  private def expectedToActual(p: ExpectedPath): ActualPath = ActualPath(
    startId = maskedId(p.startId, p.startGroup),
    startGroup = p.startGroup,
    endId = maskedId(p.endId, p.endGroup),
    endGroup = p.endGroup,
    edgeGroup = p.edgeGroup,
    hops = p.hops.map { h =>
      (h.edgeGroup, h.nodeId.map(id => maskedId(id, h.nodeGroup.get)), h.nodeGroup)
    })

  /**
   * Collect `df` and assert that its rows match exactly the given [[ExpectedPath]]s (order
   * independent). Compares start/end ids (masked), property groups, the first edge group, and
   * every hop of the `path` array — so it catches missing/extra paths, wrong edge labels, and
   * wrong intermediate nodes, not just row count.
   */
  private def comparePaths(
      df: DataFrame,
      expected: Seq[ExpectedPath]): org.scalatest.Assertion = {
    val actual = df.collect().map(rowToActual).toSet
    val expectedActuals = expected.map(expectedToActual).toSet
    assert(
      actual === expectedActuals,
      s"""|Query result mismatch.
          |  expected (${expectedActuals.size}):
          |${expectedActuals
           .map("    " + _.productIterator.mkString(", "))
           .toVector
           .sorted
           .mkString("\n")}
          |  actual (${actual.size}):
          |${actual.map("    " + _.productIterator.mkString(", ")).toVector.sorted.mkString("\n")}
          |""".stripMargin)
  }

  test("query returns rows over the fixed schema") {
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person)")
    assert(df.count() === 2)
    assert(
      df.schema.fieldNames ===
        Seq(
          "start_id",
          "start_property_group",
          "end_id",
          "end_property_group",
          "edge_property_group",
          "path"))
  }

  test("query with a scan-local filter prunes rows") {
    // Only Bob(40) has age > 30; Bob KNOWS Alice. So exactly one row, starting at Bob.
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 30")
    assert(df.count() === 1)
  }

  test("query with RETURN projects the requested columns") {
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS who")
    assert(df.schema.fieldNames.toSeq === Seq("who"))
    assert(df.count() === 2)
  }

  test("query on a disconnected pattern returns an empty DataFrame without throwing") {
    val df = pgf.query("MATCH (a:Company)-[:KNOWS]->(b:Person)")
    assert(df.count() === 0)
  }

  test("explain(logical) returns a non-empty string describing the resolved plan") {
    val s = pgf.explain("MATCH (a:Person)-[:KNOWS]->(b:Person)")
    assert(s.contains("Logical plan"))
    assert(s.contains("KNOWS"))
  }

  test("explain(physical) returns a non-empty string describing the join plan") {
    val s = pgf.explain("MATCH (a:Person)-[:KNOWS]->(b:Person)", ExplainMode.Physical)
    assert(s.contains("Physical plan"))
    assert(s.contains("join order"))
  }

  test("explain(logical) is the default mode") {
    val s = pgf.explain("MATCH (a:Person)-[:KNOWS]->(b:Person)", ExplainMode.Logical)
    assert(s.contains("Logical plan"))
  }

  test("query with bad syntax throws InvalidParseException") {
    intercept[InvalidParseException] {
      pgf.query("MATCH (a:Person OPTIONAL MATCH")
    }
  }

  test("query with an unknown label throws InvalidPropertyGroupException") {
    intercept[InvalidPropertyGroupException] {
      pgf.query("MATCH (a:Unicorn)-[:KNOWS]->(b:Person)")
    }
  }

  test("QueryOptions.maxSchemaPathLength must be positive") {
    // `PropertyGraphFrame.resolve` runs `require(maxSchemaPathLength > 0)` *before* any per-path
    // depth check, so a non-positive cap is rejected regardless of the query's actual hop count.
    // The WORKS_AT pattern here resolves to a length-1 schema path (a single Person->Company
    // hop), so this guards the positivity precondition, not per-path depth -- the real per-path
    // depth guard is covered by the boundary test below and the explain-bypass test.
    intercept[IllegalArgumentException] {
      pgf.query(
        "MATCH (a:Person)-[:WORKS_AT]->(c:Company)",
        QueryOptions(maxSchemaPathLength = 0))
    }
  }

  test("maxSchemaPathLength per-path guard rejects patterns deeper than the cap") {
    // The 2-hop KNOWS->KNOWS chain resolves to a length-2 schema path. With the cap set to 1 the
    // per-path `require(path.length <= maxSchemaPathLength)` in `resolve` fires; with the cap
    // raised to 2 the same query succeeds and returns the two 2-hop cycles.
    val gql = "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)"
    intercept[IllegalArgumentException] {
      pgf.query(gql, QueryOptions(maxSchemaPathLength = 1))
    }
    val df = pgf.query(gql, QueryOptions(maxSchemaPathLength = 2))
    comparePaths(
      df,
      Seq(
        ExpectedPath(
          startId = 1L,
          startGroup = "Person",
          endId = 1L,
          endGroup = "Person",
          edgeGroup = "KNOWS",
          hops = Seq(ExpectedHop.mid("KNOWS", 2L, "Person"), ExpectedHop.last("KNOWS"))),
        ExpectedPath(
          startId = 2L,
          startGroup = "Person",
          endId = 2L,
          endGroup = "Person",
          edgeGroup = "KNOWS",
          hops = Seq(ExpectedHop.mid("KNOWS", 1L, "Person"), ExpectedHop.last("KNOWS")))))
  }

  test("explain bypasses the maxSchemaPathLength per-path guard so users can inspect the plan") {
    // A 2-hop (length-2) pattern capped at 1: query throws, explain renders the plan instead.
    val gql = "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)"
    val opts = QueryOptions(maxSchemaPathLength = 1)
    intercept[IllegalArgumentException] {
      pgf.query(gql, opts)
    }
    val logical = pgf.explain(gql, ExplainMode.Logical, opts)
    assert(logical.contains("Logical plan"))
    val physical = pgf.explain(gql, ExplainMode.Physical, opts)
    assert(physical.contains("Physical plan"))
  }

  // -------------------------------------------------------------------------
  // Real result checks via comparePaths.
  //
  // Each test below pins down the *exact* set of matched paths (start/end ids,
  // property groups, the first edge group, and every hop of the `path` array),
  // not just the row count. Graph fixture (see beforeAll):
  //
  //   vertices: Person(1,Alice,30) Person(2,Bob,40) Company(10,Acme)
  //   edges:    KNOWS 1->2, 2->1 (directed, Person->Person)
  //             WORKS_AT 1->10    (directed, Person->Company)
  // -------------------------------------------------------------------------

  test("single-hop KNOWS returns exactly the two directed edges with a single-hop path array") {
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person)")
    comparePaths(
      df,
      Seq(
        ExpectedPath(1L, "Person", 2L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS"))),
        ExpectedPath(2L, "Person", 1L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS")))))
  }

  test("single-hop KNOWS includes the first edge group on every row") {
    // The fixed schema surfaces the *first* edge group in `edge_property_group`. For a single-hop
    // query there is only one edge, so every row must carry KNOWS -- never null, never anything
    // else.
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person)")
    val edgeGroups = df.collect().map(_.getAs[String]("edge_property_group")).toSet
    assert(edgeGroups === Set("KNOWS"))
  }

  test("backward arrow <-[:KNOWS]- swaps start and end ids") {
    // knows: 1->2, 2->1. Writing the arrow backwards binds a=dst, b=src, so we get (2,1) and (1,2)
    // -- the same set of vertex pairs but with start/end flipped relative to the forward query.
    val df = pgf.query("MATCH (a:Person)<-[:KNOWS]-(b:Person)")
    comparePaths(
      df,
      Seq(
        ExpectedPath(2L, "Person", 1L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS"))),
        ExpectedPath(1L, "Person", 2L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS")))))
  }

  test("cross-group WORKS_AT path connects Person to Company") {
    val df = pgf.query("MATCH (a:Person)-[:WORKS_AT]->(c:Company)")
    comparePaths(
      df,
      Seq(
        ExpectedPath(
          1L,
          "Person",
          10L,
          "Company",
          "WORKS_AT",
          Seq(ExpectedHop.last("WORKS_AT")))))
  }

  test("edge-label isolation: KNOWS query never returns WORKS_AT paths") {
    // Only the KNOWS rows may appear; the WORKS_AT 1->10 path must NOT leak in even though Person
    // appears on both sides of both edge groups.
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person)")
    val rows = df.collect()
    assert(rows.nonEmpty)
    rows.foreach { r =>
      assert(r.getAs[String]("edge_property_group") === "KNOWS")
      assert(r.getAs[String]("start_property_group") === "Person")
      assert(r.getAs[String]("end_property_group") === "Person")
    }
  }

  test("edge-label isolation: WORKS_AT query never returns KNOWS paths") {
    val df = pgf.query("MATCH (a:Person)-[:WORKS_AT]->(c:Company)")
    val rows = df.collect()
    assert(rows.nonEmpty)
    rows.foreach { r =>
      assert(r.getAs[String]("edge_property_group") === "WORKS_AT")
      assert(r.getAs[String]("end_property_group") === "Company")
    }
  }

  test("untyped edge -[]-> over Person->Person fans out to KNOWS only") {
    // The schema has exactly one outgoing edge from Person back to Person (KNOWS); WORKS_AT points
    // Person->Company. So an untyped Person->Person arrow must resolve to KNOWS and yield the same
    // two rows as the typed KNOWS query -- nothing more.
    val df = pgf.query("MATCH (a:Person)-[]->(b:Person)")
    comparePaths(
      df,
      Seq(
        ExpectedPath(1L, "Person", 2L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS"))),
        ExpectedPath(2L, "Person", 1L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS")))))
  }

  test("untyped trailing node () is resolved by the schema but not surfaced in end_id") {
    // MATCH (a:Person)-[:WORKS_AT]->() : the trailing () resolves to Company (the only dst of
    // WORKS_AT in the schema), so the edge is found and the row is returned. BUT the fixed output
    // schema surfaces only the first/last *named* node ids (see QueryIr Projection.Default), so
    // with only `a` named, end_id falls back to the last named node -- here `a` itself. The edge
    // group is still WORKS_AT, proving the schema-resolved hop was executed.
    val df = pgf.query("MATCH (a:Person)-[:WORKS_AT]->()")
    val rows = df.collect()
    assert(rows.length === 1)
    val r = rows.head
    assert(r.getAs[String]("start_property_group") === "Person")
    assert(r.getAs[String]("edge_property_group") === "WORKS_AT")
    // Quirk: with no named end variable, end_id/end_property_group mirror the last named node (a).
    assert(r.getAs[String]("end_id") === r.getAs[String]("start_id"))
    assert(r.getAs[String]("end_property_group") === "Person")
  }

  test("scan-local WHERE a.age > 30 leaves exactly the Bob->Alice KNOWS row") {
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 30")
    // Only Bob(40) passes the scan filter; Bob KNOWS Alice. Exactly one row.
    comparePaths(
      df,
      Seq(ExpectedPath(2L, "Person", 1L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS")))))
  }

  test("join-level WHERE a.age > b.age filters the joined frame") {
    // KNOWS: 1(30)->2(40), 2(40)->1(30). a.age > b.age: only 40 > 30, i.e. Bob->Alice.
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > b.age")
    comparePaths(
      df,
      Seq(ExpectedPath(2L, "Person", 1L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS")))))
  }

  test("RETURN a.name AS who projects the exact source-name values") {
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS who")
    assert(df.schema.fieldNames.toSeq === Seq("who"))
    // KNOWS: 1->2 (Alice), 2->1 (Bob). The projected `who` set is exactly {Alice, Bob}.
    val names = df.collect().map(_.getString(0)).toSet
    assert(names === Set("Alice", "Bob"))
  }

  test("RETURN a.name, b.name projects both endpoint names in order") {
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
    assert(df.schema.fieldNames.toSeq === Seq("name", "name"))
    val pairs = df.collect().map(r => (r.getString(0), r.getString(1))).toSet
    assert(pairs === Set(("Alice", "Bob"), ("Bob", "Alice")))
  }

  test("disconnected pattern yields an empty result set with the fixed schema") {
    val df = pgf.query("MATCH (a:Company)-[:KNOWS]->(b:Person)")
    assert(df.count() === 0)
    assert(
      df.schema.fieldNames ===
        Seq(
          "start_id",
          "start_property_group",
          "end_id",
          "end_property_group",
          "edge_property_group",
          "path"))
  }

  test("disconnected edge label yields an empty result set") {
    // There is no WORKS_AT edge touching another Person, so Person-[:WORKS_AT]->Person is empty.
    val df = pgf.query("MATCH (a:Person)-[:WORKS_AT]->(b:Person)")
    assert(df.count() === 0)
  }

  test("multi-hop path array carries intermediate node ids and groups") {
    // The 2-hop chain Person-[:WORKS_AT]->Company-[:LOCATED_IN]->City is not present in this small
    // fixture, but a 2-hop KNOWS->KNOWS chain is. Here we instead use a 2-hop KNOWS->KNOWS query
    // to verify the `path` array structure: two entries, the first with the intermediate Person
    // node, the second carrying only the edge group.
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)")
    // knows: 1->2, 2->1. A 2-hop chain a-b-c requires b's KNOWS-out to land on c. The only valid
    // 2-hop chains are 1->2->1 and 2->1->2.
    comparePaths(
      df,
      Seq(
        ExpectedPath(
          startId = 1L,
          startGroup = "Person",
          endId = 1L,
          endGroup = "Person",
          edgeGroup = "KNOWS",
          hops = Seq(ExpectedHop.mid("KNOWS", 2L, "Person"), ExpectedHop.last("KNOWS"))),
        ExpectedPath(
          startId = 2L,
          startGroup = "Person",
          endId = 2L,
          endGroup = "Person",
          edgeGroup = "KNOWS",
          hops = Seq(ExpectedHop.mid("KNOWS", 1L, "Person"), ExpectedHop.last("KNOWS")))))
  }

  test("multi-hop path array length equals the number of steps") {
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)")
    df.collect().foreach { row =>
      val pathArr = row
        .get(row.fieldIndex("path"))
        .asInstanceOf[scala.collection.Seq[_]]
      assert(pathArr.length === 2, s"2-step path must produce a 2-entry array: $row")
    }
  }

  test("combined scan-local and join predicates both apply") {
    // a.age > 30 keeps only Bob as `a` (scan-local). b.age > 30 on Alice(30) vs Bob(40)... Alice
    // is 30 so b.age > 30 is false. Result: empty.
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 30 AND b.age > 30")
    assert(df.count() === 0)
  }

  // -------------------------------------------------------------------------
  // ExpressionLowering coverage.
  //
  // The tests above only exercise `>` (Comparison.Gt), `AND`, and integer literals. The tests
  // below pin down the remaining branches of `ExpressionLowering.lower` end-to-end through the
  // public query API: OR, NOT, string literals, the rest of the comparison operators (Neq, Lte,
  // Gte), arithmetic (+/-) in WHERE and RETURN, scan-local filters on the non-start variable, and
  // non-adjacent post-filters. Same fixture:
  //   Person(1,Alice,30) Person(2,Bob,40); KNOWS 1->2, 2->1.
  // -------------------------------------------------------------------------

  test("WHERE OR keeps only the row matching either disjunct") {
    // a.age > 35 keeps only Bob(40); a.age < 28 keeps neither. OR -> only Bob remains as `a`,
    // and Bob KNOWS Alice, so exactly the Bob->Alice row.
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 35 OR a.age < 28")
    comparePaths(
      df,
      Seq(ExpectedPath(2L, "Person", 1L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS")))))
  }

  test("WHERE NOT negates a comparison predicate") {
    // NOT (a.age > 35) keeps Alice(30) and excludes Bob(40). Alice KNOWS Bob -> Alice->Bob.
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE NOT (a.age > 35)")
    comparePaths(
      df,
      Seq(ExpectedPath(1L, "Person", 2L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS")))))
  }

  test("WHERE string equality filters on a string property") {
    // Exercises Comparison(Eq) with a String Literal against a string column. a.name = 'Alice'
    // keeps only Alice as `a`; Alice KNOWS Bob -> Alice->Bob.
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice'")
    comparePaths(
      df,
      Seq(ExpectedPath(1L, "Person", 2L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS")))))
  }

  test("WHERE <> (not-equal) prunes the matching endpoint") {
    // Comparison.Neq via `<>`. a.age <> 40 excludes Bob(40) and keeps Alice(30) -> Alice->Bob.
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age <> 40")
    comparePaths(
      df,
      Seq(ExpectedPath(1L, "Person", 2L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS")))))
  }

  test("WHERE <= keeps rows at or below the threshold") {
    // Comparison.Lte. a.age <= 30 keeps only Alice(30) -> Alice->Bob.
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age <= 30")
    comparePaths(
      df,
      Seq(ExpectedPath(1L, "Person", 2L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS")))))
  }

  test("WHERE >= keeps rows at or above the threshold") {
    // Comparison.Gte. a.age >= 40 keeps only Bob(40) -> Bob->Alice.
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age >= 40")
    comparePaths(
      df,
      Seq(ExpectedPath(2L, "Person", 1L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS")))))
  }

  test("arithmetic Minus in WHERE participates in the join filter") {
    // Arithmetic(Minus) lowered inside a join-level predicate spanning adjacent a,b. a.age - 5:
    //   Alice->Bob: 30 - 5 = 25 > 40 ? no
    //   Bob->Alice: 40 - 5 = 35 > 30 ? yes  -> only Bob->Alice.
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age - 5 > b.age")
    comparePaths(
      df,
      Seq(ExpectedPath(2L, "Person", 1L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS")))))
  }

  test("arithmetic Plus in RETURN projects a computed column") {
    // Arithmetic(Plus) inside a RETURN item, projected under an explicit alias. Exercises the
    // Projection.Items path with a non-trivial expression (not a bare Variable/PropertyAccess),
    // so the alias must be taken from the AS clause.
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.age + 1 AS adjusted")
    assert(df.schema.fieldNames.toSeq === Seq("adjusted"))
    // KNOWS: 1->2 (Alice,30), 2->1 (Bob,40). adjusted = {31, 41}. The arithmetic result column
    // is LongType (integer column + integer literal promotes), so read it back as Long.
    val adjusted = df.collect().map(_.getLong(0)).toSet
    assert(adjusted === Set(31L, 41L))
  }

  test("RETURN * projects the fixed output schema") {
    // Projection.Star is handled together with Projection.Default in QueryExecutor.project, so
    // RETURN * must yield the same 6-column fixed schema and the same rows as the no-RETURN query.
    val star = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN *")
    assert(
      star.schema.fieldNames ===
        Seq(
          "start_id",
          "start_property_group",
          "end_id",
          "end_property_group",
          "edge_property_group",
          "path"))
    comparePaths(
      star,
      Seq(
        ExpectedPath(1L, "Person", 2L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS"))),
        ExpectedPath(2L, "Person", 1L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS")))))
  }

  test("single-node pattern returns each vertex with an empty path") {
    // A 0-hop path (path.steps.isEmpty): the single node is both start and end, edge_property_group
    // is null, and the path array is empty. Exercises the degenerate single-node branch of
    // QueryExecutor.executePlan / project.
    val df = pgf.query("MATCH (a:Person)")
    assert(df.count() === 2)
    val rows = df.collect()
    rows.foreach { r =>
      assert(r.getAs[String]("start_property_group") === "Person")
      assert(r.getAs[String]("end_property_group") === "Person")
      // With only one named node, start and end both reference `a`.
      assert(r.getAs[String]("start_id") === r.getAs[String]("end_id"))
      assert(r.isNullAt(r.fieldIndex("edge_property_group")))
      val pathArr = r.get(r.fieldIndex("path")).asInstanceOf[scala.collection.Seq[_]]
      assert(pathArr.isEmpty, s"0-hop path array must be empty: $r")
    }
    // The two vertices are Alice(1) and Bob(2).
    val startIds = rows.map(_.getAs[String]("start_id")).toSet
    assert(startIds === Set(maskedId(1L, "Person"), maskedId(2L, "Person")))
  }

  test("scan-local filter on the non-start variable prunes rows") {
    // b.age > 35 references only `b`, so the resolver attaches it as a scan-local filter to the
    // `b` node rather than the start `a`. Only Bob(40) passes as `b`; the only KNOWS edge into
    // Bob is 1->2, so exactly Alice->Bob survives.
    val df = pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.age > 35")
    comparePaths(
      df,
      Seq(ExpectedPath(1L, "Person", 2L, "Person", "KNOWS", Seq(ExpectedHop.last("KNOWS")))))
  }

  test("non-adjacent post-filter spans the first and last variable") {
    // A 3-variable predicate where a and c are not adjacent is classified as a post-filter and
    // applied after the join tree. The 2-hop KNOWS->KNOWS chains are 1->2->1 and 2->1->2, so a
    // and c are always the same vertex -- hence a.age > c.age is always false and the result is
    // empty. (Without the post-filter, 2 rows would be returned, so this catches regressions
    // where the post-filter is dropped.)
    val df =
      pgf.query("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) WHERE a.age > c.age")
    assert(df.count() === 0)
  }

  // ---------------------------------------------------------------------------
  // Scan reuse: output-only property join-back (end-to-end through the public API).
  // ---------------------------------------------------------------------------

  test("multi-hop output-only join-back resolves a Company property through RETURN") {
    // `c.name` is RETURN-only (no filter references it) -> output-only -> terminal join-back on the
    // masked id. WORKS_AT: (1,10) Alice -> Acme. So exactly one row, `name` = "Acme". (Per the
    // Items-projection convention, the output column is named after the property, `name`.)
    val df = pgf.query("MATCH (a:Person)-[:WORKS_AT]->(c:Company) RETURN c.name")
    assert(df.schema.fieldNames.toSeq === Seq("name"))
    val names = df.collect().map(_.getString(0)).toSet
    assert(names === Set("Acme"))
  }

  test("mixed carry + output-only resolves both a carried and a join-backed column end-to-end") {
    // `a.age` is referenced by both the filter (`a.age >= 30`) and RETURN -> carried (no join-back).
    // `a.name` is RETURN-only -> output-only -> join-backed. WORKS_AT only has Alice(1)->Acme, and
    // Alice's age is 30, so `a.age >= 30` keeps exactly Alice. Result: one row (Alice, 30). The
    // `age` column is read as Int (its physical type in the fixture).
    val df = pgf.query(
      "MATCH (a:Person)-[:WORKS_AT]->(c:Company) WHERE a.age >= 30 RETURN a.name, a.age")
    val rows = df.collect().map(r => (r.getString(0), r.getInt(1))).toSet
    assert(rows === Set(("Alice", 30)))
  }
}
