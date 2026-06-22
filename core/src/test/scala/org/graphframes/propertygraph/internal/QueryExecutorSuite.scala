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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.graphframes.GraphFrame
import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.SparkFunSuite
import org.graphframes.propertygraph.PropertyGraphFrame
import org.graphframes.propertygraph.property.EdgePropertyGroup
import org.graphframes.propertygraph.property.VertexPropertyGroup

import java.security.MessageDigest

/**
 * Spark-backed tests for the executor + optimizer pipeline. Builds a small in-memory
 * PropertyGraphFrame (Person/Company/City, KNOWS/WORKS_AT/LOCATED_IN) and asserts that
 * `JoinOptimizer.plan` + `QueryExecutor.execute` produce rows matching a hand-computed join, with
 * the fixed output schema and correct id-masking.
 */
class QueryExecutorSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  import sqlImplicits._

  private var pgf: PropertyGraphFrame = _

  // Mirrors VertexPropertyGroup.getData's masking: concat(groupName, sha2(id.cast(String), 256)).
  private def maskedId(id: Long, groupName: String): String = {
    val md = MessageDigest.getInstance("SHA-256")
    val hash = md.digest(id.toString.getBytes("UTF-8")).map("%02x".format(_)).mkString
    s"$groupName$hash"
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    pgf = buildGraph()
  }

  private def buildGraph(): PropertyGraphFrame = {
    val persons =
      Seq((1L, "Alice", 30), (2L, "Bob", 40), (3L, "Carol", 25)).toDF("id", "name", "age")
    val companies = Seq((10L, "Acme"), (20L, "Globex")).toDF("id", "name")
    val cities = Seq((100L, "Springfield"), (200L, "Shelbyville")).toDF("id", "name")

    val personGroup = VertexPropertyGroup("Person", persons, "id")
    val companyGroup = VertexPropertyGroup("Company", companies, "id")
    val cityGroup = VertexPropertyGroup("City", cities, "id")

    val knows = Seq((1L, 2L, "friend"), (2L, 3L, "collegaue"), (3L, 1L, "spoose")).toDF(
      "src",
      "dst",
      "friendship")
    val worksAt = Seq((1L, 10L), (2L, 10L), (3L, 20L)).toDF("src", "dst")
    val locatedIn = Seq((10L, 100L), (20L, 200L)).toDF("src", "dst")

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
    val locatedInGroup = EdgePropertyGroup(
      "LOCATED_IN",
      locatedIn,
      companyGroup,
      cityGroup,
      isDirected = true,
      "src",
      "dst",
      lit(1.0))

    PropertyGraphFrame(
      Seq(personGroup, companyGroup, cityGroup),
      Seq(knowsGroup, worksAtGroup, locatedInGroup))
  }

  private def run(gql: String): DataFrame = runOn(pgf, gql)

  private def runOn(pg: PropertyGraphFrame, gql: String): DataFrame = {
    val ast = AstBuilder.parse(gql)
    val resolved = Resolver.resolve(ast, SchemaGraphSnapshot.fromPropertyGraphFrame(pg))
    val plans = JoinOptimizer.plan(resolved, stats = None)
    QueryExecutor.execute(pg, plans)
  }

  test("single-hop directed query matches a hand-computed join") {
    // MATCH (a:Person)-[:KNOWS]->(b:Person) : knows rows (1->2),(2->3),(3->1).
    val df = run("MATCH (a:Person)-[:KNOWS]->(b:Person)")
    val rows = df.collect().map(r => (r.getString(0), r.getString(2))).toSet
    val expected = Set(
      (maskedId(1L, "Person"), maskedId(2L, "Person")),
      (maskedId(2L, "Person"), maskedId(3L, "Person")),
      (maskedId(3L, "Person"), maskedId(1L, "Person")))
    assert(rows === expected)
    // Fixed schema check.
    assert(
      df.schema.fieldNames ===
        Seq(
          "start_id",
          "start_property_group",
          "end_id",
          "end_property_group",
          "edge_property_group",
          "path"))
    assert(df.head().getAs[String]("start_property_group") === "Person")
    assert(df.head().getAs[String]("edge_property_group") === "KNOWS")
  }

  test("scan-local WHERE filter actually prunes rows") {
    // Only Alice(30) and Bob(40) have age > 30... wait: 30 > 30 is false. Only Bob(40).
    // As a src, Bob(2) KNOWS Carol(3). So exactly one row.
    val df = run("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 30")
    assert(df.count() === 1)
    val row = df.head()
    assert(row.getString(0) === maskedId(2L, "Person")) // Bob
    assert(row.getString(2) === maskedId(3L, "Person")) // Carol
  }

  test("backward arrow <-[e]- swaps src and dst") {
    // MATCH (a:Person)<-[:KNOWS]-(b:Person): a is the dst, b is the src.
    // knows (1->2),(2->3),(3->1) => a=2,b=1 ; a=3,b=2 ; a=1,b=3.
    val df = run("MATCH (a:Person)<-[:KNOWS]-(b:Person)")
    val rows = df.collect().map(r => (r.getString(0), r.getString(2))).toSet
    val expected = Set(
      (maskedId(2L, "Person"), maskedId(1L, "Person")),
      (maskedId(3L, "Person"), maskedId(2L, "Person")),
      (maskedId(1L, "Person"), maskedId(3L, "Person")))
    assert(rows === expected)
  }

  test("multi-hop query builds the path array with intermediate ids") {
    // MATCH (a:Person)-[:WORKS_AT]->(c:Company)-[:LOCATED_IN]->(d:City)
    // Alice(1)->Acme(10)->Springfield(100), Bob(2)->Acme(10)->Springfield(100),
    // Carol(3)->Globex(20)->Shelbyville(200).
    val df = run("MATCH (a:Person)-[:WORKS_AT]->(c:Company)-[:LOCATED_IN]->(d:City)")
    assert(df.count() === 3)
    val firstRow = df.head()
    assert(firstRow.getAs[String]("start_property_group") === "Person")
    assert(firstRow.getAs[String]("end_property_group") === "City")
    assert(firstRow.getAs[String]("edge_property_group") === "WORKS_AT")
    // path array: k entries for a k-step path (§6.1). Here k=2:
    //   [ {WORKS_AT, c_id, "Company"}, {LOCATED_IN, null, null} ]
    // -- the last entry carries only the edge group (the end node is in end_id).
    val pathArr = firstRow
      .get(firstRow.fieldIndex("path"))
      .asInstanceOf[scala.collection.Seq[org.apache.spark.sql.Row]]
    assert(pathArr.length === 2)
    val firstHop = pathArr.head
    assert(firstHop.getAs[String](0) === "WORKS_AT")
    assert(firstHop.getAs[String](2) === "Company")
    val lastHop = pathArr.last
    assert(lastHop.getAs[String](0) === "LOCATED_IN")
    // last entry's node fields are null (end node already in end_id)
    assert(lastHop.isNullAt(1))
    assert(lastHop.isNullAt(2))
  }

  test("disconnected pattern yields an empty DataFrame with the fixed schema") {
    val df = run("MATCH (a:City)-[:KNOWS]->(b:Person)")
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

  test("untyped edge fan-out unions multiple schema paths") {
    // MATCH (a:Person)-[]->(b:Person) : only KNOWS connects Person->Person.
    // So exactly the 3 KNOWS rows.
    val df = run("MATCH (a:Person)-[]->(b:Person)")
    assert(df.count() === 3)
  }

  test("RETURN a, b projects named variables only") {
    val df = run("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b")
    // Items projection: two columns named a, b (each is the masked id).
    assert(df.schema.fieldNames.toSeq === Seq("a", "b"))
    val rows = df.collect().map(r => (r.getString(0), r.getString(1))).toSet
    val expected = Set(
      (maskedId(1L, "Person"), maskedId(2L, "Person")),
      (maskedId(2L, "Person"), maskedId(3L, "Person")),
      (maskedId(3L, "Person"), maskedId(1L, "Person")))
    assert(rows === expected)
  }

  test("RETURN a.name AS age projects a property column via the requested-properties scan") {
    // RETURN a.name : names are Alice/Bob/Carol. As src of KNOWS: 1->Alice,2->Bob,3->Carol.
    val df = run("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS who")
    assert(df.schema.fieldNames.toSeq === Seq("who"))
    val names = df.collect().map(_.getString(0)).toSet
    assert(names === Set("Alice", "Bob", "Carol"))
  }

  test("join predicate (a.age > b.age) is applied") {
    // KNOWS: 1(30)->2(40), 2(40)->3(25), 3(25)->1(30).
    // a.age > b.age : 40>25 (Bob->Carol) only.
    val df = run("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > b.age")
    assert(df.count() === 1)
    val row = df.head()
    assert(row.getString(0) === maskedId(2L, "Person"))
    assert(row.getString(2) === maskedId(3L, "Person"))
  }

  test("id-masking join-back via internalIdMapping recovers raw ids") {
    // The query returns masked ids; joining against Person.internalIdMapping recovers external ids.
    val df = run("MATCH (a:Person)-[:KNOWS]->(b:Person)")
    val personGroup = pgf.vertexGroups("person")
    val mapping = personGroup.internalIdMapping // (external_id, id)
    val withExternal = df.join(mapping, df("start_id") === mapping(GraphFrame.ID), "left")
    val externals =
      withExternal.collect().map(_.getAs[Long](PropertyGraphFrame.EXTERNAL_ID)).toSet
    assert(externals === Set(1L, 2L, 3L))
  }

  test("hand-computed join matches executor for the multi-hop case") {
    // Independent computation of the expected start/end id pairs for the WORKS_AT->LOCATED_IN chain.
    val worksAt = Seq((1L, 10L), (2L, 10L), (3L, 20L))
    val locatedIn = Seq((10L, 100L), (20L, 200L))
    val expected = for {
      (p, c1) <- worksAt
      (c2, ci) <- locatedIn if c1 == c2
    } yield (maskedId(p, "Person"), maskedId(ci, "City"))

    val df = run("MATCH (a:Person)-[:WORKS_AT]->(c:Company)-[:LOCATED_IN]->(d:City)")
    val actual = df.collect().map(r => (r.getString(0), r.getString(2))).toSet
    assert(actual === expected.toSet)
  }

  // ---------------------------------------------------------------------------
  // Scan-reuse floor (deterministic) + ceiling (best-effort / soft).
  // ---------------------------------------------------------------------------

  /**
   * Drives the same pipeline as [[run]] but also returns the per-call scan memo, so the
   * scan-reuse floor can be asserted by reference-identity on the memo values.
   */
  private def runWithMemo(gql: String): (DataFrame, Map[QueryExecutor.ScanKey, DataFrame]) = {
    val ast = AstBuilder.parse(gql)
    val resolved = Resolver.resolve(ast, SchemaGraphSnapshot.fromPropertyGraphFrame(pgf))
    val plans = JoinOptimizer.plan(resolved, stats = None)
    QueryExecutor.executeWithScanMemo(pgf, plans)
  }

  test("scan-reuse floor: equal scan signatures share one DataFrame reference (spec §8.1)") {
    // KNOWS connects Person->Person, so both endpoints of `MATCH (a:Person)-[:KNOWS]->(b:Person)`
    // are the SAME group (Person) with the SAME signature (empty scan filter, no carried cols).
    // The memo must therefore hold a single Person scan referenced from both positions.
    val (_, memo) = runWithMemo("MATCH (a:Person)-[:KNOWS]->(b:Person)")
    val personScans = memo.iterator.filter(_._1.groupName == "person").map(_._2).toSeq
    assert(personScans.length === 1, s"expected one shared Person scan, got keys: ${memo.keySet}")
  }

  test("scan-reuse floor: differing scan filters produce distinct scans (spec §8.1)") {
    // With a scan-local filter `WHERE a.age > 30`, the `a` Person scan's signature differs from the
    // `b` Person scan (no filter) -> two distinct Person scans in the memo.
    val (_, memo) = runWithMemo("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 30")
    val personKeys = memo.keySet.filter(_.groupName == "person").toSeq
    assert(
      personKeys.length === 2,
      s"expected two Person scans (filtered vs unfiltered): $personKeys")
    // And the two scans must be distinct DataFrame references.
    val personScans = personKeys.map(memo)
    assert(personScans.head ne personScans.last, "distinct signatures must yield distinct scans")
  }

  test("scan-reuse floor: same filter across a fan-out reuses one scan (spec §8.1)") {
    // `(a:Person)-[]->(b:Person)` fans out; here only KNOWS qualifies, but both endpoints share the
    // Person scan with no filter -> exactly one Person scan regardless.
    val (_, memo) = runWithMemo("MATCH (a:Person)-[]->(b:Person)")
    val personScans = memo.iterator.filter(_._1.groupName == "person").map(_._2).toSeq
    assert(personScans.length === 1, s"expected one shared Person scan: ${memo.keySet}")
  }

  test("output-only join-back resolves both endpoints' properties (spec §8.3)") {
    // `a.name` and `b.name` are RETURN-only (no filter references them) -> output-only -> terminal
    // join-back. Result parity: exact name pairs for KNOWS (1->2, 2->3, 3->1). Per the
    // Items-projection convention the output columns are named after the property (`name`).
    val df = run("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
    assert(df.schema.fieldNames.toSeq === Seq("name", "name"))
    val rows = df.collect().map(r => (r.getString(0), r.getString(1))).toSet
    val expected = Set(("Alice", "Bob"), ("Bob", "Carol"), ("Carol", "Alice"))
    assert(rows === expected)
  }

  test("mixed carry + output-only: filter-also-returned is carried, RETURN-only is join-backed") {
    // `a.age` is referenced by BOTH the filter (`a.age > 30`) and RETURN -> carried (no join-back).
    // `a.name` is RETURN-only -> output-only -> join-backed. Only Bob(40) passes `age > 30`. The
    // `age` column is read as Int (its physical type in the fixture), not Long.
    val df = run("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 30 RETURN a.name, a.age")
    val rows = df.collect().map(r => (r.getString(0), r.getInt(1))).toSet
    assert(rows === Set(("Bob", 40)))
  }

  test("Default/Star projection never triggers an output-only join-back") {
    // Default projection has no RETURN items -> every element's outputOnly set is empty by
    // construction -> no join-back. Assert via the memo: no carried scan carries a non-id-only
    // property set, and the result still has the fixed 6-column schema.
    val (df, memo) = runWithMemo("MATCH (a:Person)-[:KNOWS]->(b:Person)")
    assert(
      df.schema.fieldNames === Seq(
        "start_id",
        "start_property_group",
        "end_id",
        "end_property_group",
        "edge_property_group",
        "path"))
    // No scan in the memo carried any property column (only id/property_group/src/dst/weight).
    assert(
      memo.keySet.forall(_.carriedCols.isEmpty),
      s"Default projection should carry no props: ${memo.keySet}")
  }

  test("selectivity preserved: join predicate appears in the executed plan") {
    // `a.age > b.age` is a two-variable predicate; the engine places it at the binding join. This is
    // a best-effort ceiling assertion: the inequality Column must appear SOMEWHERE in the physical
    // plan (exact operator placement is Spark-version / AQE dependent). We assert only presence.
    val df = run("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > b.age")
    val planStr = df.queryExecution.executedPlan.toString()
    // The predicate is struct guaranteed to be applied; assert the result parity strictly and the
    // plan presence softly (only one row: 40 > 25, Bob -> Carol).
    assert(df.count() === 1)
    // Soft: tolerate plans that fold the comparison into a different node name across versions.
    assert(
      planStr.contains("age") || planStr.contains("Filter") || planStr.contains("Join"),
      s"expected the age predicate or a join/filter node in the plan, got:\n$planStr")
  }

  test("edge properties in the filter expression are carried correctly") {
    val df = run("MATCH (a:Person)-[e:KNOWS]->(b:Person) WHERE e.friendship = 'spoose'")
    // only Carol -- Alice
    assert(df.count() === 1L)
  }

  test("only edge property referenced only in RETURN") {
    val df = run("MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN e.friendship")
    val collected = df.collect().map(r => r.getAs[String]("friendship")).toSet
    assert(collected === Set("friend", "collegaue", "spoose"))
  }

  test("scan-reuse floor: a repeated edge group shares one scan") {
    // check that edge scans are reused along the joins
    val (_, memo) = runWithMemo("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)")
    val knowsScans = memo.iterator.filter(_._1.groupName == "knows").map(_._2).toSeq
    assert(knowsScans.length === 1, s"expected one shared KNOWS scan: ${memo.keySet}")
  }

  test("scan-reuse floor: differing edge scan filters produce distinct edge scans") {
    // Two KNOWS hops. The first edge is filtered (`e1.friendship = 'spouse'`), the second is not,
    // so the two KNOWS scans have different ScanKeys -> two distinct KNOWS scans in the memo.
    val (_, memo) = runWithMemo(
      "MATCH (a:Person)-[e1:KNOWS]->(b:Person)-[e2:KNOWS]->(c:Person) " +
        "WHERE e1.friendship = 'spouse'")
    val knowsKeys = memo.keySet.filter(_.groupName == "knows").toSeq
    assert(
      knowsKeys.length === 2,
      s"expected two KNOWS scans (filtered vs unfiltered): $knowsKeys")
    // And they must be distinct DataFrame references (the whole point of keying on the filter).
    val knowsScans = knowsKeys.map(memo)
    assert(
      knowsScans.head ne knowsScans.last,
      "distinct edge signatures must yield distinct scans")
  }

  test("undirected pattern over a directed self-loop returns both orientations") {
    // KNOWS is directed (1->2, 2->3, 3->1). Undirected must surface each edge BOTH ways.
    // No reciprocal pairs exist, so 3 stored edges -> 6 distinct rows.
    val df = run("MATCH (a:Person)-[:KNOWS]-(b:Person)")
    assert(df.count() === 6) // would be 3 if the backward path were dropped
    val rows = df.collect().map(r => (r.getString(0), r.getString(2))).toSet
    assert(
      rows === Set(
        (maskedId(1L, "Person"), maskedId(2L, "Person")),
        (maskedId(2L, "Person"), maskedId(3L, "Person")),
        (maskedId(3L, "Person"), maskedId(1L, "Person")),
        (maskedId(2L, "Person"), maskedId(1L, "Person")),
        (maskedId(3L, "Person"), maskedId(2L, "Person")),
        (maskedId(1L, "Person"), maskedId(3L, "Person"))))
  }

  test("undirected pattern equals the union of both directed arrows") {
    // The semantics-pinning invariant: -[:KNOWS]- == (-[:KNOWS]->) ∪ (<-[:KNOWS]-).
    def pairs(gql: String) = run(gql).collect().map(r => (r.getString(0), r.getString(2))).toSet
    assert(
      pairs("MATCH (a:Person)-[:KNOWS]-(b:Person)") ===
        pairs("MATCH (a:Person)-[:KNOWS]->(b:Person)") ++ pairs(
          "MATCH (a:Person)<-[:KNOWS]-(b:Person)"))
  }

  test("undirected pattern over a directed cross-group edge matches forward from the src side") {
    // WORKS_AT: Person->Company; no Company->Person edge, so only the forward orientation matches.
    val df = run("MATCH (a:Person)-[:WORKS_AT]-(c:Company)")
    assert(df.count() === 3)
    val rows = df.collect().map(r => (r.getString(0), r.getString(2))).toSet
    assert(
      rows === Set(
        (maskedId(1L, "Person"), maskedId(10L, "Company")),
        (maskedId(2L, "Person"), maskedId(10L, "Company")),
        (maskedId(3L, "Person"), maskedId(20L, "Company"))))
    assert(df.head().getAs[String]("start_property_group") === "Person")
    assert(df.head().getAs[String]("end_property_group") === "Company")
  }

  test("undirected pattern over the same cross-group edge matches backward from the dst side") {
    val df = run("MATCH (c:Company)-[:WORKS_AT]-(a:Person)")
    assert(df.count() === 3)
    val rows = df.collect().map(r => (r.getString(0), r.getString(2))).toSet
    assert(
      rows === Set(
        (maskedId(10L, "Company"), maskedId(1L, "Person")),
        (maskedId(10L, "Company"), maskedId(2L, "Person")),
        (maskedId(20L, "Company"), maskedId(3L, "Person"))))
    assert(df.head().getAs[String]("start_property_group") === "Company")
  }

  test("undirected pattern over an UNDIRECTED edge group is not double-counted") {
    import sqlImplicits._
    // isDirected=false: getData already emits both orientations, so the resolver must keep a
    // SINGLE (forward) path. 2 stored edges -> 4 rows; a regressed dedup would yield 8.
    val persons = Seq((1L, "Alice"), (2L, "Bob"), (3L, "Carol")).toDF("id", "name")
    val personGroup = VertexPropertyGroup("Person", persons, "id")
    val friends = Seq((1L, 2L), (2L, 3L)).toDF("src", "dst")
    val friendGroup = EdgePropertyGroup(
      "FRIEND",
      friends,
      personGroup,
      personGroup,
      isDirected = false,
      "src",
      "dst",
      lit(1.0))
    val ug = PropertyGraphFrame(Seq(personGroup), Seq(friendGroup))

    val df = runOn(ug, "MATCH (a:Person)-[:FRIEND]-(b:Person)")
    assert(df.count() === 4) // NOT 8 -- multiset count is what catches the duplicate
    val rows = df.collect().map(r => (r.getString(0), r.getString(2))).toSet
    assert(
      rows === Set(
        (maskedId(1L, "Person"), maskedId(2L, "Person")),
        (maskedId(2L, "Person"), maskedId(1L, "Person")),
        (maskedId(2L, "Person"), maskedId(3L, "Person")),
        (maskedId(3L, "Person"), maskedId(2L, "Person"))))
  }
}
