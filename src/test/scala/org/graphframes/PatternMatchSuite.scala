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

package org.graphframes

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.when

/**
 * Cases to go through:
 *   - Any negated terms?
 *   - Any anonymous vertices
 *     - in non-negated terms?
 *     - in negated terms?
 *   - # named vertices grounding a negated term to non-negated terms: 2, 1, 0
 *   - Named edges?
 */
class PatternMatchSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  @transient var v: DataFrame = _
  @transient var e: DataFrame = _
  @transient var noEdges: DataFrame = _
  @transient var g: GraphFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    v = spark
      .createDataFrame(List((0L, "a", "f"), (1L, "b", "m"), (2L, "c", "m"), (3L, "d", "f")))
      .toDF("id", "attr", "gender")
    e = spark
      .createDataFrame(
        List(
          (0L, 1L, "friend"),
          (1L, 0L, "follow"),
          (1L, 2L, "friend"),
          (2L, 3L, "follow"),
          (2L, 0L, "unknown")))
      .toDF("src", "dst", "relationship")
    noEdges = v
      .select(col("id").alias("src"))
      .crossJoin(v.select(col("id").alias("dst")))
      .except(e.select("src", "dst"))
    g = GraphFrame(v, e)
  }

  override def afterAll(): Unit = {
    v = null
    e = null
    g = null
    super.afterAll()
  }

  private def compareResultToExpected[A](result: Set[A], expected: Set[A]): Unit = {
    if (result !== expected) {
      throw new AssertionError(
        "result !== expected.\n" +
          s"Result contained additional values: ${result.diff(expected)}\n" +
          s"Expected contained additional values: ${expected.diff(result)}\n" +
          s"Result: $result\n" +
          s"Expected: $expected")
    }
  }

  test("test compareResultToExpected") {
    intercept[AssertionError] {
      compareResultToExpected(Set(1, 2), Set(2, 3))
    }
  }

  test("empty query should return nothing") {
    val emptiness = g.find("")
    assert(emptiness.count() === 0)
  }

  test("filter edges and drop isolated vertices") {
    // string expression
    val s = "relationship = 'friend'"
    // column expression
    val c = col("relationship") === "friend"
    // expected subgraph vertices
    val expected_v = Set(Row(0L, "a", "f"), Row(1L, "b", "m"), Row(2L, "c", "m"))
    // expected subgraph edges
    val expected_e = Set(Row(0L, 1L, "friend"), Row(1L, 2L, "friend"))

    val res_s = g.filterEdges(s)
    assert(res_s.vertices.collect().toSet === v.collect().toSet)
    assert(res_s.edges.collect().toSet === expected_e)

    val res_c = g.filterEdges(c)
    assert(res_c.vertices.collect().toSet === v.collect().toSet)
    assert(res_c.edges.collect().toSet === expected_e)

    val res = res_s.dropIsolatedVertices()
    assert(res.vertices.collect().toSet === expected_v)
    assert(res.edges.collect().toSet === expected_e)
  }

  test("filter vertices") {
    // string expression
    val s = "id > 0"
    // column expression
    val c = col("id") > 0
    // expected subgraph vertices
    val expected_v = Set(Row(1L, "b", "m"), Row(2L, "c", "m"), Row(3L, "d", "f"))
    // expected subgraph edges
    val expected_e = Set(Row(1L, 2L, "friend"), Row(2L, 3L, "follow"))

    val res_s = g.filterVertices(s)
    assert(res_s.vertices.collect().toSet === expected_v)
    assert(res_s.edges.collect().toSet === expected_e)

    val res_c = g.filterVertices(c)
    assert(res_c.vertices.collect().toSet === expected_v)
    assert(res_c.edges.collect().toSet === expected_e)
  }

  test("triangles") {
    val triangles = g
      .find("(a)-[]->(b); (b)-[]->(c); (c)-[]->(a)")
      .select("a.id", "b.id", "c.id")

    assert(triangles.collect().toSet === Set(Row(0L, 1L, 2L), Row(2L, 0L, 1L), Row(1L, 2L, 0L)))
  }

  /* ====================================== Vertex queries ===================================== */

  test("single named vertex") {
    val vertices = g.find("(a)")

    assert(vertices.columns === Array("a"))
    val res = vertices.select("a.id", "a.attr").collect().toSet
    compareResultToExpected(res, v.select("id", "attr").collect().toSet)
  }

  /* =========================== Single-edge queries without negated terms ===================== */

  test("triplet with anonymous edge") {
    val triplets = g.find("(u)-[]->(v)")

    assert(triplets.columns === Array("u", "v"))
    val res = triplets.select("u.id", "u.attr", "v.id", "v.attr").collect().toSet
    compareResultToExpected(
      res,
      Set(
        Row(0L, "a", 1L, "b"),
        Row(1L, "b", 0L, "a"),
        Row(1L, "b", 2L, "c"),
        Row(2L, "c", 3L, "d"),
        Row(2L, "c", 0L, "a")))
  }

  test("triplet with named edge") {
    val triplets = g.find("(u)-[uv]->(v)")

    assert(triplets.columns === Array("u", "uv", "v"))
    val res = triplets
      .select("u.id", "u.attr", "uv.src", "uv.dst", "uv.relationship", "v.id", "v.attr")
      .collect()
      .toSet
    compareResultToExpected(
      res,
      Set(
        Row(0L, "a", 0L, 1L, "friend", 1L, "b"),
        Row(1L, "b", 1L, 0L, "follow", 0L, "a"),
        Row(1L, "b", 1L, 2L, "friend", 2L, "c"),
        Row(2L, "c", 2L, 3L, "follow", 3L, "d"),
        Row(2L, "c", 2L, 0L, "unknown", 0L, "a")))
  }

  test("triplet with anonymous vertex") {
    val triplets = g.find("(u)-[]->()")

    assert(triplets.columns === Array("u"))
    // Do not use compareResultToExpected since it uses sets, and we expect duplicates.
    assert(
      triplets.select("u.id", "u.attr").collect().sortBy(_.getLong(0)) === Array(
        Row(0L, "a"),
        Row(1L, "b"),
        Row(1L, "b"),
        Row(2L, "c"),
        Row(2L, "c")).sortBy(_.getLong(0)))
  }

  test("triplet with 2 anonymous vertices") {
    val triplets = g.find("()-[uv]->()")

    assert(triplets.columns === Array("uv"))
    val res = triplets.select("uv.src", "uv.dst", "uv.relationship").collect().toSet
    val expected = e.select("src", "dst", "relationship").collect().toSet
    compareResultToExpected(res, expected)
  }

  test("self-loop") {
    val myE = spark
      .createDataFrame(List((1L, 1L, "self"), (3L, 3L, "self")))
      .toDF("src", "dst", "relationship")
      .union(e)
    val myG = GraphFrame(v, myE)

    val selfLoops = myG.find("(a)-[]->(a)")
    assert(selfLoops.columns === Array("a"))
    val res = selfLoops.select("a.id").collect().toSet
    compareResultToExpected(res, Set(Row(1L), Row(3L)))

    val selfLoops2 = myG.find("(a)-[]->(b); (a)-[]->(a)")
    assert(selfLoops2.columns === Array("a", "b"))
    val res2 = selfLoops2
      .select("a.id", "b.id")
      .where("a.id != b.id")
      .collect()
      .toSet
    compareResultToExpected(res2, Set(Row(1L, 0L), Row(1L, 2L)))
  }

  test("duplicate edges") {
    val myE = spark
      .createDataFrame(List((1L, 0L, "dup"), (1L, 2L, "dup")))
      .toDF("src", "dst", "relationship")
      .union(e)
    val myG = GraphFrame(v, myE)

    val edges = myG
      .find("(a)-[]->(b)")
      .where("a.id = 1")
    val res = edges
      .select("a.id", "b.id")
      .collect()
      .sortBy(_.getLong(1))
    val expected = Array(Row(1L, 0L), Row(1L, 0L), Row(1L, 2L), Row(1L, 2L))
    assert(res === expected)
  }

  /* ======================== Multiple-edge queries without negated terms ===================== */

  test("triangle cycles") {
    val triangles = g.find("(a)-[]->(b); (b)-[]->(c); (c)-[]->(a)")

    assert(triangles.columns === Array("a", "b", "c"))
    val res = triangles
      .select("a.id", "b.id", "c.id")
      .collect()
      .toSet
    compareResultToExpected(res, Set(Row(0L, 1L, 2L), Row(2L, 0L, 1L), Row(1L, 2L, 0L)))
  }

  test("disconnected edges create an outer join") {
    val edgePairs = g.find("(a)-[]->(b); (c)-[]->(d)")

    assert(edgePairs.columns === Array("a", "b", "c", "d"))
    val res = edgePairs
      .select("a.id", "b.id", "c.id", "d.id")
      .collect()
      .toSet

    val ab = e.select(col("src").alias("a"), col("dst").alias("b"))
    val cd = e.select(col("src").alias("c"), col("dst").alias("d"))
    val expected = ab
      .crossJoin(cd)
      .collect()
      .toSet
    compareResultToExpected(res, expected)
    val numEdges = e.count()
    assert(expected.size === numEdges * numEdges)
  }

  /* ========== 2 named vertices grounding a negated term to non-negated terms =============== */

  test("edges without back edges") {
    val edges = g.find("(a)-[]->(b); !(b)-[]->(a)")

    assert(edges.columns === Array("a", "b"))
    val res = edges
      .select("a.id", "b.id")
      .collect()
      .toSet
    compareResultToExpected(res, Set(Row(1L, 2L), Row(2L, 0L), Row(2L, 3L)))
  }

  test("a->b->c but not c->a") {
    val edges = g.find("(a)-[]->(b); (b)-[]->(c); !(c)-[]->(a)")

    assert(edges.columns === Array("a", "b", "c"))
    val res = edges
      .select("a.id", "b.id", "c.id")
      .collect()
      .toSet
    assert(res === Set(Row(0L, 1L, 0L), Row(1L, 0L, 1L), Row(1L, 2L, 3L)))
  }

  test("three connected vertices not in a triangle") {
    val fof = g
      .find("(u)-[]->(v); (v)-[]->(w); !(u)-[]->(w); !(w)-[]->(u)")
      .select("u.id", "v.id", "w.id")
      .collect()
      .toSet

    compareResultToExpected(fof, Set(Row(1L, 0L, 1L), Row(0L, 1L, 0L), Row(1L, 2L, 3L)))
  }

  /* ========== 1 named vertex grounding a negated term to non-negated terms =============== */

  test("a->b but not b->c") {
    val edges = g.find("(a)-[]->(b); !(b)-[]->(c)")

    assert(edges.columns === Array("a", "b", "c"))
    val res = edges
      .select("a.id", "b.id", "c.id")
      .collect()
      .toSet
    compareResultToExpected(
      res,
      Set(
        Row(0L, 1L, 1L),
        Row(0L, 1L, 3L),
        Row(1L, 0L, 0L),
        Row(1L, 0L, 2L),
        Row(1L, 0L, 3L),
        Row(1L, 2L, 1L),
        Row(1L, 2L, 2L),
        Row(2L, 3L, 0L),
        Row(2L, 3L, 1L),
        Row(2L, 3L, 2L),
        Row(2L, 3L, 3L),
        Row(2L, 0L, 0L),
        Row(2L, 0L, 2L),
        Row(2L, 0L, 3L)))
  }

  test("a->b where b has no out edges") {
    val edges = g.find("(a)-[]->(b); !(b)-[]->()")

    assert(edges.columns === Array("a", "b"))
    val res = edges
      .select("a.id", "b.id")
      .collect()
      .toSet
    compareResultToExpected(res, Set(Row(2L, 3L)))
  }

  /* ========== 0 named vertices grounding a negated term to non-negated terms =============== */

  test("a->b but not c->d") {
    val edgePairs = g.find("(a)-[]->(b); !(c)-[]->(d)")

    assert(edgePairs.columns === Array("a", "b", "c", "d"))
    val res = edgePairs
      .select("a.id", "b.id", "c.id", "d.id")
      .collect()
      .toSet
    val expected = e
      .select(col("src").alias("a"), col("dst").alias("b"))
      .crossJoin(noEdges.select(col("src").alias("c"), col("dst").alias("d")))
      .select("a", "b", "c", "d")
      .collect()
      .toSet
    compareResultToExpected(res, expected)
    assert(expected.size === noEdges.count() * e.count()) // make sure there are no duplicates
  }

  test("a->b, c where c has no out edges") {
    val triplets = g.find("(a)-[]->(b); !(c)-[]->()")

    assert(triplets.columns === Array("a", "b", "c"))
    val res = triplets
      .select("a.id", "b.id", "c.id")
      .collect()
      .toSet
    val expected =
      Set(Row(0L, 1L, 3L), Row(1L, 0L, 3L), Row(1L, 2L, 3L), Row(2L, 3L, 3L), Row(2L, 0L, 3L))
    compareResultToExpected(res, expected)
  }

  /* ======= Varying # of named vertices grounding a negated term to non-negated terms ========= */

  // Note: This is a deceptive query.
  // Users may intend "there exists no such c connecting b->c->a," which is a different query.
  test("a->b, c without edges b->c->a") {
    val seq = g.find("(a)-[]->(b); !(b)-[]->(c); !(c)-[]->(a)")

    assert(seq.columns === Array("a", "b", "c"))
    val res = seq
      .select("a.id", "b.id", "c.id")
      .collect()
      .toSet
    val expected = Set(
      Row(0L, 1L, 3L),
      Row(1L, 0L, 2L),
      Row(1L, 0L, 3L),
      Row(1L, 2L, 1L),
      Row(1L, 2L, 2L),
      Row(2L, 3L, 0L),
      Row(2L, 3L, 2L),
      Row(2L, 3L, 3L),
      Row(2L, 0L, 0L),
      Row(2L, 0L, 2L),
      Row(2L, 0L, 3L))
    compareResultToExpected(res, expected)
  }

  test("a->b, c, d with no edges a->c, c->d") {
    val edgePairs = g.find("(a)-[]->(b); !(a)-[]->(c); !(c)-[]->(d)")

    assert(edgePairs.columns === Array("a", "b", "c", "d"))
    val res = edgePairs
      .select("a.id", "b.id", "c.id", "d.id")
      .where("a.id = 0 AND a.id != b.id") // check subset for brevity
      .collect()
      .toSet
    val expected = Set(
      Row(0L, 1L, 0L, 0L),
      Row(0L, 1L, 0L, 2L),
      Row(0L, 1L, 0L, 3L),
      Row(0L, 1L, 2L, 1L),
      Row(0L, 1L, 2L, 2L),
      Row(0L, 1L, 3L, 0L),
      Row(0L, 1L, 3L, 1L),
      Row(0L, 1L, 3L, 2L),
      Row(0L, 1L, 3L, 3L))
    compareResultToExpected(res, expected)
  }

  /* ============================== 0 non-negated terms ============================== */

  test("query without non-negated terms, with one named vertex") {
    val res = g
      .find("!(v)-[]->()")
      .select("v.id")
      .collect()
      .toSet
    compareResultToExpected(res, Set(Row(3L)))
  }

  test("query without non-negated terms, with two named vertices") {
    val res = g
      .find("!(u)-[]->(v)")
      .select("u.id", "v.id")
      .collect()
      .toSet
    val expected = noEdges
      .select(col("src").alias("u"), col("dst").alias("v"))
      .collect()
      .toSet
    compareResultToExpected(res, expected)
  }

  /* ======================== Other corner cases and implementation checks ==================== */

  test("named edges") {
    // edges whose destination leads nowhere
    val edges = g
      .find("()-[e]->(v); !(v)-[]->()")
      .select("e.src", "e.dst")
    val res = edges.collect().toSet
    compareResultToExpected(res, Set(Row(2L, 3L)))
  }

  test("a->b but not a->b") {
    val edges = g.find("(a)-[]->(b); !(a)-[]->(b)")
    assert(edges.count() === 0)

    val edges2 = g.find("(a)-[ab]->(b); !(a)-[]->(b)")
    assert(edges2.count() === 0)
  }

  test("named edge __tmp") {
    // named edge __tmp should not be removed if there is an anonymous edge
    val edges = g.find("()-[__tmp]->(v); (v)-[]->(w)")
    assert(edges.columns === Array("__tmp", "v", "w"))
  }

  test("find column order") {
    val fof = g
      .find("(u)-[e]->(v); (v)-[]->(w); !(u)-[]->(w); !(w)-[]->(u)")
      .where("u.id != v.id AND v.id != w.id AND u.id != w.id")
    assert(fof.columns === Array("u", "e", "v", "w"))
    compareResultToExpected(
      fof.select("u.id", "v.id", "w.id").collect().toSet,
      Set(Row(1L, 2L, 3L)))

    val fv = g.find("(u)")
    assert(fv.columns === Array("u"))

    val fve = g.find("(u)-[e2]->()")
    assert(fve.columns === Array("u", "e2"))

    val fed = g.find("()-[e]->(w)")
    assert(fed.columns === Array("e", "w"))
  }

  /* ================================= Invalid queries =================================== */

  test("Disallow empty term ()-[]->()") {
    intercept[InvalidParseException] {
      g.find("()-[]->()")
    }
  }

  test("Disallow named edges in negated terms") {
    intercept[InvalidParseException] {
      g.find("!()-[ab]->()")
    }
    intercept[InvalidParseException] {
      g.find("(u)-[]->(v); !(a)-[ab]->(b)")
    }
    intercept[InvalidParseException] {
      g.find("(u)-[ab]->(v); !(a)-[ab]->(b)")
    }
  }

  test("Disallow using the same name for both a vertex and an edge") {
    intercept[InvalidParseException] {
      g.find("(a)-[a]->(b)")
    }
    intercept[InvalidParseException] {
      g.find("(a)-[]->(b); (c)-[a]->(d)")
    }
  }

  /* ============================= More complex use case examples ============================== */

  test("triangles via post-hoc filter") {
    val triangles = g
      .find("(a)-[]->(b); (b)-[]->(c); (d)-[]->(e)")
      .where("c.id = d.id AND e.id = a.id")
      .select("a.id", "b.id", "c.id")

    val res = triangles.collect().toSet
    compareResultToExpected(res, Set(Row(0L, 1L, 2L), Row(2L, 0L, 1L), Row(1L, 2L, 0L)))
  }

  test("stateful predicates via UDFs") {
    val chain4 = g
      .find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")
      .where("a.id != b.id AND b.id != c.id AND c.id != a.id")

    // Using DataFrame operations, but not really operating in a stateful manner
    val chainWith2Friends = chain4.where(
      Seq("ab", "bc", "cd")
        .map(e => when(col(e)("relationship") === "friend", 1).otherwise(0))
        .reduce(_ + _) >= 2)

    assert(chainWith2Friends.count() === 4)
    chainWith2Friends
      .select("ab.relationship", "bc.relationship", "cd.relationship")
      .collect()
      .foreach {
        case Row(ab: String, bc: String, cd: String) =>
          val numFriends = Seq(ab, bc, cd).map(r => if (r == "friend") 1 else 0).sum
          assert(numFriends >= 2)
        case _ => throw new GraphFramesUnreachableException()
      }

    // Operating in a stateful manner, where cnt is the state.
    def sumFriends(cnt: Column, relationship: Column): Column = {
      when(relationship === "friend", cnt + 1).otherwise(cnt)
    }
    val condition =
      Seq("ab", "bc", "cd").foldLeft(lit(0))((cnt, e) => sumFriends(cnt, col(e)("relationship")))
    val chainWith2Friends2 = chain4.where(condition >= 2)

    compareResultToExpected(chainWith2Friends.collect().toSet, chainWith2Friends2.collect().toSet)
  }

  /* ===================================== Join elimination =================================== */

  /*
  // Join elimination will not work without Ankur's improved indexing.
  test("join elimination - simple") {
    import org.apache.spark.sql.catalyst.plans.logical.Join

    val edges = g.find("(u)-[e]->(v)", _.select("e_src", "e_dst"))
    val joins = edges.queryExecution.optimizedPlan.collect {
      case j: Join => j
    }

    assert(joins.isEmpty, s"joins was non-empty: ${joins.map(_.toString()).mkString("; ")}")
  }

  test("join elimination - with aliases") {
    import org.apache.spark.sql.catalyst.plans.logical.Join

    val edges = g.find("(u)-[]->(v)", _.select("u_id", "v_id"))
    println(edges.queryExecution.optimizedPlan)
    val joins = edges.queryExecution.optimizedPlan.collect {
      case j: Join => j
    }
    assert(joins.isEmpty, s"joins was non-empty: ${joins.map(_.toString()).mkString("; ")}")
  }
   */
}
