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

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.graphframes.GraphFrame
import org.graphframes.SparkFunSuite

/**
 * Pure-JVM tests for `PrefixEnv` (the variable -> column-prefix mapping) and `ExpressionLowering`
 * (GQL AST -> Spark `Column`).
 *
 * These two are the contract between the resolver's variable bindings and the column names the
 * executor actually builds: a prefix that disagrees with what `renameAll` emitted surfaces as an
 * `AnalysisException` deep inside a join, which is expensive to diagnose. Every lowering rule is
 * asserted against the hand-written Spark `Column` it is documented to produce.
 *
 * Building a `Column` needs no `SparkSession`, so nothing here starts Spark.
 */
class ExpressionLoweringSuite extends SparkFunSuite {

  // (a:Person)-[e:KNOWS]->(:Person)-[:KNOWS]->(c:Person)
  // Node 1 and step 1 are deliberately anonymous so the positional fallback names are exercised.
  private val knows = SchemaEdge("KNOWS", "Person", "Person", isDirected = true)

  private val path = SchemaPath(
    nodes = Vector(
      PathNode("Person", Some("a"), Seq.empty),
      PathNode("Person", None, Seq.empty),
      PathNode("Person", Some("c"), Seq.empty)),
    steps = Vector(
      PathStep(knows, traversedForward = true, Some("e"), Seq.empty),
      PathStep(knows, traversedForward = true, None, Seq.empty)))

  private val env = PrefixEnv(path)

  // ---------------------------------------------------------------------
  // PrefixEnv
  // ---------------------------------------------------------------------

  test("a named element takes its variable as prefix") {
    assert(env.nodePrefix(0) === "a")
    assert(env.nodePrefix(2) === "c")
    assert(env.edgePrefix(0) === "e")
  }

  test("an anonymous element falls back to a positional prefix") {
    assert(env.nodePrefix(1) === "node1")
    assert(env.edgePrefix(1) === "edge1")
  }

  test("positional fallbacks cannot collide with each other across kinds") {
    // node<i> and edge<i> share an index but not a prefix, so the two scans of a single hop
    // never rename onto the same column.
    assert(env.nodePrefix(1) !== env.edgePrefix(1))
  }

  test("nodeCol and edgeCol compose the prefix with the standardized column name") {
    assert(env.nodeCol(0, GraphFrame.ID) === "a_id")
    assert(env.nodeCol(1, GraphFrame.ID) === "node1_id")
    assert(env.edgeCol(0, GraphFrame.SRC) === "e_src")
    assert(env.edgeCol(1, GraphFrame.DST) === "edge1_dst")
  }

  test("join returns the bare column name for an empty prefix") {
    assert(env.join("", GraphFrame.ID) === GraphFrame.ID)
    assert(env.join("a", GraphFrame.ID) === "a_id")
  }

  test("prefixFor resolves node variables, edge variables, and nothing else") {
    assert(env.prefixFor("a") === Some("a"))
    assert(env.prefixFor("c") === Some("c"))
    assert(env.prefixFor("e") === Some("e"))
    assert(env.prefixFor("nope") === None)
  }

  test("prefixFor is case-sensitive on variable names") {
    // Labels are matched case-insensitively against the schema, but variables are user-chosen
    // bindings and are matched exactly -- `A` is simply not bound.
    assert(env.prefixFor("A") === None)
  }

  test("the raw environment maps every variable to the empty prefix") {
    assert(PrefixEnv.raw.prefixFor("anything") === Some(""))
    assert(PrefixEnv.raw.nodePrefix(0) === "")
    assert(PrefixEnv.raw.edgePrefix(0) === "")
    assert(PrefixEnv.raw.join("", "age") === "age")
  }

  // ---------------------------------------------------------------------
  // Variable / property lowering
  // ---------------------------------------------------------------------

  test("a bare variable lowers to the element's id column") {
    assert(ExpressionLowering.lower(Variable("a"), env).toString === col("a_id").toString)
  }

  test("a bare edge variable also lowers to an id column") {
    // Documented behaviour of the `Variable` rule: it always resolves to `<prefix>_id`, with no
    // special case for edges. Edge scans project src/dst/weight rather than `id`, so this column
    // only resolves if the edge group happens to carry an `id` property; the lowering itself is
    // unconditional and is pinned here so a future change to the rule is a visible one.
    assert(ExpressionLowering.lower(Variable("e"), env).toString === col("e_id").toString)
  }

  test("a property access lowers to the prefixed property column") {
    assert(
      ExpressionLowering.lower(PropertyAccess("a", "age"), env).toString === col(
        "a_age").toString)
    assert(
      ExpressionLowering.lower(PropertyAccess("e", "weight"), env).toString ===
        col("e_weight").toString)
  }

  test("lowering against the raw environment produces un-prefixed column names") {
    // Scan-local filters are applied by getData against the raw group columns, before aliasing.
    assert(
      ExpressionLowering.lower(PropertyAccess("a", "age"), PrefixEnv.raw).toString ===
        col("age").toString)
  }

  test("an unbound variable fails with a message naming the variable") {
    val e1 = intercept[IllegalArgumentException](ExpressionLowering.lower(Variable("zz"), env))
    assert(e1.getMessage.contains("'zz'"))
    assert(e1.getMessage.contains("not bound"))

    val e2 =
      intercept[IllegalArgumentException](
        ExpressionLowering.lower(PropertyAccess("zz", "p"), env))
    assert(e2.getMessage.contains("'zz'"))
    assert(e2.getMessage.contains("not bound"))
  }

  // ---------------------------------------------------------------------
  // Literals
  // ---------------------------------------------------------------------

  test("literals narrow to the matching Spark typed literal") {
    val cases: Seq[(Any, org.apache.spark.sql.Column)] = Seq(
      (null, lit(null)),
      (java.lang.Boolean.TRUE, lit(true)),
      (java.lang.Boolean.FALSE, lit(false)),
      (42L, lit(42L)),
      (42, lit(42)),
      (2.5d, lit(2.5d)),
      (2.5f, lit(2.5f)),
      ("hello", lit("hello")))
    cases.foreach { case (value, expected) =>
      assert(
        ExpressionLowering.lower(Literal(value), env).toString === expected.toString,
        s"literal $value lowered unexpectedly")
    }
  }

  test("a string literal keeps characters that survived the '' unescape") {
    assert(ExpressionLowering.lower(Literal("O'Hara"), env).toString === lit("O'Hara").toString)
  }

  test("an unsupported literal payload fails fast naming the offending type") {
    val e = intercept[IllegalArgumentException](
      ExpressionLowering.lower(Literal(new java.math.BigDecimal("1.0")), env))
    assert(e.getMessage.contains("Unsupported literal value of type"))
    assert(e.getMessage.contains("BigDecimal"))
  }

  // ---------------------------------------------------------------------
  // Operators
  // ---------------------------------------------------------------------

  test("every comparison operator maps to its Spark counterpart") {
    val left = PropertyAccess("a", "age")
    val right = Literal(30L)
    val cases = Seq(
      (Eq: CompOp, col("a_age") === lit(30L)),
      (Neq: CompOp, col("a_age") =!= lit(30L)),
      (Lt: CompOp, col("a_age") < lit(30L)),
      (Lte: CompOp, col("a_age") <= lit(30L)),
      (Gt: CompOp, col("a_age") > lit(30L)),
      (Gte: CompOp, col("a_age") >= lit(30L)))
    cases.foreach { case (op, expected) =>
      assert(
        ExpressionLowering.lower(Comparison(left, op, right), env).toString === expected.toString,
        s"comparison $op lowered unexpectedly")
    }
  }

  test("every arithmetic operator maps to its Spark counterpart") {
    val left = PropertyAccess("a", "age")
    val right = Literal(2L)
    val cases = Seq(
      (Plus: ArithOp, col("a_age") + lit(2L)),
      (Minus: ArithOp, col("a_age") - lit(2L)),
      (Mult: ArithOp, col("a_age") * lit(2L)),
      (Div: ArithOp, col("a_age") / lit(2L)),
      (Mod: ArithOp, col("a_age") % lit(2L)))
    cases.foreach { case (op, expected) =>
      assert(
        ExpressionLowering.lower(Arithmetic(left, op, right), env).toString === expected.toString,
        s"arithmetic $op lowered unexpectedly")
    }
  }

  test("NOT, AND and OR map to their Spark counterparts") {
    val p = Comparison(PropertyAccess("a", "age"), Gt, Literal(30L))
    val q = Comparison(PropertyAccess("c", "age"), Lt, Literal(50L))
    val pc = col("a_age") > lit(30L)
    val qc = col("c_age") < lit(50L)

    assert(ExpressionLowering.lower(Not(p), env).toString === (!pc).toString)
    assert(ExpressionLowering.lower(And(p, q), env).toString === (pc && qc).toString)
    assert(ExpressionLowering.lower(Or(p, q), env).toString === (pc || qc).toString)
  }

  test("a cross-variable predicate lowers both sides under their own prefixes") {
    val expr = Comparison(PropertyAccess("a", "age"), Gt, PropertyAccess("c", "age"))
    assert(
      ExpressionLowering.lower(expr, env).toString ===
        (col("a_age") > col("c_age")).toString)
  }

  test("a deeply nested expression lowers without losing any binding") {
    // NOT((a.age + 1) * 2 > c.age) AND e.weight = 1
    val expr = And(
      Not(Comparison(
        Arithmetic(Arithmetic(PropertyAccess("a", "age"), Plus, Literal(1L)), Mult, Literal(2L)),
        Gt,
        PropertyAccess("c", "age"))),
      Comparison(PropertyAccess("e", "weight"), Eq, Literal(1L)))
    val expected =
      (!((col("a_age") + lit(1L)) * lit(2L) > col("c_age"))) && (col("e_weight") === lit(1L))
    assert(ExpressionLowering.lower(expr, env).toString === expected.toString)
  }

  test("an anonymous element's properties are reachable only through its positional prefix") {
    // Node 1 is anonymous, so no variable can resolve to it -- by construction the user cannot
    // write a predicate over it, and the positional prefix exists only for internal renaming.
    assert(env.prefixFor("node1") === None)
    assert(env.nodeCol(1, "age") === "node1_age")
  }
}
