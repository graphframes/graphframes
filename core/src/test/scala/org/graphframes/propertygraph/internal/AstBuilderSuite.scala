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

import org.graphframes.InvalidParseException
import org.graphframes.SparkFunSuite

class AstBuilderSuite extends SparkFunSuite {

  test("typed node pattern with variable") {
    val ast = AstBuilder.parse("MATCH (a:Person)")
    val GraphPattern(Seq(NodePattern(Some("a"), Some("Person")))) = ast.pattern
    assert(ast.where === None)
    assert(ast.returnClause === None)
  }

  test("untyped node pattern with variable") {
    val ast = AstBuilder.parse("MATCH (x)")
    val GraphPattern(Seq(NodePattern(Some("x"), None))) = ast.pattern
  }

  test("anonymous node pattern") {
    val ast = AstBuilder.parse("MATCH ()")
    val GraphPattern(Seq(NodePattern(None, None))) = ast.pattern
  }

  test("label-only node pattern") {
    val ast = AstBuilder.parse("MATCH (:Person)")
    val GraphPattern(Seq(NodePattern(None, Some("Person")))) = ast.pattern
  }

  test("single directed edge right, typed") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS]->(b:Person)")
    val GraphPattern(elements) = ast.pattern
    assert(elements.length === 3)
    assert(elements(0) === NodePattern(Some("a"), Some("Person")))
    assert(elements(1) === EdgePattern(None, Some("KNOWS"), LeftToRight, None))
    assert(elements(2) === NodePattern(Some("b"), Some("Person")))
  }

  test("single directed edge left, with edge variable") {
    val ast = AstBuilder.parse("MATCH (a)<-[e:KNOWS]-(b)")
    val GraphPattern(Seq(_, EdgePattern(Some("e"), Some("KNOWS"), RightToLeft, None), _)) =
      ast.pattern
  }

  test("anonymous edge") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[]->(b:Person)")
    val GraphPattern(Seq(_, EdgePattern(None, None, LeftToRight, None), _)) = ast.pattern
  }

  test("multi-hop chain") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)")
    val GraphPattern(elements) = ast.pattern
    assert(elements.length === 5)
    assert(elements(2) === NodePattern(Some("b"), Some("Person")))
    assert(elements(3) === EdgePattern(None, Some("WORKS_AT"), LeftToRight, None))
    assert(elements(4) === NodePattern(Some("c"), Some("Company")))
  }

  test("WHERE comparison and AND") {
    val ast = AstBuilder.parse("MATCH (a:Person) WHERE a.age > 30 AND a.name = 'Bob'")
    val Some(
      And(
        Comparison(PropertyAccess("a", "age"), Gt, Literal(30L)),
        Comparison(PropertyAccess("a", "name"), Eq, Literal("Bob")))) = ast.where
  }

  test("WHERE OR and NOT and parentheses") {
    val ast = AstBuilder.parse("MATCH (a:Person) WHERE NOT (a.age > 30) OR a.active = TRUE")
    val Some(
      Or(
        Not(Comparison(PropertyAccess("a", "age"), Gt, Literal(30L))),
        Comparison(PropertyAccess("a", "active"), Eq, Literal(true)))) = ast.where
  }

  test("WHERE cross-pattern predicate") {
    val ast = AstBuilder.parse("MATCH (a)-[:KNOWS]->(b) WHERE a.age > b.age")
    val Some(Comparison(PropertyAccess("a", "age"), Gt, PropertyAccess("b", "age"))) = ast.where
  }

  test("WHERE additive expression") {
    val ast = AstBuilder.parse("MATCH (a) WHERE a.age + 1 > 30")
    val Some(
      Comparison(Arithmetic(PropertyAccess("a", "age"), Plus, Literal(1L)), Gt, Literal(30L))) =
      ast.where
  }

  test("WHERE subtraction additive chain is left-associative") {
    val ast = AstBuilder.parse("MATCH (a) WHERE a.x - a.y - 1 = 0")
    val Some(
      Comparison(
        Arithmetic(
          Arithmetic(PropertyAccess("a", "x"), Minus, PropertyAccess("a", "y")),
          Minus,
          Literal(1L)),
        Eq,
        Literal(0L))) = ast.where
  }

  test("both <> and != map to Neq") {
    val a1 = AstBuilder.parse("MATCH (a) WHERE a.x <> 1")
    val a2 = AstBuilder.parse("MATCH (a) WHERE a.x != 1")
    assert(a1.where === Some(Comparison(PropertyAccess("a", "x"), Neq, Literal(1L))))
    assert(a2.where === a1.where)
  }

  test("RETURN items and alias") {
    val ast = AstBuilder.parse("MATCH (a:Person) RETURN a, a.name AS person_name")
    val Some(
      ReturnItems(
        Seq(
          ReturnItem(Variable("a"), None),
          ReturnItem(PropertyAccess("a", "name"), Some("person_name"))))) = ast.returnClause
  }

  test("RETURN star") {
    val ast = AstBuilder.parse("MATCH (a:Person) RETURN *")
    assert(ast.returnClause === Some(ReturnStar))
  }

  test("RETURN omitted is parsed as None") {
    val ast = AstBuilder.parse("MATCH (a:Person)")
    assert(ast.returnClause === None)
  }

  test("keywords are case-insensitive") {
    val ast = AstBuilder.parse("match (a:Person) where a.age > 30 return a")
    assert(ast.where.isDefined)
    assert(ast.returnClause === Some(ReturnItems(Seq(ReturnItem(Variable("a"), None)))))
  }

  test("string literal with '' escape") {
    val ast = AstBuilder.parse("MATCH (a) WHERE a.name = 'O''Brien'")
    val Some(Comparison(PropertyAccess("a", "name"), Eq, Literal("O'Brien"))) = ast.where
  }

  test("decimal literal") {
    val ast = AstBuilder.parse("MATCH (a) WHERE a.score > 3.14")
    val Some(Comparison(PropertyAccess("a", "score"), Gt, Literal(3.14))) = ast.where
  }

  test("line and block comments are skipped") {
    val ast = AstBuilder.parse("""MATCH (a:Person) // trailing line comment
        |/* block
        |   comment */ WHERE a.age > 30 RETURN a
        |""".stripMargin)
    assert(ast.where.isDefined)
    assert(ast.returnClause.isDefined)
  }

  test("bare variable in RETURN") {
    val ast = AstBuilder.parse("MATCH (a:Person) RETURN a")
    val Some(ReturnItems(Seq(ReturnItem(Variable("a"), None)))) = ast.returnClause
  }

  // -----------------------------------------------------------------------
  // Reject cases (out-of-scope constructs must throw InvalidParseException).
  // -----------------------------------------------------------------------
  test("reject OPTIONAL MATCH") {
    intercept[InvalidParseException] {
      AstBuilder.parse("OPTIONAL MATCH (a:Person)")
    }
  }

  test("reject ORDER BY") {
    intercept[InvalidParseException] {
      AstBuilder.parse("MATCH (a:Person) RETURN a ORDER BY a.name")
    }
  }

  test("reject empty input") {
    intercept[InvalidParseException] {
      AstBuilder.parse("")
    }
  }

  test("reject edge with missing destination node") {
    intercept[InvalidParseException] {
      AstBuilder.parse("MATCH (a)-[e:KNOWS]->")
    }
  }

  test("single undirected edge, typed") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS]-(b:Person)")
    val GraphPattern(elements) = ast.pattern
    assert(elements(1) === EdgePattern(None, Some("KNOWS"), Undirected, None))
  }

  test("undirected edge with variable") {
    val ast = AstBuilder.parse("MATCH (a)-[e:KNOWS]-(b)")
    val GraphPattern(Seq(_, EdgePattern(Some("e"), Some("KNOWS"), Undirected, None), _)) =
      ast.pattern
  }

  test("anonymous undirected edge") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[]-(b:Person)")
    val GraphPattern(Seq(_, EdgePattern(None, None, Undirected, None), _)) = ast.pattern
  }

  test("undirected edge in a multi-hop chain mixes with directed arrows") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS]-(b:Person)-[:WORKS_AT]->(c:Company)")
    val GraphPattern(elements) = ast.pattern
    assert(elements(1) === EdgePattern(None, Some("KNOWS"), Undirected, None))
    assert(elements(3) === EdgePattern(None, Some("WORKS_AT"), LeftToRight, None))
  }

  // -----------------------------------------------------------------------
  // Scalar function calls (datetime family).
  // -----------------------------------------------------------------------
  test("function call over a property access") {
    val ast = AstBuilder.parse("MATCH (a:Person) WHERE year(a.creationDate) = 2012")
    val Some(
      Comparison(
        FunctionCall("year", Seq(PropertyAccess("a", "creationDate"))),
        Eq,
        Literal(2012L))) =
      ast.where
  }

  test("function call with multiple arguments") {
    val ast = AstBuilder.parse("MATCH (a)-[:R]->(b) WHERE datediff(a.d, b.d) > 30")
    val Some(
      Comparison(
        FunctionCall("datediff", Seq(PropertyAccess("a", "d"), PropertyAccess("b", "d"))),
        Gt,
        Literal(30L))) = ast.where
  }

  test("nested function calls inside arithmetic and comparison") {
    val ast = AstBuilder.parse("MATCH (a)-[:R]->(b) WHERE year(a.d) - year(b.d) > 1")
    val Some(
      Comparison(
        Arithmetic(
          FunctionCall("year", Seq(PropertyAccess("a", "d"))),
          Minus,
          FunctionCall("year", Seq(PropertyAccess("b", "d")))),
        Gt,
        Literal(1L))) = ast.where
  }

  test("function call over a string literal argument") {
    val ast = AstBuilder.parse("MATCH (a) WHERE a.d = date('2012-06-01')")
    val Some(
      Comparison(
        PropertyAccess("a", "d"),
        Eq,
        FunctionCall("date", Seq(Literal("2012-06-01"))))) = ast.where
  }

  test("zero-argument function call") {
    val ast = AstBuilder.parse("MATCH (a) WHERE a.d < current_timestamp()")
    val Some(Comparison(PropertyAccess("a", "d"), Lt, FunctionCall("current_timestamp", Seq()))) =
      ast.where
  }

  test("function name and same-named property coexist in one query") {
    // `a.date` is a property access; `date(...)` is a function call. The parser disambiguates by
    // the token following the IDENTIFIER (DOT vs LPAREN).
    val ast = AstBuilder.parse("MATCH (a) WHERE date(a.date) = date('2012-06-01')")
    val Some(
      Comparison(
        FunctionCall("date", Seq(PropertyAccess("a", "date"))),
        Eq,
        FunctionCall("date", Seq(Literal("2012-06-01"))))) = ast.where
  }

  test("RETURN of a function call") {
    val ast = AstBuilder.parse("MATCH (a:Person) RETURN year(a.creationDate) AS y")
    val Some(
      ReturnItems(Seq(
        ReturnItem(FunctionCall("year", Seq(PropertyAccess("a", "creationDate"))), Some("y"))))) =
      ast.returnClause
  }

  test("unknown function name still parses (grammar allows any identifier)") {
    // `frobnicate` is not in the whitelist, but the grammar accepts any IDENTIFIER as a function
    // name; rejection happens at lowering, not parsing.
    val ast = AstBuilder.parse("MATCH (a) WHERE frobnicate(a.x) = 1")
    val Some(
      Comparison(FunctionCall("frobnicate", Seq(PropertyAccess("a", "x"))), Eq, Literal(1L))) =
      ast.where
  }

  test("referencedVariables recurses into function-call arguments") {
    // Regression guard for the §4 traversal edit: a function call must contribute the variables
    // referenced by its arguments (this is what makes the resolver classify
    // `WHERE year(a.creationDate) = 2012` as scan-local on `a`).
    val expr = FunctionCall("year", Seq(PropertyAccess("a", "creationDate")))
    assert(GqlAst.referencedVariables(expr) === Set("a"))
  }

  test("lowering rejects an unknown function name") {
    // Rejection of unsupported names happens at lowering, not parse time.
    intercept[UnsupportedOperationException] {
      ExpressionLowering.lower(
        FunctionCall("frobnicate", Seq(PropertyAccess("a", "x"))),
        PrefixEnv.raw)
    }
  }

  test("lowering rejects wrong function arity") {
    intercept[UnsupportedOperationException] {
      ExpressionLowering.lower(
        FunctionCall("year", Seq(PropertyAccess("a", "x"), PropertyAccess("a", "y"))),
        PrefixEnv.raw)
    }
  }

  // -----------------------------------------------------------------------
  // Multiplicative operators (*, /, %) -- precedence and shape.
  // -----------------------------------------------------------------------
  test("multiplicative * binds tighter than additive +") {
    // a.x + a.y * a.z parses as (a.x + (a.y * a.z)).
    val ast = AstBuilder.parse("MATCH (a) WHERE a.x + a.y * a.z = 1")
    val Some(
      Comparison(
        Arithmetic(
          PropertyAccess("a", "x"),
          Plus,
          Arithmetic(PropertyAccess("a", "y"), Mult, PropertyAccess("a", "z"))),
        Eq,
        Literal(1L))) = ast.where
  }

  test("multiplicative % over a property and a literal") {
    val ast = AstBuilder.parse("MATCH (a) WHERE a.x % 512 = 0")
    val Some(
      Comparison(Arithmetic(PropertyAccess("a", "x"), Mod, Literal(512L)), Eq, Literal(0L))) =
      ast.where
  }

  test("multiplicative chain is left-associative") {
    // a.x / a.y / a.z -> ((a.x / a.y) / a.z)
    val ast = AstBuilder.parse("MATCH (a) WHERE a.x / a.y / a.z = 1")
    val Some(
      Comparison(
        Arithmetic(
          Arithmetic(PropertyAccess("a", "x"), Div, PropertyAccess("a", "y")),
          Div,
          PropertyAccess("a", "z")),
        Eq,
        Literal(1L))) = ast.where
  }

  test("mixed additive and multiplicative precedence with parentheses") {
    // (a.x + a.y) * 2 -> Arithmetic( (a.x + a.y), Mult, 2 )
    val ast = AstBuilder.parse("MATCH (a) WHERE (a.x + a.y) * 2 = 1")
    val Some(
      Comparison(
        Arithmetic(
          Arithmetic(PropertyAccess("a", "x"), Plus, PropertyAccess("a", "y")),
          Mult,
          Literal(2L)),
        Eq,
        Literal(1L))) = ast.where
  }

  // -----------------------------------------------------------------------
  // Scalar function calls (string/math/json/xml/hash families).
  // -----------------------------------------------------------------------
  test("variadic function call preserves all arguments") {
    val ast = AstBuilder.parse("MATCH (a) WHERE coalesce(a.x, a.y, a.z) = 1")
    val Some(
      Comparison(
        FunctionCall(
          "coalesce",
          Seq(PropertyAccess("a", "x"), PropertyAccess("a", "y"), PropertyAccess("a", "z"))),
        Eq,
        Literal(1L))) = ast.where
  }

  test("string-literal function argument parses as Literal") {
    val ast = AstBuilder.parse("MATCH (a) WHERE get_json_object(a.p, '$.k') = 'x'")
    val Some(
      Comparison(
        FunctionCall("get_json_object", Seq(PropertyAccess("a", "p"), Literal("$.k"))),
        Eq,
        Literal("x"))) = ast.where
  }

  test("nested arithmetic inside a function call") {
    // pmod(hash(a.id), 512) = 0 -- the sampling idiom.
    val ast = AstBuilder.parse("MATCH (a) WHERE pmod(hash(a.id), 512) = 0")
    val Some(
      Comparison(
        FunctionCall(
          "pmod",
          Seq(FunctionCall("hash", Seq(PropertyAccess("a", "id"))), Literal(512L))),
        Eq,
        Literal(0L))) = ast.where
  }

  test("lowering accepts a variadic function with many args") {
    // greatest is variadic (>=2); three args must lower without an arity error.
    ExpressionLowering.lower(
      FunctionCall(
        "greatest",
        Seq(PropertyAccess("a", "x"), PropertyAccess("a", "y"), PropertyAccess("a", "z"))),
      PrefixEnv.raw)
  }

  test("lowering rejects a property reference where a string literal is required") {
    // regexp_extract wants (col, lit-str, lit-int); passing a property as the pattern must fail.
    intercept[UnsupportedOperationException] {
      ExpressionLowering.lower(
        FunctionCall(
          "regexp_extract",
          Seq(PropertyAccess("a", "s"), PropertyAccess("a", "p"), Literal(1L))),
        PrefixEnv.raw)
    }
  }

  test("lowering rejects a property reference where an integer literal is required") {
    // sha2 wants (col, lit-int); passing a property as the bit length must fail.
    intercept[UnsupportedOperationException] {
      ExpressionLowering.lower(
        FunctionCall("sha2", Seq(PropertyAccess("a", "s"), PropertyAccess("a", "bits"))),
        PrefixEnv.raw)
    }
  }
}
