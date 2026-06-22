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
    assert(elements(1) === EdgePattern(None, Some("KNOWS"), LeftToRight))
    assert(elements(2) === NodePattern(Some("b"), Some("Person")))
  }

  test("single directed edge left, with edge variable") {
    val ast = AstBuilder.parse("MATCH (a)<-[e:KNOWS]-(b)")
    val GraphPattern(Seq(_, EdgePattern(Some("e"), Some("KNOWS"), RightToLeft), _)) = ast.pattern
  }

  test("anonymous edge") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[]->(b:Person)")
    val GraphPattern(Seq(_, EdgePattern(None, None, LeftToRight), _)) = ast.pattern
  }

  test("multi-hop chain") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)")
    val GraphPattern(elements) = ast.pattern
    assert(elements.length === 5)
    assert(elements(2) === NodePattern(Some("b"), Some("Person")))
    assert(elements(3) === EdgePattern(None, Some("WORKS_AT"), LeftToRight))
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
  test("reject variable-length path") {
    intercept[InvalidParseException] {
      AstBuilder.parse("MATCH (a)-[:KNOWS*1..5]->(b)")
    }
  }

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
    assert(elements(1) === EdgePattern(None, Some("KNOWS"), Undirected))
  }

  test("undirected edge with variable") {
    val ast = AstBuilder.parse("MATCH (a)-[e:KNOWS]-(b)")
    val GraphPattern(Seq(_, EdgePattern(Some("e"), Some("KNOWS"), Undirected), _)) = ast.pattern
  }

  test("anonymous undirected edge") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[]-(b:Person)")
    val GraphPattern(Seq(_, EdgePattern(None, None, Undirected), _)) = ast.pattern
  }

  test("undirected edge in a multi-hop chain mixes with directed arrows") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS]-(b:Person)-[:WORKS_AT]->(c:Company)")
    val GraphPattern(elements) = ast.pattern
    assert(elements(1) === EdgePattern(None, Some("KNOWS"), Undirected))
    assert(elements(3) === EdgePattern(None, Some("WORKS_AT"), LeftToRight))
  }
}
