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

package org.graphframes.pattern

import org.graphframes.{InvalidParseException, SparkFunSuite}

class PatternSuite extends SparkFunSuite {

  test("good parses") {
    assert(Pattern.parse("(abc)") === Seq(NamedVertex("abc")))

    assert(Pattern.parse("(u)-[e]->(v)") ===
      Seq(NamedEdge("e", NamedVertex("u"), NamedVertex("v"))))

    assert(Pattern.parse("()-[]->(v)") ===
      Seq(AnonymousEdge(AnonymousVertex, NamedVertex("v"))))

    assert(Pattern.parse("()-[e]->()") ===
      Seq(NamedEdge("e", AnonymousVertex, AnonymousVertex)))

    assert(Pattern.parse("(u)-[e]->(u)") ===
      Seq(NamedEdge("e", NamedVertex("u"), NamedVertex("u"))))

    assert(Pattern.parse("(u); ()-[]->(v)") ===
      Seq(NamedVertex("u"), AnonymousEdge(AnonymousVertex, NamedVertex("v"))))

    assert(Pattern.parse("(u)-[]->(v); (v)-[]->(w); !(u)-[]->(w)") ===
      Seq(
        AnonymousEdge(NamedVertex("u"), NamedVertex("v")),
        AnonymousEdge(NamedVertex("v"), NamedVertex("w")),
        Negation(
          AnonymousEdge(NamedVertex("u"), NamedVertex("w")))))
  }

  test("bad parses") {
    withClue("Failed to catch parse error with lone anonymous vertex") {
      intercept[InvalidParseException] {
        Pattern.parse("()")
      }
    }
    withClue("Failed to catch parse error with lone anonymous vertex") {
      intercept[InvalidParseException] {
        Pattern.parse("(a)-[]->(b); ()")
      }
    }
    withClue("Failed to catch parse error") {
      intercept[InvalidParseException] {
        Pattern.parse("(")
      }
    }
    withClue("Failed to catch parse error") {
      intercept[InvalidParseException] {
        Pattern.parse("->(a)")
      }
    }
    withClue("Failed to catch parse error with negated vertex") {
      intercept[InvalidParseException] {
        Pattern.parse("!(a)")
      }
    }
    withClue("Failed to catch parse error with negated named edge") {
      val msg = intercept[InvalidParseException] {
        Pattern.parse("!(a)-[ab]->(b)")
      }
      assert(msg.getMessage.contains("does not support negated named edges"))
    }
    withClue("Failed to catch parse error with negated named edge") {
      val msg = intercept[InvalidParseException] {
        Pattern.parse("!()-[ab]->()")
      }
      assert(msg.getMessage.contains("does not support negated named edges"))
    }
    withClue("Failed to catch parse error with double negative") {
      intercept[InvalidParseException] {
        Pattern.parse("!!(a)-[]->(b)")
      }
    }
    withClue("Failed to catch parse error with completely anonymous edge ()-[]->()") {
      intercept[InvalidParseException] {
        Pattern.parse("()-[]->()")
      }
    }
    withClue("Failed to catch parse error with completely anonymous negated edge !()-[]->()") {
      intercept[InvalidParseException] {
        Pattern.parse("!()-[]->()")
      }
    }
    withClue("Failed to catch parse error with reused element name") {
      intercept[InvalidParseException] {
        Pattern.parse("(a)-[]->(b); ()-[a]->()")
      }
    }
    withClue("Failed to catch parse error with reused element name") {
      intercept[InvalidParseException] {
        Pattern.parse("(a)-[a]->(b)")
      }
    }
    withClue("Failed to catch parse error with reused edge name") {
      intercept[InvalidParseException] {
        Pattern.parse("(a)-[e]->(b); ()-[e]->()")
      }
    }
  }

  test("empty pattern should be parsable") {
    Pattern.parse("")
  }

  def testFindNamedVerticesOnlyInNegatedTerms(pattern: String, expected: Seq[String]): Unit = {
    test(s"findNamedVerticesOnlyInNegatedTerms: $pattern") {
      val patterns = Pattern.parse(pattern)
      val result = Pattern.findNamedVerticesOnlyInNegatedTerms(patterns)
      assert(result === expected)
    }
  }

  testFindNamedVerticesOnlyInNegatedTerms(
    "(u)-[]->(v); (v)-[]->(w); !(u)-[]->(w)",
    Seq.empty[String])

  testFindNamedVerticesOnlyInNegatedTerms(
    "(u)-[]->(v); (v)-[]->(w)",
    Seq.empty[String])

  testFindNamedVerticesOnlyInNegatedTerms(
    "!(u)-[]->(v)",
    Seq("u", "v"))

  testFindNamedVerticesOnlyInNegatedTerms(
    "(u)-[]->(v); (v)-[]->(w); !(a)-[]->(b); !(v)-[]->(c)",
    Seq("a", "b", "c"))

  def testFindNamedElementsInOrder(pattern: String, expected: Seq[String]): Unit = {
    test(s"testFindNamedElementsInOrder: $pattern") {
      val patterns = Pattern.parse(pattern)
      val result = Pattern.findNamedElementsInOrder(patterns, includeEdges = true)
      assert(result === expected)
    }
  }

  testFindNamedElementsInOrder(
    "(u)-[]->(v); (v)-[]->(w); !(u)-[]->(w)",
    Seq("u", "v", "w"))

  testFindNamedElementsInOrder(
    "(u)-[]->(v); ()-[vw]->()",
    Seq("u", "v", "vw"))

  testFindNamedElementsInOrder(
    "(u)-[uv]->(v); (v)-[vw]->(w); !(u)-[]->(w); (x)",
    Seq("u", "uv", "v", "vw", "w", "x"))
}
