package com.databricks.dfgraph.pattern

import org.scalatest.FunSuite

class PatternSuite extends FunSuite {

  test("parser") {
    assert(Pattern.parse("(abc)") === Seq(NamedVertex("abc")))

    assert(Pattern.parse("()") === Seq(AnonymousVertex))

    assert(Pattern.parse("(u)-[e]->(v)") ===
      Seq(NamedEdge("e", NamedVertex("u"), NamedVertex("v"))))

    assert(Pattern.parse("()-[]->(v)") ===
      Seq(AnonymousEdge(AnonymousVertex, NamedVertex("v"))))

    assert(Pattern.parse("(u); ()-[]->(v)") ===
      Seq(NamedVertex("u"), AnonymousEdge(AnonymousVertex, NamedVertex("v"))))

    assert(Pattern.parse("(u)-[]->(v); (v)-[]->(w); !(u)-[]->(w)") ===
      Seq(
        AnonymousEdge(NamedVertex("u"), NamedVertex("v")),
        AnonymousEdge(NamedVertex("v"), NamedVertex("w")),
        Negation(
          AnonymousEdge(NamedVertex("u"), NamedVertex("w")))))
  }
}
