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

import org.graphframes.SparkFunSuite

/**
 * Pure-JVM tests for the explain renderers. No SparkSession required; the renderers are pure
 * functions over the resolved IR values.
 */
class GqlExplainSuite extends SparkFunSuite {

  private val schema = SchemaGraphSnapshot(
    vertexGroupNames = Set("Person", "Company", "City"),
    edges = Vector(
      SchemaEdge("KNOWS", "Person", "Person", true),
      SchemaEdge("WORKS_AT", "Person", "Company", true),
      SchemaEdge("LOCATED_IN", "Company", "City", true)))

  test("logical explain renders the path with a forward arrow") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS]->(b:Person)")
    val rq = Resolver.resolve(ast, schema)
    val out = GqlExplain.logical(rq)
    assert(out.contains("Logical plan"))
    assert(out.contains("(a:Person)"))
    // Anonymous edge renders without a variable prefix: -[KNOWS]->
    assert(out.contains("-[KNOWS]->"))
  }

  test("logical explain renders a backward arrow for <-[e]- ") {
    val ast = AstBuilder.parse("MATCH (a:Person)<-[:KNOWS]-(b:Person)")
    val rq = Resolver.resolve(ast, schema)
    val out = GqlExplain.logical(rq)
    assert(out.contains("<-[KNOWS]-"))
  }

  test("logical explain reports disconnected patterns as (none)") {
    val ast = AstBuilder.parse("MATCH (a:City)-[:KNOWS]->(b:Person)")
    val rq = Resolver.resolve(ast, schema)
    assert(rq.paths.isEmpty)
    val out = GqlExplain.logical(rq)
    assert(out.contains("(none"))
  }

  test(
    "logical explain lists scan-local filter on the node and join/post predicates separately") {
    val ast = AstBuilder.parse(
      "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 30 AND a.age > b.age RETURN a, b")
    val rq = Resolver.resolve(ast, schema)
    val out = GqlExplain.logical(rq)
    // scan-local filter rendered inline on the node
    assert(out.contains("a.age > 30"))
    // the cross-pattern predicate rendered as a join predicate
    assert(out.contains("a.age > b.age"))
    assert(out.contains("join predicates"))
    assert(out.contains("projection"))
  }

  test("physical explain renders plan order and statistics line") {
    val ast =
      AstBuilder.parse("MATCH (a:Person)-[:WORKS_AT]->(c:Company)-[:LOCATED_IN]->(d:City)")
    val rq = Resolver.resolve(ast, schema)
    val plans = JoinOptimizer.plan(rq, stats = None)
    val out = GqlExplain.physical(plans)
    assert(out.contains("Physical plan"))
    assert(out.contains("Plan 0"))
    assert(out.contains("join order: [n0, e0, n1, e1, n2]"))
    // v1 carries no statistics.
    assert(out.contains("(no statistics)"))
  }

  test("physical explain on disconnected pattern reports no plans") {
    val ast = AstBuilder.parse("MATCH (a:City)-[:KNOWS]->(b:Person)")
    val rq = Resolver.resolve(ast, schema)
    val plans = JoinOptimizer.plan(rq, stats = None)
    val out = GqlExplain.physical(plans)
    assert(out.contains("(none"))
  }
}
