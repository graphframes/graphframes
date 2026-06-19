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
 * Pure-JVM tests for `JoinOptimizer.plan`.
 */
class JoinOptimizerSuite extends SparkFunSuite {

  // Person --KNOWS--> Person
  // Person --WORKS_AT--> Company
  // Company --LOCATED_IN--> City
  private val schema = SchemaGraphSnapshot(
    vertexGroupNames = Set("Person", "Company", "City"),
    edges = Vector(
      SchemaEdge("KNOWS", "Person", "Person"),
      SchemaEdge("WORKS_AT", "Person", "Company"),
      SchemaEdge("LOCATED_IN", "Company", "City")))

  test("single-hop path yields one plan in pattern order n0,e0,n1") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS]->(b:Person)")
    val rq = Resolver.resolve(ast, schema)
    val plans = JoinOptimizer.plan(rq, stats = None)

    assert(plans.length === 1)
    val plan = plans.head
    assert(plan.order === Vector(NodeRef(0), EdgeRef(0), NodeRef(1)))
    assert(plan.statsUsed === Map.empty)
    assert(plan.projection === Projection.Default)
    assert(plan.joinPredicates === Nil)
    assert(plan.postFilters === Nil)
  }

  test("multi-hop pattern order interleaves nodes and edges") {
    val ast =
      AstBuilder.parse("MATCH (a:Person)-[:WORKS_AT]->(c:Company)-[:LOCATED_IN]->(d:City)")
    val rq = Resolver.resolve(ast, schema)
    val plans = JoinOptimizer.plan(rq, stats = None)

    assert(plans.length === 1)
    val order = plans.head.order
    assert(order === Vector(NodeRef(0), EdgeRef(0), NodeRef(1), EdgeRef(1), NodeRef(2)))
  }

  test("untyped fan-out produces one plan per enumerated path") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[]->(x)-[]->(b:City)")
    val rq = Resolver.resolve(ast, schema)
    val plans = JoinOptimizer.plan(rq, stats = None)

    // Only Person-WORKS_AT->Company-LOCATED_IN->City reaches City in two hops.
    assert(plans.length === rq.paths.length)
    assert(plans.length >= 1)
    plans.foreach { p =>
      assert(p.order.head === NodeRef(0))
      assert(p.order.last === NodeRef(2))
    }
  }

  test("predicates and projection are carried through to each plan") {
    val ast =
      AstBuilder.parse("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > b.age RETURN a, b")
    val rq = Resolver.resolve(ast, schema)
    val plans = JoinOptimizer.plan(rq, stats = None)

    assert(plans.length === 1)
    val plan = plans.head
    assert(
      plan.projection === Projection.Items(rq.projection.asInstanceOf[Projection.Items].items))
    // `a.age > b.age` spans two adjacent node vars -> classified as a join predicate.
    assert(plan.joinPredicates.length === 1)
    assert(plan.postFilters === Nil)
  }

  test("disconnected pattern (no paths) yields no plans") {
    // Company and Person are not connected by any incoming edge into Company from City, etc.
    // Construct a pattern that cannot resolve: City ->(none)-> Person in one hop is impossible
    // because no edge has City as src reaching Person. Use a label-only dead end.
    val ast = AstBuilder.parse("MATCH (a:City)-[:KNOWS]->(b:Person)")
    val rq = Resolver.resolve(ast, schema)
    assert(rq.paths.isEmpty)
    val plans = JoinOptimizer.plan(rq, stats = None)
    assert(plans.isEmpty)
  }

  test("stats argument is accepted but does not change v1 output") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS]->(b:Person)")
    val rq = Resolver.resolve(ast, schema)
    val withStats = JoinOptimizer.plan(rq, Some(GraphStatistics.Empty))
    val withoutStats = JoinOptimizer.plan(rq, None)
    assert(withStats.map(_.order) === withoutStats.map(_.order))
    assert(withStats.head.statsUsed === Map.empty)
  }

  test("defaultPlanner and identityRefiner compose to the same as plan()") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:KNOWS]->(b:Person)")
    val rq = Resolver.resolve(ast, schema)
    val direct = JoinOptimizer.plan(rq, None)
    val viaSPI = JoinOptimizer.identityRefiner(rq, JoinOptimizer.defaultPlanner(rq, None))
    assert(viaSPI.map(_.order) === direct.map(_.order))
  }
}
