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

import org.graphframes.InvalidPropertyGroupException
import org.graphframes.SparkFunSuite
import org.graphframes.propertygraph.QueryOptions

/**
 * Regression tests that lock in case-insensitive matching of vertex and edge labels in the GQL
 * resolver. A query like `MATCH (a:person)` must resolve against a schema registered as `Person`,
 * and the resolved [[PathNode]]s must carry the canonical-case names from the schema (not the
 * casing the user typed).
 */
class ResolverCaseInsensitiveSuite extends SparkFunSuite {

  // Schema names use distinctive casing so tests can distinguish query casing from canonical.
  private val schema: SchemaGraphSnapshot = SchemaGraphSnapshot(
    vertexGroupNames = Set("Person", "Company"),
    edges = Vector(
      SchemaEdge("KNOWS", "Person", "Person", true),
      SchemaEdge("WORKS_AT", "Person", "Company", true)))

  val options: QueryOptions = QueryOptions()

  // ----- vertex label resolution --------------------------------------------

  test("lowercase vertex label resolves and preserves canonical case") {
    val ast = AstBuilder.parse("MATCH (a:person)")
    val rq = Resolver.resolve(ast, schema, options)
    assert(rq.paths.length === 1)
    assert(rq.paths.head.nodes.head.vertexGroupName === "Person")
  }

  test("uppercase vertex label resolves and preserves canonical case") {
    val ast = AstBuilder.parse("MATCH (a:PERSON)-[:WORKS_AT]->(b:company)")
    val rq = Resolver.resolve(ast, schema, options)
    assert(rq.paths.length === 1)
    assert(rq.paths.head.nodes.map(_.vertexGroupName) === Vector("Person", "Company"))
  }

  test("mixed-case vertex label resolves and preserves canonical case") {
    val ast = AstBuilder.parse("MATCH (a:PeRsOn)")
    val rq = Resolver.resolve(ast, schema, options)
    assert(rq.paths.length === 1)
    assert(rq.paths.head.nodes.head.vertexGroupName === "Person")
  }

  // ----- edge label resolution ----------------------------------------------

  test("lowercase edge label resolves and preserves canonical case") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:knows]->(b:Person)")
    val rq = Resolver.resolve(ast, schema, options)
    assert(rq.paths.length === 1)
    assert(rq.paths.head.steps.head.edge.edgeGroupName === "KNOWS")
  }

  test("mixed-case edge label resolves and preserves canonical case") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:WoRkS_aT]->(b:Company)")
    val rq = Resolver.resolve(ast, schema, options)
    assert(rq.paths.length === 1)
    assert(rq.paths.head.steps.head.edge.edgeGroupName === "WORKS_AT")
  }

  // ----- combined node + edge case-insensitivity ----------------------------

  test("all-uppercase query resolves against mixed-case schema") {
    val ast = AstBuilder.parse("MATCH (a:PERSON)-[:WORKS_AT]->(b:COMPANY)")
    val rq = Resolver.resolve(ast, schema, options)
    assert(rq.paths.length === 1)
    assert(rq.paths.head.nodes.map(_.vertexGroupName) === Vector("Person", "Company"))
    assert(rq.paths.head.steps.head.edge.edgeGroupName === "WORKS_AT")
  }

  // ----- error path unchanged -----------------------------------------------

  test("unknown vertex label still throws InvalidPropertyGroupException") {
    val ast = AstBuilder.parse("MATCH (a:Nonexistent)")
    intercept[InvalidPropertyGroupException] {
      Resolver.resolve(ast, schema, options)
    }
  }

  test("unknown edge label still throws InvalidPropertyGroupException") {
    val ast = AstBuilder.parse("MATCH (a:Person)-[:HATES]->(b:Person)")
    intercept[InvalidPropertyGroupException] {
      Resolver.resolve(ast, schema, options)
    }
  }
}
