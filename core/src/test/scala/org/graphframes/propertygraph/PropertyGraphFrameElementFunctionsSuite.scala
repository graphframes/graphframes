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

package org.graphframes.propertygraph

import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.InvalidParseException
import org.graphframes.SparkFunSuite
import org.graphframes.propertygraph.property.EdgePropertyGroup
import org.graphframes.propertygraph.property.VertexPropertyGroup

/**
 * End-to-end tests for the standardized-column functions `id(n)` / `weight(e)`, which are
 * distinct from the raw properties `n.id` / `e.weight`, and for variable binding: repeated node
 * variables (cycles), rejected edge-variable misuse, and node/edge properties sharing a name.
 */
class PropertyGraphFrameElementFunctionsSuite
    extends SparkFunSuite
    with GraphFrameTestSparkContext {

  import sqlImplicits._

  private var pgf: PropertyGraphFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Primary key is `pid`, so there is no raw `id` property.
    val persons =
      Seq((1L, "Alice", 5), (2L, "Bob", 7), (3L, "Carol", 9)).toDF("pid", "name", "score")
    val personGroup = VertexPropertyGroup("Person", persons, "pid")

    // KNOWS: the weight column is named `w` (no raw `weight` property); `score` is also a Person
    // property name. 1 <-> 2 is a cycle, 2 -> 3 is not.
    val knows = Seq((1L, 2L, 0.5, 9), (2L, 1L, 2.0, 3), (2L, 3L, 4.0, 1))
      .toDF("s", "d", "w", "score")
    val knowsGroup =
      EdgePropertyGroup(
        "KNOWS",
        knows,
        personGroup,
        personGroup,
        isDirected = true,
        "s",
        "d",
        "w")

    // RATED: the weight column is literally named `weight`.
    val rated = Seq((1L, 3L, 0.5), (3L, 2L, 3.0)).toDF("s", "d", "weight")
    val ratedGroup = EdgePropertyGroup(
      "RATED",
      rated,
      personGroup,
      personGroup,
      isDirected = true,
      "s",
      "d",
      "weight")

    // LIKES: undirected, weight column named `w`.
    val likes = Seq((1L, 3L, 7.0)).toDF("s", "d", "w")
    val likesGroup =
      EdgePropertyGroup(
        "LIKES",
        likes,
        personGroup,
        personGroup,
        isDirected = false,
        "s",
        "d",
        "w")

    pgf = PropertyGraphFrame(Seq(personGroup), Seq(knowsGroup, ratedGroup, likesGroup))
  }

  // ---------------------------------------------------------------------------------------
  // id(n)
  // ---------------------------------------------------------------------------------------

  test("RETURN id(a) is the standardized id, the same value as a bare RETURN a") {
    val rows = pgf
      .query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, id(a) AS aid")
      .collect()
    assert(rows.length === 3)
    assert(rows.forall(r => r.getString(0) == r.getString(1)))
  }

  test("WHERE id(a) = <id> filters on the standardized id") {
    val aliceId = pgf
      .query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN id(a)")
      .collect()
      .head
      .getString(0)
    val rows = pgf
      .query(s"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE id(a) = '$aliceId' RETURN b.name")
      .collect()
      .map(_.getString(0))
      .toSet
    assert(rows === Set("Bob"))
  }

  test("id() of two adjacent nodes can be compared in a join predicate") {
    val rows = pgf
      .query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE id(a) <> id(b) RETURN a.name")
      .collect()
    assert(rows.length === 3)
  }

  // ---------------------------------------------------------------------------------------
  // weight(e) vs e.weight
  // ---------------------------------------------------------------------------------------

  test("RETURN weight(e) returns the weight column whatever its raw name") {
    val knows = pgf
      .query("MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN weight(e)")
      .collect()
      .map(_.getDouble(0))
      .toSet
    assert(knows === Set(0.5, 2.0, 4.0))
    val rated = pgf
      .query("MATCH (a:Person)-[e:RATED]->(b:Person) RETURN weight(e)")
      .collect()
      .map(_.getDouble(0))
      .toSet
    assert(rated === Set(0.5, 3.0))
  }

  test("WHERE weight(e) is pushed to the scan and works whatever the raw weight column name") {
    val knows = pgf
      .query("MATCH (a:Person)-[e:KNOWS]->(b:Person) WHERE weight(e) > 1 RETURN b.name")
      .collect()
      .map(_.getString(0))
      .toSet
    assert(knows === Set("Alice", "Carol"))
    val rated = pgf
      .query("MATCH (a:Person)-[e:RATED]->(b:Person) WHERE weight(e) > 1 RETURN b.name")
      .collect()
      .map(_.getString(0))
      .toSet
    assert(rated === Set("Bob"))
  }

  test("weight(e) on an undirected edge group is returned for both orientations") {
    val rows = pgf
      .query("MATCH (a:Person)-[e:LIKES]-(b:Person) WHERE weight(e) > 1 RETURN a.name, weight(e)")
      .collect()
      .map(r => (r.getString(0), r.getDouble(1)))
      .toSet
    assert(rows === Set(("Alice", 7.0), ("Carol", 7.0)))
  }

  test("e.weight is the raw property named weight, in WHERE and in RETURN alike") {
    val rows = pgf
      .query("MATCH (a:Person)-[e:RATED]->(b:Person) WHERE e.weight > 1 RETURN e.weight")
      .collect()
      .map(_.getDouble(0))
      .toSet
    assert(rows === Set(3.0))
  }

  test("weight(e) and a predicate on another element combine in a post-join filter") {
    val rows = pgf
      .query(
        "MATCH (a:Person)-[e:KNOWS]->(b:Person) WHERE weight(e) + b.score > 10 RETURN a.name")
      .collect()
      .map(_.getString(0))
      .toSet
    assert(rows === Set("Bob"))
  }

  // ---------------------------------------------------------------------------------------
  // Variable binding.
  // ---------------------------------------------------------------------------------------

  test("a repeated node variable matches only cycles") {
    val rows = pgf
      .query("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(a:Person) RETURN a.name, b.name")
      .collect()
      .map(r => (r.getString(0), r.getString(1)))
      .toSet
    assert(rows === Set(("Alice", "Bob"), ("Bob", "Alice")))
  }

  test("a predicate on a repeated node variable applies to every position") {
    val rows = pgf
      .query("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(a:Person) " +
        "WHERE a.name = 'Alice' RETURN a.name, b.name")
      .collect()
      .map(r => (r.getString(0), r.getString(1)))
      .toSet
    assert(rows === Set(("Alice", "Bob")))
  }

  test("a node and an edge with the same property name are kept apart") {
    val rows = pgf
      .query("MATCH (a:Person)-[e:KNOWS]->(b:Person) WHERE a.score > 5 AND e.score > 1 " +
        "RETURN a.score, e.score")
      .collect()
      .map(r => (r.getInt(0), r.getInt(1)))
      .toSet
    assert(rows === Set((7, 3)))
  }

  test("variable names that look like internal column prefixes do not collide") {
    val rows = pgf
      .query("MATCH (n1:Person)-[e0:KNOWS]->(n0:Person) WHERE n1.name = 'Alice' RETURN n0.name")
      .collect()
      .map(_.getString(0))
      .toSet
    assert(rows === Set("Bob"))
  }

  test("a bare edge variable is rejected before execution") {
    intercept[InvalidParseException] {
      pgf.query("MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN e")
    }
  }

  test("one name bound to both a node and an edge is rejected before execution") {
    intercept[InvalidParseException] {
      pgf.query("MATCH (a:Person)-[a:KNOWS]->(b:Person) RETURN b.name")
    }
  }
}
