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

import org.apache.spark.sql.functions.lit
import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.SparkFunSuite
import org.graphframes.propertygraph.property.EdgePropertyGroup
import org.graphframes.propertygraph.property.VertexPropertyGroup

/**
 * Regression tests for user property names that collide with the standardized columns emitted by
 * `PropertyGroup.getData`: `id` and `property_group` on vertices, `src` / `dst` / `weight` on
 * edges.
 *
 * Both `getData` overloads append the requested properties to a fixed base projection without
 * checking the requested names against the aliases that same `select` already emits:
 *
 * {{{
 *   val baseCols = Seq(idCol.alias(GraphFrame.ID), lit(name).alias(PROPERTY_GROUP_COL_NAME))
 *   filteredData.select(baseCols ++ availableProperties.map(col): _*)
 * }}}
 *
 * A raw column of the same name therefore appears twice in the scan. `QueryExecutor.renameAll`
 * maps both duplicates to the same prefixed name, and `PrefixEnv` draws no distinction between a
 * standardized column and a user property (`env.join(prefix, "id")` is exactly
 * `env.nodeCol(i, GraphFrame.ID)`), so the ambiguity reaches Spark.
 *
 * These names are not exotic. `VertexPropertyGroup.apply(name, data)` defaults the primary key to
 * `GraphFrame.ID` ("id"), and `EdgePropertyGroup.apply(..., weightColumn: Column)` performs
 * `data.withColumn(GraphFrame.WEIGHT, weightColumn)` -- so every edge group built through that
 * overload carries a raw `weight` column, and endpoint columns are conventionally `src` / `dst`.
 *
 * Each test asserts the behavior a user would expect: `a.id` is the *property* named `id` (the
 * raw key), which is distinct from the standardized, masked graph id that `getData` derives from
 * it.
 */
class PropertyNameCollisionSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  import sqlImplicits._

  private var pgf: PropertyGraphFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // `Person` carries a raw `id` (also its primary key) and a raw `property_group`, both of
    // which collide with a standardized vertex column. `name` is the non-colliding control.
    val persons = Seq((1L, "Alice", "vip"), (2L, "Bob", "standard"))
      .toDF("id", "name", "property_group")
    val personGroup = VertexPropertyGroup("Person", persons, "id")

    // The `weightColumn: Column` overload adds a raw `weight` column, so `KNOWS` collides on
    // `src`, `dst` and `weight`. `since` is the non-colliding control.
    val knows = Seq((1L, 2L, 2010), (2L, 1L, 2012)).toDF("src", "dst", "since")
    val knowsGroup = EdgePropertyGroup(
      "KNOWS",
      knows,
      personGroup,
      personGroup,
      isDirected = true,
      "src",
      "dst",
      lit(1.0))

    // Undirected groups union two orientations inside `getData`, so a collision there fails one
    // level earlier than for a directed group -- before the executor is ever reached.
    val likes = Seq((1L, 2L, 2015)).toDF("src", "dst", "since")
    val likesGroup = EdgePropertyGroup(
      "LIKES",
      likes,
      personGroup,
      personGroup,
      isDirected = false,
      "src",
      "dst",
      lit(1.0))

    pgf = PropertyGraphFrame(Seq(personGroup), Seq(knowsGroup, likesGroup))
  }

  // ---------------------------------------------------------------------------------------
  // Controls: non-colliding property names on the same fixture, so a failure below is
  // attributable to the collision rather than to the fixture or the pipeline.
  // ---------------------------------------------------------------------------------------

  test("control: a non-colliding vertex property is returned") {
    val rows = pgf
      .query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name")
      .collect()
      .map(_.getString(0))
      .toSet
    assert(rows === Set("Alice", "Bob"))
  }

  test("control: a non-colliding edge property is returned") {
    val rows = pgf
      .query("MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN e.since")
      .collect()
      .map(_.getInt(0))
      .toSet
    assert(rows === Set(2010, 2012))
  }

  // ---------------------------------------------------------------------------------------
  // Vertex-side collisions: `id` and `property_group`.
  // ---------------------------------------------------------------------------------------

  test("RETURN a.id returns the raw id property, not an ambiguous reference") {
    val rows = pgf
      .query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.id")
      .collect()
      .map(_.get(0).toString)
      .toSet
    // The `id` *property* is the raw key (1, 2). The standardized graph id is the masked
    // "Person" + sha2(...) form, which is a different value and is not what `a.id` names.
    assert(rows === Set("1", "2"))
  }

  test("RETURN a.property_group returns the raw property, not the injected group literal") {
    val rows = pgf
      .query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.property_group")
      .collect()
      .map(_.getString(0))
      .toSet
    // The user's own column holds "vip"/"standard"; `getData` separately injects the literal
    // group name "Person" under the same output name.
    assert(rows === Set("vip", "standard"))
  }

  // ---------------------------------------------------------------------------------------
  // Edge-side collisions: `weight`, `src`, `dst`.
  // ---------------------------------------------------------------------------------------

  test("RETURN e.weight returns the raw weight property") {
    val rows = pgf
      .query("MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN e.weight")
      .collect()
      .map(_.getDouble(0))
      .toSet
    assert(rows === Set(1.0))
  }

  test("RETURN e.src returns the raw src property") {
    val rows = pgf
      .query("MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN e.src")
      .collect()
      .map(_.get(0).toString)
      .toSet
    assert(rows === Set("1", "2"))
  }

  test("RETURN e.dst returns the raw dst property") {
    val rows = pgf
      .query("MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN e.dst")
      .collect()
      .map(_.get(0).toString)
      .toSet
    assert(rows === Set("1", "2"))
  }

  // ---------------------------------------------------------------------------------------
  // The two aggravated shapes.
  // ---------------------------------------------------------------------------------------

  test("a colliding property that is also carried by a predicate does not break the join") {
    // Here `id` is not merely projected: it is classified as a carried property, so the
    // duplicate survives into the adjacency condition and breaks the join itself rather than
    // the terminal join-back.
    val rows = pgf
      .query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = 1 RETURN a.id")
      .collect()
      .map(_.get(0).toString)
      .toSet
    assert(rows === Set("1"))
  }

  test("a colliding property on an undirected edge group survives the two-orientation union") {
    // `EdgePropertyGroup.getData` unions the reoriented copy, re-selecting the already
    // duplicated columns, so an undirected group fails inside `getData` itself.
    val rows = pgf
      .query("MATCH (a:Person)-[e:LIKES]-(b:Person) RETURN e.weight")
      .collect()
      .map(_.getDouble(0))
      .toSet
    assert(rows === Set(1.0))
  }
}
