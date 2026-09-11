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

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.graphframes.GraphFrame
import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.SparkFunSuite
import org.graphframes.propertygraph.property.EdgePropertyGroup
import org.graphframes.propertygraph.property.VertexPropertyGroup

/**
 * Tests for the `getData(filter, requestedProperties)` contract added for the GQL engine.
 *
 * The query executor leans on three guarantees from this method, and a regression in any of them
 * surfaces far away from here (as a missing column inside a join, or as silently doubled rows):
 *   - the standardized columns come first and are always present, whatever is requested;
 *   - requested property columns are carried through verbatim, never masked, and an unknown name
 *     is dropped rather than raising;
 *   - an undirected edge group surfaces both orientations, with the property columns copied into
 *     both halves of the union.
 */
class PropertyGroupGetDataSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  import sqlImplicits._

  private def people = Seq((1L, "Alice", 30), (2L, "Bob", 40)).toDF("id", "name", "age")

  private def personGroup = VertexPropertyGroup("Person", people, "id")

  private def edgeData = Seq((1L, 2L, "friend", 5)).toDF("src", "dst", "kind", "since")

  private def directedEdges =
    EdgePropertyGroup("KNOWS", edgeData, personGroup, personGroup, true, "src", "dst", lit(1.0))

  private def undirectedEdges =
    EdgePropertyGroup("KNOWS", edgeData, personGroup, personGroup, false, "src", "dst", lit(1.0))

  // ---------------------------------------------------------------------
  // Vertex groups.
  // ---------------------------------------------------------------------

  test("a vertex scan with no requested properties projects only the standardized columns") {
    val df = personGroup.getData(lit(true), Seq.empty)
    assert(df.columns.toSeq === Seq(GraphFrame.ID, PropertyGraphFrame.PROPERTY_GROUP_COL_NAME))
  }

  test("requested vertex properties are appended after the standardized columns") {
    val df = personGroup.getData(lit(true), Seq("age", "name"))
    assert(
      df.columns.toSeq ===
        Seq(GraphFrame.ID, PropertyGraphFrame.PROPERTY_GROUP_COL_NAME, "age", "name"))
  }

  test("requested vertex properties keep the order they were requested in") {
    // The executor passes a sorted Seq; the method itself must not re-sort, or the two would
    // silently disagree about which column is which.
    val df = personGroup.getData(lit(true), Seq("name", "age"))
    assert(df.columns.toSeq.drop(2) === Seq("name", "age"))
  }

  test("an unknown requested vertex property is dropped silently") {
    val df = personGroup.getData(lit(true), Seq("age", "does_not_exist"))
    assert(df.columns.toSeq.contains("age"))
    assert(!df.columns.toSeq.contains("does_not_exist"))
    assert(df.count() === 2)
  }

  test("requested vertex properties carry their raw values, unmasked") {
    val rows = personGroup
      .getData(lit(true), Seq("name", "age"))
      .select("name", "age")
      .collect()
      .map(r => (r.getString(0), r.getInt(1)))
      .toSet
    assert(rows === Set(("Alice", 30), ("Bob", 40)))
  }

  test("the vertex id is masked with the group name by default") {
    val ids = personGroup.getData(lit(true), Seq.empty).select(GraphFrame.ID).as[String].collect()
    assert(ids.forall(_.startsWith("Person")))
    // sha2(.., 256) is 64 hex chars appended to the group name.
    assert(ids.forall(_.length === "Person".length + 64))
    assert(ids.distinct.length === 2)
  }

  test("applyMaskOnId = false surfaces the raw primary key as a string") {
    val unmasked = VertexPropertyGroup("Person", people, "id", applyMaskOnId = false)
    val ids =
      unmasked.getData(lit(true), Seq("name")).select(GraphFrame.ID).as[String].collect().toSet
    assert(ids === Set("1", "2"))
  }

  test("the filter is applied against the raw columns, before masking") {
    val df = personGroup.getData(col("age") > 35, Seq("name"))
    assert(df.select("name").as[String].collect().toSeq === Seq("Bob"))
  }

  test("the filter composes with requested properties rather than replacing them") {
    val df = personGroup.getData(col("name") === "Alice", Seq("age"))
    assert(df.count() === 1)
    assert(df.select("age").as[Int].collect().toSeq === Seq(30))
  }

  test("the property_group column is a constant equal to the group name") {
    val groups = personGroup
      .getData(lit(true), Seq.empty)
      .select(PropertyGraphFrame.PROPERTY_GROUP_COL_NAME)
      .as[String]
      .collect()
      .toSet
    assert(groups === Set("Person"))
  }

  test("the no-argument and filter-only overloads agree with the explicit empty request") {
    val explicit = personGroup.getData(lit(true), Seq.empty).columns.toSeq
    assert(personGroup.getData().columns.toSeq === explicit)
    assert(personGroup.getData(lit(true)).columns.toSeq === explicit)
    assert(personGroup.getData(Seq.empty[String]).columns.toSeq === explicit)
  }

  test("the requested-properties-only overload applies no filter") {
    assert(personGroup.getData(Seq("age")).count() === 2)
  }

  // ---------------------------------------------------------------------
  // Edge groups.
  // ---------------------------------------------------------------------

  test("a directed edge scan projects src, dst and weight") {
    val df = directedEdges.getData(lit(true), Seq.empty)
    assert(df.columns.toSeq === Seq(GraphFrame.SRC, GraphFrame.DST, GraphFrame.WEIGHT))
    assert(df.count() === 1)
  }

  test("requested edge properties are appended after the standardized columns") {
    val df = directedEdges.getData(lit(true), Seq("kind", "since"))
    assert(
      df.columns.toSeq ===
        Seq(GraphFrame.SRC, GraphFrame.DST, GraphFrame.WEIGHT, "kind", "since"))
  }

  test("an unknown requested edge property is dropped silently") {
    val df = directedEdges.getData(lit(true), Seq("kind", "nope"))
    assert(df.columns.toSeq.contains("kind"))
    assert(!df.columns.toSeq.contains("nope"))
  }

  test("edge src and dst are masked the same way as the endpoint groups' ids") {
    // This is what lets the executor equi-join edge.src against node.id with no casting.
    val edgeRow = directedEdges.getData(lit(true), Seq.empty).collect().head
    val vertexIds =
      personGroup.getData(lit(true), Seq.empty).select(GraphFrame.ID).as[String].collect().toSet
    assert(vertexIds.contains(edgeRow.getString(0)))
    assert(vertexIds.contains(edgeRow.getString(1)))
  }

  test("an undirected edge scan surfaces both orientations") {
    val rows = undirectedEdges
      .getData(lit(true), Seq.empty)
      .select(GraphFrame.SRC, GraphFrame.DST)
      .collect()
      .map(r => (r.getString(0), r.getString(1)))
    assert(rows.length === 2)
    assert(rows(0) === ((rows(1)._2, rows(1)._1)))
  }

  test("an undirected edge scan copies requested properties into both orientations") {
    val rows = undirectedEdges
      .getData(lit(true), Seq("kind", "since"))
      .select("kind", "since")
      .collect()
      .map(r => (r.getString(0), r.getInt(1)))
    assert(rows.length === 2)
    assert(rows.toSet === Set(("friend", 5)))
  }

  test("an undirected edge scan keeps the column order stable across the union") {
    // A union that reordered columns would silently transpose property values between the two
    // orientations, so the schema of both halves must match exactly.
    val df = undirectedEdges.getData(lit(true), Seq("kind", "since"))
    assert(
      df.columns.toSeq ===
        Seq(GraphFrame.SRC, GraphFrame.DST, GraphFrame.WEIGHT, "kind", "since"))
  }

  test("an undirected edge filter is applied once, before the orientation union") {
    // Filtering to nothing must leave nothing -- not one row per orientation.
    assert(undirectedEdges.getData(col("kind") === "nobody", Seq.empty).count() === 0)
    assert(undirectedEdges.getData(col("kind") === "friend", Seq.empty).count() === 2)
  }

  test("requesting a standardized column name duplicates it (documented caveat)") {
    // The Column-weight constructor overload materialises `weight` into `data`, so `weight` is
    // both a standardized column and a member of data.columns; requesting it emits it twice.
    // The query engine never does this (it requests only property names it resolved from the
    // pattern), but the behaviour is pinned here so a future de-duplication is a deliberate
    // change rather than an accident.
    val df = directedEdges.getData(lit(true), Seq(GraphFrame.WEIGHT))
    assert(df.columns.count(_ == GraphFrame.WEIGHT) === 2)
  }
}
