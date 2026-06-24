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

import java.security.MessageDigest

/**
 * Regression tests that lock in case-insensitive label matching in the public property-graph
 * Scala API: [[PropertyGraphFrame.toGraphFrame]] and [[PropertyGraphFrame.projectionBy]]. Group
 * names passed with different casing than they were registered with must still resolve.
 */
class PropertyGraphFrameCaseInsensitiveSuite
    extends SparkFunSuite
    with GraphFrameTestSparkContext {

  import sqlImplicits._

  private var graph: PropertyGraphFrame = _

  // Groups are registered with distinctive casing; queries below deliberately use other cases.
  private val peopleName = "People"
  private val moviesName = "Movies"
  private val likesName = "Likes"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val peopleData = Seq((1L, "Alice"), (2L, "Bob")).toDF("id", "name")
    val peopleGroup = VertexPropertyGroup(peopleName, peopleData, "id")

    val moviesData = Seq((10L, "Matrix"), (20L, "Inception")).toDF("id", "title")
    val moviesGroup = VertexPropertyGroup(moviesName, moviesData, "id")

    val likesData = Seq((1L, 10L), (2L, 20L)).toDF("src", "dst")
    val likesGroup = EdgePropertyGroup(
      likesName,
      likesData,
      peopleGroup,
      moviesGroup,
      isDirected = true,
      "src",
      "dst",
      lit(1.0))

    graph = PropertyGraphFrame(Seq(peopleGroup, moviesGroup), Seq(likesGroup))
  }

  // Mirrors the internal ID masking so tests can compare against hashed IDs.
  private def sha256Hash(id: Long, groupName: String): String = {
    val md = MessageDigest.getInstance("SHA-256")
    val hash = md.digest(id.toString.getBytes("UTF-8")).map("%02x".format(_)).mkString
    s"$groupName$hash"
  }

  // ----- toGraphFrame -------------------------------------------------------

  test("toGraphFrame resolves lowercase group names") {
    val gf = graph.toGraphFrame(
      Seq("people"),
      Seq("likes"),
      Map("likes" -> lit(true)),
      Map("people" -> lit(true)))
    assert(gf.vertices.count() === 2)
    assert(gf.edges.count() === 2)
  }

  test("toGraphFrame resolves uppercase group names") {
    val gf = graph.toGraphFrame(
      Seq("PEOPLE", "MOVIES"),
      Seq("LIKES"),
      Map("LIKES" -> lit(true)),
      Map("PEOPLE" -> lit(true), "MOVIES" -> lit(true)))
    assert(gf.vertices.count() === 4)
    assert(gf.edges.count() === 2)
  }

  test("toGraphFrame resolves mixed-case group names") {
    val gf = graph.toGraphFrame(
      Seq("pEoPlE"),
      Seq("lIkEs"),
      Map("lIkEs" -> lit(true)),
      Map("pEoPlE" -> lit(true)))
    assert(gf.vertices.count() === 2)
    assert(gf.edges.count() === 2)
  }

  test("toGraphFrame preserves canonical IDs despite query casing") {
    val gf = graph.toGraphFrame(
      Seq("people"),
      Seq("likes"),
      Map("likes" -> lit(true)),
      Map("people" -> lit(true)))
    val expectedVertices = Set(sha256Hash(1L, peopleName), sha256Hash(2L, peopleName))
    val actualVertices = gf.vertices.collect().map(_.getString(0)).toSet
    assert(actualVertices === expectedVertices)
  }

  test("toGraphFrame rejects unknown vertex group") {
    intercept[IllegalArgumentException] {
      graph.toGraphFrame(
        Seq("Nonexistent"),
        Seq("likes"),
        Map("likes" -> lit(true)),
        Map("Nonexistent" -> lit(true)))
    }
  }

  test("toGraphFrame rejects unknown edge group") {
    intercept[IllegalArgumentException] {
      graph.toGraphFrame(
        Seq("people"),
        Seq("Hates"),
        Map("Hates" -> lit(true)),
        Map("people" -> lit(true)))
    }
  }

  // ----- projectionBy -------------------------------------------------------

  test("projectionBy resolves lowercase group names") {
    val projected = graph.projectionBy(
      leftBiGraphPart = "people", // registered as "People"
      rightBiGraphPart = "movies", // registered as "Movies"
      edgeGroup = "likes"
    ) // registered as "Likes"
    // Projection drops the "through" group (movies); one projected group remains.
    assert(projected.vertexPropertyGroups.map(_.name).toSet === Set(peopleName))
    // The projected edge group name echoes the query casing by design.
    assert(projected.edgesPropertyGroups.map(_.name).toSet === Set("projected_likes"))
  }

  test("projectionBy resolves uppercase group names") {
    val projected = graph.projectionBy(
      leftBiGraphPart = "PEOPLE",
      rightBiGraphPart = "MOVIES",
      edgeGroup = "LIKES")
    assert(projected.vertexPropertyGroups.map(_.name).toSet === Set(peopleName))
    assert(projected.edgesPropertyGroups.map(_.name).toSet === Set("projected_LIKES"))
  }

  test("projectionBy rejects unknown edge group") {
    intercept[NoSuchElementException] {
      graph.projectionBy(
        leftBiGraphPart = "people",
        rightBiGraphPart = "movies",
        edgeGroup = "Hates")
    }
  }
}
