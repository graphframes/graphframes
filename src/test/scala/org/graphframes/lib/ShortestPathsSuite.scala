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

package org.graphframes.lib

import org.apache.spark.sql.Row

import org.graphframes.{GraphFrameTestSparkContext, GraphFrame, SparkFunSuite, examples}

class ShortestPathsSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("Simple test") {
    val shortestPaths = Set(
      (1, Map(1 -> 0, 4 -> 2)), (2, Map(1 -> 1, 4 -> 2)), (3, Map(1 -> 2, 4 -> 1)),
      (4, Map(1 -> 2, 4 -> 0)), (5, Map(1 -> 1, 4 -> 1)), (6, Map(1 -> 3, 4 -> 1)))
    val edgeSeq = Seq((1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)).flatMap {
      case e => Seq(e, e.swap)
    } .map { case (v1, v2) => (v1.toLong, v2.toLong) }
    val edges = sqlContext.createDataFrame(edgeSeq).toDF("src", "dst")
    val v = edgeSeq.flatMap(e => e._1 :: e._2 :: Nil).map(Tuple1.apply)

    val vertices = sqlContext.createDataFrame(v).toDF("id")
    val graph = GraphFrame(vertices, edges)
    val landmarks = Seq(1, 4).map(_.toLong)
    val g2 = graph.shortestPaths.landmarks(landmarks).run()
    LabelPropagationSuite.testSchemaInvariants(graph, g2)
    val newVs = g2.vertices.select("id", "distances").collect().toSeq
    val results = newVs.map {
      case Row(v: Long, spMap: Seq[Row] @unchecked) =>
        (v, spMap.map { case Row(a: Long, b: Int) => a -> b } .toMap)
    }
    assert(results.toSet === shortestPaths)
  }

  test("friends graph") {
    val friends = examples.Graphs.friends
    val g = friends.shortestPaths.landmarks(Seq("a", "d")).run()
    val expected = Set[(String, Map[String, Int])](("a", Map("a" -> 0, "d" -> 2)), ("b", Map.empty),
      ("c", Map.empty), ("d", Map("a" -> 1, "d" -> 0)), ("e", Map("a" -> 2, "d" -> 1)),
      ("f", Map.empty), ("g", Map.empty))
    val results = g.vertices.select("id", "distances").collect().map {
      case Row(id: String, spMap: Map[String, Int] @unchecked) =>
        (id, spMap)
    }.toSet
    assert(results === expected)
  }
}
