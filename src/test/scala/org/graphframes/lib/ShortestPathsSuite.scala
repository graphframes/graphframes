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
import org.apache.spark.sql.types.DataTypes

import org.graphframes._

class ShortestPathsSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("Simple test") {
    val edgeSeq = Seq((1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)).flatMap {
      case e => Seq(e, e.swap)
    } .map { case (src, dst) => (src.toLong, dst.toLong) }
    val edges = sqlContext.createDataFrame(edgeSeq).toDF("src", "dst")
    val graph = GraphFrame.fromEdges(edges)

    // Ground truth
    val shortestPaths = Set(
      (1, Map(1 -> 0, 4 -> 2)), (2, Map(1 -> 1, 4 -> 2)), (3, Map(1 -> 2, 4 -> 1)),
      (4, Map(1 -> 2, 4 -> 0)), (5, Map(1 -> 1, 4 -> 1)), (6, Map(1 -> 3, 4 -> 1)))

    val landmarks = Seq(1, 4).map(_.toLong)
    val v2 = graph.shortestPaths.landmarks(landmarks).run()

    TestUtils.testSchemaInvariants(graph, v2)
    TestUtils.checkColumnType(v2.schema, "distances",
      DataTypes.createMapType(v2.schema("id").dataType, DataTypes.IntegerType, false))
    val newVs = v2.select("id", "distances").collect().toSeq
    val results = newVs.map {
      case Row(id: Long, spMap: Map[Long, Int] @unchecked) => (id, spMap)
    }
    assert(results.toSet === shortestPaths)
  }

  test("friends graph") {
    val friends = examples.Graphs.friends
    val v = friends.shortestPaths.landmarks(Seq("a", "d")).run()
    val expected = Set[(String, Map[String, Int])](("a", Map("a" -> 0, "d" -> 2)), ("b", Map.empty),
      ("c", Map.empty), ("d", Map("a" -> 1, "d" -> 0)), ("e", Map("a" -> 2, "d" -> 1)),
      ("f", Map.empty), ("g", Map.empty))
    val results = v.select("id", "distances").collect().map {
      case Row(id: String, spMap: Map[String, Int] @unchecked) =>
        (id, spMap)
    }.toSet
    assert(results === expected)
  }

}
