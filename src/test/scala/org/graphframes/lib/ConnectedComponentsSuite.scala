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

import scala.reflect.ClassTag

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.DataTypes

import org.graphframes._

class ConnectedComponentsSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("single vertex") {
    val v = sqlContext.createDataFrame(List(
      (0L, "a", "b"))).toDF("id", "vattr", "gender")
    // Create an empty dataframe with the proper columns.
    val e = sqlContext.createDataFrame(List((0L, 0L, 1L))).toDF("src", "dst", "test")
      .filter("src > 10")
    val g = GraphFrame(v, e)
    val comps = ConnectedComponents.run(g)
    TestUtils.testSchemaInvariants(g, comps)
    TestUtils.checkColumnType(comps.schema, "component", DataTypes.LongType)
    assert(comps.count() === 1)
    assert(comps.select("id", "component", "vattr", "gender").collect()
      === Seq(Row(0L, 0L, "a", "b")))
  }

  test("two connected vertices") {
    val v = sqlContext.createDataFrame(List(
      (0L, "a0", "b0"),
      (1L, "a1", "b1"))).toDF("id", "A", "B")
    val e = sqlContext.createDataFrame(List(
      (0L, 1L, "a01", "b01"))).toDF("src", "dst", "A", "B")
    val g = GraphFrame(v, e)
    val comps = g.connectedComponents.run()
    TestUtils.testSchemaInvariants(g, comps)
    assert(comps.count() === 2)
    val vxs = comps.sort("id").select("id", "component", "A", "B").collect()
    assert(List(Row(0L, 0L, "a0", "b0"), Row(1L, 0L, "a1", "b1")) === vxs)
  }

  test("friends graph") {
    val friends = examples.Graphs.friends
    val expected = Set(Set("a", "b", "c", "d", "e", "f"), Set("g"))
    for ((algorithm, broadcastThreshold) <-
         Seq(("graphx", 1000000), ("graphframes", 100000), ("graphframes", 1))) {
      val components = friends.connectedComponents
        .setAlgorithm(algorithm)
        .setBroadcastThreshold(broadcastThreshold)
        .run()
      assertComponents(components, expected)
    }
  }

  private def assertComponents[T: ClassTag](actual: DataFrame, expected: Set[Set[T]]): Unit = {
    // note: not using agg + collect_list because collect_list is not available in 1.6.2
    val actualComponents = actual.select("component", "id").rdd
      .map { case Row(component: Long, id: T) =>
        (component, id)
      }.groupByKey()
      .values
      .map(_.toSeq)
      .collect()
      .map { ids =>
        val idSet = ids.toSet
        assert(idSet.size === ids.size,
          s"Found duplicated component assignment in [${ids.mkString(",")}].")
        idSet
      }.toSet
    assert(actualComponents === expected)
  }
}
