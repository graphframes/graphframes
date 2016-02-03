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

import org.graphframes.{GraphFrameTestSparkContext, GraphFrame, SparkFunSuite}


class ConnectedComponentsSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("simple toy example") {
    val v = sqlContext.createDataFrame(List(
      (0L, "a", "b"))).toDF("id", "vattr", "gender")
    // Create an empty dataframe with the proper columns.
    val e = sqlContext.createDataFrame(List((0L, 0L, 1L))).toDF("src", "dst", "test").filter("src > 10")
    val g = GraphFrame(v, e)
    val comps = ConnectedComponents.run(g)
    LabelPropagationSuite.testSchemaInvariants(g, comps)
    assert(comps.vertices.count() === 1)
    assert(comps.edges.count() === 0)
    // We loose all the attributes for now, due to a limitation of the graphx implementation
    assert(comps.vertices.select("id", "component", "vattr", "gender").collect() === Seq(Row(0L, 0L, "a", "b")))
  }

  test("simple connected toy example") {
    val v = sqlContext.createDataFrame(List(
      (0L, "a0", "b0"),
      (1L, "a1", "b1"))).toDF("id", "A", "B")
    val e = sqlContext.createDataFrame(List(
      (0L, 1L, "a01", "b01"))).toDF("src", "dst", "A", "B")
    val g = GraphFrame(v, e)
    val comps = ConnectedComponents.run(g)
    LabelPropagationSuite.testSchemaInvariants(g, comps)
    assert(comps.vertices.count() === 2)
    assert(comps.edges.count() === 1)
    // We loose all the attributes for now, due to a limitation of the graphx implementation
    assert(List(Row(0L, 1L, "a01", "b01")) === comps.edges.collect())
    val vxs = comps.vertices.select("id", "component", "A", "B").collect()
    assert(List(Row(0L, 0L, "a0", "b0"), Row(1L, 0L, "a1", "b1")) === vxs)
  }

}
