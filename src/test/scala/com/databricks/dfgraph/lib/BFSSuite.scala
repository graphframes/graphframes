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

package com.databricks.dfgraph.lib

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}

import com.databricks.dfgraph.{DFGraph, DFGraphTestSparkContext, SparkFunSuite}


class BFSSuite extends SparkFunSuite with DFGraphTestSparkContext {

  // First graph uses String IDs
  @transient var v: DataFrame = _
  @transient var e: DataFrame = _
  @transient var g: DFGraph = _

  // Second graph uses Int IDs
  @transient var v2: DataFrame = _
  @transient var e2: DataFrame = _
  @transient var g2: DFGraph = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    /*
      a -> b <-> c
      ^          ^
      |          |
      d <- e --> f  Also, self-edge for f
     */
    v = sqlContext.createDataFrame(List(
      ("a", "f"),
      ("b", "f"),
      ("c", "m"),
      ("d", "f"),
      ("e", "m"),
      ("f", "m")
    )).toDF("id", "gender")
    e = sqlContext.createDataFrame(List(
      ("a", "b", "friend"),
      ("b", "c", "follow"),
      ("c", "b", "follow"),
      ("f", "c", "follow"),
      ("e", "f", "follow"),
      ("e", "d", "friend"),
      ("d", "a", "friend"),
      ("f", "f", "self")
    )).toDF("src", "dst", "relationship")
    g = DFGraph(v, e)

    v2 = sqlContext.createDataFrame(List(
      (0L, "f"),
      (1L, "m"),
      (2L, "m"),
      (3L, "f"))).toDF("id", "gender")
    e2 = sqlContext.createDataFrame(List(
      (0L, 1L),
      (1L, 2L),
      (2L, 3L),
      (2L, 0L))).toDF("src", "dst")
    g2 = DFGraph(v2, e2)
  }

  override def afterAll(): Unit = {
    v = null
    e = null
    g = null
    v2 = null
    e2 = null
    g2 = null
    super.afterAll()
  }

  test("unmatched queries should return nothing") {
    val badStart = g.bfs(col("id") === "howdy", col("id") === "a").run()
    assert(badStart.count() === 0)
    val badEnd = g.bfs(col("id") === "a", col("id") === "howdy").run()
    assert(badEnd.count() === 0)
  }

  test("0 hops, aka from=to") {
    val paths = g.bfs(col("id") === "a", col("id") === "a").run()
    assert(paths.count() === 1)
    assert(paths.columns.sorted === Array("from", "to"))
    assert(paths.select("from.id").head().getString(0) === "a")
    assert(paths.select("to.id").head().getString(0) === "a")
  }

  test("1 hop, aka single edge paths") {
    val paths = g.bfs(col("id") === "a", col("id") === "b").run()
    assert(paths.count() === 1)
    assert(paths.columns.sorted === Array("e0", "from", "to"))
    assert(paths.select("from.id", "to.id").head() === Row("a", "b"))
  }

  test("ties") {
    val paths = g.bfs(col("id") === "e", col("id") === "b").run()
    assert(paths.count() === 2)
    val expectedPathLength = 3
    assert(paths.columns.length === expectedPathLength * 2 + 1)
    paths.select("to.id").collect().foreach { case Row(id: String) =>
      assert(id === "b")
    }
  }

  test("maxPathLength: length 1") {
    val paths = g.bfs(col("id") === "e", col("id") === "f").setMaxPathLength(1).run()
    assert(paths.count() === 1)
    val paths0 = g.bfs(col("id") === "e", col("id") === "f").setMaxPathLength(0).run()
    assert(paths0.count() === 0)
  }

  test("maxPathLength: length > 1") {
    val paths = g.bfs(col("id") === "e", col("id") === "b").setMaxPathLength(3).run()
    assert(paths.count() === 2)
    val paths0 = g.bfs(col("id") === "e", col("id") === "b").setMaxPathLength(2).run()
    assert(paths0.count() === 0)
  }

  test("edge filter") {
    val paths1 = g.bfs(col("id") === "e", col("id") === "b").setEdgeFilter(col("src") !== "d").run()
    assert(paths1.count() === 1)
    paths1.select("e0.dst").collect().foreach { case Row(id: String) =>
      assert(id === "f")
    }
    val paths2 = g.bfs(col("id") === "e", col("id") === "b")
      .setEdgeFilter(col("relationship") === "friend").run()
    assert(paths2.count() === 1)
    paths2.select("e0.dst").collect().foreach { case Row(id: String) =>
      assert(id === "d")
    }
  }
}
