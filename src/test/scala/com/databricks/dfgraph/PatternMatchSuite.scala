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

package com.databricks.dfgraph

import org.apache.spark.sql.{DataFrame, Row}

class PatternMatchSuite extends SparkFunSuite with DFGraphTestSparkContext {

  @transient var v: DataFrame = _
  @transient var e: DataFrame = _
  @transient var g: DFGraph = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    v = sqlContext.createDataFrame(List(
      (0L, "a"),
      (1L, "b"),
      (2L, "c"),
      (3L, "d"))).toDF("id", "attr")
    e = sqlContext.createDataFrame(List(
      (0L, 1L),
      (1L, 2L),
      (2L, 3L),
      (2L, 0L))).toDF("src", "dst")
    g = DFGraph(v, e)
  }

  override def afterAll(): Unit = {
    v = null
    e = null
    g = null
    super.afterAll()
  }

  test("triangles") {
    val triangles = g.find("(a)-[]->(b); (b)-[]->(c); (c)-[]->(a)")
      .select("a_id", "b_id", "c_id")

    assert(triangles.collect().toSet === Set(
      Row(0L, 1L, 2L),
      Row(2L, 0L, 1L),
      Row(1L, 2L, 0L)
    ))
  }

  test("vertex queries") {
    val vertices = g.find("(a)")
    assert(vertices.columns === Array("a_id", "a_attr"))
    assert(vertices.collect().toSet === v.collect().toSet)

    val empty = g.find("()")
    assert(empty.collect() === Array.empty)
  }

  test("triplets") {
    val triplets = g.find("(u)-[]->(v)")

    assert(triplets.columns === Array("u_id", "u_attr", "v_id", "v_attr"))
    assert(triplets.collect().toSet === Set(
      Row(0L, "a", 1L, "b"),
      Row(1L, "b", 2L, "c"),
      Row(2L, "c", 3L, "d"),
      Row(2L, "c", 0L, "a")
    ))
  }

  test("negation") {
    val fof = g.find("(u)-[]->(v); (v)-[]->(w); !(u)-[]->(w); !(w)-[]->(u)")
      .select("u_id", "v_id", "w_id")

    assert(fof.collect().toSet === Set(Row(1L, 2L, 3L)))
  }

  /*
  // Join elimination will not work without Ankur's improved indexing.
  test("join elimination - simple") {
    import org.apache.spark.sql.catalyst.plans.logical.Join

    val edges = g.find("(u)-[e]->(v)", _.select("e_src", "e_dst"))
    val joins = edges.queryExecution.optimizedPlan.collect {
      case j: Join => j
    }

    assert(joins.isEmpty, s"joins was non-empty: ${joins.map(_.toString()).mkString("; ")}")
  }

  test("join elimination - with aliases") {
    import org.apache.spark.sql.catalyst.plans.logical.Join

    val edges = g.find("(u)-[]->(v)", _.select("u_id", "v_id"))
    println(edges.queryExecution.optimizedPlan)
    val joins = edges.queryExecution.optimizedPlan.collect {
      case j: Join => j
    }
    assert(joins.isEmpty, s"joins was non-empty: ${joins.map(_.toString()).mkString("; ")}")
  }
  */
}
