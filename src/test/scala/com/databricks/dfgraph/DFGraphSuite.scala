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

import java.io.File

import com.google.common.io.Files
import org.apache.commons.io.FileUtils

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

class DFGraphSuite extends SparkFunSuite with DFGraphTestSparkContext {

  import DFGraphSuite._

  var vertices: DataFrame = _
  val localVertices = Map(1L -> "A", 2L -> "B", 3L -> "C")
  val localEdges = Map((1L, 2L) -> "love", (2L, 1L) -> "hate", (2L, 3L) -> "follow")
  var edges: DataFrame = _
  var tempDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDir()
    vertices = sqlContext.createDataFrame(localVertices.toSeq).toDF("id", "name")
    edges = sqlContext.createDataFrame(localEdges.toSeq.map { case ((srcId, dstId), action) =>
      (srcId, dstId, action)
    }).toDF("src", "dst", "action")
  }

  override def afterAll(): Unit = {
    FileUtils.deleteQuietly(tempDir)
    super.afterAll()
  }

  test("construction from DataFrame") {
    val g = DFGraph(vertices, edges)
    g.vertices.collect().foreach { case Row(id: Long, name: String) =>
      assert(localVertices(id) === name)
    }
    g.edges.collect().foreach { case Row(srcId: Long, dstId: Long, action: String) =>
      assert(localEdges((srcId, dstId)) === action)
    }
    intercept[IllegalArgumentException] {
      val badVertices = vertices.select(col("id").as("uid"), col("name"))
      DFGraph(badVertices, edges)
    }
    intercept[IllegalArgumentException] {
      val badEdges = edges.select(col("src").as("srcId"), col("dst"), col("action"))
      DFGraph(vertices, badEdges)
    }
    intercept[IllegalArgumentException] {
      val badEdges = edges.select(col("src"), col("dst").as("dstId"), col("action"))
      DFGraph(vertices, badEdges)
    }
  }

  test("construction from GraphX") {
    val vv = vertices.map { case Row(id: Long, name: String) =>
      (id, VertexAttr(name))
    }
    val ee = edges.map { case Row(srcId: Long, dstId: Long, action: String) =>
      Edge(srcId, dstId, EdgeAttr(action))
    }
    val g = Graph(vv, ee)
    val dfg = DFGraph.fromGraphX(g)
    dfg.vertices.collect().foreach { case Row(id: Long, Row(name: String)) =>
      assert(localVertices(id) === name)
    }
    dfg.edges.collect().foreach {
      case Row(srcId: Long, dstId: Long, Row(action: String)) =>
        assert(localEdges((srcId, dstId)) === action)
    }
  }

  test("convert to GraphX") {
    val dfg = DFGraph(vertices, edges)
    val g = dfg.toGraphX
    g.vertices.collect().foreach { case (id0, Row(id1: Long, name: String)) =>
      assert(id0 === id1)
      assert(localVertices(id0) === name)
    }
    g.edges.collect().foreach {
      case Edge(srcId0, dstId0, Row(srcId1: Long, dstId1: Long, action: String)) =>
        assert(srcId0 === srcId1)
        assert(dstId0 === dstId1)
        assert(localEdges((srcId0, dstId0)) === action)
    }
  }

  /*
  test("save/load") {
    val g0 = new DFGraph(vertices, edges)
    val output = tempDir.getPath + "/graph"
    g0.save(output)
    val g1 = DFGraph.load(sqlContext, output)
    g1.vertices.collect().foreach { case Row(id: Long, name: String) =>
      assert(localVertices(id) === name)
    }
    g1.edges.collect().foreach { case Row(srcId: Long, dstId: Long, action: String) =>
      assert(localEdges((srcId, dstId)) === action)
    }
  }
  */
}

object DFGraphSuite {
  case class VertexAttr(name: String)
  case class EdgeAttr(action: String)
}
