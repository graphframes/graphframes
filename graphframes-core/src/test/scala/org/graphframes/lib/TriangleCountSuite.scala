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
import org.graphframes.GraphFrame
import org.graphframes.GraphFrame.quote
import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.GraphFramesUnreachableException
import org.graphframes.SparkFunSuite
import org.graphframes.TestUtils

class TriangleCountSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("Count a single triangle") {
    val edges = spark.createDataFrame(Seq(0L -> 1L, 1L -> 2L, 2L -> 0L)).toDF("src", "dst")
    val vertices = spark
      .createDataFrame(Seq((0L, "a"), (1L, "b"), (2L, "c")))
      .toDF("id", "a")
    val g = GraphFrame(vertices, edges)
    val v2 = g.triangleCount.run()
    TestUtils.testSchemaInvariants(g, v2)
    TestUtils.checkColumnType(v2.schema, "count", DataTypes.LongType)
    v2.select("id", "count", "a")
      .collect()
      .foreach {
        case Row(vid: Long, count: Long, _) => assert(count === 1)
        case _: Row => throw new GraphFramesUnreachableException()
      }
  }

  test("Count two triangles") {
    val edges = spark
      .createDataFrame(
        Seq(0L -> 1L, 1L -> 2L, 2L -> 0L) ++
          Seq(0L -> -1L, -1L -> -2L, -2L -> 0L))
      .toDF("src", "dst")
    val g = GraphFrame.fromEdges(edges)
    val v2 = g.triangleCount.run()
    v2.select("id", "count").collect().foreach {
      case Row(id: Long, count: Long) =>
        if (id == 0) {
          assert(count === 2)
        } else {
          assert(count === 1)
        }
      case _: Row => throw new GraphFramesUnreachableException()
    }
  }

  test("Count one triangles with bi-directed edges") {
    // Note: This is different from GraphX, which double-counts triangles with bidirected edges.
    val triangles = Seq(0L -> 1L, 1L -> 2L, 2L -> 0L) ++ Seq(0L -> -1L, -1L -> -2L, -2L -> 0L)
    val revTriangles = triangles.map { case (a, b) => (b, a) }
    val edges = spark.createDataFrame(triangles ++ revTriangles).toDF("src", "dst")
    val g = GraphFrame.fromEdges(edges)
    val v2 = g.triangleCount.run()
    v2.select("id", "count").collect().foreach {
      case Row(id: Long, count: Long) =>
        if (id == 0) {
          assert(count === 2)
        } else {
          assert(count === 1)
        }
      case _: Row => throw new GraphFramesUnreachableException()
    }
  }

  test("Count a single triangle with duplicate edges") {
    val edges = spark
      .createDataFrame(
        Seq(0L -> 1L, 1L -> 2L, 2L -> 0L) ++
          Seq(0L -> 1L, 1L -> 2L, 2L -> 0L))
      .toDF("src", "dst")
    val g = GraphFrame.fromEdges(edges)
    val v2 = g.triangleCount.run()
    v2.select("id", "count").collect().foreach {
      case Row(id: Long, count: Long) =>
        assert(count === 1)
      case _: Row => throw new GraphFramesUnreachableException()
    }
  }

  test("Count with dot column name") {
    val edges = sqlContext.createDataFrame(Seq(0L -> 1L, 1L -> 2L, 2L -> 0L)).toDF("src", "dst")
    val vertices = sqlContext
      .createDataFrame(Seq((0L, "a"), (1L, "b"), (2L, "c")))
      .toDF("id", "a.column")
    val g = GraphFrame(vertices, edges)
    val v2 = g.triangleCount.run()
    TestUtils.testSchemaInvariants(g, v2)
    TestUtils.checkColumnType(v2.schema, "count", DataTypes.LongType)
    v2.select("id", "count", quote("a.column"))
      .collect()
      .foreach {
        case Row(vid: Long, count: Long, _) => assert(count === 1)
        case _: Row => throw new GraphFramesUnreachableException()
      }
  }

  test("Count with backquote in column name") {
    val edges = sqlContext.createDataFrame(Seq(0L -> 1L, 1L -> 2L, 2L -> 0L)).toDF("src", "dst")
    val vertices = sqlContext
      .createDataFrame(Seq((0L, "a"), (1L, "b"), (2L, "c")))
      .toDF("id", "a `column`")
    val g = GraphFrame(vertices, edges)
    val v2 = g.triangleCount.run()
    TestUtils.testSchemaInvariants(g, v2)
    TestUtils.checkColumnType(v2.schema, "count", DataTypes.LongType)
    v2.select("id", "count", quote("a `column`"))
      .collect()
      .foreach {
        case Row(vid: Long, count: Long, _) => assert(count === 1)
        case _: Row => throw new GraphFramesUnreachableException()
      }
  }

  test("no triangle") {
    val edges = spark.createDataFrame(Seq(0L -> 1L, 1L -> 2L)).toDF("src", "dst")
    val g = GraphFrame.fromEdges(edges)
    val v2 = g.triangleCount.run()
    v2.select("count").collect().foreach {
      case Row(count: Long) =>
        assert(count === 0)
      case _: Row => throw new GraphFramesUnreachableException()
    }
  }
}
