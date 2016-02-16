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

package org.graphframes

import java.io.File

import scala.collection.mutable

import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row}

import org.graphframes.examples.Graphs
import org.graphframes.lib.AggregateMessages


class GraphFrameSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  var vertices: DataFrame = _
  val localVertices = Map(1L -> "A", 2L -> "B", 3L -> "C")
  val localEdges = Map((1L, 2L) -> "love", (2L, 1L) -> "hate", (2L, 3L) -> "follow")
  var edges: DataFrame = _
  var tempDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDir()
    // Note: We use non-local DataFrames because of a bug in Spark 1.4 which prevents us from
    //       using monotonicallyIncreasingID: SPARK-9020
    //       This fix in 1.5 will not be backported to 1.4, but we could fix it by having two
    //       implementations of indexing in GraphFrame.toGraphX.
    vertices = sqlContext.createDataFrame(sc.parallelize(localVertices.toSeq)).toDF("id", "name")
    edges = sqlContext.createDataFrame(sc.parallelize(localEdges.toSeq).map {
      case ((src, dst), action) =>
        (src, dst, action)
    }).toDF("src", "dst", "action")
  }

  override def afterAll(): Unit = {
    FileUtils.deleteQuietly(tempDir)
    super.afterAll()
  }

  test("construction from DataFrames") {
    val g = GraphFrame(vertices, edges)
    g.vertices.collect().foreach { case Row(id: Long, name: String) =>
      assert(localVertices(id) === name)
    }
    g.edges.collect().foreach { case Row(src: Long, dst: Long, action: String) =>
      assert(localEdges((src, dst)) === action)
    }
    intercept[IllegalArgumentException] {
      val badVertices = vertices.select(col("id").as("uid"), col("name"))
      GraphFrame(badVertices, edges)
    }
    intercept[IllegalArgumentException] {
      val badEdges = edges.select(col("src").as("srcId"), col("dst"), col("action"))
      GraphFrame(vertices, badEdges)
    }
    intercept[IllegalArgumentException] {
      val badEdges = edges.select(col("src"), col("dst").as("dstId"), col("action"))
      GraphFrame(vertices, badEdges)
    }
  }

  test("construction from edge DataFrame") {
    val g = GraphFrame.fromEdges(edges)
    assert(g.vertices.columns === Array("id"))
    val idsFromVertices = g.vertices.select("id").map(_.getLong(0)).collect()
    val idsFromVerticesSet = idsFromVertices.toSet
    assert(idsFromVertices.length === idsFromVerticesSet.size)
    val idsFromEdgesSet = g.edges.select("src", "dst").flatMap { case Row(src: Long, dst: Long) =>
      Seq(src, dst)
    }.collect().toSet
    assert(idsFromVerticesSet === idsFromEdgesSet)
  }

  test("construction from GraphX") {
    val vv: RDD[(Long, String)] = vertices.map { case Row(id: Long, name: String) =>
      (id, name)
    }
    val ee: RDD[Edge[String]] = edges.map { case Row(src: Long, dst: Long, action: String) =>
      Edge(src, dst, action)
    }
    val g = Graph(vv, ee)
    val gf = GraphFrame.fromGraphX(g)
    gf.vertices.select("id", "attr").collect().foreach { case Row(id: Long, name: String) =>
      assert(localVertices(id) === name)
    }
    gf.edges.select("src", "dst", "attr").collect().foreach {
      case Row(src: Long, dst: Long, action: String) =>
        assert(localEdges((src, dst)) === action)
    }
  }

  test("convert to GraphX: Long IDs") {
    val gf = GraphFrame(vertices, edges)
    val g = gf.toGraphX
    g.vertices.collect().foreach { case (id0, Row(id1: Long, name: String)) =>
      assert(id0 === id1)
      assert(localVertices(id0) === name)
    }
    g.edges.collect().foreach {
      case Edge(src0, dst0, Row(src1: Long, dst1: Long, action: String)) =>
        assert(src0 === src1)
        assert(dst0 === dst1)
        assert(localEdges((src0, dst0)) === action)
    }
  }

  test("convert to GraphX: Int IDs") {
    val vv = vertices.select(col("id").cast(IntegerType).as("id"), col("name"))
    val ee = edges.select(col("src").cast(IntegerType).as("src"),
      col("dst").cast(IntegerType).as("dst"), col("action"))
    val gf = GraphFrame(vv, ee)
    val g = gf.toGraphX
    // Int IDs should be directly cast to Long, so ID values should match.
    val vCols = gf.vColsMap
    val eCols = gf.eColsMap
    g.vertices.collect().foreach { case (id0: Long, attr: Row) =>
      val id1 = attr.getInt(vCols("id"))
      val name = attr.getString(vCols("name"))
      assert(id0 === id1)
      assert(localVertices(id0) === name)
    }
    g.edges.collect().foreach {
      case Edge(src0: Long, dst0: Long, attr: Row) =>
        val src1 = attr.getInt(eCols("src"))
        val dst1 = attr.getInt(eCols("dst"))
        val action = attr.getString(eCols("action"))
        assert(src0 === src1)
        assert(dst0 === dst1)
        assert(localEdges((src0, dst0)) === action)
    }
  }

  ignore("convert to GraphX: String IDs") {
    try {
      val vv = vertices.select(col("id").cast(StringType).as("id"), col("name"))
      val ee = edges.select(col("src").cast(StringType).as("src"),
        col("dst").cast(StringType).as("dst"), col("action"))
      val gf = GraphFrame(vv, ee)
      val g = gf.toGraphX
      // String IDs will be re-indexed, so ID values may not match.
      val vCols = gf.vColsMap
      val eCols = gf.eColsMap
      // First, get index.
      val new2oldID: Map[Long, String] = g.vertices.map { case (id: Long, attr: Row) =>
        (id, attr.getString(vCols("id")))
      }.collect().toMap
      // Same as in test with Int IDs, but with re-indexing
      g.vertices.collect().foreach { case (id0: Long, attr: Row) =>
        val id1 = attr.getString(vCols("id"))
        val name = attr.getString(vCols("name"))
        assert(new2oldID(id0) === id1)
        assert(localVertices(new2oldID(id0).toInt) === name)
      }
      g.edges.collect().foreach {
        case Edge(src0: Long, dst0: Long, attr: Row) =>
          val src1 = attr.getString(eCols("src"))
          val dst1 = attr.getString(eCols("dst"))
          val action = attr.getString(eCols("action"))
          assert(new2oldID(src0) === src1)
          assert(new2oldID(dst0) === dst1)
          assert(localEdges((new2oldID(src0).toInt, new2oldID(dst0).toInt)) === action)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  test("save/load") {
    val g0 = GraphFrame(vertices, edges)
    val vPath = new Path(tempDir.getPath, "vertices").toString
    val ePath = new Path(tempDir.getPath, "edges").toString
    g0.vertices.write.parquet(vPath)
    g0.edges.write.parquet(ePath)

    val v1 = sqlContext.read.parquet(vPath)
    val e1 = sqlContext.read.parquet(ePath)
    val g1 = GraphFrame(v1, e1)

    g1.vertices.collect().foreach { case Row(id: Long, name: String) =>
      assert(localVertices(id) === name)
    }
    g1.edges.collect().foreach { case Row(src: Long, dst: Long, action: String) =>
      assert(localEdges((src, dst)) === action)
    }
  }

  test("degree metrics") {
    val g = GraphFrame(vertices, edges)

    assert(g.outDegrees.columns === Seq("id", "outDeg"))
    val outDegrees = g.outDegrees.collect().map { case Row(id: Long, outDeg: Int) =>
      (id, outDeg)
    }.toMap
    assert(outDegrees === Map(1L -> 1, 2L -> 2))

    assert(g.inDegrees.columns === Seq("id", "inDeg"))
    val inDegrees = g.inDegrees.collect().map { case Row(id: Long, inDeg: Int) =>
      (id, inDeg)
    }.toMap
    assert(inDegrees === Map(1L -> 1, 2L -> 1, 3L -> 1))

    assert(g.degrees.columns === Seq("id", "deg"))
    val degrees = g.degrees.collect().map { case Row(id: Long, deg: Int) =>
      (id, deg)
    }.toMap
    assert(degrees === Map(1L -> 2, 2L -> 3, 3L -> 1))
  }

  test("aggregateMessages") {
    val AM = AggregateMessages
    val g = Graphs.friends
    // For each user, sum the ages of the adjacent users,
    // plus 1 for the src's sum if the edge is "friend".
    val msgToSrc = AM.dst("age") +
      when(AM.edge("relationship") === "friend", lit(1)).otherwise(0)
    val msgToDst = AM.src("age")
    val agg = g.aggregateMessages
      .sendToSrc(msgToSrc)
      .sendToDst(msgToDst)
      .agg(sum(AM.msg).as("summedAges"))
    // Convert agg to a Map, and compute the truth via brute force for comparison.
    import org.apache.spark.sql._
    val aggMap: Map[String, Long] = agg.select("id", "summedAges").collect().map {
      case Row(id: String, s: Long) => id -> s
    }.toMap
    val trueAgg: Map[String, Int] = {
      val user2age = g.vertices.select("id", "age").collect().map {
        case Row(id: String, age: Int) => id -> age
      }.toMap
      val a = mutable.HashMap.empty[String, Int]
      g.edges.select("src", "dst", "relationship").collect().foreach {
        case Row(src: String, dst: String, relationship: String) =>
          a.put(src, a.getOrElse(src, 0) + user2age(dst) + (if (relationship == "friend") 1 else 0))
          a.put(dst, a.getOrElse(dst, 0) + user2age(src))
      }
      a.toMap
    }
    aggMap.keys.foreach { case user =>
      assert(aggMap(user) === trueAgg(user), s"Failure on user $user")
    }
  }
}
