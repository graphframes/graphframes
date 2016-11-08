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

import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row}

import org.graphframes.examples.Graphs


class GraphFrameSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  import GraphFrame._

  var vertices: DataFrame = _
  val localVertices = Map(1L -> "A", 2L -> "B", 3L -> "C")
  val localEdges = Map((1L, 2L) -> "love", (2L, 1L) -> "hate", (2L, 3L) -> "follow")
  var edges: DataFrame = _
  var tempDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDir()
    vertices = sqlContext.createDataFrame(localVertices.toSeq).toDF("id", "name")
    edges = sqlContext.createDataFrame(localEdges.toSeq.map {
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
    val idsFromVertices = g.vertices.select("id").rdd.map(_.getLong(0)).collect()
    val idsFromVerticesSet = idsFromVertices.toSet
    assert(idsFromVertices.length === idsFromVerticesSet.size)
    val idsFromEdgesSet = g.edges.select("src", "dst").rdd.flatMap { case Row(src: Long, dst: Long) =>
      Seq(src, dst)
    }.collect().toSet
    assert(idsFromVerticesSet === idsFromEdgesSet)
  }

  test("construction from GraphX") {
    val vv: RDD[(Long, String)] = vertices.rdd.map { case Row(id: Long, name: String) =>
      (id, name)
    }
    val ee: RDD[Edge[String]] = edges.rdd.map { case Row(src: Long, dst: Long, action: String) =>
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
    val vCols = gf.vertexColumnMap
    val eCols = gf.edgeColumnMap
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

  test("convert to GraphX: String IDs") {
    try {
      val vv = vertices.select(col("id").cast(StringType).as("id"), col("name"))
      val ee = edges.select(col("src").cast(StringType).as("src"),
        col("dst").cast(StringType).as("dst"), col("action"))
      val gf = GraphFrame(vv, ee)
      val g = gf.toGraphX
      // String IDs will be re-indexed, so ID values may not match.
      val vCols = gf.vertexColumnMap
      val eCols = gf.edgeColumnMap
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

    assert(g.outDegrees.columns === Seq("id", "outDegree"))
    val outDegrees = g.outDegrees.collect().map { case Row(id: Long, outDeg: Int) =>
      (id, outDeg)
    }.toMap
    assert(outDegrees === Map(1L -> 1, 2L -> 2))

    assert(g.inDegrees.columns === Seq("id", "inDegree"))
    val inDegrees = g.inDegrees.collect().map { case Row(id: Long, inDeg: Int) =>
      (id, inDeg)
    }.toMap
    assert(inDegrees === Map(1L -> 1, 2L -> 1, 3L -> 1))

    assert(g.degrees.columns === Seq("id", "degree"))
    val degrees = g.degrees.collect().map { case Row(id: Long, deg: Int) =>
      (id, deg)
    }.toMap
    assert(degrees === Map(1L -> 2, 2L -> 3, 3L -> 1))
  }

  test("cache") {
    val g = GraphFrame(vertices, edges)

    g.persist(StorageLevel.MEMORY_ONLY)

    g.unpersist()
    // org.apache.spark.sql.execution.columnar.InMemoryRelation is private and not accessible
    // This has prevented us from validating DataFrame's are cached.
  }

  test("basic operations on an empty graph") {
    for (empty <- Seq(Graphs.empty[Int], Graphs.empty[Long], Graphs.empty[String])) {
      assert(empty.inDegrees.count() === 0L)
      assert(empty.outDegrees.count() === 0L)
      assert(empty.degrees.count() === 0L)
      assert(empty.triplets.count() === 0L)
    }
  }

  test("skewed long ID assignments") {
    val sqlContext = this.sqlContext
    import sqlContext.implicits._
    val n = 5
    // union a star graph and a chain graph and cast integral IDs to strings
    val star = Graphs.star(n)
    val chain = Graphs.chain(n + 1)
    val vertices = star.vertices.select(col(ID).cast("string").as(ID))
    val edges =
      star.edges.select(col(SRC).cast("string").as(SRC), col(DST).cast("string").as(DST))
        .unionAll(
          chain.edges.select(col(SRC).cast("string").as(SRC), col(DST).cast("string").as(DST)))

    val localVertices = vertices.select(ID).as[String].collect().toSet
    val localEdges = edges.select(SRC, DST).as[(String, String)].collect().toSet

    val defaultThreshold = GraphFrame.broadcastThreshold
    assert(defaultThreshold === 1000000,
      s"Default broadcast threshold should be 1000000 but got $defaultThreshold.")

    for (threshold <- Seq(0, 4, 10)) {
      GraphFrame.setBroadcastThreshold(threshold)

      val g = GraphFrame(vertices, edges)
      g.persist(StorageLevel.MEMORY_AND_DISK)

      val indexedVertices = g.indexedVertices.select(ID, LONG_ID).as[(String, Long)].collect().toMap
      assert(indexedVertices.keySet === localVertices)
      assert(indexedVertices.values.toSeq.distinct.size === localVertices.size)
      val origEdges = g.indexedEdges.select(SRC, DST).as[(String, String)].collect().toSet
      assert(origEdges === localEdges)
      g.indexedEdges
        .select(SRC, LONG_SRC, DST, LONG_DST).as[(String, Long, String, Long)]
        .collect()
        .foreach {
          case (src, longSrc, dst, longDst) =>
            assert(indexedVertices(src) === longSrc)
            assert(indexedVertices(dst) === longDst)
        }
    }

    GraphFrame.setBroadcastThreshold(defaultThreshold)
  }
}
