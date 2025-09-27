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

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.graphframes.graphx.Edge
import org.apache.spark.graphframes.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.graphframes.examples.Graphs

import java.io.File
import java.nio.file.Files

class GraphFrameSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  import GraphFrame._

  var vertices: DataFrame = _
  val localVertices: Map[Long, String] = Map(1L -> "A", 2L -> "B", 3L -> "C")
  val localEdges: Map[(Long, Long), String] =
    Map((1L, 2L) -> "love", (2L, 1L) -> "hate", (2L, 3L) -> "follow")
  var edges: DataFrame = _
  var tempDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory(null).toFile()
    vertices = spark.createDataFrame(localVertices.toSeq).toDF("id", "name")
    edges = spark
      .createDataFrame(localEdges.toSeq.map { case ((src, dst), action) =>
        (src, dst, action)
      })
      .toDF("src", "dst", "action")
  }

  override def afterAll(): Unit = {
    FileUtils.deleteQuietly(tempDir)
    super.afterAll()
  }

  test("test validate") {
    val goodG = GraphFrame(
      spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "attr"),
      spark.createDataFrame(Seq((1L, 2L), (2L, 1L), (2L, 3L))).toDF("src", "dst"))
    goodG.validate() // no exception should be thrown

    val notDistinctVertices = GraphFrame(
      spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"), (1L, "d"))).toDF("id", "attr"),
      spark.createDataFrame(Seq((1L, 2L), (2L, 1L), (2L, 3L))).toDF("src", "dst"))
    assertThrows[InvalidGraphException](notDistinctVertices.validate())

    val missingVertices = GraphFrame(
      spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "attr"),
      spark.createDataFrame(Seq((1L, 2L), (2L, 1L), (2L, 3L), (1L, 4L))).toDF("src", "dst"))
    assertThrows[InvalidGraphException](missingVertices.validate())
  }

  test("construction from DataFrames") {
    val g = GraphFrame(vertices, edges)
    g.vertices.collect().foreach {
      case Row(id: Long, name: String) =>
        assert(localVertices(id) === name)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    g.edges.collect().foreach {
      case Row(src: Long, dst: Long, action: String) =>
        assert(localEdges((src, dst)) === action)
      case _: Row => throw new GraphFramesUnreachableException()
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

  test("construction from DataFrames with dots in column names") {
    val g = GraphFrame(
      vertices.withColumnRenamed("name", "a.name"),
      edges.withColumnRenamed("action", "the.action"))
    g.vertices.collect().foreach {
      case Row(id: Long, name: String) =>
        assert(localVertices(id) === name)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    g.edges.collect().foreach {
      case Row(src: Long, dst: Long, action: String) =>
        assert(localEdges((src, dst)) === action)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    g.pageRank.maxIter(10).run()
  }

  test("construction from DataFrames with backquote in column names") {
    val g = GraphFrame(
      vertices.withColumnRenamed("name", "a `name`"),
      edges.withColumnRenamed("action", "the `action`"))
    g.vertices.collect().foreach {
      case Row(id: Long, name: String) =>
        assert(localVertices(id) === name)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    g.edges.collect().foreach {
      case Row(src: Long, dst: Long, action: String) =>
        assert(localEdges((src, dst)) === action)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    g.pageRank.maxIter(10).run()
  }

  test("construction from edge DataFrame") {
    val g = GraphFrame.fromEdges(edges)
    assert(g.vertices.columns === Array("id"))
    val idsFromVertices = g.vertices.select("id").rdd.map(_.getLong(0)).collect()
    val idsFromVerticesSet = idsFromVertices.toSet
    assert(idsFromVertices.length === idsFromVerticesSet.size)
    val idsFromEdgesSet = g.edges
      .select("src", "dst")
      .rdd
      .flatMap {
        case Row(src: Long, dst: Long) =>
          Seq(src, dst)
        case _: Row => throw new GraphFramesUnreachableException()
      }
      .collect()
      .toSet
    assert(idsFromVerticesSet === idsFromEdgesSet)
  }

  test("construction from GraphX") {
    val vv: RDD[(Long, String)] = vertices.rdd.map {
      case Row(id: Long, name: String) =>
        (id, name)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    val ee: RDD[Edge[String]] = edges.rdd.map {
      case Row(src: Long, dst: Long, action: String) =>
        Edge(src, dst, action)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    val g = Graph[String, String](vv, ee)
    val gf = GraphFrame.fromGraphX(g)
    gf.vertices.select("id", "attr").collect().foreach {
      case Row(id: Long, name: String) =>
        assert(localVertices(id) === name)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    gf.edges.select("src", "dst", "attr").collect().foreach {
      case Row(src: Long, dst: Long, action: String) =>
        assert(localEdges((src, dst)) === action)
      case _: Row => throw new GraphFramesUnreachableException()
    }
  }

  test("convert to GraphX: Long IDs") {
    val gf = GraphFrame(vertices, edges)
    val g = gf.toGraphX
    g.vertices.collect().foreach {
      case (id0, Row(id1: Long, name: String)) =>
        assert(id0 === id1)
        assert(localVertices(id0) === name)
      case _ => throw new GraphFramesUnreachableException()
    }
    g.edges.collect().foreach {
      case Edge(src0, dst0, Row(src1: Long, dst1: Long, action: String)) =>
        assert(src0 === src1)
        assert(dst0 === dst1)
        assert(localEdges((src0, dst0)) === action)
      case _ => throw new GraphFramesUnreachableException()
    }
  }

  test("convert to GraphX: Int IDs") {
    val vv = vertices.select(col("id").cast(IntegerType).as("id"), col("name"))
    val ee = edges.select(
      col("src").cast(IntegerType).as("src"),
      col("dst").cast(IntegerType).as("dst"),
      col("action"))
    val gf = GraphFrame(vv, ee)
    val g = gf.toGraphX
    // Int IDs should be directly cast to Long, so ID values should match.
    val vCols = gf.vertexColumnMap
    val eCols = gf.edgeColumnMap
    g.vertices.collect().foreach {
      case (id0: Long, attr: Row) =>
        val id1 = attr.getInt(vCols("id"))
        val name = attr.getString(vCols("name"))
        assert(id0 === id1)
        assert(localVertices(id0) === name)
      case _ => throw new GraphFramesUnreachableException()
    }
    g.edges.collect().foreach {
      case Edge(src0: Long, dst0: Long, attr: Row) =>
        val src1 = attr.getInt(eCols("src"))
        val dst1 = attr.getInt(eCols("dst"))
        val action = attr.getString(eCols("action"))
        assert(src0 === src1)
        assert(dst0 === dst1)
        assert(localEdges((src0, dst0)) === action)
      case _ => throw new GraphFramesUnreachableException()
    }
  }

  test("convert to GraphX: String IDs") {
    try {
      val vv = vertices.select(col("id").cast(StringType).as("id"), col("name"))
      val ee = edges.select(
        col("src").cast(StringType).as("src"),
        col("dst").cast(StringType).as("dst"),
        col("action"))
      val gf = GraphFrame(vv, ee)
      val g = gf.toGraphX
      // String IDs will be re-indexed, so ID values may not match.
      val vCols = gf.vertexColumnMap
      val eCols = gf.edgeColumnMap
      // First, get index.
      val new2oldID: Map[Long, String] = g.vertices
        .map { case (id: Long, attr: Row) =>
          (id, attr.getString(vCols("id")))
        }
        .collect()
        .toMap
      // Same as in test with Int IDs, but with re-indexing
      g.vertices.collect().foreach { case (id0: Long, attr: Row) =>
        val id1 = attr.getString(vCols("id"))
        val name = attr.getString(vCols("name"))
        assert(new2oldID(id0) === id1)
        assert(localVertices(new2oldID(id0).toLong) === name)
      }
      g.edges.collect().foreach { case Edge(src0: Long, dst0: Long, attr: Row) =>
        val src1 = attr.getString(eCols("src"))
        val dst1 = attr.getString(eCols("dst"))
        val action = attr.getString(eCols("action"))
        assert(new2oldID(src0) === src1)
        assert(new2oldID(dst0) === dst1)
        assert(localEdges((new2oldID(src0).toLong, new2oldID(dst0).toLong)) === action)
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

    val v1 = spark.read.parquet(vPath)
    val e1 = spark.read.parquet(ePath)
    val g1 = GraphFrame(v1, e1)

    g1.vertices.collect().foreach {
      case Row(id: Long, name: String) =>
        assert(localVertices(id) === name)
      case _ => throw new GraphFramesUnreachableException()
    }
    g1.edges.collect().foreach {
      case Row(src: Long, dst: Long, action: String) =>
        assert(localEdges((src, dst)) === action)
      case _ => throw new GraphFramesUnreachableException()
    }
  }

  test("degree metrics") {
    val g = GraphFrame(vertices, edges)

    assert(g.outDegrees.columns === Seq("id", "outDegree"))
    val outDegrees = g.outDegrees
      .collect()
      .map {
        case Row(id: Long, outDeg: Int) =>
          (id, outDeg)
        case _ => throw new GraphFramesUnreachableException()
      }
      .toMap
    assert(outDegrees === Map(1L -> 1, 2L -> 2))

    assert(g.inDegrees.columns === Seq("id", "inDegree"))
    val inDegrees = g.inDegrees
      .collect()
      .map {
        case Row(id: Long, inDeg: Int) =>
          (id, inDeg)
        case _ => throw new GraphFramesUnreachableException()
      }
      .toMap
    assert(inDegrees === Map(1L -> 1, 2L -> 1, 3L -> 1))

    assert(g.degrees.columns === Seq("id", "degree"))
    val degrees = g.degrees
      .collect()
      .map {
        case Row(id: Long, deg: Int) =>
          (id, deg)
        case _ => throw new GraphFramesUnreachableException()
      }
      .toMap
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

  test("test triplets") {
    // Basic triplets test
    val g = GraphFrame(vertices, edges)
    val triplets = g.triplets.collect()
    assert(triplets.length === localEdges.size)
    triplets.foreach {
      case Row(src: Row, edge: Row, dst: Row) =>
        assert(src.getLong(0) === edge.getLong(0)) // src.id === edge.src
        assert(dst.getLong(0) === edge.getLong(1)) // dst.id === edge.dst
        assert(localEdges((edge.getLong(0), edge.getLong(1))) === edge.getString(2))
      case _ => throw new GraphFramesUnreachableException()
    }

    // Test with attributes
    val v2 = vertices.withColumn("age", lit(10))
    val e2 = edges.withColumn("weight", lit(2.0))
    val g2 = GraphFrame(v2, e2)
    val triplets2 = g2.triplets.collect()
    triplets2.foreach {
      case Row(src: Row, edge: Row, _: Row) =>
        assert(src.getInt(2) === 10) // Check vertex attribute
        assert(edge.getDouble(3) === 2.0) // Check edge attribute
      case _ => throw new GraphFramesUnreachableException()
    }

    // Test with dots in column names
    val v3 = v2.withColumnRenamed("age", "person.age")
    val e3 = e2.withColumnRenamed("weight", "edge.weight")
    val g3 = GraphFrame(v3, e3)
    val triplets3 = g3.triplets.collect()
    triplets3.foreach {
      case Row(src: Row, edge: Row, _: Row) =>
        assert(src.getInt(2) === 10) // Check vertex attribute
        assert(edge.getDouble(3) === 2.0) // Check edge attribute
      case _ => throw new GraphFramesUnreachableException()
    }
  }

  test("nestAsCol with dots in column names") {
    val df = vertices.withColumnRenamed("name", "a.name")
    val col = nestAsCol(df, "attr")
    assert(
      df.select(col).schema === StructType(
        Seq(
          StructField(
            "attr",
            StructType(Seq(
              StructField("id", LongType, nullable = false),
              StructField("a.name", StringType, nullable = true))),
            nullable = false))))
  }

  test("nestAsCol with backquote in column names") {
    val df = vertices.withColumnRenamed("name", "a `name`")
    val col = nestAsCol(df, "attr")
    assert(
      df.select(col).schema === StructType(
        Seq(
          StructField(
            "attr",
            StructType(Seq(
              StructField("id", LongType, nullable = false),
              StructField("a `name`", StringType, nullable = true))),
            nullable = false))))
  }

  test("power iteration clustering wrapper") {
    val spark = this.spark
    import spark.implicits._
    val edges = spark
      .createDataFrame(
        Seq(
          (1, 0, 0.5),
          (2, 0, 0.5),
          (2, 1, 0.7),
          (3, 0, 0.5),
          (3, 1, 0.7),
          (3, 2, 0.9),
          (4, 0, 0.5),
          (4, 1, 0.7),
          (4, 2, 0.9),
          (4, 3, 1.1),
          (5, 0, 0.5),
          (5, 1, 0.7),
          (5, 2, 0.9),
          (5, 3, 1.1),
          (5, 4, 1.3)))
      .toDF("src", "dst", "weight")
    val vertices = Seq(0, 1, 2, 3, 4, 5).toDF("id")
    val gf = GraphFrame(vertices, edges)
    val clusters = gf
      .powerIterationClustering(k = 2, maxIter = 40, weightCol = Some("weight"))
      .collect()
      .sortBy(_.getAs[Long]("id"))
      .map(_.getAs[Int]("cluster"))
      .toSeq
    assert(Seq(0, 0, 0, 0, 1, 0) == clusters)
  }

  test("convert directed graph to undirected") {
    val v = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "name")
    val e = spark.createDataFrame(Seq((1L, 2L), (2L, 3L))).toDF("src", "dst")
    val g = GraphFrame(v, e)
    val undirected = g.asUndirected()

    // Check edge count doubled
    assert(undirected.edges.count() === 2 * g.edges.count())

    // Verify reverse edges exist
    val edges = undirected.edges.sort("src", "dst").collect()
    assert(edges.length === 4)
    assert(edges(0).getLong(0) === 1L)
    assert(edges(0).getLong(1) === 2L)
    assert(edges(1).getLong(0) === 2L)
    assert(edges(1).getLong(1) === 1L)
    assert(edges(2).getLong(0) === 2L)
    assert(edges(2).getLong(1) === 3L)
    assert(edges(3).getLong(0) === 3L)
    assert(edges(3).getLong(1) === 2L)
  }

  test("convert directed graph with edge attributes to undirected") {
    val v = spark.createDataFrame(Seq((1L, "a"), (2L, "b"))).toDF("id", "name")
    val e = spark.createDataFrame(Seq((1L, 2L, "edge1"))).toDF("src", "dst", "attr")
    val g = GraphFrame(v, e)
    val undirected = g.asUndirected()

    val edges = undirected.edges.collect()
    assert(edges.length === 2)
    assert(
      edges.exists(r => r.getLong(0) == 1L && r.getLong(1) == 2L && r.getString(2) == "edge1"))
    assert(
      edges.exists(r => r.getLong(0) == 2L && r.getLong(1) == 1L && r.getString(2) == "edge1"))
  }
}
