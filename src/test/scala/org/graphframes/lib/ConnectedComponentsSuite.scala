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

import java.io.IOException

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.storage.StorageLevel

import org.graphframes._
import org.graphframes.GraphFrame._
import org.graphframes.examples.Graphs

class ConnectedComponentsSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  // vertices and edges for pruning node optimization tests. 
  def verticesOpt: DataFrame = sqlContext.range(7L).toDF(ID)
  def edgesOpt: DataFrame = sqlContext.createDataFrame(Seq(
      (0L, 1L), (0L, 2L), (0L, 3L), (0L, 4L), (1L, 2L), (1L, 5L)
  )).toDF(SRC, DST)

  test("default params") {
    val g = Graphs.empty[Int]
    val cc = g.connectedComponents
    assert(cc.getAlgorithm === "graphframes")
    assert(cc.getBroadcastThreshold === 1000000)
    assert(cc.getCheckpointInterval === 2)
  }

  test("empty graph") {
    for (empty <- Seq(Graphs.empty[Int], Graphs.empty[Long], Graphs.empty[String])) {
      val components = empty.connectedComponents.run()
      assert(components.count() === 0L)
    }
  }

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

  test("disconnected vertices") {
    val n = 5L
    val vertices = sqlContext.range(n).toDF(ID)
    val edges = sqlContext.createDataFrame(Seq.empty[(Long, Long)]).toDF(SRC, DST)
    val g = GraphFrame(vertices, edges)
    val components = g.connectedComponents.run()
    val expected = (0L until n).map(Set(_)).toSet
    assertComponents(components, expected)
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

  test("chain graph") {
    val n = 5L
    val g = Graphs.chain(5L)
    val components = g.connectedComponents.run()
    val expected = Set((0L until n).toSet)
    assertComponents(components, expected)
  }

  test("star graph") {
    val n = 5L
    val g = Graphs.star(5L)
    val components = g.connectedComponents.run()
    val expected = Set((0L to n).toSet)
    assertComponents(components, expected)
  }

  test("two blobs") {
    val n = 5L
    val g = Graphs.twoBlobs(n.toInt)
    val components = g.connectedComponents.run()
    val expected = Set((0L until 2 * n).toSet)
    assertComponents(components, expected)
  }

  test("two components") {
    val vertices = sqlContext.range(6L).toDF(ID)
    val edges = sqlContext.createDataFrame(Seq(
      (0L, 1L), (1L, 2L), (2L, 0L),
      (3L, 4L), (4L, 5L), (5L, 3L)
    )).toDF(SRC, DST)
    val g = GraphFrame(vertices, edges)
    val components = g.connectedComponents.run()
    val expected = Set(Set(0L, 1L, 2L), Set(3L, 4L, 5L))
    assertComponents(components, expected)
  }

  test("one component, differing edge directions") {
    val vertices = sqlContext.range(5L).toDF(ID)
    val edges = sqlContext.createDataFrame(Seq(
      // 0 -> 4 -> 3 <- 2 -> 1
      (0L, 4L), (4L, 3L), (2L, 3L), (2L, 1L)
    )).toDF(SRC, DST)
    val g = GraphFrame(vertices, edges)
    val components = g.connectedComponents.run()
    val expected = Set((0L to 4L).toSet)
    assertComponents(components, expected)
  }

  test("two components and two dangling vertices") {
    val vertices = sqlContext.range(8L).toDF(ID)
    val edges = sqlContext.createDataFrame(Seq(
      (0L, 1L), (1L, 2L), (2L, 0L),
      (3L, 4L), (4L, 5L), (5L, 3L)
    )).toDF(SRC, DST)
    val g = GraphFrame(vertices, edges)
    val components = g.connectedComponents.run()
    val expected = Set(Set(0L, 1L, 2L), Set(3L, 4L, 5L), Set(6L), Set(7L))
    assertComponents(components, expected)
  }

  test("friends graph") {
    val friends = Graphs.friends
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

  test("$optStartIter parameter for pruning node optimization") {
    val intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK
    val g = GraphFrame(verticesOpt, edgesOpt)
    val cc = g.connectedComponents
    val expected = Set(Set(0L, 1L, 2L, 3L, 4L, 5L), Set(6L))

    // the optimization is performed in the first iteration.
    var components = cc.setOptStartIter(1).run()
    var iter = cc.getOptIter()
    cc.setOptIter()
    assert(1 == iter)
    assertComponents(components, expected)
    
    // the optimization is performed in the second iteration.
    components = cc.setOptStartIter(2).run()
    iter = cc.getOptIter()
    cc.setOptIter()
    assert(iter == 2)
    assertComponents(components, expected)

    // when $optStartIter <= 1 (includes 0 and negative values),
    // the optimization is performed in the first iteration.
    components = cc.setOptStartIter(0).run()
    iter = cc.getOptIter()
    cc.setOptIter()
    assert(iter == 1)
    assertComponents(components, expected)

    // when set $optStartIter bigger than the total iteration number,
    // the optimization is not performed.
    components = cc.setOptStartIter(10).run()
    iter = cc.getOptIter()
    cc.setOptIter()
    assert(iter == 0)
    assertComponents(components, expected)
  }

  test("prune process for pruning nodes optimization") {
    val intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK
    val shrinkageThreshold = 2
    // prune leaf nodes
    val Some(r1) = ConnectedComponents.pruneLeafNodes(edgesOpt, intermediateStorageLevel,
                                              verticesOpt.count(), shrinkageThreshold)
    
    val expected_v1 = Set(Row(0L), Row(1L), Row(2L))
    val expected_e1 = Set(Row(0L, 1L), Row(1L, 2L), Row(0L, 2L))

    assert (r1._1.collect().toSet == expected_v1)
    assert (r1._2.select(SRC, DST).collect().toSet == expected_e1)
    assert (r1._3 == expected_v1.size)

    // keep source nodes
    val Some(r2) = ConnectedComponents.keepSrcNodes(edgesOpt, intermediateStorageLevel,
                              verticesOpt.count(), shrinkageThreshold, edgesOpt.count())
    
    val expected_v2 = Set(Row(0L), Row(1L))
    val expected_e2 = Set(Row(0L, 1L))

    assert (r2._1.collect().toSet == expected_v2)
    assert (r2._2.select(SRC, DST).collect().toSet == expected_e2)
    assert (r2._3 == expected_v2.size)
  }

  test("shrinkage condition for pruning nodes optimization") {
    val intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK
    val shrinkageThreshold = 4
    // prune leaf nodes
    val r1 = ConnectedComponents.pruneLeafNodes(edgesOpt, intermediateStorageLevel,
                                            verticesOpt.count(), shrinkageThreshold)
    // new_vv_cnt = 3, nodeNum = 7, shrinkageThreshold = 4
    // new_vv_cnt * shrinkageThreshold > nodeNum. Do not perform the optimization.
    assert(r1 == None)

    // keep source nodes
    val r2 = ConnectedComponents.keepSrcNodes(edgesOpt, intermediateStorageLevel,
                        verticesOpt.count(), shrinkageThreshold, edgesOpt.count())
    // new_vv_cnt = 2, nodeNum = 7, shrinkageThreshold = 4
    // new_vv_cnt * shrinkageThreshold > nodeNum. Do not perform the optimization.
    assert(r2 == None)
  }

  test("large join cost for keeping sources optimization") {
    val vertices = sqlContext.range(6L).toDF(ID)
    val edges = sqlContext.createDataFrame(Seq(
      (0L, 5L), (1L, 5L), (2L, 5L), (3L, 5L), (4L, 5L)
    )).toDF(SRC, DST)
    val intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK
    val shrinkageThreshold = 2

    // keep source nodes
    val r = ConnectedComponents.keepSrcNodes(edges, intermediateStorageLevel,
                                vertices.count(), shrinkageThreshold, edges.count())
    // large join cost. Do not perform the optimization.
    assert(r == None)
  }

  test("join back for pruning node optimization") {
    val edges = sqlContext.createDataFrame(Seq(
      (0L, 1L), (0L, 2L), (0L, 3L), (0L, 4L), (1L, 2L), (1L, 5L)
    )).toDF(SRC, DST)
    val intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK

    // prune leaf nodes
    val v1 = sqlContext.range(3L).toDF(ID)
    val e1 = sqlContext.createDataFrame(Seq((0L, 1L), (0L, 2L))).toDF(SRC, DST)
    val r = ConnectedComponents.joinBack(v1, e1, edges, intermediateStorageLevel)
    val expected_r = Set(Row(0L, 0L), Row(0L, 1L), Row(0L, 2L), Row(0L, 3L), Row(0L, 4L), Row(0L, 5L))
    assert (r.collect().toSet == expected_r)

    // keep source nodes
    val v2 = sqlContext.range(2L).toDF(ID)
    val e2 = sqlContext.createDataFrame(Seq((0L, 1L))).toDF(SRC, DST)
    val r2 = ConnectedComponents.joinBack(v2, e2, edges, intermediateStorageLevel)
    assert (r2.collect().toSet == expected_r)
  }

  test("really large long IDs") {
    val max = Long.MaxValue
    val chain = examples.Graphs.chain(10L)
    val vertices = chain.vertices.select((lit(max) - col(ID)).as(ID))
    val edges = chain.edges.select((lit(max) - col(SRC)).as(SRC), (lit(max) - col(DST)).as(DST))
    val g = GraphFrame(vertices, edges)
    val components = g.connectedComponents.run()
    assert(components.count() === 10L)
    assert(components.groupBy("component").count().count() === 1L)
  }

  test("checkpoint interval") {    
    val n = 5L
    val g = Graphs.star(5L)
    val components = g.connectedComponents.run()
    val expected = Set((0L to n).toSet)

    val cc = new ConnectedComponents(g)
    assert(cc.getCheckpointInterval === 2,
      s"Default checkpoint interval should be 2, but got ${cc.getCheckpointInterval}.")

    val checkpointDir = sc.getCheckpointDir
    assert(checkpointDir.nonEmpty)

    sc.setCheckpointDir(null)
    withClue("Should throw an IOException if sc.getCheckpointDir is empty " +
      "and checkpointInterval is positive.") {
      intercept[IOException] {
        cc.run()
      }
    }

    // Checks whether the input DataFrame is from some checkpoint data.
    // TODO: The implemetnation is a little hacky.
    def isFromCheckpoint(df: DataFrame): Boolean = {
      df.queryExecution.logical.toString().toLowerCase.contains("parquet")
    }

    val components0 = cc.setCheckpointInterval(0).run()
    assertComponents(components0, expected)
    assert(!isFromCheckpoint(components0),
      "The result shouldn't depend on checkpoint data if checkpointing is disabled.")

    sc.setCheckpointDir(checkpointDir.get)

    val components1 = cc.setCheckpointInterval(1).run()
    assertComponents(components1, expected)
    assert(isFromCheckpoint(components1),
      "The result should depend on checkpoint data if checkpoint interval is 1.")

    val components10 = cc.setCheckpointInterval(10).run()
    assertComponents(components10, expected)
    assert(!isFromCheckpoint(components10),
      "The result shouldn't depend on checkpoint data if converged before first checkpoint.")
  }

  test("intermediate storage level") {
    val friends = Graphs.friends
    val expected = Set(Set("a", "b", "c", "d", "e", "f"), Set("g"))

    val cc = friends.connectedComponents
    assert(cc.getIntermediateStorageLevel === StorageLevel.MEMORY_AND_DISK)

    for (storageLevel <- Seq(StorageLevel.DISK_ONLY, StorageLevel.MEMORY_ONLY, StorageLevel.NONE)) {
      // TODO: it is not trivial to confirm the actual storage level used
      val components = cc
        .setIntermediateStorageLevel(storageLevel)
        .run()
      assertComponents(components, expected)
    }
  }

  private def assertComponents[T: ClassTag:TypeTag](
      actual: DataFrame,
      expected: Set[Set[T]]): Unit = {
    import actual.sqlContext.implicits._
    // note: not using agg + collect_list because collect_list is not available in 1.6.2 w/o hive
    val actualComponents = actual.select("component", "id").as[(Long, T)].rdd
      .groupByKey()
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
