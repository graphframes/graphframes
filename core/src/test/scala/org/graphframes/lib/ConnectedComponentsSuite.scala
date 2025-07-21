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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.LongType
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame._
import org.graphframes._
import org.graphframes.examples.Graphs

import java.io.IOException
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class ConnectedComponentsSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  Seq(true, false).foreach(useLocalCheckpoint => {
    test(s"default params${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val g = Graphs.empty[Int]
      val cc = g.connectedComponents
      assert(cc.getAlgorithm === "graphframes")
      assert(cc.getBroadcastThreshold === 1000000)
      assert(cc.getCheckpointInterval === 2)
      assert(!cc.getUseLabelsAsComponents)
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(s"empty graph${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      for (empty <- Seq(Graphs.empty[Int], Graphs.empty[Long], Graphs.empty[String])) {
        val components = empty.connectedComponents.run()
        assert(components.count() === 0L)
      }
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(s"single vertex${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val v = spark.createDataFrame(List((0L, "a", "b"))).toDF("id", "vattr", "gender")
      // Create an empty dataframe with the proper columns.
      val e = spark
        .createDataFrame(List((0L, 0L, 1L)))
        .toDF("src", "dst", "test")
        .filter("src > 10")
      val g = GraphFrame(v, e)
      val comps = ConnectedComponents.run(g)
      TestUtils.testSchemaInvariants(g, comps)
      TestUtils.checkColumnType(comps.schema, "component", DataTypes.LongType)
      assert(comps.count() === 1)
      assert(
        comps.select("id", "component", "vattr", "gender").collect()
          === Seq(Row(0L, 0L, "a", "b")))
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(s"disconnected vertices${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val n = 5L
      val vertices = spark.range(n).toDF(ID)
      val edges = spark.createDataFrame(Seq.empty[(Long, Long)]).toDF(SRC, DST)
      val g = GraphFrame(vertices, edges)
      val components = g.connectedComponents.run()
      val expected = (0L until n).map(Set(_)).toSet
      assertComponents(components, expected)
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(
      s"using labels as components${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      spark.conf.set("spark.graphframes.useLabelsAsComponents", "true")
      val vertices =
        spark.createDataFrame(Seq("a", "b", "c", "d", "e").map(Tuple1.apply)).toDF(ID)
      val edges = spark.createDataFrame(Seq.empty[(String, String)]).toDF(SRC, DST)
      val g = GraphFrame(vertices, edges)
      val components = g.connectedComponents.run()
      val expected = Seq("a", "b", "c", "d", "e").map(Set(_)).toSet
      assertComponents(components, expected)
      spark.conf.set("spark.graphframes.useLabelsAsComponents", "false")
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(
      s"don't using labels as components${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val vertices =
        spark.createDataFrame(Seq("a", "b", "c", "d", "e").map(Tuple1.apply)).toDF(ID)
      val edges = spark.createDataFrame(Seq.empty[(String, String)]).toDF(SRC, DST)
      val g = GraphFrame(vertices, edges)
      val components = g.connectedComponents.run()
      assert(components.schema("component").dataType == LongType)
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(s"two connected vertices${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val v = spark.createDataFrame(List((0L, "a0", "b0"), (1L, "a1", "b1"))).toDF("id", "A", "B")
      val e = spark.createDataFrame(List((0L, 1L, "a01", "b01"))).toDF("src", "dst", "A", "B")
      val g = GraphFrame(v, e)
      val comps = g.connectedComponents.run()
      TestUtils.testSchemaInvariants(g, comps)
      assert(comps.count() === 2)
      val vxs = comps.sort("id").select("id", "component", "A", "B").collect()
      assert(List(Row(0L, 0L, "a0", "b0"), Row(1L, 0L, "a1", "b1")) === vxs)
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(s"chain graph${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val n = 5L
      val g = Graphs.chain(5L)
      val components = g.connectedComponents.run()
      val expected = Set((0L until n).toSet)
      assertComponents(components, expected)
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(s"star graph${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val n = 5L
      val g = Graphs.star(5L)
      val components = g.connectedComponents.run()
      val expected = Set((0L to n).toSet)
      assertComponents(components, expected)
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(s"two blobs${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val n = 5L
      val g = Graphs.twoBlobs(n.toInt)
      val components = g.connectedComponents.run()
      val expected = Set((0L until 2 * n).toSet)
      assertComponents(components, expected)
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(s"two components${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val vertices = spark.range(6L).toDF(ID)
      val edges = spark
        .createDataFrame(Seq((0L, 1L), (1L, 2L), (2L, 0L), (3L, 4L), (4L, 5L), (5L, 3L)))
        .toDF(SRC, DST)
      val g = GraphFrame(vertices, edges)
      val components = g.connectedComponents.run()
      val expected = Set(Set(0L, 1L, 2L), Set(3L, 4L, 5L))
      assertComponents(components, expected)
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(
      s"one component, differing edge directions${if (useLocalCheckpoint) " with local checkpoint"
        else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val vertices = spark.range(5L).toDF(ID)
      val edges = spark
        .createDataFrame(
          Seq(
            // 0 -> 4 -> 3 <- 2 -> 1
            (0L, 4L),
            (4L, 3L),
            (2L, 3L),
            (2L, 1L)))
        .toDF(SRC, DST)
      val g = GraphFrame(vertices, edges)
      val components = g.connectedComponents.run()
      val expected = Set((0L to 4L).toSet)
      assertComponents(components, expected)
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(
      s"two components and two dangling vertices${if (useLocalCheckpoint) " with local checkpoint"
        else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val vertices = spark.range(8L).toDF(ID)
      val edges = spark
        .createDataFrame(Seq((0L, 1L), (1L, 2L), (2L, 0L), (3L, 4L), (4L, 5L), (5L, 3L)))
        .toDF(SRC, DST)
      val g = GraphFrame(vertices, edges)
      val components = g.connectedComponents.run()
      val expected = Set(Set(0L, 1L, 2L), Set(3L, 4L, 5L), Set(6L), Set(7L))
      assertComponents(components, expected)
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(s"friends graph${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
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
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(s"really large long IDs${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val max = Long.MaxValue
      val chain = examples.Graphs.chain(10L)
      val vertices = chain.vertices.select((lit(max) - col(ID)).as(ID))
      val edges = chain.edges.select((lit(max) - col(SRC)).as(SRC), (lit(max) - col(DST)).as(DST))
      val g = GraphFrame(vertices, edges)
      val components = g.connectedComponents.run()
      assert(components.count() === 10L)
      assert(components.groupBy("component").count().count() === 1L)
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }
  })

  test("set configuration from spark conf") {
    spark.conf.set("spark.graphframes.connectedComponents.algorithm", "GRAPHX")
    assert(Graphs.friends.connectedComponents.getAlgorithm == "graphx")

    spark.conf.set("spark.graphframes.connectedComponents.broadcastthreshold", "1000")
    assert(Graphs.friends.connectedComponents.getBroadcastThreshold == 1000)

    spark.conf.set("spark.graphframes.connectedComponents.checkpointinterval", "5")
    assert(Graphs.friends.connectedComponents.getCheckpointInterval == 5)

    spark.conf
      .set("spark.graphframes.connectedComponents.intermediatestoragelevel", "memory_only")
    assert(
      Graphs.friends.connectedComponents.getIntermediateStorageLevel == StorageLevel.MEMORY_ONLY)

    spark.conf.unset("spark.graphframes.connectedComponents.algorithm")
    spark.conf.unset("spark.graphframes.connectedComponents.broadcastthreshold")
    spark.conf.unset("spark.graphframes.connectedComponents.checkpointinterval")
    spark.conf.unset("spark.graphframes.connectedComponents.intermediatestoragelevel")
  }

  test("checkpoint interval") {
    val friends = Graphs.friends
    val expected = Set(Set("a", "b", "c", "d", "e", "f"), Set("g"))

    val cc = new ConnectedComponents(friends)
    assert(
      cc.getCheckpointInterval === 2,
      s"Default checkpoint interval should be 2, but got ${cc.getCheckpointInterval}.")

    val checkpointDir = sc.getCheckpointDir
    assert(checkpointDir.nonEmpty)

    sc.setCheckpointDir(null)
    withClue(
      "Should throw an IOException if sc.getCheckpointDir is empty " +
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
    assert(
      !isFromCheckpoint(components0),
      "The result shouldn't depend on checkpoint data if checkpointing is disabled.")

    sc.setCheckpointDir(checkpointDir.get)

    val components1 = cc.setCheckpointInterval(1).run()
    assertComponents(components1, expected)
    assert(
      isFromCheckpoint(components1),
      "The result should depend on checkpoint data if checkpoint interval is 1.")

    val components10 = cc.setCheckpointInterval(10).run()
    assertComponents(components10, expected)
    assert(
      !isFromCheckpoint(components10),
      "The result shouldn't depend on checkpoint data if converged before first checkpoint.")
  }

  test("intermediate storage level") {
    // disabling adaptive query execution helps assertComponents
    val enabled = spark.conf.getOption("spark.sql.adaptive.enabled")
    try {
      spark.conf.set("spark.sql.adaptive.enabled", value = false)

      val friends = Graphs.friends
      val expected = Set(Set("a", "b", "c", "d", "e", "f"), Set("g"))

      val cc = friends.connectedComponents
      assert(cc.getIntermediateStorageLevel === StorageLevel.MEMORY_AND_DISK)

      for (storageLevel <- Seq(
          StorageLevel.DISK_ONLY,
          StorageLevel.MEMORY_ONLY,
          StorageLevel.NONE)) {
        // TODO: it is not trivial to confirm the actual storage level used
        val components = cc
          .setIntermediateStorageLevel(storageLevel)
          .run()
        assertComponents(components, expected)
      }
    } finally {
      // restoring earlier conf
      if (enabled.isDefined) {
        spark.conf.set("spark.sql.adaptive.enabled", value = enabled.get)
      } else {
        spark.conf.unset("spark.sql.adaptive.enabled")
      }
    }
  }

  test("not leaking cached data") {
    val priorCachedDFsSize = spark.sparkContext.getPersistentRDDs.size

    val cc = Graphs.friends.connectedComponents
    val components = cc.run()

    components.unpersist(blocking = true)

    assert(spark.sparkContext.getPersistentRDDs.size === priorCachedDFsSize)
  }

  private def assertComponents[T: ClassTag: TypeTag](
      actual: DataFrame,
      expected: Set[Set[T]]): Unit = {
    import actual.sparkSession.implicits._
    // note: not using agg + collect_list because collect_list is not available in 1.6.2 w/o hive
    val actualComponents = actual
      .select("component", "id")
      .as[(T, T)]
      .rdd
      .groupByKey()
      .values
      .map(_.toSeq)
      .collect()
      .map { ids =>
        val idSet = ids.toSet
        assert(
          idSet.size === ids.size,
          s"Found duplicated component assignment in [${ids.mkString(",")}].")
        idSet
      }
      .toSet
    assert(actualComponents === expected)
  }
}
