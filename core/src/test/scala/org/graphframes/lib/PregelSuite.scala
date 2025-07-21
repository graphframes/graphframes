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

import org.apache.spark.sql.functions._
import org.graphframes._
import org.scalactic.Tolerance._

class PregelSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  import sqlImplicits._

  Seq(true, false).foreach(useLocalCheckpoint => {
    test(s"page rank${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val edges = Seq(
        (0L, 1L),
        (1L, 2L),
        (2L, 4L),
        (2L, 0L),
        (3L, 4L), // 3 has no in-links
        (4L, 0L),
        (4L, 2L)).toDF("src", "dst").cache()
      val vertices = GraphFrame.fromEdges(edges).outDegrees.cache()
      val numVertices = vertices.count()
      val graph = GraphFrame(vertices, edges)

      val alpha = 0.15
      // NOTE: This version doesn't handle nodes with no out-links.
      val ranks = graph.pregel
        .setMaxIter(5)
        .withVertexColumn(
          "rank",
          lit(1.0 / numVertices),
          coalesce(Pregel.msg, lit(0.0)) * (1.0 - alpha) + alpha / numVertices)
        .sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree"))
        .aggMsgs(sum(Pregel.msg))
        .run()

      val result = ranks
        .sort(col("id"))
        .select("rank")
        .as[Double]
        .collect()
      assert(result.sum === 1.0 +- 1e-6)
      val expected = Seq(0.245, 0.224, 0.303, 0.03, 0.197)
      result.zip(expected).foreach { case (r, e) =>
        assert(r === e +- 1e-3)
      }
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(s"chain propagation${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val n = 5
      val verDF = (1 to n).toDF("id").repartition(3)
      val edgeDF = (1 until n)
        .map(x => (x, x + 1))
        .toDF("src", "dst")
        .repartition(3)

      val graph = GraphFrame(verDF, edgeDF)

      val resultDF = graph.pregel
        .setMaxIter(n - 1)
        .withVertexColumn(
          "value",
          when(col("id") === lit(1), lit(1)).otherwise(lit(0)),
          when(Pregel.msg > col("value"), Pregel.msg).otherwise(col("value")))
        .sendMsgToDst(when(Pregel.dst("value") =!= Pregel.src("value"), Pregel.src("value")))
        .aggMsgs(max(Pregel.msg))
        .run()

      assert(resultDF.sort("id").select("value").as[Int].collect() === Array.fill(n)(1))
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(
      s"reverse chain propagation${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val n = 5
      val verDF = (1 to n).toDF("id").repartition(3)
      val edgeDF = (1 until n)
        .map(x => (x + 1, x))
        .toDF("src", "dst")
        .repartition(3)

      val graph = GraphFrame(verDF, edgeDF)

      val resultDF = graph.pregel
        .setMaxIter(n - 1)
        .withVertexColumn(
          "value",
          when(col("id") === lit(1), lit(1)).otherwise(lit(0)),
          when(Pregel.msg > col("value"), Pregel.msg).otherwise(col("value")))
        .sendMsgToSrc(when(Pregel.dst("value") =!= Pregel.src("value"), Pregel.dst("value")))
        .aggMsgs(max(Pregel.msg))
        .run()

      assert(resultDF.sort("id").select("value").as[Int].collect() === Array.fill(n)(1))
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(s"chain propagation with termination${if (useLocalCheckpoint) " with local checkpoint"
      else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val n = 5
      val verDF = (1 to n).toDF("id").repartition(3)
      val edgeDF = (1 until n)
        .map(x => (x, x + 1))
        .toDF("src", "dst")
        .repartition(3)

      val graph = GraphFrame(verDF, edgeDF)

      val resultDF = graph.pregel
        .setMaxIter(1000)
        .setEarlyStopping(true)
        .withVertexColumn(
          "value",
          when(col("id") === lit(1), lit(1)).otherwise(lit(0)),
          when(Pregel.msg > col("value"), Pregel.msg).otherwise(col("value")))
        .sendMsgToDst(when(Pregel.dst("value") =!= Pregel.src("value"), Pregel.src("value")))
        .aggMsgs(max(Pregel.msg))
        .run()

      assert(resultDF.sort("id").select("value").as[Int].collect() === Array.fill(n)(1))
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }
  })

  test("new vertex column is based on the nullable column") {
    val verDF = Seq(1L, 2L, 3L, 4L)
      .toDF("id")
      .withColumn(
        "nullableColumn",
        when(col("id") % lit(2) === lit(0), lit(null)).otherwise(lit(1)))
    val edgeDF = Seq((1L, 2L), (2L, 3L), (3L, 4L), (4L, 1L)).toDF("src", "dst")
    val graph = GraphFrame(verDF, edgeDF)
    val pregel = graph.pregel
      .withVertexColumn(
        "newColumn",
        when(col("nullableColumn").isNull, lit(0)).otherwise(lit(1)),
        col("newColumn") + Pregel.msg)
      .sendMsgToDst(lit(1))
      .aggMsgs(last(Pregel.msg))
      .setCheckpointInterval(0)
      .setMaxIter(1)

    val resultDF = pregel.run()
    assert(
      resultDF
        .select("id", "newColumn")
        .collect()
        .map(r => r.getAs[Long]("id") -> r.getAs[Int]("newColumn"))
        .toMap === Map(1L -> 2, 2L -> 1, 3L -> 2, 4L -> 1))
  }
}
