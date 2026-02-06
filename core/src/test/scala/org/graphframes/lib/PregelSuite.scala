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

import org.apache.spark.sql.functions.*
import org.graphframes.*
import org.scalactic.Tolerance.*

class PregelSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  import sqlImplicits.*

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

  test("requiredSrcColumns - only specified columns are used in triplets") {
    // Test that requiredSrcColumns correctly limits the columns in triplets
    // This is a memory optimization test - we verify the result is correct
    // with only required source columns

    val edges = Seq((0L, 1L), (1L, 2L), (2L, 4L), (2L, 0L), (3L, 4L), (4L, 0L), (4L, 2L))
      .toDF("src", "dst")
      .cache()
    val vertices = GraphFrame.fromEdges(edges).outDegrees.cache()
    val numVertices = vertices.count()
    val graph = GraphFrame(vertices, edges)

    val alpha = 0.15
    // PageRank only needs "rank" and "outDegree" from source vertex
    val ranks = graph.pregel
      .setMaxIter(5)
      .withVertexColumn(
        "rank",
        lit(1.0 / numVertices),
        coalesce(Pregel.msg, lit(0.0)) * (1.0 - alpha) + alpha / numVertices)
      .sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree"))
      .aggMsgs(sum(Pregel.msg))
      .requiredSrcColumns("rank", "outDegree")
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
  }

  test("requiredDstColumns - only specified columns are used in triplets") {
    // Test that requiredDstColumns correctly limits the columns in triplets
    // Reverse chain propagation where we only need dst("value") from destination

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
      .requiredDstColumns("value") // Only need "value" from destination
      .run()

    assert(resultDF.sort("id").select("value").as[Int].collect() === Array.fill(n)(1))
  }

  test("requiredSrcColumns and requiredDstColumns together") {
    // Test using both requiredSrcColumns and requiredDstColumns
    // Chain propagation where we need "value" from both src and dst

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
      .requiredSrcColumns("value") // Only need "value" from source
      .requiredDstColumns("value") // Only need "value" from destination
      .run()

    assert(resultDF.sort("id").select("value").as[Int].collect() === Array.fill(n)(1))
  }

  test("requiredSrcColumns with empty list uses all columns (default behavior)") {
    // Verify that not calling requiredSrcColumns means all columns are used
    // This is the same as the original page rank test

    val edges = Seq((0L, 1L), (1L, 2L), (2L, 4L), (2L, 0L), (3L, 4L), (4L, 0L), (4L, 2L))
      .toDF("src", "dst")
      .cache()
    val vertices = GraphFrame.fromEdges(edges).outDegrees.cache()
    val numVertices = vertices.count()
    val graph = GraphFrame(vertices, edges)

    val alpha = 0.15
    val ranks = graph.pregel
      .setMaxIter(5)
      .withVertexColumn(
        "rank",
        lit(1.0 / numVertices),
        coalesce(Pregel.msg, lit(0.0)) * (1.0 - alpha) + alpha / numVertices)
      .sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree"))
      .aggMsgs(sum(Pregel.msg))
      // No requiredSrcColumns or requiredDstColumns - should use all columns
      .run()

    val result = ranks
      .sort(col("id"))
      .select("rank")
      .as[Double]
      .collect()
    assert(result.sum === 1.0 +- 1e-6)
  }

  test("automatic dst join skipping - PageRank only uses src columns") {
    // PageRank only references Pregel.src("rank") and Pregel.src("outDegree"),
    // so the second join (for dst vertex state) should be automatically skipped.
    // This test verifies the optimization produces correct results.

    val edges = Seq((0L, 1L), (1L, 2L), (2L, 4L), (2L, 0L), (3L, 4L), (4L, 0L), (4L, 2L))
      .toDF("src", "dst")
      .cache()
    val vertices = GraphFrame.fromEdges(edges).outDegrees.cache()
    val numVertices = vertices.count()
    val graph = GraphFrame(vertices, edges)

    val alpha = 0.15
    // PageRank only uses Pregel.src(...) - dst state should be automatically skipped
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
  }

  test("automatic dst join NOT skipped when dst columns are referenced") {
    // This test uses Pregel.dst("value") in the message expression,
    // so the second join must NOT be skipped.

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
      // This references BOTH src and dst - dst join should NOT be skipped
      .sendMsgToDst(when(Pregel.dst("value") =!= Pregel.src("value"), Pregel.src("value")))
      .aggMsgs(max(Pregel.msg))
      .run()

    assert(resultDF.sort("id").select("value").as[Int].collect() === Array.fill(n)(1))
  }

  test("automatic dst join NOT skipped when skipMessagesFromNonActiveVertices is enabled") {
    // When skipMessagesFromNonActiveVertices is true, we need dst._pregel_is_active,
    // so the second join must NOT be skipped even if message expressions don't use dst.

    val n = 5
    val verDF = (1 to n).toDF("id").repartition(3)
    val edgeDF = (1 until n)
      .map(x => (x, x + 1))
      .toDF("src", "dst")
      .repartition(3)

    val graph = GraphFrame(verDF, edgeDF)

    // This only uses Pregel.src("value"), but skipMessagesFromNonActiveVertices
    // requires dst._pregel_is_active, so dst join should NOT be skipped
    val resultDF = graph.pregel
      .setMaxIter(n - 1)
      .setSkipMessagesFromNonActiveVertices(true)
      .setUpdateActiveVertexExpression(Pregel.msg.isNotNull)
      .withVertexColumn(
        "value",
        when(col("id") === lit(1), lit(1)).otherwise(lit(0)),
        when(Pregel.msg > col("value"), Pregel.msg).otherwise(col("value")))
      .sendMsgToDst(Pregel.src("value"))
      .aggMsgs(max(Pregel.msg))
      .run()

    assert(resultDF.sort("id").select("value").as[Int].collect() === Array.fill(n)(1))
  }

  test("automatic dst join skipping - sendMsgToSrc with only edge columns") {
    // When sending messages to src using only edge columns, dst join should be skipped

    val edges = Seq((1L, 0L, 10L), (2L, 1L, 20L), (3L, 2L, 30L), (4L, 3L, 40L))
      .toDF("src", "dst", "weight")
      .cache()
    val vertices = (0L to 4L).toDF("id").cache()

    val graph = GraphFrame(vertices, edges)

    // Only uses Pregel.edge("weight") - dst join should be skipped
    val resultDF = graph.pregel
      .setMaxIter(1)
      .withVertexColumn("received", lit(0L), coalesce(Pregel.msg, col("received")))
      .sendMsgToSrc(Pregel.edge("weight"))
      .aggMsgs(sum(Pregel.msg))
      .run()

    // Each src vertex receives the weight from its outgoing edge
    val received = resultDF.sort("id").select("received").as[Long].collect()
    assert(received(0) === 0L) // vertex 0: no outgoing edges
    assert(received(1) === 10L) // vertex 1: edge 1->0 with weight 10
    assert(received(2) === 20L) // vertex 2: edge 2->1 with weight 20
    assert(received(3) === 30L) // vertex 3: edge 3->2 with weight 30
    assert(received(4) === 40L) // vertex 4: edge 4->3 with weight 40
  }

  test("automatic dst join skipping - edge columns only") {
    // When message expressions only reference edge columns, dst join should be skipped

    val edges =
      Seq((0L, 1L, 1.0), (1L, 2L, 2.0), (2L, 3L, 3.0)).toDF("src", "dst", "weight").cache()
    val vertices = Seq(0L, 1L, 2L, 3L).toDF("id").cache()
    val graph = GraphFrame(vertices, edges)

    // Only uses Pregel.edge("weight") - dst join should be skipped
    val result = graph.pregel
      .setMaxIter(1) // Single iteration to simplify testing
      .withVertexColumn("total", lit(0.0), coalesce(Pregel.msg, col("total")))
      .sendMsgToDst(Pregel.edge("weight"))
      .aggMsgs(sum(Pregel.msg))
      .run()

    // Verify results: vertex 1 gets weight 1.0, vertex 2 gets 2.0, vertex 3 gets 3.0
    val totals = result.sort("id").select("total").as[Double].collect()
    assert(totals(0) === 0.0 +- 1e-6) // vertex 0: no incoming edges
    assert(totals(1) === 1.0 +- 1e-6) // vertex 1: edge 0->1 with weight 1.0
    assert(totals(2) === 2.0 +- 1e-6) // vertex 2: edge 1->2 with weight 2.0
    assert(totals(3) === 3.0 +- 1e-6) // vertex 3: edge 2->3 with weight 3.0
  }

  test("automatic dst join skipping - sendMsgToDst with only src columns in message") {
    // When sendMsgToDst is used but the message expression only references src columns,
    // the second join should be skipped. The dst.id needed for message routing is
    // obtained from the edge's dst column, not from a vertex join.

    val edges = Seq((0L, 1L), (1L, 2L), (2L, 3L)).toDF("src", "dst").cache()
    val vertices = (0L to 3L).toDF("id").cache()
    val graph = GraphFrame(vertices, edges)

    // sendMsgToDst but message only uses Pregel.src("id") - dst join should be skipped
    val result = graph.pregel
      .setMaxIter(1)
      .withVertexColumn("received", lit(0L), coalesce(Pregel.msg, col("received")))
      .sendMsgToDst(Pregel.src("id")) // Message only uses src.id
      .aggMsgs(sum(Pregel.msg))
      .run()

    // Each dst vertex receives the src.id from incoming edges
    val received = result.sort("id").select("received").as[Long].collect()
    assert(received(0) === 0L) // vertex 0: no incoming edges
    assert(received(1) === 0L) // vertex 1: edge 0->1, receives src.id = 0
    assert(received(2) === 1L) // vertex 2: edge 1->2, receives src.id = 1
    assert(received(3) === 2L) // vertex 3: edge 2->3, receives src.id = 2
  }
}
