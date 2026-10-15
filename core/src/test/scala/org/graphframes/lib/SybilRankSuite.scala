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
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StringType
import org.graphframes.GraphFrame
import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.SparkFunSuite
import org.graphframes.TestUtils

class SybilRankSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  private val eps: Double = 1e-6

  private def ranksOf(df: DataFrame): Map[Any, Double] =
    df.collect().map(row => row.get(0) -> row.getDouble(1)).toMap

  private def assertRanks(actual: Map[Any, Double], expected: Map[Any, Double]): Unit = {
    assert(
      actual.keySet == expected.keySet,
      s"vertex sets differ: ${actual.keySet} vs ${expected.keySet}")
    expected.foreach { case (id, rank) =>
      val got = actual(id)
      assert(
        math.abs(got - rank) < eps,
        s"rank of vertex $id is $got, expected $rank; all ranks: $actual")
    }
  }

  private def starGraph(): GraphFrame = {
    val vertices = spark
      .createDataFrame(Seq((0, "t"), (1, "a"), (2, "b"), (3, "c")))
      .toDF("id", "name")
    val edges = spark.createDataFrame(Seq((0, 1), (0, 2), (0, 3))).toDF("src", "dst")
    GraphFrame(vertices, edges)
  }

  private def pathGraph(): GraphFrame = {
    val vertices = spark
      .createDataFrame(Seq((0, "a"), (1, "b"), (2, "c"), (3, "d")))
      .toDF("id", "name")
    val edges = spark.createDataFrame(Seq((0, 1), (1, 2), (2, 3))).toDF("src", "dst")
    GraphFrame(vertices, edges)
  }

  test("star graph, single trusted center, one power iteration") {
    val g = starGraph()
    val ranks = ranksOf(g.sybilRank.setTrustedVertices(Seq(0)).run())
    // N = 4, so the default number of iterations is ceil(log10(4)) = 1.
    // The trusted center starts with rank 4.0 and distributes 4/3 to every leaf.
    assertRanks(ranks, Map(0 -> 0.0, 1 -> 4.0 / 3.0, 2 -> 4.0 / 3.0, 3 -> 4.0 / 3.0))
  }

  test("path graph, multiple power iterations via the iteration multiplier") {
    val g = pathGraph()

    // ceil(5 * log10(4)) = 4 power iterations
    val ranks = ranksOf(g.sybilRank.setTrustedVertices(Seq(0)).setIterationMultiplier(5.0).run())
    assertRanks(ranks, Map(0 -> 0.3125, 1 -> 0.0, 2 -> 0.28125, 3 -> 0.0))
  }

  test("path graph, explicit maxIter overrides the iteration multiplier") {
    val g = pathGraph()

    val ranks = ranksOf(g.sybilRank.setTrustedVertices(Seq(0)).maxIter(2).run())
    assertRanks(ranks, Map(0 -> 1.0, 1 -> 0.0, 2 -> 0.5, 3 -> 0.0))
  }

  test("weighted star distributes rank proportionally to the edge weights") {
    val vertices = spark.createDataFrame(Seq((0, "t"), (1, "a"), (2, "b"))).toDF("id", "name")
    val edges = spark.createDataFrame(Seq((0, 1, 3.0), (0, 2, 1.0))).toDF("src", "dst", "weight")
    val g = GraphFrame(vertices, edges)

    // totalTrust = N = 3, so the center starts with rank 3.0 and has degree 4.0
    val ranks = ranksOf(g.sybilRank.setTrustedVertices(Seq(0)).setWeightCol("weight").run())
    assertRanks(ranks, Map(0 -> 0.0, 1 -> 0.75, 2 -> 0.75))
  }

  test("unit weight column is equivalent to the unweighted graph") {
    val vertices = spark
      .createDataFrame(Seq((0, "t"), (1, "a"), (2, "b"), (3, "c")))
      .toDF("id", "name")
    val weightedEdges = spark
      .createDataFrame(Seq((0, 1, 1.0), (0, 2, 1.0), (0, 3, 1.0)))
      .toDF("src", "dst", "weight")
    val unweightedEdges = spark.createDataFrame(Seq((0, 1), (0, 2), (0, 3))).toDF("src", "dst")

    val weighted = ranksOf(
      GraphFrame(vertices, weightedEdges).sybilRank
        .setTrustedVertices(Seq(0))
        .setWeightCol("weight")
        .run())
    val unweighted =
      ranksOf(GraphFrame(vertices, unweightedEdges).sybilRank.setTrustedVertices(Seq(0)).run())
    assertRanks(weighted, unweighted)
  }

  test("multiple trusted vertices share the total trust") {
    val vertices = spark
      .createDataFrame(Seq((0, "a"), (1, "b"), (2, "c"), (3, "d")))
      .toDF("id", "name")
    val edges = spark.createDataFrame(Seq((0, 1), (2, 3), (0, 2))).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    // totalTrust = 4, two trusted vertices start with rank 2.0 each
    val ranks = ranksOf(g.sybilRank.setTrustedVertices(Seq(0, 2)).run())
    assertRanks(ranks, Map(0 -> 0.5, 1 -> 1.0, 2 -> 0.5, 3 -> 1.0))
  }

  test("trusted vertices can be specified with a boolean column") {
    val g = starGraph()
    val trusted = g.vertices.withColumn("trusted", col("id") === 0)
    val withCol = GraphFrame(trusted, g.edges)
    val byCol = ranksOf(withCol.sybilRank.setTrustedVerticesCol("trusted").run())
    val byIds = ranksOf(g.sybilRank.setTrustedVertices(Seq(0)).run())
    assertRanks(byCol, byIds)
  }

  test("trusted vertices can have string IDs") {
    val vertices = spark
      .createDataFrame(Seq(("a", "t"), ("b", "x"), ("c", "y"), ("d", "z")))
      .toDF("id", "name")
    val edges = spark.createDataFrame(Seq(("a", "b"), ("a", "c"), ("a", "d"))).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val ranks = ranksOf(g.sybilRank.setTrustedVertices(Seq("a")).run())
    assertRanks(ranks, Map("a" -> 0.0, "b" -> 4.0 / 3.0, "c" -> 4.0 / 3.0, "d" -> 4.0 / 3.0))
  }

  test("directed and undirected runs differ after two iterations on a path") {
    val g = pathGraph()

    val undirected = ranksOf(g.sybilRank.setTrustedVertices(Seq(0)).maxIter(2).run())
    assertRanks(undirected, Map(0 -> 1.0, 1 -> 0.0, 2 -> 0.5, 3 -> 0.0))

    // In the directed mode the degree is the weighted out-degree, so the undirected
    // rank of vertex 0 (kept because of the incoming message from 1) becomes zero and
    // the ranks travel further along the path.
    val directed =
      ranksOf(g.sybilRank.setTrustedVertices(Seq(0)).maxIter(2).setIsDirected(true).run())
    assertRanks(directed, Map(0 -> 0.0, 1 -> 0.0, 2 -> 4.0, 3 -> 0.0))
  }

  test("total trust can be set explicitly") {
    val g = starGraph()
    val ranks = ranksOf(g.sybilRank.setTrustedVertices(Seq(0)).setTotalTrust(8.0).run())
    assertRanks(ranks, Map(0 -> 0.0, 1 -> 8.0 / 3.0, 2 -> 8.0 / 3.0, 3 -> 8.0 / 3.0))
  }

  test("isolated vertices get zero rank and produce no NaNs") {
    val vertices = spark
      .createDataFrame(Seq((0, "t"), (1, "a"), (2, "b"), (3, "c"), (9, "iso")))
      .toDF("id", "name")
    val edges = spark.createDataFrame(Seq((0, 1), (0, 2), (0, 3))).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    // N = 5, the trusted center starts with rank 5.0
    val ranks = ranksOf(g.sybilRank.setTrustedVertices(Seq(0)).run())
    assertRanks(ranks, Map(0 -> 0.0, 1 -> 5.0 / 3.0, 2 -> 5.0 / 3.0, 3 -> 5.0 / 3.0, 9 -> 0.0))
    ranks.values.foreach(r => assert(!r.isNaN, s"unexpected NaN rank: $ranks"))
  }

  test("isolated trusted vertex decays to zero rank") {
    val vertices = spark
      .createDataFrame(Seq((0, "a"), (1, "b"), (2, "c"), (3, "d"), (9, "iso")))
      .toDF("id", "name")
    val edges = spark.createDataFrame(Seq((0, 1), (0, 2), (0, 3))).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val ranks = ranksOf(g.sybilRank.setTrustedVertices(Seq(9)).run())
    assertRanks(ranks, Map(0 -> 0.0, 1 -> 0.0, 2 -> 0.0, 3 -> 0.0, 9 -> 0.0))
  }

  test("graph without edges") {
    val vertices = spark.createDataFrame(Seq((0, "a"), (1, "b"))).toDF("id", "name")
    val edges = spark.createDataFrame(Seq.empty[(Int, Int)]).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val ranks = ranksOf(g.sybilRank.setTrustedVertices(Seq(0)).run())
    assertRanks(ranks, Map(0 -> 0.0, 1 -> 0.0))
  }

  test("single vertex graph") {
    val vertices = spark.createDataFrame(Seq((0, "a"))).toDF("id", "name")
    val edges = spark.createDataFrame(Seq.empty[(Int, Int)]).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val ranks = ranksOf(g.sybilRank.setTrustedVertices(Seq(0)).run())
    assertRanks(ranks, Map(0 -> 0.0))
  }

  test("self loop does not produce NaNs") {
    val vertices = spark.createDataFrame(Seq((0, "a"))).toDF("id", "name")
    val edges = spark.createDataFrame(Seq((0, 0))).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    // Undirected: the self loop contributes twice to the degree (2.0), and both message
    // directions deliver 1.0 * 1/2, so the updated rank is 1.0 / 2.0.
    val ranks = ranksOf(g.sybilRank.setTrustedVertices(Seq(0)).run())
    assertRanks(ranks, Map(0 -> 0.5))
  }

  test("all vertices trusted") {
    val vertices = spark
      .createDataFrame(Seq((0, "a"), (1, "b"), (2, "c"), (3, "d"), (4, "e")))
      .toDF("id", "name")
    val complete = for {
      i <- 0 until 5
      j <- (i + 1) until 5
    } yield (i, j)
    val edges = spark.createDataFrame(complete).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val ranks =
      ranksOf(g.sybilRank.setTrustedVertices(Seq(0, 1, 2, 3, 4)).run())
    assertRanks(ranks, Map(0 -> 0.25, 1 -> 0.25, 2 -> 0.25, 3 -> 0.25, 4 -> 0.25))
  }

  test("total rank decays over iterations") {
    val vertices = spark
      .createDataFrame(Seq((0, "a"), (1, "b"), (2, "c"), (3, "d"), (4, "e")))
      .toDF("id", "name")
    val edges = spark
      .createDataFrame(Seq((0, 1), (1, 2), (2, 3), (3, 4), (4, 0)))
      .toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val sums = (1 to 3).map { iters =>
      g.sybilRank
        .setTrustedVertices(Seq(0))
        .maxIter(iters)
        .run()
        .agg(sum("sybil_rank"))
        .collect()
        .head
        .getDouble(0)
    }
    // Non-increasing on unweighted graphs without isolated vertices, strictly decreasing here.
    assert(sums(0) <= 5.0 + eps, s"$sums")
    assert(sums(1) < sums(0), s"$sums")
    assert(sums(2) < sums(1), s"$sums")
  }

  test("result schema") {
    val g = starGraph()
    val result = g.sybilRank.setTrustedVertices(Seq(0)).run()
    TestUtils.checkColumnType(result.schema, "sybil_rank", DataTypes.DoubleType)
    assert(result.columns.toSet == Set("id", "sybil_rank"))
  }

  test("no trusted vertices specified") {
    val g = starGraph()
    intercept[IllegalArgumentException] {
      g.sybilRank.run()
    }
  }

  test("both trusted vertices and trusted column specified") {
    val g = starGraph()
    intercept[IllegalArgumentException] {
      g.sybilRank.setTrustedVertices(Seq(0)).setTrustedVerticesCol("id").run()
    }
  }

  test("trusted vertices must exist in the graph") {
    val g = starGraph()
    intercept[IllegalArgumentException] {
      g.sybilRank.setTrustedVertices(Seq(0, 42)).run()
    }
  }

  test("trusted vertices column must exist and be boolean") {
    val g = starGraph()
    intercept[IllegalArgumentException] {
      g.sybilRank.setTrustedVerticesCol("no_such_column").run()
    }
    intercept[IllegalArgumentException] {
      g.sybilRank.setTrustedVerticesCol("name").run()
    }
  }

  test("weight column must exist and be numeric") {
    val vertices = spark.createDataFrame(Seq((0, "a"), (1, "b"))).toDF("id", "name")
    val edges = spark.createDataFrame(Seq((0, 1, 1.0))).toDF("src", "dst", "weight")
    val g = GraphFrame(vertices, edges)

    intercept[IllegalArgumentException] {
      g.sybilRank.setTrustedVertices(Seq(0)).setWeightCol("no_such_column").run()
    }
    val stringWeightEdges = edges.withColumn("weight", col("weight").cast(StringType))
    intercept[IllegalArgumentException] {
      GraphFrame(vertices, stringWeightEdges).sybilRank
        .setTrustedVertices(Seq(0))
        .setWeightCol("weight")
        .run()
    }
  }

  test("total trust and iteration multiplier must be positive") {
    val g = starGraph()
    intercept[IllegalArgumentException] {
      g.sybilRank.setTrustedVertices(Seq(0)).setTotalTrust(-1.0).run()
    }
    intercept[IllegalArgumentException] {
      g.sybilRank.setTrustedVertices(Seq(0)).setIterationMultiplier(0.0).run()
    }
  }

  test("empty trusted vertices sequence") {
    val g = starGraph()
    intercept[IllegalArgumentException] {
      g.sybilRank.setTrustedVertices(Seq.empty).run()
    }
  }

  test("accessor from GraphFrame") {
    val g = starGraph()
    val ranks = ranksOf(g.sybilRank.setTrustedVertices(Seq(0)).run())
    assert(ranks.size == 4)
  }
}
