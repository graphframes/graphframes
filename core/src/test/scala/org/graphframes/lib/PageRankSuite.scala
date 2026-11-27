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

import org.apache.spark.sql.functions.abs
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.DataTypes
import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.SparkFunSuite
import org.graphframes.TestUtils
import org.graphframes.examples.Graphs

class PageRankSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  val n = 100L

  // Test dispatch: the deprecated GraphX-based implementation ("graphx") and the native
  // DataFrame implementation ("graphframes"). PageRankV2 has a single implementation, so only
  // the "graphframes" algorithm is dispatched for it.
  private val algorithms = Seq("graphx", "graphframes")

  algorithms.foreach { algo =>
    test(s"Star example ($algo)") {
      val g = Graphs.star(n)
      val resetProb = 0.15
      val errorTol = 1.0e-5
      if (algo == "graphx") {
        val pr = g.pageRank
          .resetProbability(resetProb)
          .tol(errorTol)
          .run()
        TestUtils.testSchemaInvariants(g, pr)
        TestUtils.checkColumnType(pr.vertices.schema, "pagerank", DataTypes.DoubleType)
        TestUtils.checkColumnType(pr.edges.schema, "weight", DataTypes.DoubleType)
        pr.unpersist()
      } else {
        val pr = g.pageRankV2
          .resetProbability(resetProb)
          .tol(errorTol)
          .run()
        // The result is the vertex state only: all the original vertices (including the sink
        // leaves of the star) plus the normalized ranks.
        TestUtils.checkColumnType(pr.schema, PageRankV2.PAGERANKS, DataTypes.DoubleType)
        assert(pr.count() === g.vertices.count())
        val rankSum = pr.agg(sum(col(PageRankV2.PAGERANKS))).head().getDouble(0)
        assert(math.abs(rankSum - 1.0) < 1.0e-9, s"ranks must sum up to 1.0, got $rankSum")
        // The ranks must agree with the deprecated implementation (normalized to 1.0).
        val reference = g.pageRank.resetProbability(resetProb).tol(errorTol).run()
        val referenceSum =
          reference.vertices.agg(sum(col("pagerank"))).head().getDouble(0)
        val maxDiff = pr
          .join(reference.vertices, Seq("id"))
          .select(abs(col(PageRankV2.PAGERANKS) - col("pagerank") / lit(referenceSum)))
          .collect()
          .map(_.getDouble(0))
          .max
        assert(
          maxDiff < 1.0e-3,
          s"PageRankV2 diverges from the deprecated implementation: $maxDiff")
        pr.unpersist()
        reference.unpersist()
      }
    }

    test(s"friends graph with personalized PageRank ($algo)") {
      if (algo == "graphx") {
        val results =
          Graphs.friends.pageRank.resetProbability(0.15).maxIter(10).sourceId("a").run()
        val gRank =
          results.vertices.filter(col("id") === "g").select("pagerank").first().getDouble(0)
        assert(
          gRank === 0.0,
          s"User g (Gabby) doesn't connect with a. So its pagerank should be 0 but we got $gRank.")
        results.unpersist()
      } else {
        val results =
          Graphs.friends.pageRankV2.resetProbability(0.15).maxIter(10).sourceId("a").run()
        val gRank =
          results.filter(col("id") === "g").select(PageRankV2.PAGERANKS).first().getDouble(0)
        assert(
          gRank === 0.0,
          s"User g (Gabby) doesn't connect with a. So its pagerank should be 0 but we got $gRank.")
        results.unpersist()
      }
    }

    test(s"graph with three disconnected components ($algo)") {
      import sqlImplicits._

      val v = Seq((0L, "a"), (1L, "b"), (2L, "c"), (3L, "d"), (4L, "e"), (5L, "f"), (6L, "g"))
        .toDF("id", "name")

      val e = Seq(
        (0L, 1L, "friend"), // First component: a->b->c
        (1L, 2L, "friend"),
        (3L, 4L, "friend"), // Second component: d->e->f
        (4L, 5L, "friend")
        // Third component: isolated vertex g (6)
      ).toDF("src", "dst", "relationship")

      val g = org.graphframes.GraphFrame(v, e)
      val originalIds = v.select("id").collect().map(_.getLong(0)).toSet

      if (algo == "graphx") {
        val results = g.pageRank.resetProbability(0.15).maxIter(10).run()
        // Verify that all original vertices are present in the result
        assert(
          results.vertices.count() === v.count(),
          "PageRank results should contain all original vertices")
        val resultIds = results.vertices.select("id").collect().map(_.getLong(0)).toSet
        assert(originalIds === resultIds, "PageRank results should preserve all vertex IDs")
        results.unpersist()
      } else {
        val results = g.pageRankV2.resetProbability(0.15).maxIter(10).run()
        assert(
          results.count() === v.count(),
          "PageRankV2 results should contain all original vertices")
        val resultIds = results.select("id").collect().map(_.getLong(0)).toSet
        assert(originalIds === resultIds, "PageRankV2 results should preserve all vertex IDs")
        results.unpersist()
      }
    }
  }
}
