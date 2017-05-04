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

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.ml.linalg.{SQLDataTypes, SparseVector}

import org.graphframes.examples.Graphs
import org.graphframes.{GraphFrameTestSparkContext, SparkFunSuite, TestUtils}

class PageRankSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  val n = 100

  test("Star example") {
    val g = Graphs.star(n)
    val resetProb = 0.15
    val errorTol = 1.0e-5
    val pr = g.pageRank
      .resetProbability(resetProb)
      .tol(errorTol).run()
    TestUtils.testSchemaInvariants(g, pr)
    TestUtils.checkColumnType(pr.vertices.schema, "pagerank", DataTypes.DoubleType)
    TestUtils.checkColumnType(pr.edges.schema, "weight", DataTypes.DoubleType)
  }

  test("friends graph with personalized PageRank") {
    val results = Graphs.friends.pageRank.resetProbability(0.15).maxIter(10).sourceId("a").run()

    val gRank = results.vertices.filter(col("id") === "g").select("pagerank").first().getDouble(0)
    assert(gRank === 0.0,
      s"User g (Gabby) doesn't connect with a. So its pagerank should be 0 but we got $gRank.")
  }

  test("Star example parallel personalized PageRank") {
    val g = Graphs.star(n)
    val resetProb = 0.15
    val numIter = 10
    val vertexIds: Array[Any] = Array(1L, 2L, 3L)
    val pr = g.parallelPersonalizedPageRank
      .numIter(numIter)
      .sources(vertexIds)
      .resetProbability(resetProb)
      .run()

    TestUtils.testSchemaInvariants(g, pr)
    TestUtils.checkColumnType(pr.vertices.schema, "pagerank", SQLDataTypes.VectorType)
    TestUtils.checkColumnType(pr.edges.schema, "weight", DataTypes.DoubleType)
  }

  test("friends graph with parallel personalized PageRank") {
    val g = Graphs.friends
    val resetProb = 0.15
    val numIter = 10
    val vertexIds: Array[Any] = Array("a")
    val pr = g.parallelPersonalizedPageRank
      .numIter(numIter)
      .sources(vertexIds)
      .resetProbability(resetProb)
      .run()

    val prInvalid = pr.vertices
      .select("pagerank")
      .filter(vertexIds.size != _.get(0).asInstanceOf[SparseVector].size)
    val prInvalidSize = prInvalid.count()
    if (0L != prInvalidSize) {
      prInvalid.show(10)
      assert(false,
        s"found $prInvalidSize entries with invalid number of returned personalized pagerank vector")
    }

    val gRank = pr.vertices
      .filter(col("id") === "g")
      .select("pagerank")
      .first().get(0).asInstanceOf[SparseVector]
    assert(gRank.numNonzeros === 0,
      s"User g (Gabby) doesn't connect with a. So its pagerank should be 0 but we got ${gRank.numNonzeros}.")
  }

}
