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

package com.databricks.dfgraph.lib

import org.apache.spark.sql.SQLContext

import com.databricks.dfgraph.{DFGraph, DFGraphTestSparkContext, SparkFunSuite}

class PageRankSuite extends SparkFunSuite with DFGraphTestSparkContext {

  val n = 100

  test("Star example") {
    val g = PageRankSuite.starGraph(sqlContext, n)
    val resetProb = 0.15
    val errorTol = 1.0e-5
    val pr = PageRank.runUntilConvergence(g, errorTol, resetProb)
    LabelPropagationSuite.testSchemaInvariants(g, pr)
  }
}

object PageRankSuite {
  def starGraph(sqlContext: SQLContext, n: Int): DFGraph = {
    val vertices = sqlContext.createDataFrame(Seq((0, "root")) ++ (1 to n).map { i =>
      (i, s"node-$i")
    }).toDF("id", "v_attr1")
    val edges = sqlContext.createDataFrame((1 to n).map { i =>
      (i, 0, s"edge-$i")
    }).toDF("src", "dst", "e_attr1")
    DFGraph(vertices, edges)

  }
}
