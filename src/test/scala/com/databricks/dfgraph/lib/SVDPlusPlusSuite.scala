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

import org.apache.spark.sql.Row

import com.databricks.dfgraph.{DFGraph, DFGraphTestSparkContext, SparkFunSuite}

class SVDPlusPlusSuite extends SparkFunSuite with DFGraphTestSparkContext {

  test("Test SVD++ with mean square error on training set") {

    val svdppErr = 8.0
    val data = sc.textFile(getClass.getResource("/als-test.data").getFile).map { line =>
      val fields = line.split(",")
      (fields(0).toLong * 2, fields(1).toLong * 2 + 1, fields(2).toDouble)
    }
    val edges = sqlContext.createDataFrame(data).toDF("src", "dst", "weight")
    val vs = data.flatMap(r => r._1 :: r._2 :: Nil).collect().distinct.map(x => Tuple1(x))
    val vertices = sqlContext.createDataFrame(vs).toDF("id")
    val g = DFGraph(vertices, edges)

    val conf = SVDPlusPlus.Conf(10, 2, 0.0, 5.0, 0.007, 0.007, 0.005, 0.015) // 2 iterations
    val (g2, _) = SVDPlusPlus.run(g, conf)
    LabelPropagationSuite.testSchemaInvariants(g, g2)
    val err = g2.vertices.select(DFGraph.ID, "column4").map { case Row(vid: Long, vd: Double) =>
      if (vid % 2 == 1) vd else 0.0
    }.reduce(_ + _) / g.edges.count()
    assert(err <= svdppErr)
  }
}
