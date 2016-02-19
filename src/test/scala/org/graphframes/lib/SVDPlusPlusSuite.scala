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

import org.graphframes.{GraphFrame, GraphFrameTestSparkContext, SparkFunSuite}
import org.graphframes.examples.Graphs

import org.apache.spark.sql.Row

class SVDPlusPlusSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("Test SVD++ with mean square error on training set") {

    val svdppErr = 8.0
    val g = Graphs.ALSSyntheticData()

    val g2 = g.svdPlusPlus.maxIterations(2).run()
    LabelPropagationSuite.testSchemaInvariants(g, g2)
    val err = g2.vertices.select(GraphFrame.ID, "column4").map { case Row(vid: Long, vd: Double) =>
      if (vid % 2 == 1) vd else 0.0
    }.reduce(_ + _) / g.edges.count()
    assert(err <= svdppErr)
  }
}
