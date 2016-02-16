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

package org.graphframes.examples

import org.graphframes.{GraphFrame, GraphFrameTestSparkContext, SparkFunSuite}
import org.graphframes.examples.BeliefPropagation._
import org.graphframes.examples.Graphs.gridIsingModel


class BeliefPropagationSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("BP using GraphX aggregateMessages") {
    // Create graphical model g.
    val n = 2
    val g = gridIsingModel(sqlContext, n)
    println("ORIGINAL MODEL: v")
    g.vertices.sort("id").show()
    g.edges.sort("src", "dst").show()

    // Run BP
    val results = runBP(g, 1)

    // Display beliefs.
    val beliefs = results.vertices.select("id", "belief")
    beliefs.sort("id").show()
  }

  test("BP using GraphFrame aggregateMessages") {
    // Create graphical model g.
    val n = 2
    val g = gridIsingModel(sqlContext, n)
    /*
    val gv = g.vertices.select("id", "a", "i", "j")
      .map(r => (r.getString(0), r.getDouble(1), r.getInt(2), r.getInt(3)))
    gv.cache().count()
    val gv2 = sqlContext.createDataFrame(gv).toDF("id", "a", "i", "j")
    val ge = g.edges.select("src", "dst", "b")
      .map(r => (r.getString(0), r.getString(1), r.getDouble(2)))
    ge.cache().count()
    val ge2 = sqlContext.createDataFrame(ge).toDF("src", "dst", "b")
    println("ORIGINAL MODEL: v")
    gv2.sort("id").show()
    ge2.sort("src", "dst").show()
    val finalG = GraphFrame(gv2, ge2)
*/
    // Run BP
    g.vertices.cache().show()
    g.edges.cache().show()
    val results = runBP2(g, 1)

    // Display beliefs.
    val beliefs = results.vertices.select("id", "belief")
    beliefs.sort("id").show()
  }
}
