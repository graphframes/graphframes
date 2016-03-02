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

import org.apache.spark.graphx.{lib => graphxlib}
import org.apache.spark.sql.DataFrame

import org.graphframes.GraphFrame

/**
 * Run static Label Propagation for detecting communities in networks.
 *
 * Each node in the network is initially assigned to its own community. At every iteration, nodes
 * send their community affiliation to all neighbors and update their state to the mode community
 * affiliation of incoming messages.
 *
 * LPA is a standard community detection algorithm for graphs. It is very inexpensive
 * computationally, although (1) convergence is not guaranteed and (2) one can end up with
 * trivial solutions (all nodes are identified into a single community).
 *
 * The resulting DataFrame contains all the original vertex information and one additional column:
 *  - label (`LongType`): label of community affiliation
 */
class LabelPropagation private[graphframes] (private val graph: GraphFrame) extends Arguments {

  private var maxIter: Option[Int] = None

  /**
   * The max number of iterations of LPA to be performed. Because this is a static
   * implementation, the algorithm will run for exactly this many iterations.
   */
  def maxIter(value: Int): this.type = {
    maxIter = Some(value)
    this
  }

  def run(): DataFrame = {
    LabelPropagation.run(
      graph,
      check(maxIter, "maxIter"))
  }
}


private object LabelPropagation {
  private def run(graph: GraphFrame, maxIter: Int): DataFrame = {
    val gx = graphxlib.LabelPropagation.run(graph.cachedTopologyGraphX, maxIter)
    GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(LABEL_ID)).vertices
  }

  private val LABEL_ID = "label"

}
