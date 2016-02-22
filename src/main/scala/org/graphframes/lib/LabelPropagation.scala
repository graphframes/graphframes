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

import org.graphframes.GraphFrame

/**
 * Run static Label Propagation for detecting communities in networks.
 *
 * Each node in the network is initially assigned to its own community. At every superstep, nodes
 * send their community affiliation to all neighbors and update their state to the mode community
 * affiliation of incoming messages.
 *
 * LPA is a standard community detection algorithm for graphs. It is very inexpensive
 * computationally, although (1) convergence is not guaranteed and (2) one can end up with
 * trivial solutions (all nodes are identified into a single community).
 *
 * The resulting vertices DataFrame contains one additional column:
 *  - label: (same type as vertex id) label of community affiliation
 *
 * The resulting edges DataFrame is the same as the original edges DataFrame.
 */
class LabelPropagation private[graphframes] (private val graph: GraphFrame) extends Arguments {

  private var maxSteps: Option[Int] = None

  /**
   * The number of supersteps of LPA to be performed. Because this is a static
   * implementation, the algorithm will run for exactly this many supersteps.
   */
  def maxSteps(value: Int): this.type = {
    maxSteps = Some(value)
    this
  }

  def run(): GraphFrame = {
    LabelPropagation.run(
      graph,
      check(maxSteps, "maxSteps"))
  }
}


private object LabelPropagation {
  private def run(graph: GraphFrame, maxSteps: Int): GraphFrame = {
    val gx = graphxlib.LabelPropagation.run(graph.cachedTopologyGraphX, maxSteps)
    GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(LABEL_ID))
  }

  private val LABEL_ID = "label"

}
