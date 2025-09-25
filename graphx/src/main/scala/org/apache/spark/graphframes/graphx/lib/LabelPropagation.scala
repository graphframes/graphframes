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

package org.apache.spark.graphframes.graphx.lib
import org.apache.spark.graphframes.graphx._

import scala.annotation.nowarn
import scala.reflect.ClassTag

/** Label Propagation algorithm. */
object LabelPropagation {

  /**
   * Run static Label Propagation for detecting communities in networks.
   *
   * Each node in the network is initially assigned to its own community. At every superstep,
   * nodes send their community affiliation to all neighbors and update their state to the mode
   * community affiliation of incoming messages.
   *
   * LPA is a standard community detection algorithm for graphs. It is very inexpensive
   * computationally, although (1) convergence is not guaranteed and (2) one can end up with
   * trivial solutions (all nodes are identified into a single community).
   *
   * @tparam ED
   *   the edge attribute type (not used in the computation)
   *
   * @param graph
   *   the graph for which to compute the community affiliation
   * @param maxSteps
   *   the number of supersteps of LPA to be performed. Because this is a static implementation,
   *   the algorithm will run for exactly this many supersteps.
   *
   * @return
   *   a graph with vertex attributes containing the label of community affiliation
   */
  def run[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[VertexId, ED] = {
    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

    val lpaGraph = graph.mapVertices { case (vid, _) => vid }
    def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Vector[Long])] = {
      Iterator((e.srcId, Vector(e.dstAttr)), (e.dstId, Vector(e.srcAttr)))
    }
    def mergeMessage(left: Vector[Long], right: Vector[Long]): Vector[Long] = left ++ right

    @nowarn
    def vertexProgram(vid: VertexId, attr: Long, message: Vector[Long]): Long = {
      if (message.isEmpty) attr
      else message.groupBy(f => f).map(f => (f._1, f._2.size)).maxBy(f => (f._2, -f._1))._1
    }
    val initialMessage = Vector[Long]()
    Pregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }
}
