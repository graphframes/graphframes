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
 * Compute the strongly connected component (SCC) of each vertex and return a graph with the
 * vertex value containing the lowest vertex id in the SCC containing that vertex.
 *
 * The edges have the same schema as the original edges.
 *
 * The resulting verticexs have the following columns:
 *  - id: the id of the vertex
 *  - weight (double): the normalized weight (page rank) of this vertex.
 *
 * @param graph the graph for which to compute the SCC
 * @return a graph with vertex attributes containing the smallest vertex id in each SCC
 */
class StronglyConnectedComponents private[graphframes] (private val graph: GraphFrame) extends Arguments {
  private var numIters: Option[Int] = None

  def numIterations(iterations: Int): this.type = {
    numIters = Some(iterations)
    this
  }

  def run(): GraphFrame = {
    StronglyConnectedComponents.run(graph, check(numIters, "numIterations"))
  }
}


/** Strongly connected components algorithm implementation. */
private object StronglyConnectedComponents {
  private def run(graph: GraphFrame, numIters: Int): GraphFrame = {
    val gx = graphxlib.StronglyConnectedComponents.run(graph.cachedTopologyGraphX, numIters)
    GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(COMPONENT_ID))
  }

  private[graphframes] val COMPONENT_ID = "component"
}
