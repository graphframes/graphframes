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
 * Connected components algorithm.
 *
 * Computes the connected component membership of each vertex and return a graph with each vertex
 * assigned a component ID.
 *
 * The resulting vertices DataFrame contains one additional column:
 *  - component: (same type as vertex id) the id of some vertex in the connected component,
 *    used as a unique identifier for this component
 *
 * The resulting edges DataFrame is the same as the original edges DataFrame.
 */
class ConnectedComponents private[graphframes] (private val graph: GraphFrame) extends Arguments {

  /**
   * Runs the algorithm.
   */
  def run(): GraphFrame = {
    ConnectedComponents.run(graph)
  }
}

private object ConnectedComponents {

  def run(graph: GraphFrame): GraphFrame = {
    val gx = graphxlib.ConnectedComponents.run(graph.cachedTopologyGraphX)
    GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(COMPONENT_ID))
  }

  private val COMPONENT_ID = "component"
}