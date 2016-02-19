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
 * Computes the number of triangles passing through each vertex.
 *
 * The edges of the resulting graph have the same schema as the original edges.
 *
 * The vertices of the resulting graph have the following schema:
 *  - id: the id of the vertex
 *  - count (int): the count of triangles
 *  - the other columns that were part of the vertex dataframe.
 *
 * The algorithm is relatively straightforward and can be computed in three steps:
 *
 * <ul>
 * <li>Compute the set of neighbors for each vertex
 * <li>For each edge compute the intersection of the sets and send the count to both vertices.
 * <li> Compute the sum at each vertex and divide by two since each triangle is counted twice.
 * </ul>
 *
 * Note that the input graph should have its edges in canonical direction
 * (i.e. the `sourceId` less than `destId`).
 */
class TriangleCount private[graphframes] (private val graph: GraphFrame) extends Arguments {

  def run(): GraphFrame = {
    TriangleCount.run(graph)
  }
}

// TODO(tjh) graphX requires the graph to be partitioned.
private object TriangleCount {

  private def run(graph: GraphFrame): GraphFrame = {
    val gx = graphxlib.TriangleCount.run(graph.cachedTopologyGraphX)
    GraphXConversions.fromGraphX(graph, gx, Seq(COUNT_ID), Nil)
  }

  private val COUNT_ID = "count"


}
