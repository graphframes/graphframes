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

import org.apache.spark.graphx.{lib => graphxlib}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import com.databricks.dfgraph.DFGraph

/**
 * Compute the number of triangles passing through each vertex.
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
// TODO(tjh) graphX requires the graph to be partitioned.
object TriangleCount {

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
   * @param graph
   * @return
   */
  def run(graph: DFGraph): DFGraph = {
    val gx = graphxlib.TriangleCount.run(graph.cachedTopologyGraphX)
    GraphXConversions.fromGraphX(graph, gx, Seq(COUNT_ID), Nil)
//
//      .mapVertices { case (vid, vlabel) =>
//        Row(vid, vlabel)
//      }
//
//    val s = graph.vertices.schema(DFGraph.ID)
//    val vStruct = StructType(List(
//      s.copy(name = DFGraph.LONG_ID),
//      StructField(COUNT_ID, IntegerType, nullable = false)))
//    GraphXConversions.fromRowGraphX(graph, gx, graph.edges.schema, vStruct)
  }

  private val COUNT_ID = "count"
}
