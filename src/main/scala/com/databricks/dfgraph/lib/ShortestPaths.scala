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

import scala.reflect._
import scala.reflect.runtime.universe._

import org.apache.spark.graphx.{lib => graphxlib}

import com.databricks.dfgraph.DFGraph

/**
 * Computes shortest paths to the given set of landmark vertices, returning a graph where each
 * vertex attribute is a map containing the shortest-path distance to each reachable landmark.
 */
object ShortestPaths {

  /**
   * Computes shortest paths to the given set of landmark vertices.
   *
   * @param graph the graph for which to compute the shortest paths
   * @param landmarks the list of landmark vertex ids. Shortest paths will be computed to each
   * landmark.
   *
   * @return a graph where each vertex attribute is a map containing the shortest-path distance to
   * each reachable landmark vertex.
   */
  def run[VertexId: TypeTag](graph: DFGraph, landmarks: Seq[VertexId]): DFGraph = {
    // TODO(tjh) Ugly cast for now
    val t = typeOf[VertexId]
    require(typeOf[Long] =:= t, s"Only long are supported for now, type was ${t}")
    val gx = graphxlib.ShortestPaths.run(
      graph.cachedTopologyGraphX,
      landmarks.map(_.asInstanceOf[Long])).mapVertices { case (_, m) => m.toSeq }
    GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(DISTANCE_ID))
  }

  private val DISTANCE_ID = "distance"

  class Builder private[dfgraph] (graph: DFGraph) extends Arguments {
    private var lmarks: Option[Seq[Long]] = None

    def setLandmarks[VertexType: ClassTag](landmarks: Seq[VertexType]): this.type = {
      val ct = implicitly[ClassTag[VertexType]]
      require(ct == classTag[Long], "Only long are supported for now")
      lmarks = Some(landmarks.map(_.asInstanceOf[Long]))
      this
    }

    def run(): DFGraph = {
      ShortestPaths.run(graph, check(lmarks, "landmarks"))
    }
  }
}
