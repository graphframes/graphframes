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
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame

/**
 * Compute the strongly connected component (SCC) of each vertex and return a DataFrame with each
 * vertex assigned to the SCC containing that vertex.
 *
 * The resulting DataFrame contains all the original vertex information and one additional column:
 *  - component (`LongType`): unique ID for this component
 */
class StronglyConnectedComponents private[graphframes] (private val graph: GraphFrame)
  extends Arguments {

  private var maxIter: Option[Int] = None
  private var intermediateVertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  private var intermediateEdgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY

  /**
    * Sets storage level for intermediate `Graph` vertices that require multiple passes (default: ``MEMORY_ONLY``).
    */
  def setIntermediateVertexStorageLevel(value: StorageLevel): this.type = {
    intermediateVertexStorageLevel = value
    this
  }

  /**
    * Gets storage level for intermediate `Graph` vertices that require multiple passes.
    */
  def getIntermediateVertexStorageLevel: StorageLevel = intermediateVertexStorageLevel

  /**
    * Sets storage level for intermediate `Graph` edges that require multiple passes (default: ``MEMORY_ONLY``).
    */
  def setIntermediateEdgeStorageLevel(value: StorageLevel): this.type = {
    intermediateEdgeStorageLevel = value
    this
  }

  /**
    * Gets storage level for intermediate `Graph` edges that require multiple passes.
    */
  def getIntermediateEdgeStorageLevel: StorageLevel = intermediateEdgeStorageLevel

  def maxIter(value: Int): this.type = {
    maxIter = Some(value)
    this
  }

  def run(): DataFrame = {
    StronglyConnectedComponents.run(graph, check(maxIter, "maxIter"), intermediateVertexStorageLevel, intermediateEdgeStorageLevel)
  }
}


/** Strongly connected components algorithm implementation. */
private object StronglyConnectedComponents {
  private def run(
      graph: GraphFrame,
      numIter: Int,
      intermediateVertexStorageLevel: StorageLevel,
      intermediateEdgeStorageLevel: StorageLevel): DataFrame = {
    val gx = graphxlib.StronglyConnectedComponents.run(
      graph.cachedTopologyGraphXWithStorageLevel(intermediateVertexStorageLevel, intermediateEdgeStorageLevel),
      numIter)
    GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(COMPONENT_ID)).vertices
  }

  private[graphframes] val COMPONENT_ID = "component"
}
