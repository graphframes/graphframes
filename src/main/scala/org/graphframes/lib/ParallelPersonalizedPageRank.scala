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
import org.graphframes.{GraphFrame, Logging}

class ParallelPersonalizedPageRank private[graphframes] (
      private val graph: GraphFrame) extends Arguments {

  private var resetProb: Option[Double] = Some(0.15)
  private var numIter: Option[Int] = None
  private var srcIds: Array[Any] = Array()

  /** Source vertex for a Personalized Page Rank (optional) */
  def sources(values: Array[Any]): this.type = {
    this.srcIds = values
    this
  }

  /** Reset probability "alpha" */
  def resetProbability(value: Double): this.type = {
    resetProb = Some(value)
    this
  }

  /** Number of iterations to run */
  def numIter(value: Int): this.type = {
    this.numIter = Some(value)
    this
  }

  def run(): GraphFrame = {
    require(numIter != None, s"Number of iterations must be provided")
    require(srcIds.nonEmpty, s"Source vertices Ids must be provided")
    ParallelPersonalizedPageRank.run(graph, numIter.get, resetProb.get, srcIds)
  }
}

private object ParallelPersonalizedPageRank {

  /** Default name for the pagerank column. */
  private val PAGERANK = "pagerank"

  /** Default name for the weight column. */
  private val WEIGHT = "weight"

  /**
   * Run Personalized PageRank for a fixed number of iterations, for a
   * set of starting nodes in parallel. Returns a graph with vertex attributes
   * containing the pagerank relative to all starting nodes (as a sparse vector) and
   * edge attributes the normalized edge weight
   *
   * @param graph     The graph on which to compute personalized pagerank
   * @param numIter   The number of iterations to run
   * @param resetProb The random reset probability
   * @param sources   The list of sources to compute personalized pagerank from
   * @return the graph with vertex attributes
   *         containing the pagerank relative to all starting nodes as a vector
   *         edge attributes the normalized edge weight
   */
  def run(
      graph: GraphFrame, 
      numIter: Int, 
      resetProb: Double, 
      sources: Array[Any]): GraphFrame = {

    val longSrcIds = sources.map(GraphXConversions.integralId(graph, _))
    val gx = graphxlib.GraphXHelpers.runParallelPersonalizedPageRank(
      graph.cachedTopologyGraphX, numIter, resetProb, longSrcIds)
    GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(PAGERANK), edgeNames = Seq(WEIGHT))
  }
}
