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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.graphframes.GraphFramesConf
import org.graphframes.GraphFrame
import org.graphframes.Logging
import org.graphframes.WithBroadcastThreshold
import org.graphframes.WithCheckpointInterval
import org.graphframes.WithIntermediateStorageLevel
import org.graphframes.WithLocalCheckpoints
import org.graphframes.WithMaxIter
import org.graphframes.WithUseLabelsAsComponents

/**
 * Connected Components algorithm.
 *
 * Computes the connected component membership of each vertex and returns a DataFrame of vertex
 * information with each vertex assigned a component ID.
 *
 * The resulting DataFrame contains all the vertex information and one additional column:
 *   - component (`LongType`): unique ID for this component
 */
class ConnectedComponents private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with Logging
    with WithCheckpointInterval
    with WithBroadcastThreshold
    with WithIntermediateStorageLevel
    with WithUseLabelsAsComponents
    with WithMaxIter
    with WithLocalCheckpoints {

  import ConnectedComponents._

  private var algorithm: String = GraphFramesConf
    .getConnectedComponentsAlgorithm
    .getOrElse(ALGO_TWO_PHASE)

  setCheckpointInterval(
    GraphFramesConf.getConnectedComponentsCheckpointInterval.getOrElse(checkpointInterval))
  setBroadcastThreshold(
    GraphFramesConf.getConnectedComponentsBroadcastThreshold.getOrElse(broadcastThreshold))
  setIntermediateStorageLevel(
    GraphFramesConf.getConnectedComponentsStorageLevel.getOrElse(intermediateStorageLevel))
  setUseLabelsAsComponents(
    GraphFramesConf.getUseLabelsAsComponents.getOrElse(useLabelsAsComponents))
  setUseLocalCheckpoints(GraphFramesConf.getUseLocalCheckpoints.getOrElse(useLocalCheckpoints))

  /**
   * Sets the algorithm to use for computing connected components. Supported values:
   *   - [[ConnectedComponents.ALGO_GRAPHX]]: use the GraphX implementation
   *   - [[ConnectedComponents.ALGO_GRAPHFRAMES]]: deprecated alias for
   *     [[ConnectedComponents.ALGO_TWO_PHASE]]
   *   - [[ConnectedComponents.ALGO_TWO_PHASE]]: use the two-phase label propagation
   *     implementation
   *   - [[ConnectedComponents.ALGO_RANDOMIZED_CONTRACTION]]: use the randomized contraction
   *     implementation
   */
  def setAlgorithm(value: String): this.type = {
    val normalized = value.toLowerCase
    normalized match {
      case ALGO_GRAPHX | ALGO_TWO_PHASE | ALGO_RANDOMIZED_CONTRACTION =>
        algorithm = normalized
      case ALGO_GRAPHFRAMES =>
        logWarn(
          s"Algorithm '$ALGO_GRAPHFRAMES' is deprecated and will be removed in a future release. " +
            s"Using '$ALGO_TWO_PHASE' instead.")
        algorithm = ALGO_TWO_PHASE
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported algorithm: '$value'. " +
            s"Supported values are: $ALGO_GRAPHX, $ALGO_TWO_PHASE, " +
            s"$ALGO_RANDOMIZED_CONTRACTION, $ALGO_GRAPHFRAMES (deprecated).")
    }
    this
  }

  /**
   * Runs the algorithm.
   */
  def run(): DataFrame = {
    algorithm match {
      case ALGO_GRAPHX =>
        TwoPhase.runGraphX(graph, maxIter.getOrElse(Int.MaxValue))
      case ALGO_TWO_PHASE =>
        TwoPhase.run(
          graph,
          broadcastThreshold = broadcastThreshold,
          checkpointInterval = checkpointInterval,
          intermediateStorageLevel = intermediateStorageLevel,
          useLabelsAsComponents = useLabelsAsComponents,
          maxIter = maxIter,
          useLocalCheckpoints = useLocalCheckpoints)
      case ALGO_RANDOMIZED_CONTRACTION =>
        RandomizedContraction.run(
          graph,
          useLabelsAsComponents = useLabelsAsComponents,
          intermediateStorageLevel = intermediateStorageLevel,
          isGraphPrepared = false)
    }
  }
}

object ConnectedComponents extends Logging {

  private[graphframes] val COMPONENT = "component"
  private[graphframes] val ORIG_ID = "orig_id"

  val ALGO_GRAPHX = "graphx"

  /**
   * @deprecated
   *   Use [[ALGO_TWO_PHASE]] instead.
   */
  val ALGO_GRAPHFRAMES = "graphframes"

  val ALGO_TWO_PHASE = "two-phase"
  val ALGO_RANDOMIZED_CONTRACTION = "randomized_contraction"

  /**
   * Runs connected components with default parameters.
   */
  def run(graph: GraphFrame): DataFrame = {
    new ConnectedComponents(graph).run()
  }
}
