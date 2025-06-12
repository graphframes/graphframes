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
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.graphframes.WithAlgorithmChoice
import org.graphframes.WithCheckpointInterval
import org.graphframes.WithMaxIter
import org.graphframes.catalyst.GraphFramesFunctions

/**
 * Run static Label Propagation for detecting communities in networks.
 *
 * Each node in the network is initially assigned to its own community. At every iteration, nodes
 * send their community affiliation to all neighbors and update their state to the mode community
 * affiliation of incoming messages.
 *
 * LPA is a standard community detection algorithm for graphs. It is very inexpensive
 * computationally, although (1) convergence is not guaranteed and (2) one can end up with trivial
 * solutions (all nodes are identified into a single community).
 *
 * The resulting DataFrame contains all the original vertex information and one additional column:
 *   - label (`LongType`): label of community affiliation
 */
class LabelPropagation private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with WithAlgorithmChoice
    with WithCheckpointInterval
    with WithMaxIter {

  def run(): DataFrame = {
    val maxIterChecked = check(maxIter, "maxIter")
    algorithm match {
      case "graphx" => LabelPropagation.runInGraphX(graph, maxIterChecked)
      case "graphframes" =>
        LabelPropagation.runInGraphFrames(graph, maxIterChecked, checkpointInterval)
    }
  }
}

private object LabelPropagation {
  private def runInGraphX(graph: GraphFrame, maxIter: Int): DataFrame = {
    val gx = graphxlib.LabelPropagation.run(graph.cachedTopologyGraphX, maxIter)
    GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(LABEL_ID)).vertices
  }

  private def runInGraphFrames(
      graph: GraphFrame,
      maxIter: Int,
      checkpointInterval: Int,
      isDirected: Boolean = true): DataFrame = {
    // Overall:
    // - Initial labels - IDs
    // - Active vertex col (halt voting) - did the label changed?
    // - Choosing a new label - top across neighbours (tie-braking is determenistic)

    var pregel = graph.pregel
      .withVertexColumn(
        LABEL_ID,
        col(GraphFrame.ID).alias(LABEL_ID),
        GraphFramesFunctions.keyWithMaxValue(Pregel.msg))
      .setMaxIter(maxIter)
      .setStopIfAllNonActiveVertices(true)
      .setEarlyStopping(false)
      .setCheckpointInterval(checkpointInterval)
      .setSkipMessagesFromNonActiveVertices(false)
      .setUpdateActiveVertexExpression(col(LABEL_ID) =!= GraphFramesFunctions
        .keyWithMaxValue(Pregel.msg))

    if (isDirected) {
      pregel = pregel.sendMsgToDst(col(LABEL_ID))
    } else {
      pregel = pregel.sendMsgToDst(col(LABEL_ID)).sendMsgToSrc(col(LABEL_ID))
    }

    pregel = pregel.aggMsgs(
      reduce(
        collect_list(Pregel.msg),
        lit(Map.empty[Long, Int]),
        (acc, x) => map_concat(acc, map(coalesce(acc.getItem(x) + lit(1), lit(1))))))

    pregel.run()
  }

  private val LABEL_ID = "label"

}
