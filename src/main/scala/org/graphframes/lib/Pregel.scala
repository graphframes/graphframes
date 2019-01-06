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

import org.graphframes.GraphFrame
import org.graphframes.GraphFrame._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{array, col, explode, struct}

/**
 * Implements a Pregel-like bulk-synchronous message-passing API based on DataFrame operations.
 *
 * See <a href="https://doi.org/10.1145/1807167.1807184">Malewicz et al., Pregel: a system for large-scale graph
 * processing</a> for a detailed description of the Pregel algorithm.
 *
 * You can construct a Pregel instance using either this constructor or [[org.graphframes.GraphFrame#pregel]],
 * then use builder pattern to describe the operations, and then call [[run]] to start a run.
 * It returns a DataFrame of vertices from the last iteration.
 *
 * When a run starts, it expands the vertices DataFrame using column expressions defined by [[withVertexColumn]].
 * Those additional vertex properties can be changed during Pregel iterations.
 * In each Pregel iteration, there are three phases:
 *   - Given each edge triplet, generate messages and specify target vertices to send,
 *     described by [[sendMsgToDst]] and [[sendMsgToSrc]].
 *   - Aggregate messages by target vertex IDs, described by [[aggMsgs]].
 *   - Update additional vertex properties based on aggregated messages and states from previous iteration,
 *     described by [[withVertexColumn]].
 *
 * Please find what columns you can reference at each phase in the method API docs.
 *
 * You can control the number of iterations by [[setMaxIter]] and check API docs for advanced controls.
 *
 * Example code for Page Rank:
 *
 * {{{
 *   val edges = ...
 *   val vertices = GraphFrame.fromEdges(edges).outDegrees.cache()
 *   val numVertices = vertices.count()
 *   val graph = GraphFrame(vertices, edges)
 *   val alpha = 0.15
 *   val ranks = graph.pregel
 *     .withVertexColumn("rank", lit(1.0 / numVertices),
 *       coalesce(Pregel.msg, lit(0.0)) * (1.0 - alpha) + alpha / numVertices)
 *     .sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree"))
 *     .aggMsgs(sum(Pregel.msg))
 *     .run()
 * }}}
 *
 * @param graph The graph that Pregel will run on.
 * @see [[org.graphframes.GraphFrame#pregel]]
 * @see <a href="https://doi.org/10.1145/1807167.1807184">
 *        Malewicz et al., Pregel: a system for large-scale graph processing.
 *      </a>
 */
class Pregel(val graph: GraphFrame) {

  private val withVertexColumnList = collection.mutable.ListBuffer.empty[(String, Column, Column)]

  private var maxIter: Int = 10
  private var checkpointInterval = 2

  private var sendMsgs = collection.mutable.ListBuffer.empty[(Column, Column)]
  private var aggMsgsCol: Column = null

  private val CHECKPOINT_NAME_PREFIX = "pregel"

  /** Sets the max number of iterations (default: 10). */
  def setMaxIter(value: Int): this.type = {
    maxIter = value
    this
  }

  /**
   * Sets the number of iterations between two checkpoints (default: 2).
   *
   * This is an advanced control to balance query plan optimization and checkpoint data I/O cost.
   * In most cases, you should keep the default value.
   *
   * Checkpoint is disabled if this is set to 0.
   */
  def setCheckpointInterval(value: Int): this.type = {
    checkpointInterval = value
    this
  }

  /**
   * Defines an additional vertex column at the start of run and how to update it in each iteration.
   *
   * You can call it multiple times to add more than one additional vertex columns.
   *
   * @param colName the name of the additional vertex column.
   *                It cannot be an existing vertex column in the graph.
   * @param initialExpr the expression to initialize the additional vertex column.
   *                    You can reference all original vertex columns in this expression.
   * @param updateAfterAggMsgsExpr the expression to update the additional vertex column after messages aggregation.
   *                               You can reference all original vertex columns, additional vertex columns, and the
   *                               aggregated message column using [[Pregel$#msg]].
   *                               If the vertex received no messages, the message column would be null.
   */
  def withVertexColumn(colName: String, initialExpr: Column, updateAfterAggMsgsExpr: Column): this.type = {
    // TODO: check if this column exists.
    require(colName != null && colName != ID && colName != Pregel.MSG_COL_NAME,
      "additional column name cannot be null and cannot be the same name with ID column or " +
      "msg column.")
    require(initialExpr != null, "additional column should provide a nonnull initial expression.")
    require(updateAfterAggMsgsExpr != null, "additional column should provide a nonnull " +
      "updateAfterAggMsgs expression.")
    withVertexColumnList += Tuple3(colName, initialExpr, updateAfterAggMsgsExpr)
    this
  }

  /**
   * Defines a message to send to the source vertex of each edge triplet.
   *
   * You can call it multiple times to send more than one messages.
   *
   * @param msgExpr the expression of the message to send to the source vertex given a (src, edge, dst) triplet.
   *                Source/destination vertex properties and edge properties are nested under columns `src`, `dst`,
   *                and `edge`, respectively.
   *                You can reference them using [[Pregel$#src]], [[Pregel$#dst]], and [[Pregel$#edge]].
   *                Null messages are not included in message aggregation.
   * @see [[sendMsgToDst]]
   */
  def sendMsgToSrc(msgExpr: Column): this.type = {
    sendMsgs += Tuple2(Pregel.src(ID), msgExpr)
    this
  }

  /**
   * Defines a message to send to the destination vertex of each edge triplet.
   *
   * You can call it multiple times to send more than one messages.
   *
   * @param msgExpr the message expression to send to the destination vertex given a (`src`, `edge`, `dst`) triplet.
   *                Source/destination vertex properties and edge properties are nested under columns `src`, `dst`,
   *                and `edge`, respectively.
   *                You can reference them using [[Pregel$#src]], [[Pregel$#dst]], and [[Pregel$#edge]].
   *                Null messages are not included in message aggregation.
   * @see [[sendMsgToSrc]]
   */
  def sendMsgToDst(msgExpr: Column): this.type = {
    sendMsgs += Tuple2(Pregel.dst(ID), msgExpr)
    this
  }

  /**
   * Defines how messages are aggregated after grouped by target vertex IDs.
   *
   * @param aggExpr the message aggregation expression, such as `sum(Pregel.msg)`.
   *                You can reference the message column by [[Pregel$#msg]] and the vertex ID by [[GraphFrame$#ID]],
   *                while the latter is usually not used.
   */
  def aggMsgs(aggExpr: Column): this.type = {
    aggMsgsCol = aggExpr
    this
  }

  /**
   * Runs the defined Pregel algorithm.
   *
   * @return the result vertex DataFrame from the final iteration including both original and additional columns.
   */
  def run(): DataFrame = {
    require(sendMsgs.length > 0, "We need to set at least one message expression for pregel running.")
    require(aggMsgsCol != null, "We need to set aggMsgs for pregel running.")
    require(maxIter >= 1, "The max iteration number should be >= 1.")
    require(checkpointInterval >= 0, "The checkpoint interval should be >= 0, 0 indicates no checkpoint.")
    require(withVertexColumnList.size > 0, "There should be at least one additional vertex columns for updating.")

    val sendMsgsColList = sendMsgs.toList.map { case (id, msg) =>
      struct(id.as(ID), msg.as("msg"))
    }

    val initVertexCols = withVertexColumnList.toList.map { case (colName, initExpr, _) =>
      initExpr.as(colName)
    }
    val updateVertexCols = withVertexColumnList.toList.map { case (colName, _, updateExpr) =>
      updateExpr.as(colName)
    }

    var currentVertices = graph.vertices.select((col("*") :: initVertexCols): _*)
    var vertexUpdateColDF: DataFrame = null

    val edges = graph.edges

    var iteration = 1

    val shouldCheckpoint = checkpointInterval > 0

    while (iteration <= maxIter) {
      val tripletsDF = currentVertices.select(struct(col("*")).as(SRC))
        .join(edges.select(struct(col("*")).as(EDGE)), Pregel.src(ID) === Pregel.edge(SRC))
        .join(currentVertices.select(struct(col("*")).as(DST)), Pregel.edge(DST) === Pregel.dst(ID))

      var msgDF: DataFrame = tripletsDF
        .select(explode(array(sendMsgsColList: _*)).as("msg"))
        .select(col("msg.id"), col("msg.msg").as(Pregel.MSG_COL_NAME))

      val newAggMsgDF = msgDF
        .filter(Pregel.msg.isNotNull)
        .groupBy(ID)
        .agg(aggMsgsCol.as(Pregel.MSG_COL_NAME))

      val verticesWithMsg = currentVertices.join(newAggMsgDF, Seq(ID), "left_outer")

      var newVertexUpdateColDF = verticesWithMsg.select((col(ID) :: updateVertexCols): _*)

      if (shouldCheckpoint && iteration % checkpointInterval == 0) {
        // do checkpoint, use lazy checkpoint because later we will materialize this DF.
        newVertexUpdateColDF = newVertexUpdateColDF.checkpoint(eager = false)
        // TODO: remove last checkpoint file.
      }
      newVertexUpdateColDF.cache()
      newVertexUpdateColDF.foreachPartition(_ => {}) // materialize it

      if (vertexUpdateColDF != null) {
        vertexUpdateColDF.unpersist()
      }
      vertexUpdateColDF = newVertexUpdateColDF

      currentVertices = graph.vertices.join(vertexUpdateColDF, ID)

      iteration += 1
    }

    currentVertices
  }

}

/**
 * Constants and utilities for the Pregel algorithm.
 */
object Pregel extends Serializable {

  /**
   * A constant column name for generated and aggregated messages.
   *
   * The vertices DataFrame must not contain this column.
   */
  val MSG_COL_NAME = "_pregel_msg_"

  /**
   * References the message column in aggregating messages and updating additional vertex columns.
   *
   * @see [[Pregel.aggMsgs]] and [[Pregel.withVertexColumn]]
   */
  val msg: Column = col(MSG_COL_NAME)

  /**
   * References a source vertex column in generating messages to send.
   *
   * @param colName the vertex column name.
   * @see [[Pregel.sendMsgToSrc]] and [[Pregel.sendMsgToDst]]
   */
  def src(colName: String): Column = col(GraphFrame.SRC + "." + colName)

  /**
   * References a destination vertex column in generating messages to send.
   *
   * @param colName the vertex column name.
   * @see [[Pregel.sendMsgToSrc]] and [[Pregel.sendMsgToDst]]
   */
  def dst(colName: String): Column = col(GraphFrame.DST + "." + colName)

  /**
   * References an edge column in generating messages to send.
   *
   * @param colName the edge column name.
   * @see [[Pregel.sendMsgToSrc]] and [[Pregel.sendMsgToDst]]
   */
  def edge(colName: String): Column = col(GraphFrame.EDGE + "." + colName)
}
