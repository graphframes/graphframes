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

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.struct
import org.graphframes.GraphFrame
import org.graphframes.GraphFrame._
import org.graphframes.Logging

import java.io.IOException
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

/**
 * Implements a Pregel-like bulk-synchronous message-passing API based on DataFrame operations.
 *
 * See <a href="https://doi.org/10.1145/1807167.1807184">Malewicz et al., Pregel: a system for
 * large-scale graph processing</a> for a detailed description of the Pregel algorithm.
 *
 * You can construct a Pregel instance using either this constructor or
 * [[org.graphframes.GraphFrame#pregel]], then use builder pattern to describe the operations, and
 * then call [[run]] to start a run. It returns a DataFrame of vertices from the last iteration.
 *
 * When a run starts, it expands the vertices DataFrame using column expressions defined by
 * [[withVertexColumn]]. Those additional vertex properties can be changed during Pregel
 * iterations. In each Pregel iteration, there are three phases:
 *   - Given each edge triplet, generate messages and specify target vertices to send, described
 *     by [[sendMsgToDst]] and [[sendMsgToSrc]].
 *   - Aggregate messages by target vertex IDs, described by [[aggMsgs]].
 *   - Update additional vertex properties based on aggregated messages and states from previous
 *     iteration, described by [[withVertexColumn]].
 *
 * Please find what columns you can reference at each phase in the method API docs.
 *
 * You can control the number of iterations by [[setMaxIter]] and check API docs for advanced
 * controls.
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
 * @param graph
 *   The graph that Pregel will run on.
 * @see
 *   [[org.graphframes.GraphFrame#pregel]]
 * @see
 *   <a href="https://doi.org/10.1145/1807167.1807184"> Malewicz et al., Pregel: a system for
 *   large-scale graph processing. </a>
 */
class Pregel(val graph: GraphFrame) extends Logging {

  private val withVertexColumnList = collection.mutable.ListBuffer.empty[(String, Column, Column)]

  private var maxIter: Int = 10
  private var checkpointInterval = 2
  private var earlyStopping = false
  private var stopIfAllNonActiveVertices = false
  private var skipMessagesFromNonActiveVertices = false
  private var initialActiveVertexExpression = lit(true)
  private var updateActiveVertexExpression = lit(true)

  private val sendMsgs = collection.mutable.ListBuffer.empty[(Column, Column)]
  private var aggMsgsCol: Column = null

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
   * Should Pregel stop earlier in case of no new messages to send?
   *
   * Early stopping allows to terminate Pregel before reaching maxIter by checking is there any
   * non-null message or not. While in some cases it may gain significant performance boost, it
   * other cases it can tend to performance degradation, because checking is messages DataFrame is
   * empty or not is an action and requires materialization of the Spark Plan with some additional
   * computations.
   *
   * In the case when user can assume a good value of maxIter it is recommended to leave this
   * value to the default "false". In the case when it is hard to estimate an amount of iterations
   * required for convergence, it is recommended to set this value to "false" to avoid iterating
   * over convergence until reaching maxIter. When this value is "true", maxIter can be set to a
   * bigger value without risks.
   *
   * @param value
   *   should Pregel checks for the termination condition on each step
   * @return
   */
  def setEarlyStopping(value: Boolean): this.type = {
    earlyStopping = value
    this
  }

  /**
   * Should Pregel stop earlier in case all the vertices are marked as non active.
   *
   * This feature allows to terminate Pregel before reaching maxIter by checking are there active
   * vertex left. A good example of activity check is PageRank: (see Malewicz, Grzegorz, et al.
   * "Pregel: a system for large-scale graph processing." Proceedings of the 2010 ACM SIGMOD
   * International Conference on Management of data. 2010., a part about voting to halt)
   *   - after each iteration we are checking is the change in rank less than tolerance and if so,
   *     we can mark vertex as non active
   *   - if all the vertices are non active, we stop iterations
   * @param value
   *   should Pregel stop earlier by vertices voting
   * @return
   */
  def setStopIfAllNonActiveVertices(value: Boolean): this.type = {
    stopIfAllNonActiveVertices = value
    this
  }

  /**
   * Set the initial expression for the active/non-active flag per vertex.
   *
   * In most of the cases the default expression (true for all the vertices) should works fine.
   * For some cases it makes sense to set a custom expression. A good example is
   * multiple-landmarks shortest-paths algorithm:
   *   - the only initially active vertices in that case should be landmarkds, because only this
   *     vertices initially have non-null distances but all the other vertices have null distances
   *     and there is no reason to mark them active initially.
   * @param expression
   *   an initial expression that will be used to create an active-flag vertex column
   * @return
   */
  def setInitialActiveVertexExpression(expression: Column): this.type = {
    initialActiveVertexExpression = expression
    this
  }

  /**
   * Set an expression that will be used after each superstep to update the active-flag vertex
   * column.
   *
   * An example is PageRank algorithm: in that case such an expression may looks like abs(old_rank -
   * new_rank) >= tolerance
   *
   * @param expression
   *   an expression, that will be used after each superstep to update the active-flag vertex
   *   column
   * @return
   */
  def setUpdateActiveVertexExpression(expression: Column): this.type = {
    updateActiveVertexExpression = expression
    this
  }

  /**
   * With a true value, Pregel will not generate messages from vertices, marked as non active.
   *
   * For example, for Shortest Paths, there is no reason to pass distances from vertices, for that
   * these distances did not change at the latest iteration. It allows significantly reduce an
   * amount of generated messages.
   *
   * Be careful, for algorithms like Label Propagation or Pregel, even if the vertex is not
   * active, we still need to generate messages, otherwise algorithm will return an incorrect
   * result!
   *
   * @param value
   *   should Pregel skip generation of messages for non active vertices.
   * @return
   */
  def setSkipMessagesFromNonActiveVertices(value: Boolean): this.type = {
    skipMessagesFromNonActiveVertices = value
    this
  }

  /**
   * Defines an additional vertex column at the start of run and how to update it in each
   * iteration.
   *
   * You can call it multiple times to add more than one additional vertex columns.
   *
   * @param colName
   *   the name of the additional vertex column. It cannot be an existing vertex column in the
   *   graph.
   * @param initialExpr
   *   the expression to initialize the additional vertex column. You can reference all original
   *   vertex columns in this expression.
   * @param updateAfterAggMsgsExpr
   *   the expression to update the additional vertex column after messages aggregation. You can
   *   reference all original vertex columns, additional vertex columns, and the aggregated
   *   message column using [[Pregel$#msg]]. If the vertex received no messages, the message
   *   column would be null.
   */
  def withVertexColumn(
      colName: String,
      initialExpr: Column,
      updateAfterAggMsgsExpr: Column): this.type = {
    // TODO: check if this column exists.
    require(
      colName != null && colName != ID && colName != Pregel.MSG_COL_NAME,
      "additional column name cannot be null and cannot be the same name with ID column or " +
        "msg column.")
    require(initialExpr != null, "additional column should provide a nonnull initial expression.")
    require(
      updateAfterAggMsgsExpr != null,
      "additional column should provide a nonnull " +
        "updateAfterAggMsgs expression.")
    withVertexColumnList += Tuple3(colName, initialExpr, updateAfterAggMsgsExpr)
    this
  }

  /**
   * Defines a message to send to the source vertex of each edge triplet.
   *
   * You can call it multiple times to send more than one messages.
   *
   * @param msgExpr
   *   the expression of the message to send to the source vertex given a (src, edge, dst)
   *   triplet. Source/destination vertex properties and edge properties are nested under columns
   *   `src`, `dst`, and `edge`, respectively. You can reference them using [[Pregel$#src]],
   *   [[Pregel$#dst]], and [[Pregel$#edge]]. Null messages are not included in message
   *   aggregation.
   * @see
   *   [[sendMsgToDst]]
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
   * @param msgExpr
   *   the message expression to send to the destination vertex given a (`src`, `edge`, `dst`)
   *   triplet. Source/destination vertex properties and edge properties are nested under columns
   *   `src`, `dst`, and `edge`, respectively. You can reference them using [[Pregel$#src]],
   *   [[Pregel$#dst]], and [[Pregel$#edge]]. Null messages are not included in message
   *   aggregation.
   * @see
   *   [[sendMsgToSrc]]
   */
  def sendMsgToDst(msgExpr: Column): this.type = {
    sendMsgs += Tuple2(Pregel.dst(ID), msgExpr)
    this
  }

  /**
   * Defines how messages are aggregated after grouped by target vertex IDs.
   *
   * @param aggExpr
   *   the message aggregation expression, such as `sum(Pregel.msg)`. You can reference the
   *   message column by [[Pregel$#msg]] and the vertex ID by [[GraphFrame$#ID]], while the latter
   *   is usually not used.
   */
  def aggMsgs(aggExpr: Column): this.type = {
    aggMsgsCol = aggExpr
    this
  }

  /**
   * Runs the defined Pregel algorithm.
   *
   * @return
   *   the result vertex DataFrame from the final iteration including both original and additional
   *   columns.
   */
  def run(): DataFrame = {
    require(
      sendMsgs.length > 0,
      "We need to set at least one message expression for pregel running.")
    require(aggMsgsCol != null, "We need to set aggMsgs for pregel running.")
    require(maxIter >= 1, "The max iteration number should be >= 1.")
    require(
      checkpointInterval >= 0,
      "The checkpoint interval should be >= 0, 0 indicates no checkpoint.")
    require(
      withVertexColumnList.size > 0,
      "There should be at least one additional vertex columns for updating.")

    val sendMsgsColList = sendMsgs.toList.map { case (id, msg) =>
      struct(id.as(ID), msg.as("msg"))
    }

    val initVertexCols = withVertexColumnList.toList.map { case (colName, initExpr, _) =>
      initExpr.as(colName)
    }
    val updateVertexCols = withVertexColumnList.toList.map { case (colName, _, updateExpr) =>
      updateExpr.as(colName)
    }

    var currentVertices = graph.vertices.select(
      (Seq(
        col("*"),
        initialActiveVertexExpression.alias(Pregel.ACTIVE_FLAG_COL)) ++ initVertexCols): _*)
    var vertexUpdateColDF: DataFrame = null

    val edges = graph.edges

    var iteration = 1

    val shouldCheckpoint = checkpointInterval > 0

    if (shouldCheckpoint && graph.spark.sparkContext.getCheckpointDir.isEmpty) {
      // Spark Connect workaround
      graph.spark.conf.getOption("spark.checkpoint.dir") match {
        case Some(d) => graph.spark.sparkContext.setCheckpointDir(d)
        case None =>
          throw new IOException(
            "Checkpoint directory is not set. Please set it first using sc.setCheckpointDir()" +
              "or by specifying the conf 'spark.checkpoint.dir'.")
      }
    }

    breakable {
      while (iteration <= maxIter) {
        logInfo(s"start Pregel iteration $iteration / $maxIter")
        var tripletsDF = currentVertices
          .select(struct(col("*")).as(SRC))
          .join(edges.select(struct(col("*")).as(EDGE)), Pregel.src(ID) === Pregel.edge(SRC))
          .join(
            currentVertices.select(struct(col("*")).as(DST)),
            Pregel.edge(DST) === Pregel.dst(ID))

        if (skipMessagesFromNonActiveVertices) {
          tripletsDF = tripletsDF.filter(
            Pregel.src(Pregel.ACTIVE_FLAG_COL) || Pregel.dst(Pregel.ACTIVE_FLAG_COL))
        }

        val msgDF: DataFrame = tripletsDF
          .select(explode(array(sendMsgsColList: _*)).as("msg"))
          .select(col("msg.id"), col("msg.msg").as(Pregel.MSG_COL_NAME))
          .filter(Pregel.msg.isNotNull)

        if (earlyStopping && msgDF.isEmpty) {
          logInfo(
            s"there are no more non-null messages; Pregel stops earlier at iteration $iteration")
          if (vertexUpdateColDF != null) {
            vertexUpdateColDF.unpersist()
          }
          break()
        }

        val newAggMsgDF = msgDF
          .groupBy(ID)
          .agg(aggMsgsCol.as(Pregel.MSG_COL_NAME))

        val verticesWithMsg = currentVertices.join(newAggMsgDF, Seq(ID), "left_outer")

        var newVertexUpdateColDF = verticesWithMsg.select(
          (Seq(
            col(ID),
            updateActiveVertexExpression.alias(Pregel.ACTIVE_FLAG_COL)) ++ updateVertexCols): _*)

        if (shouldCheckpoint && iteration % checkpointInterval == 0) {
          // do checkpoint, use lazy checkpoint because later we will materialize this DF.
          newVertexUpdateColDF = newVertexUpdateColDF.checkpoint(eager = false)
          // TODO: remove last checkpoint file.
        }
        newVertexUpdateColDF.cache()
        newVertexUpdateColDF.count() // materialize it

        if (vertexUpdateColDF != null) {
          vertexUpdateColDF.unpersist()
        }
        vertexUpdateColDF = newVertexUpdateColDF

        currentVertices = graph.vertices.join(vertexUpdateColDF, ID)

        if (stopIfAllNonActiveVertices) {
          if (currentVertices.filter(col(Pregel.ACTIVE_FLAG_COL)).isEmpty) {
            logInfo(
              s"all the verties are non-active; Pregel stops earlier at iteration $iteration")
            break()
          }
        }

        iteration += 1
      }
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
   * A constant column name for active vertex flag.
   */
  val ACTIVE_FLAG_COL = "_pregel_is_active"

  /**
   * References the message column in aggregating messages and updating additional vertex columns.
   *
   * @see
   *   [[Pregel.aggMsgs]] and [[Pregel.withVertexColumn]]
   */
  val msg: Column = col(MSG_COL_NAME)

  /**
   * References a source vertex column in generating messages to send.
   *
   * @param colName
   *   the vertex column name.
   * @see
   *   [[Pregel.sendMsgToSrc]] and [[Pregel.sendMsgToDst]]
   */
  def src(colName: String): Column = col(GraphFrame.SRC + "." + colName)

  /**
   * References a destination vertex column in generating messages to send.
   *
   * @param colName
   *   the vertex column name.
   * @see
   *   [[Pregel.sendMsgToSrc]] and [[Pregel.sendMsgToDst]]
   */
  def dst(colName: String): Column = col(GraphFrame.DST + "." + colName)

  /**
   * References an edge column in generating messages to send.
   *
   * @param colName
   *   the edge column name.
   * @see
   *   [[Pregel.sendMsgToSrc]] and [[Pregel.sendMsgToDst]]
   */
  def edge(colName: String): Column = col(GraphFrame.EDGE + "." + colName)
}
