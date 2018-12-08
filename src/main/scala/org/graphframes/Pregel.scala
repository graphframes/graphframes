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

package org.graphframes

import org.graphframes.GraphFrame._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{array, col, explode, struct, when}

/**
 * This class implement pregel on GraphFrame.
 *
 * We can get the Pregel instance by `graphFrame.pregel`, or construct it via a `graph`
 * argument. and call a series of methods, then call method `run` to start pregel.
 * It will return a DataFrame which is the vertices dataframe generated in the last round.
 *
 * When pregel `run` start, first, it will initialize some columns in vertices dataframe,
 * which defined by `withVertexColumn` (user can call it multiple times),
 * and then start iteration.
 *
 * Once the iteration start, in each supersteps of pregel, include 3 phases:
 * phase-1) generate the `triplets` dataframe, and generate the “msg” to send.
 * The target vertex to send and the msg is set via `sendMsg` method.
 * phase-2) Do msg aggregation. It will use the aggregation column which is set via
 * `aggMsgs` method. Each vertex aggregates those messages which it receives.
 * Now vertices dataframe owns a new column which value is the aggregated msgs
 * (received by each vertex). Now update vertex property columns, the update expressions
 * is set by `updateVertexColumn` method and return the new vertices dataframe.
 *
 * The pregel iteration will run `maxIter` time, which can be set via `setMaxIter` method.
 *
 * @param graph The graph which pregel will run on.
 */
class Pregel(val graph: GraphFrame) {

  private val withVertexColumnList = collection.mutable.ListBuffer.empty[(String, Column, Column)]

  private var maxIter: Int = 10
  private var checkpointInterval = 2

  private var sendMsgs = collection.mutable.ListBuffer.empty[(Column, Column)]
  private var aggMsgsCol: Column = null

  private val CHECKPOINT_NAME_PREFIX = "pregel"

  /** Set max iteration number for the pregel running. Default value is 10 */
  def setMaxIter(value: Int): this.type = {
    maxIter = value
    this
  }

  /**
   * Set the period to do the checkpoint when running pregel.
   * If set to zero or negative value, then do not checkpoint.
   * Default value is 2
   */
  def setCheckpointInterval(value: Int): this.type = {
    checkpointInterval = value
    this
  }

  /**
   * Use this method to set those vertex columns which will be initialized before
   * pregel rounds start, and these columns will be updated after vertex receive
   * aggregated messages in each round.
   *
   * @param colName the column name of initialized column in vertex Dataframe
   * @param initialExpr The column expression used to initialize the column.
   * @param updateAfterAggMsgsExpr The column expression used to update the column.
   *                               Note that this sql expression can reference all
   *                               vertex columns and an extra message column
   *                               `Pregel.msg`. If the vertex receive no messages,
   *                               The msg column will be null, otherwise will be
   *                               the aggregated result of all received messages.
   */
  def withVertexColumn(colName: String, initialExpr: Column,
                       updateAfterAggMsgsExpr: Column): this.type = {
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
   * Set the message column. In each round of pregel, each triplet
   * (src-edge-dst) will generate zero or one message and the message will
   * be sent to the src vertex of this triplet.
   *
   * @param msgExpr The message expression. It is a sql expression and it
   *                can reference all propertis in the triplet, in the way
   *                `Pregel.src("src_col_name)`, `Pregel.edge("edge_col_name)`,
   *                `Pregel.dst("dst_col_name)`. If `msgExpr` is null, pregel
   *                will not send message.
   */
  def sendMsgToSrc(msgExpr: Column): this.type = {
    sendMsgs += Tuple2(Pregel.src(ID), msgExpr)
    this
  }


  /**
   * Set the message column. In each round of pregel, each triplet
   * (src-edge-dst) will generate zero or one message and the message will
   * be sent to the dst vertex of this triplet.
   *
   * @param msgExpr The message expression. It is a sql expression and it
   *                can reference all propertis in the triplet, in the way
   *                `Pregel.src("src_col_name)`, `Pregel.edge("edge_col_name)`,
   *                `Pregel.dst("dst_col_name)`. If `msgExpr` is null, pregel
   *                will not send message.
   */
  def sendMsgToDst(msgExpr: Column): this.type = {
    sendMsgs += Tuple2(Pregel.dst(ID), msgExpr)
    this
  }

  /**
   * Set the aggregation expression which used to aggregate messages which received
   * by each vertex.
   *
   * @param aggExpr The aggregation expression, such as `sum(Pregel.msgCol)`
   */
  def aggMsgs(aggExpr: Column): this.type = {
    aggMsgsCol = aggExpr
    this
  }

  /**
   * After set a series of things via above methods, then call this method to run
   * pregel, and it will return the result vertex dataframe, which will include all
   * updated columns in the final rounds of pregel.
   *
   * @return the result vertex dataframe
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
        // do checkpoint
        newVertexUpdateColDF = newVertexUpdateColDF.checkpoint()
        // TODO: remove last checkpoint file.
      }
      newVertexUpdateColDF.cache()

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


object Pregel extends Serializable {

  val MSG_COL_NAME = "_pregel_msg_"

  /**
   * The message column. `Pregel.aggMsgs` method argument and `Pregel.updateVertexColumn`
   * argument `col` can reference this message column.
   */
  val msg: Column = col(MSG_COL_NAME)

  /**
   * construct the column from src vertex columns.
   * This column can only be used in the message sql expression.
   *
   * @param colName the column name in the vertex columns.
   */
  def src(colName: String): Column = col(GraphFrame.SRC + "." + colName)

  /**
   * construct the column from dst vertex columns.
   * This column can only be used in the message sql expression.
   *
   * @param colName the column name in the vertex columns.
   */
  def dst(colName: String): Column = col(GraphFrame.DST + "." + colName)

  /**
   * construct the column from edge columns.
   * This column can only be used in the message sql expression.
   *
   * @param colName the column name in the edge columns.
   */
  def edge(colName: String): Column = col(GraphFrame.EDGE + "." + colName)
}
