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

import org.apache.spark.sql.SQLHelpers.expr
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.graphframes.{GraphFrame, Logging}

/**
 * This is a primitive for implementing graph algorithms.
 * This method aggregates messages from the neighboring edges and vertices of each vertex.
 *
 * For each triplet (source vertex, edge, destination vertex) in [[GraphFrame.triplets]],
 * this can send a message to the source and/or destination vertices.
 *  - `AggregateMessages.sendToSrc()` sends a message to the source vertex of each
 *    triplet
 *  - `AggregateMessages.sendToDst()` sends a message to the destination vertex of each
 *    triplet
 *  - `AggregateMessages.agg` specifies a series of aggregation functions for aggregating the
 *    messages sent to each vertex.  It also runs the aggregations, computing a DataFrame
 *    with one row for each vertex which receives > 0 messages.  The DataFrame has the following
 *    columns:
 *     - vertex column ID (named [[GraphFrame.ID]])
 *     - each aggregate from messages sent to vertex (with the names given to the `Column` specified
 *       in `AggregateMessages.agg()`)
 *
 * When specifying the messages and aggregation functions, the user may reference columns using:
 *  - [[AggregateMessages.src]]: column for source vertex of edge
 *  - [[AggregateMessages.edge]]: column for edge
 *  - [[AggregateMessages.dst]]: column for destination vertex of edge
 *  - [[AggregateMessages.msg]]: message sent to vertex (for aggregation function)
 *
 * Note: If you use this operation to write an iterative algorithm, you may want to use
 * [[AggregateMessages$.getCachedDataFrame getCachedDataFrame()]] as a workaround for caching
 * issues.
 *
 * @example We can use this function to compute the in-degree of each vertex
 * {{{
 * val g: GraphFrame = Graph.textFile("twittergraph")
 * val inDeg: DataFrame =
 *   g.aggregateMessages().sendToDst(lit(1)).agg(sum(AggregateMessagesBuilder.msg))
 * }}}
 */
class AggregateMessages private[graphframes] (private val g: GraphFrame)
  extends Arguments with Serializable {

  import org.graphframes.GraphFrame.{DST, ID, SRC}

  private var msgToSrc: Seq[Column] = Vector()

  /** Send message to source vertex */
  def sendToSrc(value: Column): this.type = {
    msgToSrc :+= value
    this
  }

  /** Send message to source vertex, specifying SQL expression as a String */
  def sendToSrc(value: String): this.type = sendToSrc(expr(value))

  private var msgToDst: Seq[Column] = Vector()

  /** Send message to destination vertex */
  def sendToDst(value: Column): this.type = {
    msgToDst :+= value
    this
  }

  /** Send message to destination vertex, specifying SQL expression as a String */
  def sendToDst(value: String): this.type = sendToDst(expr(value))

  /**
   * Run the aggregation, returning the resulting DataFrame of aggregated messages.
   * This is a lazy operation, so the DataFrame will not be materialized until an action is
   * executed on it.
   *
   * This returns a DataFrame with schema:
   *  - column "id": vertex ID
   *  - aggCol: aggregate result
   *  - aggCols: one column with the result of each additional defined aggregation
   * If you need to join this with the original [[GraphFrame.vertices]], you can run an inner
   * join of the form:
   * {{{
   *   val g: GraphFrame = ...
   *   val aggResult = g.AggregateMessagesBuilder.sendToSrc(msg).agg(aggFunc)
   *   aggResult.join(g.vertices, ID)
   * }}}
   */
  def agg(aggCol: Column, aggCols: Column*): DataFrame = {
    def removeColumnNamePrefix(columnName: String) = columnName match {
      case cn if cn.startsWith(s"${GraphFrame.SRC}.") => cn.substring(s"${GraphFrame.SRC}.".length)
      case cn if cn.startsWith(s"${GraphFrame.DST}.") => cn.substring(s"${GraphFrame.DST}.".length)
      case cn if cn.startsWith(s"${GraphFrame.EDGE}.") => cn.substring(s"${GraphFrame.EDGE}.".length)
      case cn  => cn
    }
    def msgColumn(df: DataFrame) = df.columns.filter(_ != ID) match {
      case Array(c) => df.withColumnRenamed(c, "MSG")
      case columns => df.select(
        df(ID),
        struct(columns.map(c => df(s"`${c}`").as(removeColumnNamePrefix(c))) :_*).as("MSG"))
    }
    require(msgToSrc.nonEmpty || msgToDst.nonEmpty, s"To run GraphFrame.aggregateMessages," +
      s" messages must be sent to src, dst, or both.  Set using sendToSrc(), sendToDst().")
    val triplets = g.triplets
    val sentMsgsToSrc = msgToSrc.headOption.map { _ =>
      val msgsToSrc = msgColumn(triplets.select((msgToSrc :+ triplets(SRC)(ID).as(ID)): _*))
      // Inner join: only send messages to vertices with edges
      msgsToSrc.join(g.vertices, ID)
        .select(msgsToSrc("MSG"), col(ID))
    }
    val sentMsgsToDst = msgToDst.headOption.map { _ =>
      val msgsToDst = msgColumn(triplets.select((msgToDst :+ triplets(DST)(ID).as(ID)): _*))
      msgsToDst.join(g.vertices, ID)
        .select(msgsToDst("MSG"), col(ID))
    }
    val unionedMsgs = (sentMsgsToSrc, sentMsgsToDst) match {
      case (Some(toSrc), Some(toDst)) =>
        toSrc.unionAll(toDst)
      case (Some(toSrc), None) => toSrc
      case (None, Some(toDst)) => toDst
      case _ =>
        // Should never happen. Specify this case to avoid compilation warnings.
        throw new RuntimeException("AggregateMessages: No messages were specified to be sent.")
    }
    unionedMsgs.groupBy(ID).agg(aggCol, aggCols:_*)
  }
}

object AggregateMessages extends Logging with Serializable {

  /** Reference for source column, used for specifying messages */
  def src: Column = col(GraphFrame.SRC)

  /** Reference for destination column, used for specifying messages */
  def dst: Column = col(GraphFrame.DST)

  /** Reference for edge column, used for specifying messages */
  def edge: Column = col(GraphFrame.EDGE)

  /** Reference for message column, used for specifying aggregation function */
  def msg: Column = col("MSG")

  /**
   * Cache a DataFrame, and returned the cached version. For iterative DataFrame-based algorithms.
   *
   * WARNING: This is NOT the same as `DataFrame.cache()`.
   *          The original DataFrame will NOT be cached.
   *
   * This is a workaround for SPARK-13346, which makes it difficult to use DataFrames in iterative
   * algorithms.  This workaround converts the DataFrame to an RDD, caches the RDD, and creates
   * a new DataFrame.  This is important for avoiding the creation of extremely complex DataFrame
   * query plans when using DataFrames in iterative algorithms.
   */
  def getCachedDataFrame(df: DataFrame): DataFrame = {
    val rdd = df.rdd.cache()
    // rdd.count()
    df.sqlContext.createDataFrame(rdd, df.schema)
  }
}
