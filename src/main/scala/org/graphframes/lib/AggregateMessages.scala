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

import org.apache.spark.Logging
import org.apache.spark.sql.SQLHelpers.expr
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

import org.graphframes.GraphFrame


/**
 * This is a primitive for implementing graph algorithms.
 * This method aggregates messages from the neighboring edges and vertices of each vertex.
 *
 * For each triplet (source vertex, edge, destination vertex) in [[GraphFrame.triplets]],
 * this can send a message to the source and/or destination vertices.
 *  - [[AggregateMessages.Builder.sendToSrc()]] sends a message to the source vertex of each
 *    triplet
 *  - [[AggregateMessages.Builder.sendToDst()]] sends a message to the destination vertex of each
 *    triplet
 *  - [[AggregateMessages.Builder.agg()]] specifies an aggregation function for aggregating the
 *    messages sent to each vertex.  It also runs the aggregation, computing a DataFrame
 *    with one row for each vertex which receives > 0 messages.  The DataFrame has 2 columns:
 *     - vertex column ID (named [[GraphFrame.ID]])
 *     - aggregate from messages sent to vertex (with the name given to the [[Column]] specified
 *       in [[AggregateMessages.Builder.agg()]])
 *
 * When specifying the messages and aggregation function, the user may reference columns using:
 *  - [[AggregateMessages.src]]: column for source vertex of edge
 *  - [[AggregateMessages.edge]]: column for edge
 *  - [[AggregateMessages.dst]]: column for destination vertex of edge
 *  - [[AggregateMessages.msg]]: message sent to vertex (for aggregation function)
 *
 * @example We can use this function to compute the in-degree of each vertex
 * {{{
 * val g: GraphFrame = Graph.textFile("twittergraph")
 * val inDeg: DataFrame =
 *   g.aggregateMessages().sendToDst(lit(1)).agg(sum(AggregateMessages.msg))
 * }}}
 */
object AggregateMessages extends Logging with Serializable {

  class Builder private[graphframes] (g: GraphFrame)
    extends Arguments with Serializable {

    private var msgToSrc: Option[Column] = None

    /** Send message to source vertex */
    def sendToSrc(value: Column): this.type = {
      msgToSrc = Some(value)
      this
    }

    /** Send message to source vertex, specifying SQL expression as a String */
    def sendToSrc(value: String): this.type = sendToSrc(expr(value))

    private var msgToDst: Option[Column] = None

    /** Send message to destination vertex */
    def sendToDst(value: Column): this.type = {
      msgToDst = Some(value)
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
     * If you need to join this with the original [[GraphFrame.vertices]], you can run an inner
     * join of the form:
     * {{{
     *   val g: GraphFrame = ...
     *   val aggResult = g.aggregateMessages.sendToSrc(msg).agg(aggFunc)
     *   aggResult.join(g.vertices, ID)
     * }}}
     */
    def agg(aggCol: Column): DataFrame = {
      require(msgToSrc.nonEmpty || msgToDst.nonEmpty, s"To run GraphFrame.aggregateMessages," +
        s" messages must be sent to src, dst, or both.  Set using sendToSrc(), sendToDst().")
      val triplets = g.triplets
      import org.graphframes.GraphFrame.{DST, ID, SRC}
      val sentMsgsToSrc = msgToSrc.map { msg =>
        val msgsToSrc = triplets.select(msg.as("MSG"), triplets(SRC)(ID).as(ID))
        // Inner join: only send messages to vertices with edges
        msgsToSrc.join(g.vertices, ID)
          .select(msgsToSrc("MSG"), col(ID))
      }
      val sentMsgsToDst = msgToDst.map { msg =>
        val msgsToDst = triplets.select(msg.as("MSG"), triplets(DST)(ID).as(ID))
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
      unionedMsgs.count()
      println("AGGREGATE MESSAGES: unionedMsgs")
      unionedMsgs.show()
      unionedMsgs.groupBy(ID).agg(aggCol)
    }
  }

  /** Reference for source column, used for specifying messages */
  def src: Column = col(GraphFrame.SRC)

  /** Reference for destination column, used for specifying messages */
  def dst: Column = col(GraphFrame.DST)

  /** Reference for edge column, used for specifying messages */
  def edge: Column = col(GraphFrame.EDGE)

  /** Reference for message column, used for specifying aggregation function */
  def msg: Column = col("MSG")
}
