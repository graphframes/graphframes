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
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.size
import org.apache.spark.sql.graphframes.SparkShims
import org.graphframes.GraphFrame
import org.graphframes.WithDirection

/**
 * Computes all simple paths between source and destination vertices.
 *
 * This algorithm enumerates paths up to `maxPathLength` hops. It supports directed and undirected
 * traversal as well as optional edge filtering.
 *
 * Returned DataFrame schema:
 *   - `path`: array of vertex ids in traversal order
 *   - `len`: number of edges in the path (Long)
 */
class AllPaths private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with Serializable
    with WithDirection {

  private var maxPathLength: Int = 10
  private var fromExpression: Column = _
  private var toExpression: Column = _
  private var edgeFilterExpression: Option[Column] = None

  def fromExpr(value: Column): this.type = {
    fromExpression = value
    this
  }

  def fromExpr(value: String): this.type = fromExpr(expr(value))

  def toExpr(value: Column): this.type = {
    toExpression = value
    this
  }

  def toExpr(value: String): this.type = toExpr(expr(value))

  def maxPathLength(value: Int): this.type = {
    require(value > 0, s"AllPaths maxPathLength must be > 0, but was set to $value")
    maxPathLength = value
    this
  }

  def edgeFilter(value: Column): this.type = {
    edgeFilterExpression = Some(value)
    this
  }

  def edgeFilter(value: String): this.type = edgeFilter(expr(value))

  def run(): DataFrame = {
    require(fromExpression != null, "fromExpr is required.")
    require(toExpression != null, "toExpr is required.")

    val traversalGraph = if (isDirected) {
      graph
    } else {
      val edgeColumns = graph.edges.columns.toSeq
      val reversed = graph.edges.select(
        (Seq(
          col(GraphFrame.DST).alias(GraphFrame.SRC),
          col(GraphFrame.SRC).alias(GraphFrame.DST)) ++
          edgeColumns.filterNot(c => c == GraphFrame.SRC || c == GraphFrame.DST).map(col)): _*)
      GraphFrame(graph.vertices, graph.edges.unionByName(reversed))
    }

    val agg = traversalGraph.aggregateNeighbors
      .setStartingVertices(fromExpression)
      .setMaxHops(maxPathLength)
      .setTargetCondition(SparkShims.applyExprToCol(graph.spark, toExpression, "dst_attributes"))
      .setStoppingCondition(
        array_contains(col("path"), AggregateNeighbors.dstAttr(GraphFrame.ID)))
      .addAccumulator(
        "path",
        array(col(GraphFrame.ID)),
        concat(col("path"), array(AggregateNeighbors.dstAttr(GraphFrame.ID))))
      .setRequiredVertexAttributes(Seq(GraphFrame.ID))

    edgeFilterExpression.foreach { ef =>
      agg.setEdgeFilter(SparkShims.applyExprToCol(graph.spark, ef, "edge_attributes"))
      agg.setRequiredEdgeAttributes(graph.edges.columns.toSeq)
    }

    agg
      .run()
      .select(col("path"), size(col("path")).cast("long").alias("len"))
      .distinct()
  }
}
