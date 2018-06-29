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
import org.apache.spark.sql.functions.{array, col, explode, when}

import org.graphframes.GraphFrame
import org.graphframes.GraphFrame.{DST, ID, LONG_DST, LONG_SRC, SRC}

/**
 * Computes the number of triangles passing through each vertex.
 *
 * This algorithm ignores edge direction; i.e., all edges are treated as undirected.
 * In a multigraph, duplicate edges will be counted only once.
 *
 * Note that this provides the same algorithm as GraphX, but GraphX assumes the user provides
 * a graph in the correct format.  In Spark 2.0+, GraphX can automatically canonicalize
 * the graph to put it in this format.
 *
 * The returned DataFrame contains all the original vertex information and one additional column:
 *  - count (`LongType`): the count of triangles
 */
class TriangleCount private[graphframes] (private val graph: GraphFrame) extends Arguments {

  def run(): DataFrame = {
    TriangleCount.run(graph)
  }
}

private object TriangleCount {

  private def run(graph: GraphFrame): DataFrame = {
    // Dedup edges by flipping them to have LONG_SRC < LONG_DST
    // TODO (when we drop support for Spark 1.4): Use functions greatest, smallest instead of UDFs
    val dedupedE = graph.indexedEdges
      .filter(s"$LONG_SRC != $LONG_DST")
      .selectExpr(
        s"if($LONG_SRC < $LONG_DST, $SRC, $DST) as $SRC",
        s"if($LONG_SRC < $LONG_DST, $DST, $SRC) as $DST")
      .dropDuplicates(Seq(SRC, DST))
    val g2 = GraphFrame(graph.vertices, dedupedE)

    // Because SRC < DST, there exists only one type of triangles:
    // - Non-cycle with one edge flipped.  These are counted 1 time each by motif finding.
    val triangles = g2.find("(a)-[]->(b); (b)-[]->(c); (a)-[]->(c)")

    val triangleCounts = triangles
      .select(explode(array(col("a.id"), col("b.id"), col("c.id"))).as(ID))
      .groupBy(ID)
      .count()

    val v = graph.vertices
    val countsCol = when(col("count").isNull, 0L).otherwise(col("count"))
    val newV = v.join(triangleCounts, v(ID) === triangleCounts(ID), "left_outer")
      .select(countsCol.as(COUNT_ID) +: v.columns.map(v.apply) :_ *)
    newV
  }

  private val COUNT_ID = "count"
}
