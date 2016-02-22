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

import org.apache.spark.sql.functions.{col, struct, udf, when}

import org.graphframes.GraphFrame
import org.graphframes.GraphFrame.{DST, ID, LONG_DST, LONG_ID, LONG_SRC, SRC}


/**
 * Computes the number of triangles passing through each vertex.
 *
 * This algorithm ignores edge direction; i.e., all edges are treated as undirected.
 * In a multigraph, duplicate edges will be counted only once.  Note that this is different from
 * GraphX, which will count a triangle (a,b,c) twice if all edges are bidirectional.
 *
 * The returned vertices DataFrame contains one additional column:
 *  - count (`LongType`): the count of triangles
 *
 * The returned edges DataFrame is the same as the original edges DataFrame.
 */
class TriangleCount private[graphframes] (private val graph: GraphFrame) extends Arguments {

  def run(): GraphFrame = {
    TriangleCount.run(graph)
  }
}

private object TriangleCount {

  private def run(graph: GraphFrame): GraphFrame = {
    // Dedup edges by flipping them to have LONG_SRC < LONG_DST
    // TODO (when we drop support for Spark 1.4): Use functions greatest, smallest instead of UDFs
    val dedupedE = graph.indexedEdges.selectExpr(SRC, DST,
      s"if($LONG_SRC < $LONG_DST, $LONG_SRC, $LONG_DST) as $LONG_SRC",
      s"if($LONG_SRC < $LONG_DST, $LONG_DST, $LONG_SRC) as $LONG_DST")
      .dropDuplicates(Seq(LONG_SRC, LONG_DST))
      .select(SRC, DST)
    val g2 = GraphFrame(graph.vertices, dedupedE)

    // 2 types of triangles:
    // - cycles: edges forming a cycle.  These are counted 3 times each by motif finding.
    // - noncycles: cycle with one edge flipped.  These are counted 1 time each by motif finding.
    val cycles = g2.find("(a)-[]->(b); (b)-[]->(c); (c)-[]->(a)")
      .select(col("a.id").as("a"), col("b.id").as("b"), col("c.id").as("c"))
      .filter("a != b AND b != c AND c != a")
    val noncycles = g2.find("(a)-[]->(b); (b)-[]->(c); (a)-[]->(c)")
      .select(col("a.id").as("a"), col("b.id").as("b"), col("c.id").as("c"))
      .filter("a != b AND b != c AND c != a")

    // TODO: Can we make this more efficient using lateral view explode?
    // Count the triangle for each vertex.
    // - Since cycles are counted 3x, we can just keep "a"
    // - Since noncycles are counted once, we need to count each of a,b,c
    val allTriangles = noncycles.select(col("a").as(ID))
      .unionAll(noncycles.select(col("b").as(ID)))
      .unionAll(noncycles.select(col("c").as(ID)))
      .unionAll(cycles.select(col("a").as(ID)))
    val triangleCounts = allTriangles.groupBy(ID).count()
    val v = graph.vertices
    val countsCol = when(triangleCounts("count").isNotNull, triangleCounts("count"))
      .otherwise(0).as(COUNT_ID)
    val newV = v.join(triangleCounts, v(ID) === triangleCounts(ID), "left_outer")
      .select(countsCol +: v.columns.map(v.apply) :_ *)
    GraphFrame(newV, graph.edges)
  }

  private val COUNT_ID = "count"
}
