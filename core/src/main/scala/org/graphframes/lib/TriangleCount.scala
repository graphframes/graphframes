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
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame
import org.graphframes.Logging
import org.graphframes.WithIntermediateStorageLevel

/**
 * Computes the number of triangles passing through each vertex.
 *
 * This algorithm ignores edge direction; i.e., all edges are treated as undirected. In a
 * multigraph, duplicate edges will be counted only once.
 *
 * **WARNING** This implementation is based on intersections of neighbor sets, which requires
 * collecting both SRC and DST neighbors per edge! This will blow up memory in case the graph
 * contains very high-degree nodes (power-law networks). Consider sampling strategies for that
 * case!
 *
 * The returned DataFrame contains all the original vertex information and one additional column:
 *   - count (`LongType`): the count of triangles
 */
class TriangleCount private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with Serializable
    with WithIntermediateStorageLevel {

  def run(): DataFrame = {
    TriangleCount.run(graph, intermediateStorageLevel)
  }
}

private object TriangleCount extends Logging {
  import org.graphframes.GraphFrame.*

  private def prepareGraph(graph: GraphFrame): GraphFrame = {
    // Dedup edges by flipping them to have SRC < DST
    // Remove self-loops
    val dedupedE = graph.edges
      .filter(col(SRC) =!= col(DST))
      .select(
        when(col(SRC) < col(DST), col(SRC)).otherwise(col(DST)).as(SRC),
        when(col(SRC) < col(DST), col(DST)).otherwise(col(SRC)).as(DST))
      .distinct()

    // Prepare the graph with no isolated vertices.
    GraphFrame(graph.vertices.select(ID), dedupedE).dropIsolatedVertices()
  }

  private def run(graph: GraphFrame, intermediateStorageLevel: StorageLevel): DataFrame = {
    val g2 = prepareGraph(graph)

    val verticesWithNeighbors = g2.aggregateMessages
      .setIntermediateStorageLevel(intermediateStorageLevel)
      .sendToSrc(AggregateMessages.dst(ID))
      .sendToDst(AggregateMessages.src(ID))
      .agg(collect_set(AggregateMessages.msg).alias("neighbors"))

    val triangles = verticesWithNeighbors
      .select(col(ID), col("neighbors").alias("src_set"))
      .join(g2.edges, col(ID) === col(SRC))
      .drop(ID)
      .join(
        verticesWithNeighbors.select(col(ID), col("neighbors").alias("dst_set")),
        col(ID) === col(DST))
      .drop(ID)
      // Count of common neighbors of SRC and DST
      .withColumn("triplets", array_size(array_intersect(col("src_set"), col("dst_set"))))
      .filter(col("triplets") > lit(0))
      .persist(intermediateStorageLevel)

    val srcTriangles = triangles.groupBy(SRC).agg(sum(col("triplets")).alias("src_triplets"))
    val dstTriangles = triangles.groupBy(DST).agg(sum(col("triplets")).alias("dst_triplets"))

    val result = graph.vertices
      .join(srcTriangles, col(ID) === col(SRC), "left_outer")
      .join(dstTriangles, col(ID) === col(DST), "left_outer")
      // Each triangle counted twice, so divide by 2.
      .withColumn(
        COUNT_ID,
        floor(
          when(col("src_triplets").isNull && col("dst_triplets").isNull, lit(0))
            .when(col("src_triplets").isNull, col("dst_triplets"))
            .when(col("dst_triplets").isNull, col("src_triplets"))
            .otherwise(col("src_triplets") + col("dst_triplets")) / lit(2)))

    result.persist(intermediateStorageLevel)
    result.count()
    verticesWithNeighbors.unpersist()
    triangles.unpersist()
    resultIsPersistent()
    result
  }

  private val COUNT_ID = "count"
}
