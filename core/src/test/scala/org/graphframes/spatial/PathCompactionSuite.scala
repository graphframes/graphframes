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

package org.graphframes.spatial

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.*
import org.graphframes.GraphFrame
import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.GraphFramesUnreachableException
import org.graphframes.SparkFunSuite
import org.scalatest.BeforeAndAfterAll

import scala.annotation.nowarn
import scala.collection.mutable

class PathCompactionSuite
    extends SparkFunSuite
    with GraphFrameTestSparkContext
    with BeforeAndAfterAll {

  private var vertices: DataFrame = _
  private var edges: DataFrame = _
  private var graph: GraphFrame = _

  /**
   * Test graph (vertices carry a numeric "value" attribute so aggregations are testable):
   *   - `0 -> 1 -> 2 -> 3 -> 4 -> 5`: chain, interior = 1,2,3,4 between 0 and 5
   *   - `6 -> 7 -> 8 -> 9 -> 10`: chain, interior = 7,8,9 between 6 and 10
   *   - `11 -> 12 -> 13 -> 11`: isolated cycle (11,12,13)
   *
   * To keep things deterministic we rebuild per test from this base.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    vertices = spark.range(0L, 15L).toDF(GraphFrame.ID).withColumn("value", col(GraphFrame.ID))
    edges = spark
      .createDataFrame(
        Seq(
          (0L, 1L),
          (1L, 2L),
          (2L, 3L),
          (3L, 4L),
          (4L, 5L),
          (6L, 7L),
          (7L, 8L),
          (8L, 9L),
          (9L, 10L),
          (11L, 12L),
          (12L, 13L),
          (13L, 11L)))
      .toDF(GraphFrame.SRC, GraphFrame.DST)
    graph = GraphFrame(vertices, edges)
  }

  private def collectSorted(df: DataFrame): Seq[(Long, Long)] = {
    df.select(col(GraphFrame.SRC), col(GraphFrame.DST))
      .collect()
      .map(r => (r.getLong(0), r.getLong(1)))
      .sorted
      .toSeq
  }

  test("simple chain compacts to a single edge") {
    // Subgraph containing only the first chain: 0 -> 1 -> 2 -> 3 -> 4 -> 5.
    // Interior {1,2,3,4} compacts to (0,5); every original edge touches an interior id
    // and is dropped, so the whole graph collapses to a single compacted edge.
    val subVertices =
      spark.range(0L, 6L).toDF(GraphFrame.ID).withColumn("value", col(GraphFrame.ID))
    val subEdges = spark
      .createDataFrame(Seq((0L, 1L), (1L, 2L), (2L, 3L), (3L, 4L), (4L, 5L)))
      .toDF(GraphFrame.SRC, GraphFrame.DST)
    val subGraph = GraphFrame(subVertices, subEdges)

    val result = new PathCompaction(subGraph).run()
    val rows = collectSorted(result)
    assert(rows === Seq((0L, 5L)))
  }

  test("multiple disjoint chains each compact independently; non-compacted edges survive") {
    // Chains (0->...->5) and (6->...->10) compact to (0,5) and (6,10). The cycle
    // (11,12,13) is dropped by default, so its vertices are not "interior" and its three
    // original edges survive unchanged. All original edges of the two chains touch interior
    // ids and are dropped.
    val result = new PathCompaction(graph).run()
    val rows = collectSorted(result)
    assert(rows === Seq((0L, 5L), (6L, 10L), (11L, 12L), (12L, 13L), (13L, 11L)))
  }

  test("isolated cycle survives as original edges when dropped by default") {
    // Graph with only the cycle. dropCompactedCycles defaults to true, so the cycle is not
    // compacted and its edges are returned unchanged (none are dropped).
    val cycleEdges = spark
      .createDataFrame(Seq((11L, 12L), (12L, 13L), (13L, 11L)))
      .toDF(GraphFrame.SRC, GraphFrame.DST)
    val cycleGraph = GraphFrame.fromEdges(cycleEdges)
    val result = new PathCompaction(cycleGraph).run()
    val rows = collectSorted(result)
    assert(rows === Seq((11L, 12L), (12L, 13L), (13L, 11L)))
  }

  test("cycle is emitted as a self-loop on the minimal id when not dropped") {
    // With dropCompactedCycles = false the cycle is compacted into a self-loop (11, 11);
    // all three original cycle edges touch interior ids {11,12,13} and are dropped.
    val cycleEdges = spark
      .createDataFrame(Seq((11L, 12L), (12L, 13L), (13L, 11L)))
      .toDF(GraphFrame.SRC, GraphFrame.DST)
    val cycleGraph = GraphFrame.fromEdges(cycleEdges)
    val result = new PathCompaction(cycleGraph).setDropCompactedCycles(false).run()
    val rows = collectSorted(result)
    assert(rows === Seq((11L, 11L)))
  }

  test("default count aggregator reports the number of compacted interior vertices") {
    val result = new PathCompaction(graph).run()
    val collected = result
      .filter(col("count").isNotNull)
      .select(col(GraphFrame.SRC), col(GraphFrame.DST), col("count"))
      .collect()
      .map {
        case Row(src: Long, dst: Long, c: Long) =>
          (src, dst) -> c
        case _ => throw new GraphFramesUnreachableException()
      }
      .toMap

    // Chain 0->1->2->3->4->5: 4 interior vertices.
    assert(collected((0L, 5L)) === 4L)
    // Chain 6->7->8->9->10: 3 interior vertices.
    assert(collected((6L, 10L)) === 3L)
    // Surviving (un-compacted) cycle edges carry null count (3 cycle edges).
    assert(result.filter(col("count").isNull).count() === 3L)
  }

  test("user aggregation is evaluated over interior vertices; surviving edges get null") {
    val result = new PathCompaction(graph)
      .withAggExpression("ids", sort_array(collect_set(GraphFrame.ID)))
      .requiredVertexAttributes(Seq("value"))
      .withAggExpression("sum_value", sum("value"))
      .run()

    @nowarn val collected: Seq[(Long, Long, Seq[Long], java.lang.Long)] = result
      .filter(col("ids").isNotNull)
      .select(col(GraphFrame.SRC), col(GraphFrame.DST), col("ids"), col("sum_value"))
      .collect()
      .map(r =>
        (
          r.getLong(0),
          r.getLong(1),
          r.getAs[mutable.WrappedArray[Long]](2).toSeq,
          r.getAs[java.lang.Long](3)))

    val byEndpoint = collected.map(r => (r._1, r._2) -> r).toMap

    // Chain 0->1->2->3->4->5: interior = 1,2,3,4 ; sum_value = 1+2+3+4 = 10
    val chain1 = byEndpoint((0L, 5L))
    assert(chain1._3 === Seq(1L, 2L, 3L, 4L))
    assert(chain1._4 === 10L)

    // Chain 6->7->8->9->10: interior = 7,8,9 ; sum_value = 7+8+9 = 24
    val chain2 = byEndpoint((6L, 10L))
    assert(chain2._3 === Seq(7L, 8L, 9L))
    assert(chain2._4 === 24L)

    // Surviving cycle edges carry null aggregations (3 cycle edges).
    assert(result.filter(col("ids").isNull).count() === 3L)
  }

  test("alwaysKeepVertices splits a chain at the kept vertex") {
    // Keep vertex 3: candidates become {1,2} and {4}. Component {1,2} compacts to (0,3);
    // component {4} compacts to (3,5); chain 6->...->10 is unaffected -> (6,10). Interior
    // ids are {1,2,4,7,8,9}; every original chain edge touches one of them and is dropped.
    // The cycle is not compacted (dropCompactedCycles = true) so its edges survive.
    val result = new PathCompaction(graph)
      .setAlwaysKeepVertices(col(GraphFrame.ID) === 3L)
      .run()
    val rows = collectSorted(result)
    assert(rows.toSet === Set((0L, 3L), (3L, 5L), (6L, 10L), (11L, 12L), (12L, 13L), (13L, 11L)))
  }

  test("alwaysDropVertices removes a vertex before compaction") {
    // Drop vertex 2: filterVertices prunes edges touching 2, severing the first chain into
    // 0->1 and 3->4->5. Vertex 4 keeps inDeg=1 (from 3) and outDeg=1 (to 5), so it compacts
    // to (3, 5); chain 6->...->10 is untouched -> (6, 10). Interior ids are {4,7,8,9}; the
    // edges (0,1) and the three cycle edges do not touch any interior id and survive.
    val result = new PathCompaction(graph)
      .setAlwaysDropVertices(col(GraphFrame.ID) === 2L)
      .run()
    val rows = collectSorted(result)
    assert(rows === Seq((0L, 1L), (3L, 5L), (6L, 10L), (11L, 12L), (12L, 13L), (13L, 11L)))
  }

  test("no candidates returns all original edges with null aggregations") {
    // A star graph: center has outDegree 5, leaves have inDegree 1 outDegree 0. No vertex is a
    // candidate, so nothing is compacted and all 5 original edges survive carrying null
    // aggregations. The schema always starts with src, dst, then the default "count", then any
    // user-registered aggregations.
    val starEdges = spark
      .createDataFrame(Seq((0L, 1L), (0L, 2L), (0L, 3L), (0L, 4L), (0L, 5L)))
      .toDF(GraphFrame.SRC, GraphFrame.DST)
    val starGraph = GraphFrame.fromEdges(starEdges)
    val result = new PathCompaction(starGraph)
      .withAggExpression("ids", collect_set(GraphFrame.ID))
      .run()
    assert(result.count() === 5L)
    assert(result.columns.toSeq === Seq(GraphFrame.SRC, GraphFrame.DST, "count", "ids"))
    assert(result.filter(col("count").isNull).count() === 5L)
  }
}
