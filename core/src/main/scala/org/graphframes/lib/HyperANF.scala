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
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.hll_sketch_agg
import org.apache.spark.sql.functions.hll_union_agg
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf
import org.graphframes.GraphFrame
import org.graphframes.Logging
import org.graphframes.WithIntermediateStorageLevel
import org.graphframes.WithLgNomEntries

/**
 * HyperANF-style approximation of the neighbourhood function on top of GraphFrames.
 *
 * This implementation is inspired by
 * [[https://arxiv.org/pdf/1011.5599 Vigna, Paolo; Boldi, Marco; Rosa, Sebastiano. "HyperANF: Approximating the Neighbourhood Function of Very Large Graphs on a Budget." arXiv preprint arXiv:1011.5599 (2010)]].
 *
 * The input graph is treated as directed: for each vertex, reachability is computed by following
 * outgoing edges from `src` to `dst`.
 *
 * Compared with the cumulative neighbourhood-function presentation in the paper, this
 * implementation returns one column per hop, `hop_0`, `hop_1`, `hop_2`, ..., `hop_N`. The `hop_0`
 * column contains a HyperLogLog sketch of the source vertex itself, and each `hop_k` column for
 * `k >= 1` contains a HyperLogLog sketch of the set of vertices reachable in exactly `k` hops. To
 * derive the cumulative approximate neighbourhood function for distances up to some hop `k`, a
 * user can combine `hop_0` through `hop_k` with `hll_union` and then apply `hll_sketch_estimate`
 * to the merged sketch.
 *
 * The computation can also be restricted to a subgraph by supplying an edge filter expression via
 * [[setEdgesFilterExpression]]. A common use case is to filter on `src`, for example
 * `src IN (...)`, to obtain sketches only for a selected set of starting vertices.
 *
 * @param graph
 *   input graph whose directed edges are used for reachability expansion
 */
class HyperANF private[graphframes] (graph: GraphFrame)
    extends Serializable
    with Logging
    with WithIntermediateStorageLevel
    with WithLgNomEntries {
  private var nHops: Int = 3
  private var edgesFilterExpression: Column = lit(true)

  /**
   * Sets the edge filter expression used before running the computation.
   *
   * Only edges satisfying this predicate participate in the directed reachability expansion. This
   * effectively runs the algorithm on the subgraph induced by the filtered edge set.
   *
   * A common use case is filtering on `src`, for example `src IN (...)`, to limit the result to a
   * chosen set of starting vertices.
   *
   * @param value
   *   filter expression applied to `graph.edges`
   * @return
   *   this HyperANF instance
   */
  def setEdgesFilterExpression(value: Column): this.type = {
    edgesFilterExpression = value
    this
  }

  /**
   * Sets the maximum hop distance to compute.
   *
   * The result will contain `hop_0`, `hop_1`, `hop_2`, ..., `hop_N`, where `N` is the configured
   * number of hops.
   *
   * @param value
   *   positive number of hops to compute
   * @return
   *   this HyperANF instance
   */
  def setNHops(value: Int): this.type = {
    require(value > 0, "n-hops cannot be nagative or zero")
    nHops = value
    this
  }

  /**
   * Runs the HyperANF-style computation.
   *
   * The returned `DataFrame` has one row per source vertex present in the filtered edge set. It
   * contains the vertex id column `id` and one sketch column per hop: `hop_0`, `hop_1`, `hop_2`,
   * ..., `hop_N`. The `hop_0` column stores a HyperLogLog sketch containing `id` itself. Each
   * `hop_k` column for `k >= 1` stores a HyperLogLog sketch for the set of vertices reachable
   * from `id` in exactly `k` directed hops.
   *
   * To obtain an approximate cumulative neighbourhood size up to hop `k`, union `hop_0` through
   * `hop_k` with `hll_union` and then apply `hll_sketch_estimate`.
   *
   * @return
   *   a `DataFrame` with exact-hop HyperLogLog sketches per source vertex
   */
  def run(): DataFrame = {
    val edges =
      graph.edges
        .filter(edgesFilterExpression)
        .select(GraphFrame.SRC, GraphFrame.DST)
        .persist(intermediateStorageLevel)
    var hop = 1

    val hop0func = udf(HyperANF.hll(lgNomEntries))
    var state = edges
      .groupBy(col(GraphFrame.SRC).alias(GraphFrame.ID))
      .agg(hll_sketch_agg(GraphFrame.DST, lgNomEntries).alias("hop_1"))
      .select(col(GraphFrame.ID), hop0func(col(GraphFrame.ID)).alias("hop_0"), col("hop_1"))

    while (hop < nHops) {
      hop += 1

      val n_state = edges
        .join(
          state.select(GraphFrame.ID, s"hop_${hop - 1}"),
          col(GraphFrame.DST) === col(GraphFrame.ID),
          "left")
        .groupBy(col(GraphFrame.SRC).alias(GraphFrame.ID))
        .agg(hll_union_agg(s"hop_${hop - 1}").alias(s"hop_${hop}"))

      state = state.join(n_state, GraphFrame.ID)
    }

    val result = state.persist(intermediateStorageLevel)
    // materialize
    val _ = result.count()
    resultIsPersistent()

    edges.unpersist()

    result
  }
}

private object HyperANF extends Serializable {
  def hll(lgNomEntries: Int): Any => Array[Byte] = (id) => {
    val sketch = new org.apache.datasketches.hll.HllSketch(lgNomEntries)
    sketch.update(id.toString())
    sketch.toCompactByteArray()
  }
}
