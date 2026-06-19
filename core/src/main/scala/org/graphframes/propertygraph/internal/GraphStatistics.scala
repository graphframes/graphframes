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

package org.graphframes.propertygraph.internal

/**
 * Per-column statistics for a property group. All fields optional and additive: future statistics
 * providers can fill more of them without changing callers or the path/plan types.
 *
 * v1 providers populate at most `distinctCount`/`nullCount`/`min`/`max`; most leave everything
 * `None`.
 *
 * @param distinctCount
 *   estimated number of distinct values, if known.
 * @param nullCount
 *   number of nulls, if known.
 * @param min
 *   minimum value, if known.
 * @param max
 *   maximum value, if known.
 */
private[propertygraph] final case class ColumnStatistics(
    distinctCount: Option[Long] = None,
    nullCount: Option[Long] = None,
    min: Option[Any] = None,
    max: Option[Any] = None)

/**
 * Per-group statistics (a vertex property group or an edge property group).
 *
 * v1 fills only `rowCount`; `sizeInBytes` and `columns` are reserved for richer future providers
 * (Parquet footers, CBO stats, …). All fields optional and additive.
 *
 * @param rowCount
 *   number of rows in the group, if known.
 * @param sizeInBytes
 *   estimated on-disk / in-memory size, if known.
 * @param columns
 *   per-column stats, keyed by column name.
 */
private[propertygraph] final case class GroupStatistics(
    rowCount: Option[Long] = None,
    sizeInBytes: Option[Long] = None,
    columns: Map[String, ColumnStatistics] = Map.empty)

/**
 * Statistics source SPI. The optimizer queries it by group name; the *source* is pluggable so
 * that future implementations (Parquet footer min/max, managed-table CBO, …) can be swapped in
 * with no change to callers or to the path/plan types.
 *
 * v1: the optimizer does not yet consume live statistics (it plans in pattern order); this trait
 * exists so the future optimization plugs in without API churn. `GraphStatistics.Empty` is the
 * no-op implementation used until then.
 */
private[propertygraph] trait GraphStatistics {

  /** Statistics for the named vertex property group; empty stats if unknown. */
  def vertexGroup(name: String): GroupStatistics

  /** Statistics for the named edge property group; empty stats if unknown. */
  def edgeGroup(name: String): GroupStatistics
}

private[propertygraph] object GraphStatistics {

  /** A no-op provider returning empty stats for every group. */
  val Empty: GraphStatistics = new GraphStatistics {
    override def vertexGroup(name: String): GroupStatistics = GroupStatistics()
    override def edgeGroup(name: String): GroupStatistics = GroupStatistics()
  }

  /**
   * Build a provider that caches `rowCount` per group on first access by calling `df.count()`.
   * The cache is built lazily and shared across `vertexGroup`/`edgeGroup` lookups. All
   * non-rowCount fields stay empty.
   *
   * NOTE: wired but not yet consumed by the v1 optimizer; parked for the statistics-driven join
   * ordering described in design §5.4/§6.
   */
  def cachedRowCount(
      vertexRowCounts: Map[String, Long],
      edgeRowCounts: Map[String, Long]): GraphStatistics = new GraphStatistics {
    override def vertexGroup(name: String): GroupStatistics =
      vertexRowCounts
        .get(name)
        .map(c => GroupStatistics(rowCount = Some(c)))
        .getOrElse(GroupStatistics())
    override def edgeGroup(name: String): GroupStatistics =
      edgeRowCounts
        .get(name)
        .map(c => GroupStatistics(rowCount = Some(c)))
        .getOrElse(GroupStatistics())
  }
}
