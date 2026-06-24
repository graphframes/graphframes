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
 * Turns a [[ResolvedQuery]] into a sequence of [[JoinPlan]]s (the physical plan). This is the
 * optimization boundary: it is the only place where join order is decided and where statistics
 * are (optionally) consumed.
 *
 * Disconnected patterns (`query.paths.isEmpty`) yield no plans; the executor then produces an
 * empty result DataFrame.
 */
private[propertygraph] object JoinOptimizer {

  /**
   * Default entry point.
   */
  def plan(query: ResolvedQuery, stats: Option[GraphStatistics]): Seq[JoinPlan] =
    defaultPlanner(query, stats)

  /**
   * Planner SPI: `ResolvedQuery x Option[GraphStatistics] => Seq[JoinPlan]`.
   */
  type Planner = (ResolvedQuery, Option[GraphStatistics]) => Seq[JoinPlan]

  /**
   * Refinement SPI applied after the default planner: `(ResolvedQuery, Seq[JoinPlan]) =>
   * Seq[JoinPlan]`. Lets handwritten rules reorder / prune plans without replacing the planner.
   */
  type PlanRefiner = (ResolvedQuery, Seq[JoinPlan]) => Seq[JoinPlan]

  /** planner: pattern order, no statistics consumption. */
  val defaultPlanner: Planner = (query, _) => patternOrderPlans(query)

  /** refiner: identity (no-op). */
  val identityRefiner: PlanRefiner = (_, plans) => plans

  /**
   * Build one `JoinPlan` per path, joining elements in the order they were written (`n0, e0, n1,
   * e1, …, n_{k}`). `statsUsed` is empty; predicates/projection are carried through from the
   * resolved query.
   */
  private def patternOrderPlans(query: ResolvedQuery): Seq[JoinPlan] =
    query.paths.map { path =>
      val order = patternOrder(path)
      JoinPlan(
        path = path,
        order = order,
        statsUsed = Map.empty,
        joinPredicates = query.joinPredicates,
        postFilters = query.postFilters,
        projection = query.projection)
    }

  /** `n0, e0, n1, e1, …, n_{k}` for a path with `k` steps (`k+1` nodes). */
  private[propertygraph] def patternOrder(path: SchemaPath): Vector[PathElementRef] = {
    val b = Vector.newBuilder[PathElementRef]
    b += NodeRef(0)
    path.steps.indices.foreach { i =>
      b += EdgeRef(i)
      b += NodeRef(i + 1)
    }
    b.result()
  }
}
