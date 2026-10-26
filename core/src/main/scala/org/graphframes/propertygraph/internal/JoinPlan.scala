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
 * A reference to one element of a [[SchemaPath]] within a [[JoinPlan]]'s join order. The order is
 * expressed at the granularity of individual nodes and edges (not step indices).
 */
private[propertygraph] sealed trait PathElementRef
private[propertygraph] final case class NodeRef(index: Int) extends PathElementRef {
  require(index >= 0)
}
private[propertygraph] final case class EdgeRef(index: Int) extends PathElementRef {
  require(index >= 0)
}

/**
 * The physical plan for one [[SchemaPath]]. Self-contained: it carries everything the executor
 * needs, so `explain(physical)` can render path + order + the statistics that drove it without
 * re-running resolution.
 *
 * @param path
 *   the resolved schema path this plan executes (topology + per-node scan filters + directions).
 * @param order
 *   element-level join order: a sequence of [[NodeRef]] / [[EdgeRef]] into `path.nodes` /
 *   `path.steps`. The executor scans and joins in this order.
 * @param statsUsed
 *   the per-group statistics that drove `order`.
 * @param joinPredicates
 *   WHERE conjuncts spanning exactly two adjacent node variables; applied as join conditions.
 * @param postFilters
 *   WHERE conjuncts spanning 3+ variables / non-adjacent / any edge variable; applied after the
 *   join tree.
 * @param projection
 *   the RETURN shape (Default / Star / Items).
 */
private[propertygraph] final case class JoinPlan(
    path: SchemaPath,
    order: Vector[PathElementRef],
    statsUsed: Map[String, GroupStatistics],
    joinPredicates: Seq[Expression],
    postFilters: Seq[Expression],
    projection: Projection) {

  override def toString: String = {
    val orderStr = order
      .map {
        case NodeRef(i) => s"n$i"
        case EdgeRef(i) => s"e$i"
      }
      .mkString("[", ", ", "]")
    val projStr = projection match {
      case Projection.Default => "DEFAULT"
      case Projection.Star => "*"
      case Projection.Items(items) => items.mkString("Items(", ", ", ")")
    }
    s"JoinPlan(path=$path, order=$orderStr, projection=$projStr)"
  }
}
