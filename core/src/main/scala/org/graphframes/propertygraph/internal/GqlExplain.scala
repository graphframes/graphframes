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
 * Read-only renderers over the two intermediate IR values:
 *   - [[logical]] renders a [[ResolvedQuery]] (the resolved schema paths, WHERE classification,
 *     and projection);
 *   - [[physical]] renders a `Seq[JoinPlan]` (per plan: the path, element-level join order, the
 *     statistics that drove it, and the predicates).
 *
 * Both are pure JVM; neither touches Spark.
 */
private[propertygraph] object GqlExplain {

  /** Render the logical (resolved) plan. */
  def logical(query: ResolvedQuery): String = {
    val b = Vector.newBuilder[String]
    b += "Logical plan (resolved):"

    if (query.paths.isEmpty) {
      b += "  schema paths: (none -- pattern is disconnected in the schema graph)"
    } else {
      b += s"  schema paths (${query.paths.size}):"
      query.paths.zipWithIndex.foreach { case (p, idx) =>
        b += s"    [$idx] ${renderPath(p)}"
      }
    }

    b += s"  join predicates (${query.joinPredicates.size}):"
    query.joinPredicates.foreach(e => b += s"    - ${renderExpr(e)}")
    b += s"  post-join filters (${query.postFilters.size}):"
    query.postFilters.foreach(e => b += s"    - ${renderExpr(e)}")
    b += s"  projection: ${renderProjection(query.projection)}"

    b.result().mkString("\n")
  }

  /** Render the physical plan (one block per JoinPlan). */
  def physical(plans: Seq[JoinPlan]): String = {
    val b = Vector.newBuilder[String]
    b += s"Physical plan (${plans.size} join plan(s)):"
    if (plans.isEmpty) {
      b += "  (none -- pattern is disconnected in the schema graph; no Spark execution)"
    } else {
      plans.zipWithIndex.foreach { case (plan, idx) =>
        b += s"  Plan $idx:"
        b += s"    path: ${renderPath(plan.path)}"
        b += s"    join order: ${renderOrder(plan.order)}"
        b += s"    statistics: ${renderStats(plan.statsUsed)}"
        b += s"    join predicates (${plan.joinPredicates.size}):"
        plan.joinPredicates.foreach(e => b += s"      - ${renderExpr(e)}")
        b += s"    post-join filters (${plan.postFilters.size}):"
        plan.postFilters.foreach(e => b += s"      - ${renderExpr(e)}")
        b += s"    projection: ${renderProjection(plan.projection)}"
      }
    }
    b.result().mkString("\n")
  }

  // ---------------------------------------------------------------------
  // Renderers.
  // ---------------------------------------------------------------------

  private def renderPath(path: SchemaPath): String = {
    // (a:Person)-[e1:KNOWS]->(b:Person)<-[e2:LIKES]-(c:Person)
    val sb = new StringBuilder
    path.nodes.zipWithIndex.foreach { case (node, i) =>
      sb.append(renderNode(node))
      if (i < path.steps.length) {
        sb.append(renderStep(path.steps(i)))
      }
    }
    sb.toString
  }

  private def renderNode(node: PathNode): String = {
    val v = node.variable.getOrElse("_")
    val filter =
      if (node.scanFilter.isEmpty) ""
      else node.scanFilter.map(renderExpr).mkString("{", " AND ", "}")
    s"($v:${node.vertexGroupName}$filter)"
  }

  private def renderStep(step: PathStep): String = {
    val v = step.variable.map(n => s"$n:").getOrElse("")
    val body = s"[$v${step.edge.edgeGroupName}]"
    // Forward step: -(body)-> ; backward step: <-(body)-
    if (step.traversedForward) s"-$body->" else s"<-$body-"
  }

  private def renderOrder(order: Vector[PathElementRef]): String =
    order
      .map {
        case NodeRef(i) => s"n$i"
        case EdgeRef(i) => s"e$i"
      }
      .mkString("[", ", ", "]")

  private def renderStats(stats: Map[String, GroupStatistics]): String =
    if (stats.isEmpty) "(no statistics)"
    else
      stats.toVector
        .sortBy(_._1)
        .map { case (k, v) =>
          s"$k=${v.rowCount.map(c => s"rows=$c").getOrElse("?")}"
        }
        .mkString("{", ", ", "}")

  private def renderProjection(projection: Projection): String = projection match {
    case Projection.Default => "(default: matched IDs of first/last named nodes)"
    case Projection.Star => "* (all matched variables)"
    case Projection.Items(items) =>
      items
        .map { it =>
          val core = renderExpr(it.expression)
          it.alias match {
            case Some(a) => s"$core AS $a"
            case None => core
          }
        }
        .mkString("items: [", ", ", "]")
  }

  private def renderExpr(expr: Expression): String = expr match {
    case Literal(value) =>
      value match {
        case null => "NULL"
        case s: String => s"'$s'"
        case other => String.valueOf(other)
      }
    case Variable(name) => name
    case PropertyAccess(variable, property) => s"$variable.$property"
    case Comparison(left, op, right) =>
      s"(${renderExpr(left)} ${renderCompOp(op)} ${renderExpr(right)})"
    case Arithmetic(left, op, right) =>
      s"(${renderExpr(left)} ${renderAddOp(op)} ${renderExpr(right)})"
    case Not(e) => s"(NOT ${renderExpr(e)})"
    case And(left, right) => s"(${renderExpr(left)} AND ${renderExpr(right)})"
    case Or(left, right) => s"(${renderExpr(left)} OR ${renderExpr(right)})"
  }

  private def renderCompOp(op: CompOp): String = op match {
    case Eq => "="
    case Neq => "<>"
    case Lt => "<"
    case Lte => "<="
    case Gt => ">"
    case Gte => ">="
  }

  private def renderAddOp(op: AddOp): String = op match {
    case Plus => "+"
    case Minus => "-"
  }
}
