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
 * The output of resolution (`Resolver`). This is the logical, schema-resolved representation of a
 * GQL query, before any join ordering / statistics / execution.
 *
 * @param paths
 *   1..N concrete `SchemaPath`s fanned out from untyped/ambiguous pattern elements. Empty when
 *   the pattern is disconnected in the schema graph (no Spark execution will be needed
 *   downstream): fast-fail.
 * @param joinPredicates
 *   WHERE conjuncts that span exactly two adjacent node variables; applied as join conditions.
 * @param postFilters
 *   WHERE conjuncts that span 3+ variables, non-adjacent variables, or any edge variable; applied
 *   after the join tree. (Scan-local predicates live on each `PathNode`, not here.)
 * @param projection
 *   the `RETURN` shape, or `Projection.Default` when `RETURN` is omitted.
 */
private[propertygraph] final case class ResolvedQuery(
    paths: Seq[SchemaPath],
    joinPredicates: Seq[Expression],
    postFilters: Seq[Expression],
    projection: Projection)

/**
 * Encodes `RETURN *` vs explicit items vs the omitted default. See design §5.3.
 *
 * `Default` projects the first and last *named* node IDs of each path (anonymous `()` nodes are
 * not surfaced), matching the fixed output schema in the proposal §6.
 */
private[propertygraph] sealed trait Projection

private[propertygraph] object Projection {
  case object Default extends Projection // RETURN omitted
  case object Star extends Projection // RETURN *
  final case class Items(items: Seq[ReturnItem]) extends Projection // RETURN expr [AS alias], ...
}
