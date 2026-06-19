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

package org.graphframes.propertygraph

/**
 * Options for [[PropertyGraphFrame.query]] / [[PropertyGraphFrame.explain]].
 *
 * @param enableStatistics
 *   whether the optimizer may consume statistics for join ordering (default `true`). In v1 the
 *   optimizer plans in pattern order regardless, so this flag is accepted for API stability and
 *   reserved for the future statistics-driven ordering (design §5.4/§6).
 * @param maxSchemaPathLength
 *   cap on schema-path enumeration depth, to bound the fan-out of untyped/ambiguous patterns
 *   (default `10`).
 */
final case class QueryOptions(enableStatistics: Boolean = true, maxSchemaPathLength: Int = 10)

/** Selects which plan to render via [[PropertyGraphFrame.explain]]. */
sealed trait ExplainMode

object ExplainMode {
  case object Logical extends ExplainMode
  case object Physical extends ExplainMode
}
