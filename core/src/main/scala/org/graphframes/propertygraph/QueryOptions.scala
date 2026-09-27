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
 * Options for query resolution and optimization.
 *
 * @param enableStatistics
 *   whether the optimizer may consume statistics for join ordering (default `true`).
 * @param maxSchemaPathLength
 *   cap on schema-path enumeration depth, to bound the fan-out of untyped/ambiguous patterns
 *   (default `10`).
 * @param maxVarLength
 *   maximum length of a variable-length pattern (`[e*1..N]`), controlling how many hops a
 *   repeating edge pattern may expand to (default `5`).
 * @param maxEnumeratedPaths
 *   maximal number of paths in the schema-graph that will be processed; each path results in one
 *   Spark SQL execution plan and all of them are unioned at the end (default `32`).
 */
final case class QueryOptions(
    enableStatistics: Boolean = true,
    maxSchemaPathLength: Int = 10,
    maxVarLength: Int = 5,
    maxEnumeratedPaths: Int = 32)

object QueryOptions {

  /**
   * Creates a new [[QueryOptions]] instance with all fields set to their default values.
   *
   * This is a convenience factory method intended primarily for Java and Py4J callers, who cannot
   * use Scala's default argument syntax directly. Scala users should prefer the default
   * constructor, e.g. `QueryOptions()`.
   *
   * @return
   *   a fresh [[QueryOptions]] with
   *   - `enableStatistics` = `true`
   *   - `maxSchemaPathLength` = `10`
   *   - `maxVarLength` = `5`
   *   - `maxEnumeratedPaths` = `32`
   */
  def withDefualts: QueryOptions = QueryOptions()

  /**
   * Creates a new [[QueryOptions]] instance with the specified
   * [[QueryOptions.maxSchemaPathLength maxSchemaPathLength]] value, leaving all other fields at
   * their defaults.
   *
   * This is a convenience factory method intended primarily for Java and Py4J callers who cannot
   * use Scala named-argument syntax directly. Scala users should prefer
   * `QueryOptions(maxSchemaPathLength = n)`.
   *
   * @param maxSchemaPathLength
   *   the maximum schema-path enumeration depth to use; see [[QueryOptions.maxSchemaPathLength]]
   *   for the effect of this setting. Must be non-negative.
   * @return
   *   a new [[QueryOptions]] with
   *   - `enableStatistics` = `true`
   *   - `maxSchemaPathLength` = the supplied value
   *   - `maxVarLength` = `5`
   *   - `maxEnumeratedPaths` = `32`
   */
  def withMaxSchemaPathLength(maxSchemaPathLength: Int): QueryOptions =
    QueryOptions(maxSchemaPathLength = maxSchemaPathLength)
}

/** Selects which plan to render via explain. */
sealed trait ExplainMode

object ExplainMode {
  case object Logical extends ExplainMode
  case object Physical extends ExplainMode
}
