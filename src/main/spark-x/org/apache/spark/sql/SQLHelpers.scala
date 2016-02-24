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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions

object SQLHelpers {
  def getExpr(col: Column): Expression = col.expr

  def expr(e: String): Column = functions.expr(e)

  /**
   * Appends each record with a unique ID (uniq_id) and groups existing fields under column "row".
   */
  def zipWithUniqueId(df: DataFrame): DataFrame = {
    df.select(
      struct(df.columns.map(c => df(c)) :_*).as("row"),
      functions.monotonicallyIncreasingId().as("uniq_id"))
  }
}
