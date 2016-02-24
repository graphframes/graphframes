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

import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object SQLHelpers {
  def getExpr(col: Column): Expression = col.expr

  def expr(e: String): Column = new Column(new SqlParser().parseExpression(e))

  /**
   * Appends each record with a unique ID (uniq_id) and groups existing fields under column "row".
   * This is a workaround for SPARK-9020 and SPARK-13473.
   */
  def zipWithUniqueId(df: DataFrame): DataFrame = {
    val sqlContext = df.sqlContext
    val schema = df.schema
    val rdd = df.rdd.zipWithUniqueId().map { case (row, id) =>
      Row(row, id)
    }
    val outputSchema = StructType(Seq(
      StructField("row", schema, false), StructField("uniq_id", LongType, false)))
    sqlContext.createDataFrame(rdd, outputSchema)
  }
}
