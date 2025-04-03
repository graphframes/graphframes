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

package org.apache.spark.sql.graphframes

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.{DataFrame => ClassicDataFrame, Dataset, ExpressionUtils, SparkSession => ClassicSparkSession}
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.functions.{col, expr}

import org.graphframes.{GraphFrame, Logging}
import org.graphframes.GraphFrame.nestAsCol

object SparkShims {

  /**
   * Apply the given SQL expression (such as `id = 3`) to the field in a column, rather than to
   * the column itself.
   *
   * @param expr
   *   SQL expression, such as `id = 3`
   * @param colName
   *   Column name, such as `myVertex`
   * @return
   *   SQL expression applied to the column fields, such as `myVertex.id = 3`
   */
  def applyExprToCol(spark: SparkSession, expr: Column, colName: String) = {
    val converted = spark.asInstanceOf[ClassicSparkSession].converter(expr.node)
    ExpressionUtils.column(converted.transform { case UnresolvedAttribute(nameParts) =>
      UnresolvedAttribute(colName +: nameParts)
    })
  }

  def createDataFrame(spark: SparkSession, plan: LogicalPlan): DataFrame = {
    Dataset.ofRows(spark.asInstanceOf[ClassicSparkSession], plan)
  }

  def planFromDataFrame(df: DataFrame): LogicalPlan = {
    df.asInstanceOf[ClassicDataFrame].logicalPlan
  }

  def createColumn(expr: Expression): Column = {
    Column(expr)
  }
}
