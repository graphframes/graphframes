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

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.annotation.nowarn
import scala.collection.mutable

object SparkShims {

  /**
   * Extracts all top-level column name prefixes from a Column expression.
   *
   * For nested column references like "src.id" or "edge.weight", this extracts the first
   * component ("src", "edge"). This is useful for analyzing which struct columns are referenced
   * in an expression.
   *
   * @param spark
   *   the SparkSession (unused in Spark 3, included for API compatibility with Spark 4)
   * @param expr
   *   the Column expression to analyze
   * @return
   *   a Set of column name prefixes found in the expression
   */
  @nowarn
  def extractColumnPrefixes(spark: SparkSession, expr: Column): Set[String] = {
    val prefixes = mutable.Set.empty[String]
    expr.expr.foreach {
      case UnresolvedAttribute(nameParts) if nameParts.nonEmpty =>
        prefixes += nameParts.head
      case _ => // ignore other expression types
    }
    prefixes.toSet
  }

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
  @nowarn
  def applyExprToCol(spark: SparkSession, expr: Column, colName: String): Column = {
    new Column(expr.expr.transform { case UnresolvedAttribute(nameParts) =>
      UnresolvedAttribute(colName +: nameParts)
    })
  }

  def createDataFrame(spark: SparkSession, plan: LogicalPlan): DataFrame = {
    Dataset.ofRows(spark, plan)
  }

  def planFromDataFrame(df: DataFrame): LogicalPlan = {
    df.logicalPlan
  }

  def createColumn(expr: Expression): Column = {
    Column(expr)
  }
}
