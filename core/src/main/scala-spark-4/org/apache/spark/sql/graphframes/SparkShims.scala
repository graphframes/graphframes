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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GetStructField
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.ClassicConversions.*
import org.apache.spark.sql.classic.DataFrame as ClassicDataFrame
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.classic.ExpressionUtils
import org.apache.spark.sql.classic.SparkSession as ClassicSparkSession

import scala.collection.mutable

object SparkShims {

  /**
   * Extracts all column references from a Column expression, returning a map from top-level
   * prefix to the set of nested field names accessed under that prefix.
   *
   * For nested column references like "src.id" or "edge.weight", this returns Map("src" ->
   * Set("id"), "edge" -> Set("weight")). For top-level references like "src" (the whole struct),
   * it returns Map("src" -> Set()).
   *
   * This handles both unresolved expressions (UnresolvedAttribute, UnresolvedExtractValue) and
   * resolved expressions (AttributeReference, GetStructField).
   *
   * @param spark
   *   the SparkSession (needed for expression conversion in Spark 4)
   * @param expr
   *   the Column expression to analyze
   * @return
   *   a Map from column prefix to the set of nested field names accessed
   */
  def extractColumnReferences(spark: SparkSession, expr: Column): Map[String, Set[String]] = {
    val refs = mutable.Map.empty[String, mutable.Set[String]]

    def addRef(prefix: String, field: Option[String]): Unit = {
      val fields = refs.getOrElseUpdate(prefix, mutable.Set.empty[String])
      field.foreach(fields += _)
    }

    val converted = spark.asInstanceOf[ClassicSparkSession].converter(expr.node)
    converted.foreach {
      // Unresolved: col("src.id") or Pregel.src("id") -> UnresolvedAttribute(Seq("src", "id"))
      case UnresolvedAttribute(nameParts) if nameParts.nonEmpty =>
        addRef(nameParts.head, nameParts.lift(1))

      // Unresolved: col("src")("id") -> UnresolvedExtractValue
      case UnresolvedExtractValue(child, extraction) =>
        child match {
          case UnresolvedAttribute(nameParts) if nameParts.nonEmpty =>
            extraction match {
              case Literal(fieldName: String, _) => addRef(nameParts.head, Some(fieldName))
              case _ => addRef(nameParts.head, None) // Unknown field access
            }
          case _ => // Nested extraction we can't easily parse
        }

      // Resolved: AttributeReference for top-level columns
      case attr: AttributeReference =>
        addRef(attr.name, None)

      // Resolved: GetStructField for nested field access like struct.field
      case GetStructField(child, _, Some(fieldName)) =>
        child match {
          case attr: AttributeReference => addRef(attr.name, Some(fieldName))
          case _ => // Nested struct access we can't easily parse
        }

      case _ => // ignore other expression types
    }

    refs.map { case (k, v) => k -> v.toSet }.toMap
  }

  /**
   * Extracts all top-level column name prefixes from a Column expression.
   *
   * For nested column references like "src.id" or "edge.weight", this extracts the first
   * component ("src", "edge"). This is useful for analyzing which struct columns are referenced
   * in an expression.
   *
   * @param spark
   *   the SparkSession (needed for expression conversion in Spark 4)
   * @param expr
   *   the Column expression to analyze
   * @return
   *   a Set of column name prefixes found in the expression
   */
  def extractColumnPrefixes(spark: SparkSession, expr: Column): Set[String] =
    extractColumnReferences(spark, expr).keySet

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
  def applyExprToCol(spark: SparkSession, expr: Column, colName: String): Column = {
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
