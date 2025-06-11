package org.apache.spark.sql.graphframes

import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.classic.ExpressionUtils
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}

object GraphFramesHelpers {
  def applyExprToCol(spark: SparkSession, expr: Column, colName: String): Column = {
    // Code is provided by github.com/Kimahriman
    val converted = spark.asInstanceOf[ClassicSparkSession].converter(expr.node)
    ExpressionUtils.column(converted.transform { case UnresolvedAttribute(nameParts) =>
      UnresolvedAttribute(colName +: nameParts)
    })
  }
}
