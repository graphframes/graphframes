package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression

object SQLHelpers {
  def getExpr(col: Column): Expression = col.expr
}
