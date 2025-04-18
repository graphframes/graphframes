package org.graphframes.catalyst

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.MapType
import org.graphframes.GraphFramesUnreachableException

private[graphframes] object GraphFramesFunctions {
  def keyWithMaxValue(mapCol: Column): Column = new Column(KeyWithMaxValue(mapCol.expr))
}

private[graphframes] case class KeyWithMaxValue(child: Expression)
    extends UnaryExpression
    with CodegenFallback {

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(newChild)

  override def dataType: DataType = child.dataType match {
    case t: MapType => t.valueType
    case _: DataType => throw new GraphFramesUnreachableException()
  }

  override protected def nullSafeEval(input: Any): Any = {
    input match {
      case map: Map[Long, Int] @unchecked => map.maxBy { case (key, value) => (value, key) }._1
      case _ => throw new GraphFramesUnreachableException()
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, eval => s"${eval}.maxBy{ case (key, value) => (value, key) }._1)")
}
