package org.apache.spark.sql.graphframes.expressions

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udaf
import org.apache.spark.sql.types.*
import org.apache.spark.sql.types.DataType
import org.graphframes.GraphFramesUnsupportedVertexTypeException
import org.graphframes.internal.CollectionCompat

import scala.collection.Searching.*
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe.TypeTag

case class KMinSampling[T: TypeTag](size: Int)(implicit ord: Ordering[T])
    extends Aggregator[Row, ArrayBuffer[(T, Long)], Seq[T]]
    with Serializable {

  override def zero: ArrayBuffer[(T, Long)] = ArrayBuffer.empty

  // tie-breaking by the vertex ID
  private val ordering = new Ordering[(T, Long)] {
    override def compare(x: (T, Long), y: (T, Long)): Int = {
      val wCmp = java.lang.Long.compare(x._2, y._2)
      if (wCmp != 0) wCmp else ord.compare(x._1, y._1)
    }
  }

  override def reduce(b: ArrayBuffer[(T, Long)], a: Row): ArrayBuffer[(T, Long)] = {
    // fast-path: buffer is already full of "strong" elements
    // the case of "inflencer" vertex
    if (b.size >= size) {
      val last = b.last
      val wCmp = java.lang.Long.compare(a.getAs[Long](1), last._2)
      if (wCmp > 0) return b
      if (wCmp == 0 && ord.compare(a.getAs[T](0), last._1) >= 0) return b
    }

    // fast-path failed; long-path
    val searchRes: SearchResult = b.search((a.getAs[T](0), a.getAs[Long](1)))(ordering)
    val idx = searchRes.insertionPoint

    b.insert(idx, (a.getAs[T](0), a.getAs[Long](1)))

    if (b.size > size) {
      b.remove(size, b.size - size)
    }
    b
  }

  override def merge(
      b1: ArrayBuffer[(T, Long)],
      b2: ArrayBuffer[(T, Long)]): ArrayBuffer[(T, Long)] = {

    b1 ++= b2
    // Collections.sort for 2.12 and sortInPlace for 2.13
    CollectionCompat.sortBuffer(b1, ordering)

    if (b1.size > size) {
      b1.remove(size, b1.size - size)
    }

    b1
  }

  override def finish(reduction: ArrayBuffer[(T, Long)]): Seq[T] = reduction.map(_._1).toSeq
  // TODO: replace by Kryo after 4.0.2 is released, see SPARK-52819
  override def bufferEncoder: Encoder[ArrayBuffer[(T, Long)]] =
    Encoders.javaSerialization[ArrayBuffer[(T, Long)]]
  override def outputEncoder: Encoder[Seq[T]] = ExpressionEncoder[Seq[T]]()
}

object KMinSampling extends Serializable {
  def getEncoder(spark: SparkSession, dataType: DataType, colNames: Seq[String]): Encoder[Row] = {
    // That is very stupid way actually. But it is the only way with public API
    spark
      .createDataFrame(
        java.util.List.of[Row](),
        StructType(
          StructField(colNames(0), dataType) :: StructField(colNames(1), LongType) :: Nil))
      .encoder
  }

  def fromSparkType(dataType: DataType, size: Int, encoder: Encoder[Row]): UserDefinedFunction = {
    dataType match {
      case StringType => udaf(KMinSampling[java.lang.String](size), encoder)
      case ShortType => udaf(KMinSampling[java.lang.Short](size), encoder)
      case ByteType => udaf(KMinSampling[java.lang.Byte](size), encoder)
      case IntegerType => udaf(KMinSampling[java.lang.Integer](size), encoder)
      case LongType => udaf(KMinSampling[java.lang.Long](size), encoder)
      case _ => throw new GraphFramesUnsupportedVertexTypeException("unsupported vertex type")
    }
  }
}
