package org.apache.spark.sql.graphframes.expressions

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import scala.reflect.runtime.universe.TypeTag

import collection.mutable.ArrayBuffer

case class Reservoir[T](seq: ArrayBuffer[T], elements: Int) extends Serializable

case class ReservoirSamplingAgg[T: TypeTag](size: Int)
    extends Aggregator[T, Reservoir[T], Seq[T]]
    with Serializable {

  override def zero: Reservoir[T] = Reservoir[T](ArrayBuffer.empty, 0)

  override def reduce(b: Reservoir[T], a: T): Reservoir[T] = {
    if (b.seq.size < size) {
      Reservoir(b.seq += a, b.elements + 1)
    } else {
      val j = java.util.concurrent.ThreadLocalRandom.current().nextInt(b.elements + 1)
      if (j < size) {
        b.seq(j) = a
      }
      Reservoir(b.seq, b.elements + 1)
    }
  }

  private def mergeFull(left: Reservoir[T], right: Reservoir[T]): Reservoir[T] = {
    val total_cnt = left.elements + right.elements
    val rng = java.util.concurrent.ThreadLocalRandom.current()
    val pLeft = left.elements.toDouble / total_cnt.toDouble

    var newSeq = ArrayBuffer.empty[T]
    val leftCloned = left.seq.clone()
    val rightCloned = right.seq.clone()
    for (_ <- (1 to size)) {
      if (rng.nextDouble() <= pLeft) {
        newSeq = newSeq += leftCloned.remove(rng.nextInt(leftCloned.size))
      } else {
        newSeq = newSeq += rightCloned.remove(rng.nextInt(rightCloned.size))
      }
    }

    Reservoir(newSeq, total_cnt)
  }

  private def mergeTwoPartial(left: Reservoir[T], right: Reservoir[T]): Reservoir[T] = {
    val total_cnt = left.elements + right.elements
    val rng = java.util.concurrent.ThreadLocalRandom.current()
    if (total_cnt <= size) {
      Reservoir(left.seq ++ right.seq, total_cnt)
    } else {
      val currElements = left.seq ++ right.seq.slice(0, size - left.elements)
      var currSize = size + 1

      for (i <- ((size - left.elements) to right.elements)) {
        val j = rng.nextInt(currSize)
        if (j < size) {
          currElements(j) = right.seq(i)
        }
        currSize += 1
      }

      Reservoir(currElements, currSize)
    }
  }

  private def mergePartialRight(left: Reservoir[T], right: Reservoir[T]): Reservoir[T] = {
    val total_cnt = left.elements + right.elements
    val pLeft = left.elements.toDouble / total_cnt.toDouble
    val currElements = ArrayBuffer.empty[T]
    val rng = java.util.concurrent.ThreadLocalRandom.current()

    // TODO: I'm nor actually sure
    // that we need to clone it.
    // Does Spark handle it by itself?
    // Is there any chance the link shared between tasks?
    val clonedLeft = left.seq.clone()
    val clonedRight = right.seq.clone()
    for (_ <- (1 to size)) {
      if ((clonedRight.isEmpty) || (rng.nextDouble() <= pLeft)) {
        val idx = rng.nextInt(clonedLeft.size)
        currElements += clonedLeft.remove(idx)
      } else {
        val idx = rng.nextInt(clonedRight.size)
        currElements += clonedRight.remove(idx)
      }
    }

    Reservoir(currElements, total_cnt)
  }

  override def merge(b1: Reservoir[T], b2: Reservoir[T]): Reservoir[T] = {
    val (left, right) = if (b1.seq.size > b2.seq.size) {
      (b1, b2)
    } else {
      (b2, b1)
    }

    if (left.elements < size) {
      mergeTwoPartial(left, right)
    } else if (right.elements < size) {
      mergePartialRight(left, right)
    } else {
      mergeFull(left, right)
    }
  }

  override def finish(reduction: Reservoir[T]): Seq[T] = reduction.seq.toSeq

  override def bufferEncoder: Encoder[Reservoir[T]] = Encoders.product

  override def outputEncoder: Encoder[Seq[T]] = ExpressionEncoder[Seq[T]]
}
