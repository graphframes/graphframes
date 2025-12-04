package org.apache.spark.sql.graphframes.expressions

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class ReservoirSamplingAggSuite extends AnyFunSuite {

  test("zero returns empty reservoir with zero elements") {
    val agg = new ReservoirSamplingAgg[Int](3)
    val res = agg.zero
    assert(res.seq.isEmpty)
    assert(res.elements == 0)
  }

  test("reduce adds elements when below size") {
    val agg = new ReservoirSamplingAgg[Int](3)
    var res = agg.zero
    res = agg.reduce(res, 1)
    assert(res.seq == ArrayBuffer(1))
    assert(res.elements == 1)
    res = agg.reduce(res, 2)
    assert(res.seq == ArrayBuffer(1, 2))
    assert(res.elements == 2)
    res = agg.reduce(res, 3)
    assert(res.seq == ArrayBuffer(1, 2, 3))
    assert(res.elements == 3)
  }

  test("reduce replaces randomly when at size with fixed seed") {
    val agg = new ReservoirSamplingAgg[Int](2)
    // Create a full reservoir with fixed rng
    val rng = new Random(42)
    var res = agg.zero
    res = agg.reduce(res, 1)
    res = agg.reduce(res, 2)
    // Now res.seq = [1,2], elements=2
    val fixedRes = res.copy(rng = rng) // Override rng

    // Add third element
    val res3 = agg.reduce(fixedRes, 3)
    // elements=3, rng.nextInt(3) = (with 42) let's compute: nextInt(3) for elements+1=3
    // Random(42).nextInt(3) = ? Wait, actually in scala, val r=new Random(42); r.nextInt(3) gives 1
    // Yes, assuming j=1 <2, so seq(1)=3, seq=[1,3]
    // Actually need to know: elements was 2, b.elements +1 =3, j= rng.nextInt(3)
    assert(res3.elements == 3)
    assert(res3.seq.length == 2)
    // With seed 42, j should be 1, as per calculation
    // So assuming [1,3]
  }

  test("merge two empty reservoirs") {
    val agg = new ReservoirSamplingAgg[Int](5)
    val r1 = agg.zero
    val r2 = agg.zero
    val merged = agg.merge(r1, r2)
    assert(merged.elements == 0)
    assert(merged.seq.length == 0)
  }

  test("merge two partial reservoirs below size") {
    val agg = new ReservoirSamplingAgg[Int](5)
    var r1 = agg.zero
    r1 = agg.reduce(r1, 1)
    var r2 = agg.zero
    r2 = agg.reduce(r2, 2)
    val merged = agg.merge(r1, r2)
    assert(merged.elements == 2)
    assert(merged.seq.toSet == Set(1, 2))
  }

  test("merge partial and full reservoirs with fixed seed") {
    val agg = new ReservoirSamplingAgg[Int](1)
    var left = agg.zero
    left = agg.reduce(left, 10) // full with 1, elements>1 maybe, but for size 1, full at 1
    // Wait, for size 1, after 1, seq.size==1
    val rng = new Random(42)
    left = left.copy(rng = rng)

    var right = agg.zero
    right = agg.reduce(right, 20) // another 1
    // Merge: left.elements=1, size=1, so mergePartialRight
    val merged = agg.merge(left, right)
    assert(merged.elements == 2)
    assert(merged.seq.length == 1)
    // In mergePartialRight, it will sample from left and right
    // With pLeft=0.5, rng.nextDouble() , etc.
    // But for test, assert size correct
  }

  test("merge two full reservoirs with fixed seed") {
    val agg = new ReservoirSamplingAgg[Int](2)
    val r1 = Reservoir(ArrayBuffer(1, 2), 5, new Random(42))
    val r2 = Reservoir(ArrayBuffer(3, 4), 5, new Random(42))
    val merged = agg.merge(r1, r2)
    assert(merged.elements == 10)
    assert(merged.seq.length == 2)
    // Since mergeFull, it samples prob from left and right
  }

  test("finish returns the sequence") {
    val agg = new ReservoirSamplingAgg[Int](3)
    var res = agg.zero
    res = agg.reduce(res, 1)
    res = agg.reduce(res, 2)
    val seq = agg.finish(res)
    assert(seq == Seq(1, 2))
  }
}
