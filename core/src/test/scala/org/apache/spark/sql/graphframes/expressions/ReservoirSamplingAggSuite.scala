package org.apache.spark.sql.graphframes.expressions

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer

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
    var res = agg.zero
    res = agg.reduce(res, 1)
    res = agg.reduce(res, 2)
    // Now res.seq = [1,2], elements=2
    val fixedRes = res.copy()

    // Add third element
    val res3 = agg.reduce(fixedRes, 3)
    assert(res3.elements == 3)
    assert(res3.seq.length == 2)
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
    left = agg.reduce(left, 10)
    left = left.copy()

    var right = agg.zero
    right = agg.reduce(right, 20)
    val merged = agg.merge(left, right)
    assert(merged.elements == 2)
    assert(merged.seq.length == 1)
  }

  test("merge two full reservoirs with fixed seed") {
    val agg = new ReservoirSamplingAgg[Int](2)
    val r1 = Reservoir(ArrayBuffer(1, 2), 5)
    val r2 = Reservoir(ArrayBuffer(3, 4), 5)
    val merged = agg.merge(r1, r2)
    assert(merged.elements == 10)
    assert(merged.seq.length == 2)
  }

  test("finish returns the sequence") {
    val agg = new ReservoirSamplingAgg[Int](3)
    var res = agg.zero
    res = agg.reduce(res, 1)
    res = agg.reduce(res, 2)
    val seq = agg.finish(res)
    assert(seq == Seq(1, 2))
  }

  test("uniformity of sampling") {
    // WARNING!
    // this test is slightly non determenistic
    // in a very rare case (1 from 50) it may fail
    // so just re-run it.
    val numElements = 5000
    val numSamples = 5000
    val sampleSize = 500
    val sequence = (1 to numElements).toArray

    // Count frequencies of each element across all samples
    val frequencyMap = scala.collection.mutable.Map[Int, Int]()
    for (i <- 0 until numElements) {
      frequencyMap += (i + 1 -> 0)
    }

    // Perform multiple samplings
    for (_ <- 0 until numSamples) {
      val agg = new ReservoirSamplingAgg[Int](sampleSize)
      var res = agg.zero

      // Fill reservoir with all elements in sequence
      for (element <- sequence) {
        res = agg.reduce(res, element)
      }

      // Collect sampled elements
      val sampled = agg.finish(res)
      for (element <- sampled) {
        frequencyMap(element) += 1
      }
    }

    // Check uniformity - each element should be sampled roughly the same number of times
    val expectedFreq = numSamples * sampleSize.toDouble / numElements
    val tolerance = 0.2 // 20% tolerance
    val minExpected = expectedFreq * (1 - tolerance)
    val maxExpected = expectedFreq * (1 + tolerance)

    for ((element, count) <- frequencyMap) {
      assert(
        count >= minExpected && count <= maxExpected,
        s"Element $element was sampled $count times, expected between $minExpected and $maxExpected")
    }
  }
}
