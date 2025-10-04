package org.graphframes.lib

import org.graphframes.GraphFrame
import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.SparkFunSuite

import scala.annotation.nowarn
import scala.collection.mutable

class DetectingCyclesSuite extends SparkFunSuite with GraphFrameTestSparkContext {
  test("test detecting cycles") {
    val graph = GraphFrame(
      spark
        .createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"), (4L, "d"), (5L, "e")))
        .toDF("id", "attr"),
      spark
        .createDataFrame(Seq((1L, 2L), (2L, 3L), (3L, 1L), (1L, 4L), (2L, 5L)))
        .toDF("src", "dst"))
    val res = graph.detectingCycles.setUseLocalCheckpoints(true).run()
    assert(res.count() == 3)
    @nowarn val collected =
      res
        .sort(GraphFrame.ID)
        .select(DetectingCycles.foundSeqCol)
        .collect()
        .map(r => r.getAs[mutable.WrappedArray[Long]](0))

    assert(collected(0) == Seq(1, 2, 3, 1))
    assert(collected(1) == Seq(2, 3, 1, 2))
    assert(collected(2) == Seq(3, 1, 2, 3))
    res.unpersist()
  }

  test("test no cycles") {
    val graph = GraphFrame(
      spark
        .createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"), (4L, "d"), (5L, "e")))
        .toDF("id", "attr"),
      spark
        .createDataFrame(Seq((1L, 2L), (2L, 3L), (3L, 4L), (4L, 5L)))
        .toDF("src", "dst"))
    val res = graph.detectingCycles.setUseLocalCheckpoints(true).run()
    assert(res.count() == 0)
    res.unpersist()
  }

  test("test multiple cycles from one source") {
    val graph = GraphFrame(
      spark
        .createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"), (4L, "d"), (5L, "e")))
        .toDF("id", "attr"),
      spark
        .createDataFrame(Seq((1L, 2L), (2L, 1L), (1L, 3L), (3L, 1L), (2L, 5L), (5L, 1L)))
        .toDF("src", "dst"))
    val res = graph.detectingCycles.setUseLocalCheckpoints(true).run()
    assert(res.count() == 7)
    @nowarn val collected =
      res
        .sort(GraphFrame.ID, DetectingCycles.foundSeqCol)
        .select(DetectingCycles.foundSeqCol)
        .collect()
        .map(r => r.getAs[mutable.WrappedArray[Long]](0))
    assert(collected(0) == Seq(1, 2, 1))
    assert(collected(1) == Seq(1, 2, 5, 1))
    assert(collected(2) == Seq(1, 3, 1))
    assert(collected(3) == Seq(2, 1, 2))
    assert(collected(4) == Seq(2, 5, 1, 2))
    assert(collected(5) == Seq(3, 1, 3))
    assert(collected(6) == Seq(5, 1, 2, 5))
    res.unpersist()
  }
}
