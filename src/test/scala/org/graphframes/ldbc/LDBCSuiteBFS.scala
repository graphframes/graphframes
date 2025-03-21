package org.graphframes.ldbc

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.graphframes.examples.LDBCUtils
import org.graphframes.{GraphFrame, GraphFrameTestSparkContext, SparkFunSuite}

class LDBCSuiteBFS extends SparkFunSuite with GraphFrameTestSparkContext {
  test("Test LDBC BFS with GraphFrames") {
    assume(sys.env.contains("LDBC_TEST_ROOT"))
    LDBCUtils.downloadLDBCIfNotExists()

    val g = LDBCUtils.getLDBCGraph(spark, makeUndirected = false)
    val expectedBFSResults = LDBCUtils.getBFSExpectedResults(spark)
    val bfsTargetVertex = LDBCUtils.getBFSTarget

    val result = g.shortestPaths
      .landmarks(Seq(bfsTargetVertex))
      .runInGraphFrames(isDirected = false)
      .select(
        col(GraphFrame.ID),
        col("distances").getItem(bfsTargetVertex).cast(LongType).alias("distance"))
      .na
      .fill(Map("distance" -> LDBCUtils.UNREACHABLE_ID_VALUE))

    val nonMatchedRows = result
      .join(expectedBFSResults, Seq(GraphFrame.ID), "left")
      .select(
        col(GraphFrame.ID),
        col("distance"),
        col("expectedDistance"),
        (col("distance") === col("expectedDistance")).alias("matched"))
      .collect()
      .filterNot(_.getAs[Boolean]("matched"))
      .map(r =>
        (
          r.getAs[Long](GraphFrame.ID),
          r.getAs[Long]("distance"),
          r.getAs[Long]("expectedDistance")))

    assert(nonMatchedRows.isEmpty)
  }

  test("Test LDBC BFS with GraphX") {
    assume(sys.env.contains("LDBC_TEST_ROOT"))
    LDBCUtils.downloadLDBCIfNotExists()

    val g = LDBCUtils.getLDBCGraph(spark)
    val expectedBFSResults = LDBCUtils.getBFSExpectedResults(spark)
    val bfsTargetVertex = LDBCUtils.getBFSTarget

    val result = g.shortestPaths
      .landmarks(Seq(bfsTargetVertex))
      .run()
      .select(
        col(GraphFrame.ID),
        col("distances").getItem(bfsTargetVertex).cast(LongType).alias("distance"))
      .na
      .fill(Map("distance" -> LDBCUtils.UNREACHABLE_ID_VALUE))

    val nonMatchedRows = result
      .join(expectedBFSResults, Seq(GraphFrame.ID), "left")
      .select(
        col(GraphFrame.ID),
        col("distance"),
        col("expectedDistance"),
        (col("distance") === col("expectedDistance")).alias("matched"))
      .collect()
      .filterNot(_.getAs[Boolean]("matched"))
      .map(r =>
        (
          r.getAs[Long](GraphFrame.ID),
          r.getAs[Long]("distance"),
          r.getAs[Long]("expectedDistance")))

    assert(nonMatchedRows.isEmpty)
  }
}
