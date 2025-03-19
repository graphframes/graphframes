package org.graphframes.ldbc

import org.graphframes.{GraphFrame, GraphFrameTestSparkContext, SparkFunSuite}
import org.graphframes.examples.LDBCUtils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType

class LDBCSuiteBFS extends SparkFunSuite with GraphFrameTestSparkContext {
  test("Test LDBC BFS") {
    assume(sys.env.contains("LDBC_TEST_ROOT"))
    LDBCUtils.downloadLDBCIfNotExists()

    val enabled = spark.conf.getOption("spark.sql.adaptive.enabled")
    try {
      // disable AQE
      spark.conf.set("spark.sql.adaptive.enabled", value = false)
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
    } finally {
      if (enabled.isDefined) {
        spark.conf.set("spark.sql.adaptive.enabled", value = enabled.get)
      } else {
        spark.conf.unset("spark.sql.adaptive.enabled")
      }
    }
  }
}
