package org.graphframes.ldbc

import org.graphframes.{GraphFrame, GraphFrameTestSparkContext, SparkFunSuite}
import org.graphframes.examples.LDBCUtils
import org.apache.spark.sql.functions.col

class LDBCSuiteCDLP extends SparkFunSuite with GraphFrameTestSparkContext {
  test("LDBC CDLP") {
    assume(sys.env.contains("LDBC_TEST_ROOT"))
    LDBCUtils.downloadLDBCIfNotExists()
    val g = LDBCUtils.getLDBCGraph(spark)
    val expectedCDLPResults = LDBCUtils.getCDLPExpectedResults(spark)
    val cdlpMaxIter = LDBCUtils.getCDLPMaxIter

    val result = g.labelPropagation.maxIter(cdlpMaxIter).run()
    val nonMatchedRows = result
      .join(expectedCDLPResults, Seq(GraphFrame.ID), "inner")
      .select(
        col(GraphFrame.ID),
        col("label"),
        col("expectedLabel"),
        (col("label") === col("expectedLabel")).alias("matched"))
      .collect()
      .filterNot(_.getAs[Boolean]("matched"))
      .map(r =>
        (r.getAs[Long](GraphFrame.ID), r.getAs[Long]("label"), r.getAs[Long]("expectedLabel")))

    assert(nonMatchedRows.isEmpty)
  }
}
