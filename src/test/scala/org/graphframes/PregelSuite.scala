package org.graphframes

import org.apache.spark.sql.functions._

class PregelSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  import testImplicits._

  def almostEqual(a: Double, b: Double, relTol: Double): Boolean = {
    return math.abs(a - b) <= math.abs(a) * relTol
  }

  test("page rank") {

    val vecDF1 = Seq(
      (0L, 1),
      (1L, 1),
      (2L, 2),
      (3L, 0),
      (4L, 3)
    ).toDF("id", "outDegree")

    val edgeDF1 = Seq(
      (0L, 1L, 2.0),
      (1L, 2L, 3.0),
      (2L, 3L, 1.0),
      (2L, 0L, 2.0),
      (4L, 0L, 3.0),
      (4L, 2L, 1.0),
      (4L, 3L, 2.0)
    ).toDF("src", "dst", "length")

    vecDF1.cache()
    edgeDF1.cache()

    val N = vecDF1.count()
    val graph1 = GraphFrame(vecDF1, edgeDF1)

    val pr_alpha = 0.85

    val pageRankResultDF = new Pregel(graph1)
      .setMaxIter(15)
      .withVertexColumn("rank", lit(1.0 / N), col(Pregel.MSG_COL_NAME))
      .sendMsgToDst(col("src.rank") / col("src.outDegree"))
      .aggMsgs(sum(col(Pregel.MSG_COL_NAME)) * lit(pr_alpha) + lit(1.0 - pr_alpha) / N)
      .run()

    val res = pageRankResultDF.sort(col("id"))
      .select("rank").as[Double].collect()
    res.zip(Array(0.194, 0.195, 0.252, 0.194, 0.2)).foreach { case (a, b) =>
      assert(almostEqual(a, b, relTol = 1e-2))
    }
  }

  test("chain propagation") {
    val n = 5
    val verDF = (1 to n).toDF("id").repartition(3)
    val edgeDF = (1 until n).map(x => (x, x + 1))
      .toDF("src", "dst").repartition(3)

    val graph = GraphFrame(verDF, edgeDF)

    val resultDF = graph.pregel
      .setMaxIter(n - 1)
      .withVertexColumn("value",
        when(col("id") === lit(1), lit(1)).otherwise(lit(0)),
        when(Pregel.msg > col("value"), Pregel.msg).otherwise(col("value"))
      )
      .sendMsgToDst(
        when(Pregel.dst("value") =!= Pregel.src("value"), Pregel.src("value"))
      )
      .aggMsgs(max(Pregel.msg))
      .run()

    assert(resultDF.select("value").as[Int].collect() === Array.fill(n)(1))
  }

}
