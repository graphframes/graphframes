package com.databricks.dfgraph.lib

import com.databricks.dfgraph.{DFGraph, DFGraphTestSparkContext, SparkFunSuite}
import org.apache.spark.sql.Row

/**
 * Created by tjhunter on 1/18/16.
 */
class ShortestPathsSuite extends SparkFunSuite with DFGraphTestSparkContext {

  test("Simple test") {
    val shortestPaths = Set(
      (1, Map(1 -> 0, 4 -> 2)), (2, Map(1 -> 1, 4 -> 2)), (3, Map(1 -> 2, 4 -> 1)),
      (4, Map(1 -> 2, 4 -> 0)), (5, Map(1 -> 1, 4 -> 1)), (6, Map(1 -> 3, 4 -> 1)))
    val edgeSeq = Seq((1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)).flatMap {
      case e => Seq(e, e.swap)
    } .map { case (v1, v2) => (v1.toLong, v2.toLong) }
    val edges = sqlContext.createDataFrame(edgeSeq).toDF("src", "dst")
    val v = edgeSeq.flatMap(e => e._1 :: e._2 :: Nil).map(Tuple1.apply)

    val vertices = sqlContext.createDataFrame(v).toDF("id")
    val graph = DFGraph(vertices, edges)
    val landmarks = Seq(1, 4).map(_.toLong)
    val newVs = ShortestPaths.run(graph, landmarks).vertices.collect().toSeq
    val results = newVs.map {
      case Row(v: Long, spMap: Map[Long, Int] @unchecked) =>
        (v, spMap.mapValues(i => i))
    }
    assert(results.toSet === shortestPaths)

  }
}
