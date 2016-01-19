package com.databricks.dfgraph.lib

import org.apache.spark.sql.Row

import com.databricks.dfgraph.{DFGraph, DFGraphTestSparkContext, SparkFunSuite}

class TriangleCountSuite extends SparkFunSuite with DFGraphTestSparkContext {

  test("Count a single triangle") {
    val edges = sqlContext.createDataFrame(Array(0L -> 1L, 1L -> 2L, 2L -> 0L)).toDF("src", "dst")
    val vertices = sqlContext.createDataFrame(Seq((0L, ""), (1L, ""), (2L, ""))).toDF("id", "v_attr1")
    val g = DFGraph(vertices, edges)
    val g2 = TriangleCount.run(g)
    g2.vertices.collect().foreach { case Row(vid: Long, count: Int) => assert(count === 1) }
  }

}
