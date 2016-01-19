package com.databricks.dfgraph.lib

import com.databricks.dfgraph.{DFGraph, DFGraphTestSparkContext, SparkFunSuite}

class PageRankSuite extends SparkFunSuite with DFGraphTestSparkContext {

  val n = 100

  def starGraph(): DFGraph  = {
    val vertices = sqlContext.createDataFrame(Seq((0, "root")) ++ (1 to n).map { i =>
      (i, s"node-$i")
    }).toDF("id", "v_attr1")
    val edges = sqlContext.createDataFrame((1 to n).map { i =>
      (i, 0, s"edge-$i")
    }).toDF("src", "dst", "e_attr1")
    DFGraph(vertices, edges)
  }

  test("Star example") {
    val g = starGraph()
    val resetProb = 0.15
    val errorTol = 1.0e-5
    PageRank.runUntilConvergence(g, errorTol, resetProb)
  }
}
