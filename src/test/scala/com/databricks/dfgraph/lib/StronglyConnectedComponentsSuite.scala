package com.databricks.dfgraph.lib

import org.apache.spark.sql.Row

import com.databricks.dfgraph.{DFGraph, DFGraphTestSparkContext, SparkFunSuite}

class StronglyConnectedComponentsSuite extends SparkFunSuite with DFGraphTestSparkContext {
  test("Island Strongly Connected Components") {
    val vertices = sqlContext.createDataFrame((1L to 5L).map(Tuple1.apply)).toDF("id")
    val edges = sqlContext.createDataFrame(Seq.empty[(Long, Long)]).toDF("src", "dst")
    val graph = DFGraph(vertices, edges)
    val c = StronglyConnectedComponents.run(graph, 5)
    LabelPropagationSuite.testSchemaInvariants(graph, c)
    for (Row(id: Long, scc: Long) <- c.vertices.collect()) {
      assert(id === scc)
    }
  }
}
