package com.databricks.dfgraph

import org.apache.spark.graphx.Graph
import org.scalatest.FunSuite

class DFGraphSuite extends FunSuite with LocalSparkContext {
  test("instantiate DFGraph from VertexRDD and EdgeRDD") {
    withSpark { sc =>
      val ring = (0L to 100L).zip((1L to 99L) :+ 0L)
      val doubleRing = ring ++ ring
      val graph = Graph.fromEdgeTuples(sc.parallelize(doubleRing), 1)
      val vtxRdd = graph.vertices
      val edgeRdd = graph.edges

      val graphBuiltFromDFGraph = DFGraph(vtxRdd, edgeRdd).toGraph()

      assert(graphBuiltFromDFGraph.vertices.count() === graph.vertices.count())
      assert(graphBuiltFromDFGraph.edges.count() === graph.edges.count())
    }
  }

}

