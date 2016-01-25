package com.databricks.dfgraph.lib

import com.databricks.dfgraph.{DFGraph, DFGraphTestSparkContext, SparkFunSuite}

class LabelPropagationSuite extends SparkFunSuite with DFGraphTestSparkContext {

  val n = 5

  // A graph with 2 cliques connected by a single link.
  def create(): DFGraph = {
    val edges1 = for (v1 <- 0 until n; v2 <- 0 until n) yield (v1.toLong, v2.toLong, s"$v1-$v2")
    val edges2 = for {
      v1 <- n until (2 * n)
      v2 <- n until (2 * n) } yield (v1.toLong, v2.toLong, s"$v1-$v2")
    val edges = edges1 ++ edges2 :+ (0L, n.toLong, s"0-$n")
    val vertices = (0 until (2 * n)).map { v => (v.toLong, s"$v", v) }
    val e = sqlContext.createDataFrame(edges).toDF("src", "dst", "e_attr1")
    val v = sqlContext.createDataFrame(vertices).toDF("id", "v_attr1", "v_attr2")
    DFGraph(v, e)
  }

  test("Toy example") {
    val g = create()
    val labels = LabelPropagation.run(g, 4 * n)
    LabelPropagationSuite.testSchemaInvariants(g, labels)
    val clique1 = labels.vertices.filter(s"id < $n").select("label").collect().toSeq.map(_.getLong(0)).toSet
    assert(clique1.size === 1)
    val clique2 = labels.vertices.filter(s"id >= $n").select("label").collect().toSeq.map(_.getLong(0)).toSet
    assert(clique2.size === 1)
    assert(clique1 !== clique2)
  }
}

object LabelPropagationSuite {
  import DFGraph._
  def testSchemaInvariant(g: DFGraph): Unit = {
    // The ID should be present
    val vs = g.vertices.schema
    val es = g.edges.schema
    vs(ID)
    es(SRC)
    es(DST)
  }
  // Runs some tests on a transform of the DFGraph
  def testSchemaInvariants(before: DFGraph, after: DFGraph): Unit = {
    testSchemaInvariant(before)
    testSchemaInvariant(after)
    // The IDs, source and destination columns should be of the same type
    // with the same metadata.
    assert(before.vertices.schema(ID) == after.vertices.schema(ID))
    assert(before.edges.schema(SRC) == after.edges.schema(SRC))
    assert(before.edges.schema(DST) == after.edges.schema(DST))
    // All the columns before should be found after (with some extra columns,
    // potentially).
    for (f <- before.vertices.schema.iterator) {
      assert(before.vertices.schema(f.name) == after.vertices.schema(f.name),
      s"${before.vertices.schema} != ${after.vertices.schema}")
    }

    for (f <- before.edges.schema.iterator) {
      assert(before.edges.schema(f.name) == after.edges.schema(f.name),
        s"${before.edges.schema} != ${after.edges.schema}")
    }
  }
}