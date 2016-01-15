package com.databricks.dfgraph.lib

import com.databricks.dfgraph.{DFGraph, DFGraphTestSparkContext, SparkFunSuite}
import org.apache.spark.sql.{Row, DataFrame}

import scala.collection.generic.SeqFactory

class ConnectedComponentsSuite extends SparkFunSuite with DFGraphTestSparkContext {
  @transient var v: DataFrame = _
  @transient var e: DataFrame = _
  @transient var g: DFGraph = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    v = sqlContext.createDataFrame(List(
      (0L, "a", "f"),
      (1L, "b", "m"),
      (2L, "c", "m"),
      (3L, "d", "f"))).toDF("id", "attr", "gender")
    e = sqlContext.createDataFrame(List(
      (0L, 1L, "friend"),
      (1L, 2L, "friend"),
      (2L, 3L, "follow"),
      (2L, 0L, "unknown"))).toDF("src", "dst", "relationship")
    g = DFGraph(v, e)
  }

  override def afterAll(): Unit = {
    v = null
    e = null
    g = null
    super.afterAll()
  }

  test("simple toy example") {
    val v = sqlContext.createDataFrame(List(
      (0L, "a", "b"))).toDF("id", "attr", "gender")
    // Create an empty dataframe with the proper columns.
    val e = sqlContext.createDataFrame(List((0L, 0L, 1L))).toDF("src", "dst", "test").filter("src > 10")
    val g = DFGraph(v, e)
    val comps = ConnectedComponents.run(g)
    assert(comps.vertices.count() === 1)
    assert(comps.edges.count() === 0)
    // We loose all the attributes for now, due to a limitation of the graphx implementation
    assert(comps.vertices.collect() === Seq(Row(0L, 0L)))
  }

  test("simple connected toy example") {
    val v = sqlContext.createDataFrame(List(
      (0L, "a0", "b0"),
      (1L, "a1", "b1"))).toDF("id", "A", "B")
    val e = sqlContext.createDataFrame(List(
      (0L, 1L, "a01", "b01"))).toDF("src", "dst", "A", "B")
    val g = DFGraph(v, e)
    val comps = ConnectedComponents.run(g)
    assert(comps.vertices.count() === 2)
    assert(comps.edges.count() === 1)
    // We loose all the attributes for now, due to a limitation of the graphx implementation
    assert(List(Row(0L, 1L, "a01", "b01")) === comps.edges.collect())
    assert(List(Row(0L, 0L), Row(1L, 0L)) === comps.vertices.collect())
  }

}
