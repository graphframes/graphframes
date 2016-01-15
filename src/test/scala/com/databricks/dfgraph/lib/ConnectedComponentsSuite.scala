package com.databricks.dfgraph.lib

import com.databricks.dfgraph.{DFGraph, DFGraphTestSparkContext, SparkFunSuite}
import org.apache.spark.sql.DataFrame

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

  test("empty query should return nothing") {
    val emptiness = g.find("")
    assert(emptiness.count() === 0)
  }
}
