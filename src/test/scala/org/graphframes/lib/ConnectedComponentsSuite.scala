/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.graphframes.lib

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

import org.graphframes._
import org.graphframes.examples.Graphs

class ConnectedComponentsSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  def compareImpl(g: GraphFrame, origResults: DataFrame): Unit = {
    def collectComps(results: DataFrame): Set[Array[String]] = {
      results.select("id", "component").rdd.map { case Row(id, component: Long) =>
        component -> id.toString
      }.groupByKey().values.map(_.toArray.sorted).collect().toSet
    }
    val origComps = collectComps(origResults)
    val newResults = ConnectedComponentsDF.run(g)
    val newComps = collectComps(newResults)
    assert(origComps.size === newComps.size)
    origComps.toArray.map(_.mkString(",")).sorted.zip(newComps.toArray.map(_.mkString(",")).sorted)
      .foreach { case (c1, c2) =>
        assert(c1 === c2)
      }
    assert(origResults.columns.sorted === newResults.columns.sorted)
  }
  /*

  test("single vertex") {
    val v = sqlContext.createDataFrame(List(
      (0L, "a", "b"))).toDF("id", "vattr", "gender")
    // Create an empty dataframe with the proper columns.
    val e = sqlContext.createDataFrame(List((0L, 0L, 1L))).toDF("src", "dst", "test")
      .filter("src > 10")
    val g = GraphFrame(v, e)
    val comps = ConnectedComponents.run(g)
    TestUtils.testSchemaInvariants(g, comps)
    TestUtils.checkColumnType(comps.schema, "component", DataTypes.LongType)
    assert(comps.count() === 1)
    assert(comps.select("id", "component", "vattr", "gender").collect()
      === Seq(Row(0L, 0L, "a", "b")))

    compareImpl(g, comps)
  }

  test("two connected vertices") {
    val v = sqlContext.createDataFrame(List(
      (0L, "a0", "b0"),
      (1L, "a1", "b1"))).toDF("id", "A", "B")
    val e = sqlContext.createDataFrame(List(
      (0L, 1L, "a01", "b01"))).toDF("src", "dst", "A", "B")
    val g = GraphFrame(v, e)
    val comps = g.connectedComponents.run()
    TestUtils.testSchemaInvariants(g, comps)
    assert(comps.count() === 2)
    val vxs = comps.sort("id").select("id", "component", "A", "B").collect()
    assert(List(Row(0L, 0L, "a0", "b0"), Row(1L, 0L, "a1", "b1")) === vxs)

    compareImpl(g, comps)
  }
  */

  test("friends graph") {
    val friends = examples.Graphs.friends
    val comps = friends.connectedComponents.run()

    compareImpl(friends, comps)
  }

  test("a few grid graphs") {
    val nComps = 1 // 3
    val n = 4
    def concat = udf { (a: String, b: String) => a + b }
    val edges: DataFrame = Range(0, nComps).map { i =>
      val compEdges: DataFrame = examples.Graphs.gridIsingModel(sqlContext, n).edges
      compEdges.select(concat.apply(col("src"), lit(i.toString)).as("src"),
        concat.apply(col("dst"), lit(i.toString)).as("dst"))
    }.reduce(_.unionAll(_))
    val g = GraphFrame.fromEdges(edges)
    val comps = g.connectedComponents.run()
    val numActualComps = comps.select("component").distinct.count()
    assert(nComps === numActualComps)
    compareImpl(g, comps)
  }

  test("handleSkew: 1 star") {
    val n = 20
    val g0 = Graphs.star(n)
    val g = GraphFrame(g0.vertices.select(col("id").cast("long")),
      g0.edges.select(col("src").cast("long"), col("dst").cast("long")))
    // Test handleSkew
    val (e, bigComponents) = ConnectedComponentsDF.handleSkew(g.edges, g.vertices.count(), 10, 0.1)
    val v = bigComponents.values.foldLeft(g.vertices.select("id")) { case (v_, c) => v_.except(c) }
    assert(v.count() === 1)
    assert(e.count() === 0)
    assert(bigComponents.size === 1)
    assert(bigComponents.head._1 === 0)
    val bigComp0: Array[Long] = bigComponents.head._2.select("id").map(_.getLong(0)).collect()
    assert(bigComp0.length === n)
    assert(!bigComp0.contains(0L))
    // Test connectedComponents
    val comps = ConnectedComponentsDF.run(g)
    val numActualComps = comps.select("component").distinct.count()
    assert(numActualComps === 1)
    compareImpl(g, comps)
  }

  test("handleSkew: multiple disconnected stars") {
    val n = 20
    val g0 = shiftVertexIds(Graphs.star(n).edges, 0)
    val g1 = shiftVertexIds(Graphs.star(n - 1).edges, n + 1)
    val g2 = shiftVertexIds(Graphs.star(n - 2).edges, 2 * n + 1)
    val edges = g0.edges.unionAll(g1.edges).unionAll(g2.edges)
      .select(col("src").cast("long"), col("dst").cast("long"))
    val g = GraphFrame.fromEdges(edges)
    // Test handleSkew
    val (e, bigComponents) = ConnectedComponentsDF.handleSkew(g.edges, g.vertices.count(), 10, 0.1)
    val v = bigComponents.values.foldLeft(g.vertices.select("id")) { case (v_, c) => v_.except(c) }
    assert(v.select("id").rdd.map(_.getLong(0)).collect().sorted === Array(0, n + 1, 2 * n + 1))
    assert(e.count() === 0)
    assert(bigComponents.size === 3)
    def checkComponent(vid: Long, size: Long): Unit = {
      assert(bigComponents.contains(vid))
      val comp: Array[Long] = bigComponents(vid).select("id").map(_.getLong(0)).collect()
      assert(comp.length === size)
      assert(!comp.contains(vid))
    }
    checkComponent(0, n)
    checkComponent(n + 1, n - 1)
    checkComponent(2 * n + 1, n - 2)
    // Test connectedComponents
    val comps = ConnectedComponentsDF.run(g)
    val numActualComps = comps.select("component").distinct.count()
    assert(numActualComps === 3)
    compareImpl(g, comps)
  }

  test("handleSkew: multiple connected stars with close connections") {
    // Connections are very close (center-to-center), so handleSkew should find a single component.
    val n = 20
    val (size0, size1, size2) = (n, n - 2, n - 4)
    val (base0, base1, base2: Int) = (0, size0, size0 + size1)
    val g0 = shiftVertexIds(Graphs.star(size0).edges, base0)
    val g1 = shiftVertexIds(Graphs.star(size1).edges, base1)
    val g2 = shiftVertexIds(Graphs.star(size2).edges, base2)
    val edges = g0.edges.unionAll(g1.edges).unionAll(g2.edges)
      .select(col("src").cast("long"), col("dst").cast("long"))
    val g = GraphFrame.fromEdges(edges)
    // Test handleSkew
    val (e, bigComponents) = ConnectedComponentsDF.handleSkew(g.edges, g.vertices.count(), 10, 0.1)
    val v = bigComponents.values.foldLeft(g.vertices.select("id")) { case (v_, c) => v_.except(c) }
    assert(v.select("id").rdd.map(_.getLong(0)).collect().sorted === Array(base0))
    assert(e.count() === 0)
    assert(bigComponents.size === 1)
    assert(bigComponents.contains(base0))
    val comp: Array[Long] = bigComponents(base0).select("id").map(_.getLong(0)).collect()
    assert(comp.length === g.vertices.count() - 1)
    assert(!comp.contains(base0))
    // Test connectedComponents
    val comps = ConnectedComponentsDF.run(g)
    val numActualComps = comps.select("component").distinct.count()
    assert(numActualComps === 1)
    compareImpl(g, comps)
  }

  test("handleSkew: multiple connected stars with distant connections") {
    // Connections are far away, so handleSkew should find 3 pieces of the component.
    // The 3 pieces should remain connected in the resulting subgraph.
    val n = 20L
    val (size0, size1, size2) = (n, n - 1, n - 2)
    val (base0, base1, base2) = (0L, size0 + 1, size0 + size1 + 2)
    val g0 = shiftVertexIds(Graphs.star(size0.toInt).edges, base0)
    val g1 = shiftVertexIds(Graphs.star(size1.toInt).edges, base1)
    val g2 = shiftVertexIds(Graphs.star(size2.toInt).edges, base2)
    val extraEdges = sqlContext.createDataFrame(Seq(
      (base0 + 1, -1L), (-1L, base1 + 1),
      (base1 + 2, -2L), (-2L, base2 + 1)
    )).toDF("src", "dst")
    val edges = g0.edges.unionAll(g1.edges).unionAll(g2.edges).unionAll(extraEdges)
      .select(col("src").cast("long"), col("dst").cast("long"))
    val g = GraphFrame.fromEdges(edges)
    // Test handleSkew
    val (e, bigComponents) = ConnectedComponentsDF.handleSkew(g.edges, g.vertices.count(), 10, 0.1)
    val v = bigComponents.values.foldLeft(g.vertices.select("id")) { case (v_, c) => v_.except(c) }
    assert(v.select("id").rdd.map(_.getLong(0)).collect().sorted ===
      Array(base0, base1, base2, -1L, -2L).sorted)
    assert(e.select("src", "dst").map(r => Set(r.getLong(0), r.getLong(1))).collect().toSet ===
      Array(Set(base0, -1L), Set(-1L, base1), Set(base1, -2L), Set(-2L, base2)).toSet)
    assert(bigComponents.size === 3)
    def checkComponent(vid: Long, size: Long): Unit = {
      assert(bigComponents.contains(vid))
      val comp: Array[Long] = bigComponents(vid).select("id").map(_.getLong(0)).collect()
      assert(comp.length === size)
      assert(!comp.contains(vid))
    }
    checkComponent(base0, size0)
    checkComponent(base1, size1)
    checkComponent(base2, size2)
    // Test connectedComponents
    val comps = ConnectedComponentsDF.run(g)
    val numActualComps = comps.select("component").distinct.count()
    assert(1 === numActualComps)
    compareImpl(g, comps)
  }

  private def shiftVertexIds(edges: DataFrame, i: Long): GraphFrame = {
    require(Seq(DataTypes.LongType, DataTypes.IntegerType).contains(edges.schema("src").dataType))
    val newEdges = edges.select((col("src") + i).as("src"),
      (col("dst") + i).as("dst"))
    GraphFrame.fromEdges(newEdges)
  }
}
