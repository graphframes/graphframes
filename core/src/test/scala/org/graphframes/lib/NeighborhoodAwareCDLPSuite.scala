package org.graphframes.lib

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataTypes
import org.graphframes.GraphFrame
import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.SparkFunSuite
import org.graphframes.TestUtils

class NeighborhoodAwareCDLPSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("basic flow: one iteration propagates strongest incoming label") {
    assume(TestUtils.requireSparkVersionGT(4, 1, spark.version))

    val vertices = spark.createDataFrame(Seq(1L, 2L, 3L).map(Tuple1(_))).toDF("id")
    val edges = spark.createDataFrame(Seq((1L, 2L), (2L, 3L), (3L, 1L))).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val result = new NeighborhoodAwareCDLP(g)
      .maxIter(1)
      .setA(1.0)
      .setC(0.0)
      .run()

    TestUtils.testSchemaInvariants(g, result)
    TestUtils.checkColumnType(result.schema, "label", DataTypes.LongType)

    val labels = result
      .select("id", "label")
      .collect()
      .map { row =>
        row.getLong(0) -> row.getLong(1)
      }
      .toMap

    assert(labels === Map(1L -> 3L, 2L -> 1L, 3L -> 2L))

    result.unpersist()
  }

  test(
    "different a/c weights change winner between direct-link mass and common-neighbor overlap") {
    assume(TestUtils.requireSparkVersionGT(4, 1, spark.version))

    val vertices = spark
      .createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "B"), (4L, "T"), (7L, "X"), (8L, "Y")))
      .toDF("id", "initLabel")

    // For destination 4:
    // - src 1 has overlap 2 with dst 4 via neighbors {7, 8}
    // - src 2 and src 3 each have overlap 0 with dst 4
    // Messages are aggregated by label:
    //   label A total = a + 2c
    //   label B total = a + a = 2a
    // So lowering c favors B; increasing c favors A.
    val edges = spark
      .createDataFrame(Seq((1L, 4L), (2L, 4L), (3L, 4L), (4L, 7L), (4L, 8L), (1L, 7L), (1L, 8L)))
      .toDF("src", "dst")

    val g = GraphFrame(vertices, edges)

    val lowC = new NeighborhoodAwareCDLP(g)
      .maxIter(1)
      .setInitialLabelCol("initLabel")
      .setA(1.0)
      .setC(0.0)
      .run()

    val highC = new NeighborhoodAwareCDLP(g)
      .maxIter(1)
      .setInitialLabelCol("initLabel")
      .setA(0.2)
      .setC(1.0)
      .run()

    TestUtils.checkColumnType(lowC.schema, "label", DataTypes.StringType)
    TestUtils.checkColumnType(highC.schema, "label", DataTypes.StringType)

    val labelWithLowC = lowC.filter("id = 4").select("label").collect().head.getString(0)
    val labelWithHighC = highC.filter("id = 4").select("label").collect().head.getString(0)

    assert(labelWithLowC === "B")
    assert(labelWithHighC === "A")

    lowC.unpersist()
    highC.unpersist()
  }

  test("isolated vertex keeps its own ID label") {
    assume(TestUtils.requireSparkVersionGT(4, 1, spark.version))

    val vertices = spark.createDataFrame(Seq(1L, 2L, 99L).map(Tuple1(_))).toDF("id")
    val edges = spark.createDataFrame(Seq((1L, 2L))).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val result = new NeighborhoodAwareCDLP(g)
      .maxIter(3)
      .setA(1.0)
      .setC(0.5)
      .run()

    assert(result.count() == 3)
    val isolatedLabel =
      result.filter(col("id") === lit(99L)).select("label").collect().head.getLong(0)
    assert(isolatedLabel === 99L)

    result.unpersist()
  }

  test("disconnected graph propagates labels independently per component") {
    assume(TestUtils.requireSparkVersionGT(4, 1, spark.version))

    val vertices = spark.createDataFrame(Seq(1L, 2L, 3L, 10L, 11L, 12L).map(Tuple1(_))).toDF("id")
    val edges = spark
      .createDataFrame(Seq((1L, 2L), (2L, 3L), (10L, 11L), (11L, 12L)))
      .toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val result = new NeighborhoodAwareCDLP(g)
      .maxIter(1)
      .setA(1.0)
      .setC(0.0)
      .run()

    val labels = result
      .select("id", "label")
      .collect()
      .map { row =>
        row.getLong(0) -> row.getLong(1)
      }
      .toMap

    assert(labels === Map(1L -> 1L, 2L -> 1L, 3L -> 2L, 10L -> 10L, 11L -> 10L, 12L -> 11L))

    result.unpersist()
  }

  test("changing only c can flip the winning label") {
    assume(TestUtils.requireSparkVersionGT(4, 1, spark.version))

    val vertices = spark
      .createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "B"), (4L, "T"), (7L, "X"), (8L, "Y")))
      .toDF("id", "initLabel")

    // For destination 4:
    //   label A score = a + 2c   (from src 1)
    //   label B score = 2a       (from src 2 and src 3)
    // Keep a fixed and vary only c.
    val edges = spark
      .createDataFrame(Seq((1L, 4L), (2L, 4L), (3L, 4L), (4L, 7L), (4L, 8L), (1L, 7L), (1L, 8L)))
      .toDF("src", "dst")

    val g = GraphFrame(vertices, edges)

    val lowC = new NeighborhoodAwareCDLP(g)
      .maxIter(1)
      .setInitialLabelCol("initLabel")
      .setA(1.0)
      .setC(0.0)
      .run()

    val highC = new NeighborhoodAwareCDLP(g)
      .maxIter(1)
      .setInitialLabelCol("initLabel")
      .setA(1.0)
      .setC(1.0)
      .run()

    val labelWithLowC = lowC.filter("id = 4").select("label").collect().head.getString(0)
    val labelWithHighC = highC.filter("id = 4").select("label").collect().head.getString(0)

    assert(labelWithLowC === "B")
    assert(labelWithHighC === "A")

    lowC.unpersist()
    highC.unpersist()
  }

  test("changing only a can flip the winning label") {
    assume(TestUtils.requireSparkVersionGT(4, 1, spark.version))

    val vertices = spark
      .createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "B"), (4L, "T"), (7L, "X"), (8L, "Y")))
      .toDF("id", "initLabel")

    // For destination 4, with c fixed at 0.5:
    //   label A score = a + 1
    //   label B score = 2a
    // High a favors B; low a favors A.
    val edges = spark
      .createDataFrame(Seq((1L, 4L), (2L, 4L), (3L, 4L), (4L, 7L), (4L, 8L), (1L, 7L), (1L, 8L)))
      .toDF("src", "dst")

    val g = GraphFrame(vertices, edges)

    val highA = new NeighborhoodAwareCDLP(g)
      .maxIter(1)
      .setInitialLabelCol("initLabel")
      .setA(1.0)
      .setC(0.5)
      .run()

    val lowA = new NeighborhoodAwareCDLP(g)
      .maxIter(1)
      .setInitialLabelCol("initLabel")
      .setA(0.1)
      .setC(0.5)
      .run()

    val labelWithHighA = highA.filter("id = 4").select("label").collect().head.getString(0)
    val labelWithLowA = lowA.filter("id = 4").select("label").collect().head.getString(0)

    assert(labelWithHighA === "B")
    assert(labelWithLowA === "A")

    highA.unpersist()
    lowA.unpersist()
  }

  test("setIsDirected(false) changes propagation by adding reverse links") {
    assume(TestUtils.requireSparkVersionGT(4, 1, spark.version))

    val vertices = spark.createDataFrame(Seq(1L, 2L).map(Tuple1(_))).toDF("id")
    val edges = spark.createDataFrame(Seq((1L, 2L))).toDF("src", "dst")
    val g = GraphFrame(vertices, edges)

    val directed = new NeighborhoodAwareCDLP(g)
      .maxIter(1)
      .setA(1.0)
      .setC(0.0)
      .setIsDirected(true)
      .run()

    val undirected = new NeighborhoodAwareCDLP(g)
      .maxIter(1)
      .setA(1.0)
      .setC(0.0)
      .setIsDirected(false)
      .run()

    val directedLabels = directed
      .select("id", "label")
      .collect()
      .map(r => r.getLong(0) -> r.getLong(1))
      .toMap
    val undirectedLabels = undirected
      .select("id", "label")
      .collect()
      .map(r => r.getLong(0) -> r.getLong(1))
      .toMap

    assert(directedLabels === Map(1L -> 1L, 2L -> 1L))
    assert(undirectedLabels === Map(1L -> 2L, 2L -> 1L))

    directed.unpersist()
    undirected.unpersist()
  }

  test("undirected mode matches explicitly symmetrized directed edge set") {
    assume(TestUtils.requireSparkVersionGT(4, 1, spark.version))

    val vertices = spark.createDataFrame(Seq(1L, 2L, 3L, 4L).map(Tuple1(_))).toDF("id")
    val directedEdges =
      spark.createDataFrame(Seq((1L, 2L), (2L, 3L), (2L, 4L))).toDF("src", "dst")
    val symmetrizedEdges = spark
      .createDataFrame(Seq((1L, 2L), (2L, 1L), (2L, 3L), (3L, 2L), (2L, 4L), (4L, 2L)))
      .toDF("src", "dst")

    val g = GraphFrame(vertices, directedEdges)
    val gSym = GraphFrame(vertices, symmetrizedEdges)

    val internalUndirected = new NeighborhoodAwareCDLP(g)
      .maxIter(1)
      .setA(1.0)
      .setC(0.0)
      .setIsDirected(false)
      .run()

    val explicitSymDirected = new NeighborhoodAwareCDLP(gSym)
      .maxIter(1)
      .setA(1.0)
      .setC(0.0)
      .setIsDirected(true)
      .run()

    val internalMap = internalUndirected
      .select("id", "label")
      .collect()
      .map(r => r.getLong(0) -> r.getLong(1))
      .toMap
    val explicitMap = explicitSymDirected
      .select("id", "label")
      .collect()
      .map(r => r.getLong(0) -> r.getLong(1))
      .toMap

    assert(internalMap === explicitMap)

    internalUndirected.unpersist()
    explicitSymDirected.unpersist()
  }
}
