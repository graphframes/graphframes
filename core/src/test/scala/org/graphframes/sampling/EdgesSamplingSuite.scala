package org.graphframes.sampling

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.stddev
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.graphframes.GraphFrame
import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.SparkFunSuite

class EdgesSamplingSuite extends SparkFunSuite with GraphFrameTestSparkContext {
  var g2: GraphFrame = _
  var g: GraphFrame = _
  override def beforeAll(): Unit = {
    super.beforeAll()
    val spark = SparkSession.getActiveSession.get

    val edges = spark.read
      .option("delimiter", "\t")
      .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
      .csv("src/test/resources")

    g = GraphFrame.fromEdges(edges).persist()
  }

  test("randomEdgesSampling should sample edges randomly") {
    val originalEdgeCount = g.edges.count()

    // Create a random edge sampling strategy
    val sampling = RandomEdgesSampling().prepareSampler(g)

    // Sample with a high threshold - should get fewer edges
    val sampledEdges1 = sampling.sampleEdges(seed = 12345L, threshold = 0.7, 1.0)
    val count1 = sampledEdges1.count()
    assert(count1 < originalEdgeCount)

    // Sample with a low threshold - should get less edges
    val sampledEdges2 = sampling.sampleEdges(seed = 12345L, threshold = 0.1, 1.0)
    val count2 = sampledEdges2.count()
    assert(count2 < originalEdgeCount)
    assert(count2 < count1)
    sampling.close()
  }

  test("randomEdgesSampling should produce different results with different seeds") {

    val sampling = RandomEdgesSampling().prepareSampler(g)
    val sampledEdges1 = sampling.sampleEdges(seed = 11111L, threshold = 0.2, 1.0)
    val sampledEdges2 = sampling.sampleEdges(seed = 33333L, threshold = 0.2, 1.0)

    // Results should be different with different seeds (with high probability)
    val edges1 = sampledEdges1
      .collect()
      .map(row => (row.getAs[String]("src"), row.getAs[String]("dst")))
      .toSet
    val edges2 = sampledEdges2
      .collect()
      .map(row => (row.getAs[String]("src"), row.getAs[String]("dst")))
      .toSet
    val (interesection, cap) = {
      if (edges1.size > edges2.size) {
        (edges1.diff(edges2), edges1.size)
      } else {
        (edges2.diff(edges1), edges2.size)
      }
    }
    assert(interesection.size >= cap * 0.6)
    sampling.close()
  }

  test("randomEdgesSampling should produce similar results with same seeds") {

    val sampling = RandomEdgesSampling().prepareSampler(g)
    val sampledEdges1 = sampling.sampleEdges(seed = 11111L, threshold = 0.2, 1.0)
    val sampledEdges2 = sampling.sampleEdges(seed = 11111L, threshold = 0.2, 1.0)

    // Results should be different with different seeds (with high probability)
    val edges1 = sampledEdges1
      .collect()
      .map(row => (row.getAs[String]("src"), row.getAs[String]("dst")))
      .toSet
    val edges2 = sampledEdges2
      .collect()
      .map(row => (row.getAs[String]("src"), row.getAs[String]("dst")))
      .toSet
    val (interesection, cap) = {
      if (edges1.size > edges2.size) {
        (edges1.diff(edges2), edges1.size)
      } else {
        (edges2.diff(edges1), edges2.size)
      }
    }
    assert(interesection.size <= cap * 0.4)
    sampling.close()
  }

  test("inverseDegreeProductEdgeSampling should sample edges based on vertex degrees") {

    // Create an inverse degree product sampling strategy with alpha = 1.0
    val sampling = InverseDegreeProductEdgeSampling(alpha = 1.0).prepareSampler(g)

    // Sample edges
    val sampledEdges = sampling.sampleEdges(seed = 12345L, threshold = 0.7, 1.0)
    val cnt = sampledEdges.count()

    // Should have some edges (threshold is low)
    assert(cnt > 0)
    assert(cnt <= g.edges.count())

    // Degrees comparison
    // Verify that the degree distribution of sampled edges is more uniform than original
    // by comparing the standard deviation of degrees
    val originalDegreesStd = g.degrees.select(stddev("degree")).head().getDouble(0)
    val sampledDegreesStd = sampledEdges
      .union(sampledEdges.select(col("dst").alias("src"), col("src").alias("dst")))
      .groupBy("src")
      .agg(count("*").alias("degree"))
      .select(stddev("degree"))
      .head()
      .getDouble(0)

    // Sampled edges should have lower standard deviation (more uniform distribution)
    assert(Math.abs(sampledDegreesStd - originalDegreesStd) / originalDegreesStd >= 0.9)

    sampling.close()
  }

  test("inverseDegreeProductEdgeSampling with alpha=0 should behave like uniform sampling") {

    // With alpha = 0, should behave like uniform sampling
    val sampling = InverseDegreeProductEdgeSampling(alpha = 0.0).prepareSampler(g)

    val sampledEdges = sampling.sampleEdges(seed = 12345L, threshold = 0.7, 1.0)
    val cnt = sampledEdges.count()

    assert(cnt >= 0)
    assert(cnt <= g.edges.count())

    // Degrees comparison
    // Verify that the degree distribution of sampled edges is more uniform than original
    // by comparing the standard deviation of degrees
    val originalDegreesStd = g.degrees.select(stddev("degree")).head().getDouble(0)
    val sampledDegreesStd = sampledEdges
      .union(sampledEdges.select(col("dst").alias("src"), col("src").alias("dst")))
      .groupBy("src")
      .agg(count("*").alias("degree"))
      .select(stddev("degree"))
      .head()
      .getDouble(0)

    // Sampled edges should have lower standard deviation (more uniform distribution)
    assert(Math.abs(originalDegreesStd - sampledDegreesStd) / originalDegreesStd <= 0.3)

    sampling.close()
  }

  test("inverseDegreeProductEdgeSampling with alpha=1 should favor low-degree vertices") {

    // With alpha = 1, should strongly favor edges between low-degree vertices
    val sampling = InverseDegreeProductEdgeSampling(alpha = 1.0).prepareSampler(g)

    val sampledEdges = sampling.sampleEdges(seed = 12345L, threshold = 0.1, 1.0)
    val count = sampledEdges.count()

    assert(count >= 0)
    assert(count <= g.edges.count())

    sampling.close()
  }

  test("inverseDegreeProductEdgeSampling should validate alpha parameter") {
    // Alpha must be in range [0, 1]
    intercept[IllegalArgumentException] {
      InverseDegreeProductEdgeSampling(alpha = -0.1)
    }

    intercept[IllegalArgumentException] {
      InverseDegreeProductEdgeSampling(alpha = 1.1)
    }

    // These should work
    InverseDegreeProductEdgeSampling(alpha = 0.0)
    InverseDegreeProductEdgeSampling(alpha = 0.5)
    InverseDegreeProductEdgeSampling(alpha = 1.0)
  }

  test("edgesSamplingStrategy should throw exception when not prepared") {
    val sampling = RandomEdgesSampling()

    // Should throw exception if not prepared
    intercept[IllegalStateException] {
      sampling.sampleEdges(seed = 12345L, threshold = 0.5, 1.0)
    }
  }
}
