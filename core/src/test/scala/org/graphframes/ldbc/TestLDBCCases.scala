package org.graphframes.ldbc

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.abs
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.graphframes.GraphFrame
import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.SparkFunSuite
import org.graphframes.examples.LDBCUtils

import java.io.File
import java.nio.file._
import java.util.Properties

class TestLDBCCases extends SparkFunSuite with GraphFrameTestSparkContext {
  private val resourcesPath = Path.of(new File("target").toURI)
  private val unreachableID = 9223372036854775807L

  private def readUndirectedUnweighted(pathPrefix: String): GraphFrame = {
    var edges = spark.read
      .option("delimiter", " ")
      .option("header", "false")
      .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
      .csv(s"${pathPrefix}.e")
      .toDF("src", "dst")

    // TODO: replace by symmetrize when #548 is done!
    edges = edges
      .select("src", "dst")
      .union(edges.select(col("dst").alias("src"), col("src").alias("dst")))

    val nodes = spark.read
      .text(s"${pathPrefix}.v")
      .toDF("id")
      .select(col("id").cast(LongType))

    GraphFrame(nodes, edges)
  }

  private def readProperties(path: Path): Properties = {
    val props = new Properties()
    val stream = Files.newInputStream(path)
    props.load(stream)
    stream.close()
    props
  }

  private lazy val ldbcTestBFSUndirected: (GraphFrame, DataFrame, Long) = {
    LDBCUtils.downloadLDBCIfNotExists(resourcesPath, LDBCUtils.TEST_BFS_UNDIRECTED)
    val caseRoot = resourcesPath.resolve(LDBCUtils.TEST_BFS_UNDIRECTED)

    val expectedPath = caseRoot.resolve(s"${LDBCUtils.TEST_BFS_UNDIRECTED}-BFS")

    val expectedDistances = spark.read
      .option("delimiter", " ")
      .option("header", "false")
      .schema(StructType(Seq(StructField("id", LongType), StructField("distance", IntegerType))))
      .csv(expectedPath.toString)
      .toDF("id", "distance")
    val props = readProperties(caseRoot.resolve(s"${LDBCUtils.TEST_BFS_UNDIRECTED}.properties"))
    (
      readUndirectedUnweighted(s"${caseRoot.toString}/${LDBCUtils.TEST_BFS_UNDIRECTED}"),
      expectedDistances,
      props.getProperty(s"graph.${LDBCUtils.TEST_BFS_UNDIRECTED}.bfs.source-vertex").toLong)
  }

  Seq("graphframes", "graphx").foreach { algo =>
    test(s"test undirected BFS with LDBC for impl ${algo}") {
      val testCase = ldbcTestBFSUndirected
      val srcVertex = testCase._3
      val spResult = testCase._1.shortestPaths
        .landmarks(Seq(srcVertex))
        .setAlgorithm(algo)
        .run()
        .select(
          col(GraphFrame.ID),
          col("distances").getItem(srcVertex).cast(LongType).alias("got_distance"))
        .na
        .fill(Map("got_distance" -> unreachableID))

      assert(spResult.count() == testCase._1.vertices.count())
      assert(
        spResult
          .join(testCase._2, Seq("id"), "left")
          .filter(col("got_distance") =!= col("distance"))
          .collect()
          .isEmpty)

    }
  }

  private lazy val ldbcTestCDLPUndirected: (GraphFrame, DataFrame, Int) = {
    LDBCUtils.downloadLDBCIfNotExists(resourcesPath, LDBCUtils.TEST_CDLP_UNDIRECTED)
    val caseRoot = resourcesPath.resolve(LDBCUtils.TEST_CDLP_UNDIRECTED)

    val expectedPath = caseRoot.resolve(s"${LDBCUtils.TEST_CDLP_UNDIRECTED}-CDLP")

    val expectedCommunities = spark.read
      .option("delimiter", " ")
      .option("header", "false")
      .schema(StructType(Seq(StructField("id", LongType), StructField("community", LongType))))
      .csv(expectedPath.toString)
      .toDF("id", "community")
    val props = readProperties(caseRoot.resolve(s"${LDBCUtils.TEST_CDLP_UNDIRECTED}.properties"))
    (
      readUndirectedUnweighted(s"${caseRoot.toString}/${LDBCUtils.TEST_CDLP_UNDIRECTED}"),
      expectedCommunities,
      props.getProperty(s"graph.${LDBCUtils.TEST_CDLP_UNDIRECTED}.cdlp.max-iterations").toInt)
  }

  Seq("graphx", "graphframes").foreach { algo =>
    test(s"test undirected CDLP with LDBC for algo ${algo}") {
      val testCase = ldbcTestCDLPUndirected
      val cdlpResults = testCase._1.labelPropagation.setAlgorithm(algo).maxIter(testCase._3).run()
      assert(cdlpResults.count() == testCase._1.vertices.count())
      assert(
        cdlpResults
          .join(testCase._2, Seq("id"), "left")
          .filter(col("label") =!= col("community"))
          .collect()
          .isEmpty)
    }
  }

  private lazy val ldbcTestPageRankUndirected: (GraphFrame, DataFrame, Double, Int) = {
    LDBCUtils.downloadLDBCIfNotExists(resourcesPath, LDBCUtils.TEST_PR_UNDIRECTED)
    val caseRoot = resourcesPath.resolve(LDBCUtils.TEST_PR_UNDIRECTED)

    val expectedPath = caseRoot.resolve(s"${LDBCUtils.TEST_PR_UNDIRECTED}-PR")

    val expectedRanks = spark.read
      .option("delimiter", " ")
      .option("header", "false")
      .schema(StructType(Seq(StructField("id", LongType), StructField("pr", DoubleType))))
      .csv(expectedPath.toString)
      .toDF("id", "pr")

    val props = readProperties(caseRoot.resolve(s"${LDBCUtils.TEST_PR_UNDIRECTED}.properties"))
    (
      readUndirectedUnweighted(s"${caseRoot.toString}/${LDBCUtils.TEST_PR_UNDIRECTED}"),
      expectedRanks,
      props.getProperty(s"graph.${LDBCUtils.TEST_PR_UNDIRECTED}.pr.damping-factor").toDouble,
      props.getProperty(s"graph.${LDBCUtils.TEST_PR_UNDIRECTED}.pr.num-iterations").toInt)
  }

  // TODO: add graphframes after finishing #569
  Seq("graphx").foreach { algo =>
    test(s"test undirected PR with LDBC for algo ${algo}") {
      val testCase = ldbcTestPageRankUndirected
      val prResults = testCase._1.pageRank
        .resetProbability(1.0 - testCase._3)
        .maxIter(testCase._4)
        .run()
        .vertices

      // Normalize??
      val sumPR = prResults.agg(sum(col("pagerank"))).collect().head.getAs[Double](0)
      val prResultsNormalized = prResults.withColumn("pagerank", col("pagerank") / lit(sumPR))
      assert(prResults.count() == testCase._1.vertices.count())
      assert(
        prResultsNormalized
          .join(testCase._2, Seq("id"), "left")
          .filter(abs(col("pagerank") - col("pr")) >= lit(1e-4))
          .collect()
          .isEmpty)
    }
  }

  private lazy val ldbcTestWCCUndirected: (GraphFrame, DataFrame) = {
    LDBCUtils.downloadLDBCIfNotExists(resourcesPath, LDBCUtils.TEST_WCC_UNDIRECTED)
    val caseRoot = resourcesPath.resolve(LDBCUtils.TEST_WCC_UNDIRECTED)

    val expectedPath = caseRoot.resolve(s"${LDBCUtils.TEST_WCC_UNDIRECTED}-WCC")

    val expectedComponents = spark.read
      .option("delimiter", " ")
      .option("header", "false")
      .schema(StructType(Seq(StructField("id", LongType), StructField("wcomp", LongType))))
      .csv(expectedPath.toString)
      .toDF("id", "wcomp")

    (
      readUndirectedUnweighted(s"${caseRoot.toString}/${LDBCUtils.TEST_WCC_UNDIRECTED}"),
      expectedComponents)
  }

  Seq("graphframes", "graphx").foreach { algo =>
    test(s"test undirected WCC with LDBC for impl ${algo}") {
      val testCase = ldbcTestWCCUndirected
      val ccResults = testCase._1.connectedComponents.setAlgorithm(algo).run()
      assert(ccResults.count() == testCase._1.vertices.count())
      assert(
        ccResults
          .join(testCase._2, Seq("id"), "left")
          .filter(col("wcomp") =!= col("component"))
          .collect()
          .isEmpty)
    }
  }
}
