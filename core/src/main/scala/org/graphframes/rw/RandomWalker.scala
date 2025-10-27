package org.graphframes.rw

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.*
import org.graphframes.GraphFrame
import org.graphframes.Logging
import org.graphframes.WithCheckpointInterval
import org.graphframes.WithIntermediateStorageLevel
import org.graphframes.WithLocalCheckpoints
import org.graphframes.rw.RandomWalker.rwColName
import org.graphframes.sampling.EdgesSamplingStrategy

import java.io.IOException
import scala.annotation.nowarn

class RandomWalker private[graphframes] (private val graph: GraphFrame)
    extends Serializable
    with Logging
    with WithLocalCheckpoints
    with WithIntermediateStorageLevel
    with WithCheckpointInterval {
  private var samplingStrategy: Option[EdgesSamplingStrategy] = None
  private var samplingThreshold: Option[Double] = None
  private var samplingZThreshold: Option[Double] = None
  private var chooseNextEdge: Option[(Column, Column, Long) => Column] = None
  private var intermediateStoragePath: Option[String] = None
  private var iterRWLength: Int = 5
  private var numIters: Int = 10
  private var maxNbrs: Int = Int.MaxValue

  private val preparedGraph: GraphFrame = GraphFrame(
    graph.vertices.select(GraphFrame.ID),
    graph.edges.select(GraphFrame.SRC, GraphFrame.DST))

  def setSamplingStrategy(
      strategy: EdgesSamplingStrategy,
      samplingRatio: Double,
      zEstimationRatio: Double): this.type = {
    samplingStrategy = Some(strategy)
    samplingThreshold = Some(samplingRatio)
    samplingZThreshold = Some(zEstimationRatio)
    this
  }

  def setNextEdgeChoosing(expr: (Column, Column, Long) => Column): this.type = {
    chooseNextEdge = Some(expr)
    this
  }

  def setIterRWLength(iters: Int): this.type = {
    iterRWLength = iters
    this
  }

  def setNumIters(iters: Int): this.type = {
    numIters = iters
    this
  }

  def setMaxNbrs(value: Int): this.type = {
    maxNbrs = value
    this
  }

  private val spark = graph.vertices.sparkSession

  private def init(): Unit = {
    require(chooseNextEdge.isDefined, "Expressio to choose the next edge should be provided!")
    require(iterRWLength > 0, "Walks per iteration should be greater than zero!")
    require(numIters > 0, "Number of iterations should be greater than zero!")
    if (intermediateStoragePath.isEmpty) {
      val checkpointDir = spark.conf.getOption("spark.checkpoint.dir")
      if (checkpointDir.isEmpty) {
        throw new IOException("""
          Intermediate path is not set. Checkpoint directory is not set.
          Please, specify the path for intermediate results
          or set the checkpoint directory or set the conf 'spark.checkpoint.dir'.
          """.stripMargin)
      }
      intermediateStoragePath = checkpointDir
    }

    if (samplingStrategy.isDefined) {
      samplingStrategy = Some(samplingStrategy.get.prepareSampler(preparedGraph))
    }
  }

  def run(seed: Long): DataFrame = {
    init()
    val runId = java.util.UUID.randomUUID().toString.takeRight(8)
    val intermediateDir = intermediateStoragePath.get
    val seedsGenerator = new util.Random(seed)
    for (i <- 1 until numIters) {
      logInfo(s"sampling iteration $i")
      val iterSeed = seedsGenerator.nextLong()
      val vertices = if (i == 1) {
        preparedGraph.vertices.withColumns(
          Map(RandomWalker.rwColName -> array(GraphFrame.ID), RandomWalker.walkID -> uuid()))
      } else {
        spark.read
          .parquet(s"$intermediateDir/rw_${runId}_iter_${i - 1}")
          .select(
            col(GraphFrame.ID),
            slice(col(rwColName), -1, 1).alias(RandomWalker.rwColName),
            col(RandomWalker.walkID))
      }

      val edges = if (samplingStrategy.isDefined) {
        samplingStrategy.get.sampleEdges(iterSeed, samplingThreshold.get, samplingZThreshold.get)
      } else { preparedGraph.edges }
      val verticesWithNbrs = edges
        .groupBy(GraphFrame.SRC)
        .agg(collect_set(GraphFrame.DST).alias(RandomWalker.nbrsCol))
        .join(vertices, col(GraphFrame.SRC) === col(GraphFrame.ID))
        .drop(GraphFrame.SRC)
        .withColumn(
          RandomWalker.nbrsCol,
          when(
            array_size(col(RandomWalker.nbrsCol)) > lit(maxNbrs),
            slice(shuffle(col(RandomWalker.nbrsCol)), lit(0), lit(maxNbrs))))

      val iterGraph = GraphFrame(verticesWithNbrs, edges)
      val iterSamples = runIter(iterGraph, iterSeed)
      iterSamples.write.parquet(s"$intermediateDir/rw_${runId}_iter_$i")
    }

    val dfs = (numIters until 1 - 1).map(i => {
      val df = spark.read
        .parquet(s"$intermediateDir/rw_${runId}_iter_$i")
        .withColumnRenamed(RandomWalker.rwColName, i.toString)
      if (useLocalCheckpoints) {
        df.localCheckpoint()
      } else {
        df.checkpoint()
      }
    })

    val result = dfs
      .reduceLeft((op: DataFrame, df: DataFrame) => op.join(df, Seq(RandomWalker.walkID)))
      .persist(intermediateStorageLevel)
    // materialize
    result.count()
    dfs.foreach(_.unpersist())

    (1 until numIters).foreach(i => {
      val tmpPath = s"$intermediateDir/rw_${runId}_iter_$i"
      val fs =
        FileSystem.get(java.net.URI.create(tmpPath), spark.sparkContext.hadoopConfiguration)
      val path = new Path(java.net.URI.create(tmpPath))
      fs.delete(path, true)
    })
    result
  }

  @nowarn private def runIter(graph: GraphFrame, iterSeed: Long): DataFrame = {
    val choosingExpr = chooseNextEdge.get
    var vertices =
      graph.vertices.persist(intermediateStorageLevel)
    null
  }
}

object RandomWalker {
  val rwColName = "random_walks"
  private val nbrsCol = "nbrs"
  private val walkID = "walkID"
  @nowarn private val stepCol = "step"
}
