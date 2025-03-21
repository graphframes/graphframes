package org.graphframes.benchmarks

import java.util.concurrent.TimeUnit
import org.graphframes.examples.LDBCUtils
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import org.apache.spark.sql.SparkSession

import java.nio.file.Files

class LDBCBenchmarkSuite {
  private def spark: SparkSession = {
    SparkSession.getActiveSession match {
      case Some(session) => session
      case None =>
        val spark = SparkSession
          .builder()
          .master("local[4]")
          .appName("GraphFramesBenchmarks")
          .config("spark.sql.shuffle.partitions", 4)
          .getOrCreate()

        val checkpointDir = Files.createTempDirectory(this.getClass.getName).toString
        spark.sparkContext.setLogLevel("ERROR")
        spark.sparkContext.setCheckpointDir(checkpointDir)
        spark
    }
  }
  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def benchmarkBFS(blackhole: Blackhole): Unit = {
    LDBCUtils.downloadLDBCIfNotExists()
    val g = LDBCUtils.getLDBCGraph(spark)
    val bfsTargetVertex = LDBCUtils.getBFSTarget

    val results: Unit = g.shortestPaths
      .landmarks(Seq(bfsTargetVertex))
      .run()
      .write
      .mode("overwrite")
      .format("noop")
      .save()
    blackhole.consume(results)
  }
}
