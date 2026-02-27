package org.graphframes.benchmarks

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(
  value = 1,
  jvmArgs = Array(
    "-Xmx10g",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED"))
class ShortestPathsBenchmark extends LDBCBenchmarkBase {
  @Param(Array("graphframes", "graphx"))
  var algorithm: String = _

  @Param(Array("1"))
  var checkPointInterval: String = _

  @Param(Array("1"))
  var startingVertex: String = _

  @Benchmark
  def benchmarkShortestPaths(blackhole: Blackhole): Unit = {
    val sourceVertex = startingVertex.toLong

    val spResults = if (algorithm == "graphx") {
      graph.shortestPaths
        .setAlgorithm("graphx")
        .landmarks(Seq(sourceVertex))
        .run()
    } else {
      graph.shortestPaths
        .setAlgorithm("graphframes")
        .landmarks(Seq(sourceVertex))
        .setCheckpointInterval(checkPointInterval.toInt)
        .setUseLocalCheckpoints(useLocalCheckpoints.toBoolean)
        .run()
    }

    val res: Unit = spResults.write.format("noop").mode("overwrite").save()
    blackhole.consume(res)
  }
}
