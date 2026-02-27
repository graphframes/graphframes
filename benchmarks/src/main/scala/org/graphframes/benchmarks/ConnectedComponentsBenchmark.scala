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
class ConnectedComponentsBenchmark extends LDBCBenchmarkBase {
  @Param(Array("graphframes", "graphx"))
  var algorithm: String = _

  @Param(Array("1000000", "-1"))
  var broadcastThreshold: String = _

  @Benchmark
  def benchmarkConnectedComponents(blackhole: Blackhole): Unit = {
    val ccResults = if (algorithm == "graphx") {
      graph.connectedComponents
        .setAlgorithm("graphx")
        .run()
    } else {
      graph.connectedComponents
        .setUseLocalCheckpoints(true)
        .setAlgorithm("graphframes")
        .setBroadcastThreshold(broadcastThreshold.toInt)
        .setUseLocalCheckpoints(useLocalCheckpoints.toBoolean)
        .run()
    }

    val res: Unit = ccResults.write.format("noop").mode("overwrite").save()
    blackhole.consume(res)
  }
}
