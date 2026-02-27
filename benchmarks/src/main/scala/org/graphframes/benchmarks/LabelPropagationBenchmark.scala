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
class LabelPropagationBenchmark extends LDBCBenchmarkBase {
  @Param(Array("graphframes", "graphx"))
  var algorithm: String = _

  @Param(Array("10"))
  var maxIter: String = _

  @Benchmark
  def benchmarkLabelPropagation(blackhole: Blackhole): Unit = {
    val cdlpResults = if (algorithm == "graphx") {
      graph.labelPropagation
        .setAlgorithm("graphx")
        .maxIter(maxIter.toInt)
        .run()
    } else {
      graph.labelPropagation
        .setAlgorithm("graphframes")
        .maxIter(maxIter.toInt)
        .setUseLocalCheckpoints(useLocalCheckpoints.toBoolean)
        .run()
    }

    val res: Unit = cdlpResults.write.format("noop").mode("overwrite").save()
    blackhole.consume(res)
  }
}
