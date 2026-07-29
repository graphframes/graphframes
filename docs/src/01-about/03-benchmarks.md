# Benchmarks

## Graphalytics Benchmarks

This benchmark is to test the performance of GraphFrames algorithms, not Apache Spark itself. So, all the graphs are
read from Parquet files on disk and persisted in memory in the serialized format. As a result, only the time of GraphFrames
algorithms is measured, and the time to read/parse source files, serialize, and persist the data is not measured.

### Configurations

- **Serializer:** `org.apache.spark.serializer.KryoSerializer`
- **GraphFrame checkpoints:** `localCheckpoints`
- **Spark Version:** ${spark.version}
- **Scala Version:** ${scala.version}
- **VM:** standard GitHub Actions runner for open source projects.

### Graph: wiki-Talk

- **Vertices:** 2M
- **Edges:** 5M
- **Size Category:** _XS_
- **Source files format:** `Parquet`

| Algorithm                        | Measurements                                                        | Time (s)                                                      |
| -------------------------------- | ------------------------------------------------------------------- | ------------------------------------------------------------- |
| Shortest Paths Graphframes       | ${benchmarks.benchmarkShortestPaths.graphframes.measurements}       | ${benchmarks.benchmarkShortestPaths.graphframes.metric}       |
| Shortest Paths GraphX            | ${benchmarks.benchmarkShortestPaths.graphx.measurements}            | ${benchmarks.benchmarkShortestPaths.graphx.metric}            |
| Connected Components Graphframes | ${benchmarks.benchmarkConnectedComponents.graphframes.measurements} | ${benchmarks.benchmarkConnectedComponents.graphframes.metric} |
| Connected Components GraphX      | ${benchmarks.benchmarkConnectedComponents.graphx.measurements}      | ${benchmarks.benchmarkConnectedComponents.graphx.metric}      |
| Label Propagation GraphFrames    | ${benchmarks.benchmarkLabelPropagation.graphframes.measurements}    | ${benchmarks.benchmarkLabelPropagation.graphframes.metric}    |
| Label Propagation GraphX         | ${benchmarks.benchmarkLabelPropagation.graphx.measurements}         | ${benchmarks.benchmarkLabelPropagation.graphx.metric}         |
