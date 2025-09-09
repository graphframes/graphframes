# Benchmarks

## Graphalytics Benchmarks

This benchmark is to test the performance of GraphFrames algorithms, not Apache Spark itself. So, all the graphs are
read from the disk and persisted in memory in the serialized format. In the result, only the time of GraphFrames
algorithms is measured and the time of reading of the CSV, serialization and persisting the data does not measure.

### Configurations

- **Serializer:** `org.apache.spark.serializer.KryoSerializer`
- **GraphFrame checkpoints:** `localCheckpoints`
- **Spark Version:** ${spark.version}
- **Scala Version:** ${scala.version}
- **VM:** standard GitHub Actions runner for open source projects.

### Graph: wiki-Talk

- **Vertices:** 2M
- **Edges:** 5M
- **Size Category:** *XS*
- **Source files format:** `CSV`-like

| Algorithm                                      | Measurements                                           | Time (s)                                         |                                   
|------------------------------------------------|--------------------------------------------------------|--------------------------------------------------|
| Shortest Paths Graphframes                     | ${benchmarks.benchmarkSP.measurements}                 | ${benchmarks.benchmarkSP.metric}                 |
| Shortest Paths Graphframes (Local Checkpoints) | ${benchmarks.benchmarkSPlocalCheckpoints.measurements} | ${benchmarks.benchmarkSPlocalCheckpoints.metric} |
| Shortest Paths GraphX                          | ${benchmarks.benchmarkSPGraphX.measurements}           | ${benchmarks.benchmarkSPGraphX.metric}           |
| Connected Components Graphframes               | ${benchmarks.benchmarkCC.measurements}                 | ${benchmarks.benchmarkCC.metric}                 |
| Connected Components GraphX                    | ${benchmarks.benchmarkCCGraphX.measurements}           | ${benchmarks.benchmarkCCGraphX.metric}           |
| Label Propagation GraphFrames                  | ${benchmarks.benchmarkCDLP.measurements}               | ${benchmarks.benchmarkCDLP.metric}               |