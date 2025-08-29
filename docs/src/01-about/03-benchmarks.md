# Benchmarks

## Graphalytics Benchmarks

This benchmark is to test the performance of GraphFrames algorithms, not Apache Spark itself. So, all the graphs are read from the disk and persisted in memory in the serialized format. In the result, only the time of GraphFrames algorithms is measured and the time of reading and serialization of CSV does not measure. While the time of GraphFrames algorithms is about seconds, the time needed for Spark to read and serialize the CSV of edges is about ten minutes (for the `graph500-24` with 260M rows).

### Configurations

- **Serializer:** `org.apache.spark.serializer.KryoSerializer`
- **GraphFrame checkpoints:** `localCheckpoints`

### graph500-24

- **Vertices:** 8M
- **Edges:** 260M
- **Size Category:** *M*
- **Source files format:** `CSV`-like

| Algorithm            | Measurements                           | Time (s)                         | Error (s)                        | Confidence Interval (99.9%)                                             |
|----------------------|----------------------------------------|----------------------------------|----------------------------------|-------------------------------------------------------------------------|
| Shortest Paths       | ${benchmarks.benchmarkSP.measurements} | ${benchmarks.benchmarkSP.metric} | ${benchmarks.benchmarkSP.stdErr} | [${benchmarks.benchmarkSP.ciLeft}, ${benchmarks.benchmarkSP.ciRight}\\] |
| Connected Components | ${benchmarks.benchmarkCC.measurements} | ${benchmarks.benchmarkCC.metric} | ${benchmarks.benchmarkCC.stdErr} | [${benchmarks.benchmarkCC.ciLeft}, ${benchmarks.benchmarkCC.ciRight}\\] |