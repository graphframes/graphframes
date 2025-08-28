# Benchmarks

## Graphalytics Benchmarks

All the graphs are read from the disk and persisted in memory in the serialized format.

### graph500-24

- **Vertices:** 8M
- **Edges:** 260M
- **Size Category:** *M*
- **Source files format:** `CSV`-like

| Algorithm            | Measurements                           | Time (s)                         | Error (s)                        | Confidence Interval (99.9%)                                             |
|----------------------|----------------------------------------|----------------------------------|----------------------------------|-------------------------------------------------------------------------|
| Shortest Paths       | ${benchmarks.benchmarkSP.measurements} | ${benchmarks.benchmarkSP.metric} | ${benchmarks.benchmarkSP.stdErr} | [${benchmarks.benchmarkSP.ciLeft}, ${benchmarks.benchmarkSP.ciRight}\\] |
| Connected Components | ${benchmarks.benchmarkCC.measurements} | ${benchmarks.benchmarkCC.metric} | ${benchmarks.benchmarkCC.stdErr} | [${benchmarks.benchmarkCC.ciLeft}, ${benchmarks.benchmarkCC.ciRight}\\] |