# GraphFrames Benchmarks

JMH-based benchmarks for GraphFrames algorithms using LDBC Graphalytics datasets.

## Available Benchmarks

| Benchmark                      | Description                               |
| ------------------------------ | ----------------------------------------- |
| `ShortestPathsBenchmark`       | Shortest paths from a source vertex       |
| `ConnectedComponentsBenchmark` | Connected components detection            |
| `LabelPropagationBenchmark`    | Community detection via label propagation |

## Running Benchmarks

### Basic Usage

```bash
sbt "benchmarks/jmh:run -i 3 -wi 1 -f 1 -p graphName=wiki-Talk org.graphframes.benchmarks.ShortestPathsBenchmark"
```

### Parameters

| Parameter   | Values                  | Description                                |
| ----------- | ----------------------- | ------------------------------------------ |
| `graphName` | See Available Graphs    | LDBC graph dataset to use                  |
| `algorithm` | `graphframes`, `graphx` | Algorithm implementation                   |
| `maxIter`   | integer (default: 10)   | Max iterations (iterative algorithms only) |

### Examples

Run all algorithms on wiki-Talk:

```bash
sbt "benchmarks/jmh:run -p graphName=wiki-Talk org.graphframes.benchmarks.ShortestPathsBenchmark"
```

Run only GraphX implementation:

```bash
sbt "benchmarks/jmh:run -p algorithm=graphx -p graphName=cit-Patents org.graphframes.benchmarks.ConnectedComponentsBenchmark"
```

Run with custom iteration count:

```bash
sbt "benchmarks/jmh:run -p maxIter=5 -p graphName=wiki-Talk org.graphframes.benchmarks.LabelPropagationBenchmark"
```

## Available Graphs

| Graph         | Vertices | Edges  | Weighted |
| ------------- | -------- | ------ | -------- |
| `wiki-Talk`   | ~2.4M    | ~5.0M  | No       |
| `cit-Patents` | ~3.8M    | ~16.5M | No       |
| `kgs`         | ~800K    | ~23.5M | Yes      |
| `graph500-22` | ~4.2M    | ~67M   | No       |
| `graph500-23` | ~8.4M    | ~134M  | No       |
| `graph500-24` | ~16.8M   | ~268M  | No       |
| `graph500-25` | ~33.6M   | ~537M  | No       |

Test graphs (small, for validation):

- `test-bfs-directed`, `test-bfs-undirected`
- `test-cdlp-directed`, `test-cdlp-undirected`
- `test-pr-directed`, `test-pr-undirected`
- `test-wcc-directed`, `test-wcc-undirected`

## Data Format

Benchmarks use LDBC Graphalytics parquet format:

- Vertices: `{graphName}-v.parquet` with column `id`
- Edges: `{graphName}-e.parquet` with columns `src`, `dst` (and `weight` for weighted graphs)

Data is downloaded on first use and cached in `target/ldbc-cache/`.

## Notes

- Benchmarks require ~10GB heap memory (configured automatically)
- Data files are cached locally to avoid repeated downloads
- The old `LDBCBenchmarkSuite` is deprecated; use the new split benchmarks
