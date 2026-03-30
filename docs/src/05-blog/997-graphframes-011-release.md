# GraphFrames 0.11.0 release

- **Published:** 2026-03-30T00:00:00Z
- **Title:** GraphFrames 0.11.0 release
- **Summary:** This release brings a new Connected Components algorithm based on Randomized Contraction, a major refactoring of the CC API, automatic Pregel optimization that skips unnecessary joins, graph embeddings via random walks with Word2Vec and Hash2Vec, a PySpark Property Graph API, approximate triangle counting with DataSketches, and various bug fixes.

## Connected Components: new algorithm & refactoring

The Connected Components implementation received a major overhaul in this release. The algorithm selection has been refactored into clean, separate modules and a new algorithm variant has been added.

### Three algorithm choices

GraphFrames now ships three distinct algorithms for computing connected components:

| Algorithm | Key | Description |
|-----------|-----|-------------|
| GraphX | `graphx` | The original GraphX-based implementation |
| Two Phase | `two_phase` | Label propagation with big-star/small-star steps (default) |
| Randomized Contraction | `randomized_contraction` | Based on [Bogeholz et al. (ICDE 2020)](https://ieeexplore.ieee.org/document/9101327) |

### Randomized Contraction

The new `randomized_contraction` algorithm works by iteratively contracting the graph using random linear functions. At each step, vertices are mapped through a randomized `a*x + b` transformation (with overflow-safe arithmetic via a custom Spark SQL expression) and merged. The process repeats until no edges remain, then reconstructs the final component labels in reverse order.

This approach can be more memory-efficient for graphs with very large connected components because intermediate results are checkpointed to parquet rather than held in memory.

```scala
val components = graph.connectedComponents
  .setAlgorithm("randomized_contraction")
  .run()
```

### API refactoring

The algorithm implementations have been extracted into dedicated files (`TwoPhase.scala`, `RandomizedContraction.scala`) while `ConnectedComponents` itself becomes a clean API facade. A new `setIsGraphPrepared()` flag lets advanced users skip the graph preparation step when they know their data already satisfies the requirements (deduplicated vertices, normalized edges with `src < dst`, no self-loops).

All parameters are also configurable via Spark config:

```scala
spark.conf.set("spark.graphframes.connectedComponents.algorithm", "randomized_contraction")
```

## Pregel: automatic join skipping

A significant memory optimization was added to the Pregel API. GraphFrames now automatically analyzes message expressions at the start of `run()` to detect whether destination vertex columns are actually referenced. If they are not, as is the case for algorithms like PageRank and Shortest Paths that only read source vertex state, the expensive second join (attaching destination vertex state to triplets) is skipped entirely.

### How it works

Standard Pregel triplet construction requires two joins:

1. Join source vertices with edges
2. Join the result with destination vertices

The optimization uses `SparkShims.extractColumnReferences()` to inspect the AST of all message expressions. If no `Pregel.dst(...)` columns are accessed (or only `dst.id`, which is always available from the edge itself), the second join is replaced with a lightweight struct construction:

```scala
// Instead of an expensive join:
srcWithEdges.join(currentVertices.select(...), ...)

// A minimal struct is created from the edge column:
srcWithEdges.withColumn(DST, struct(col("edge_dst").as(ID)))
```

This is fully automatic and requires no changes to existing code. Algorithms that do reference destination state (like Label Propagation) continue to perform both joins as before.

### Benchmarks

We'd love community help benchmarking this optimization at scale. The `benchmarks/` directory includes JMH benchmarks for Shortest Paths, Label Propagation, and Connected Components using LDBC graphs. Run them against `main` and a prior version to measure the improvement:

```bash
sbt "benchmarks/Jmh/run -i 3 -wi 1 -f 1 .*ShortestPathsBenchmark.*"
```

## Graph embeddings & random walks

This release delivers the graph ML features previewed in the 0.10.0 roadmap: a complete pipeline for generating vertex embeddings via random walks.

### Random walks

The `RandomWalkWithRestart` algorithm generates vertex sequences by performing random walks across the graph. At each step, the walker either continues to a random neighbor or restarts from the origin with a configurable probability.

```scala
import org.graphframes.rw.RandomWalkWithRestart

val rw = new RandomWalkWithRestart()
  .setRestartProbability(0.1)
  .setNumWalksPerNode(5)
  .setNumBatches(5)
  .setBatchSize(10)
  .setTemporaryPrefix("/tmp/random_walks")
```

Key design choices:

- **Batched execution**: walks are generated in batches persisted to temporary parquet files, keeping memory bounded regardless of graph size
- **Deterministic sampling**: neighbors are sampled via `KMinSampling` (min-hash with xxhash64), ensuring fault-tolerant reproducibility
- **Edge direction support**: walks can respect or ignore edge directionality

### Embedding models

Two sequence-to-vector models are available:

| Model | Strengths | Typical dimensions | Scale |
|-------|-----------|-------------------|-------|
| **Word2Vec** | Higher quality embeddings, well-studied | 50-300 | ~20M vertices |
| **Hash2Vec** | No vocabulary needed, constant memory per element | 512+ | Billions of vertices |

Hash2Vec is based on [Argerich et al. (2016)](https://arxiv.org/abs/1608.08940) and uses MurmurHash3 to avoid storing explicit vocabularies. Internally it uses a custom `PagedMatrixDouble` structure with 4096-element pages for cache-friendly, GC-efficient vector accumulation.

### End-to-end pipeline

The `RandomWalkEmbeddings` class ties everything together:

```scala
import org.graphframes.embeddings.RandomWalkEmbeddings

val embeddings = new RandomWalkEmbeddings()
  .setRandomWalks(rw)
  .setSequenceModel(Right(new Hash2Vec().setEmbeddingsDim(512)))
  .setAggregateNeighbors(true)  // +20% quality via neighborhood averaging
  .run(graph)

// Returns DataFrame with all vertex columns + "embedding" column
embeddings.show()
```

Enabling `aggregateNeighbors` applies a GraphSAGE-style convolution that averages sampled neighbor embeddings and concatenates them with the node's own vector, improving downstream task performance by 20%+ in synthetic benchmarks.

### Use cases

- **Node classification**: predict vertex labels from learned representations
- **Link prediction**: score potential edges by embedding similarity
- **Community detection**: cluster vertices in embedding space
- **Anomaly detection**: identify outliers far from their neighborhood in embedding space

## PySpark Property Graph API

A new `PropertyGraphFrame` API was added for PySpark, enabling multi-typed graphs where different vertex and edge groups coexist in a single logical structure.

### Core classes

- **`VertexPropertyGroup`**: wraps a DataFrame of vertices with a name, primary key, and optional ID masking (SHA256 hashing to prevent collisions across groups)
- **`EdgePropertyGroup`**: wraps edges with source/destination property groups, direction, and optional weights
- **`PropertyGraphFrame`**: manages collections of vertex and edge groups

### Example

```python
from graphframes.pg import PropertyGraphFrame, VertexPropertyGroup, EdgePropertyGroup

users = VertexPropertyGroup("users", users_df, primary_key_column="user_id")
products = VertexPropertyGroup("products", products_df, primary_key_column="product_id")

purchases = EdgePropertyGroup(
    "purchases", purchases_df,
    src_property_group=users, dst_property_group=products,
    is_directed=True,
    src_column_name="user_id", dst_column_name="product_id",
    weight_column_name="amount"
)

pg = PropertyGraphFrame([users, products], [purchases])

# Convert to a standard GraphFrame and run algorithms
gf = pg.to_graphframe([users, products], [purchases])
components = gf.connectedComponents()

# Join results back to original vertex data
result = pg.join_vertices(components, [users])
```

### Bipartite projection

`PropertyGraphFrame` also supports bipartite graph projection, creating edges between vertices of the same type that share neighbors in the other partition:

```python
projected = pg.projection_by(users, products, purchases,
                             new_edge_weight=lambda w1, w2: w1 + w2)
```

## Other new features

### Approximate triangle counting

A new approximate triangle counting algorithm was added using [Apache DataSketches](https://datasketches.apache.org/). This provides fast, memory-efficient triangle count estimates for large graphs where exact counting would be prohibitively expensive.

### Aggregate Neighbors API

A new `AggregateNeighbors` class implements multi-hop breadth-first traversal with customizable accumulators, stopping conditions, target conditions, and edge filters. This is useful for computing reachability, path-based features, and neighborhood aggregations.

### Pattern matching improvements

- **Anonymous vertices** in variable-length patterns: `()-[*1..3]->(v)`
- **Undirected fixed-length patterns**: `(u)-[*2]-(v)`
- **Chaining with fixed-length patterns** now works correctly

## Bug fixes

- **K-Core**: fixed swapped `sendMsgToSrc`/`sendMsgToDst` column references that produced incorrect results (#802)
- **ConnectedComponents**: reverted a performance regression introduced after 0.9.3 (#772)
- **Fixed-length pattern chaining**: resolved parse errors when chaining motifs with fixed-length patterns (#771)
- **SVD++**: updated documentation and code (#779)
- **String IDs**: `powerIterationClustering` now supports string vertex IDs (#773)
- **Spark 4.1**: tests now run against Spark 4.1

## Future steps

- Benchmark the Pregel join-skipping optimization at scale and publish results
- Explore additional embedding algorithms and GNN-style convolutions
- Continue improving Spark 4.x and Scala 3 compatibility
