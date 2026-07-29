# Internals

To learn how GraphFrames works internally to combine graph and relational queries, check out the paper [GraphFrames: An Integrated API for Mixing Graph and Relational Queries, Dave et al. 2016](https://people.eecs.berkeley.edu/~matei/papers/2016/grades_graphframes.pdf).

## Overview

GraphFrames gives users an API and abstractions for working with graphs, pattern matching, and running algorithms. Under the hood, all these operations are translated into standard relational operations – select, join, group by, aggregate – over DataFrames. DataFrames are just data in tabular form. The translated logical plan runs on an Apache Spark cluster. The user always gets results back as a DataFrame, which is simply a table.

@:image(/img/graphframes-internals/graphframes-overview.png) {
    intrinsicWidth = 600
    alt = "An overview of GraphFrames and Apache Spark connection"
    title = "GraphFrames Overview"
}

## Graph Representation

The main abstraction is the @:scaladoc(org.graphframes.GraphFrame) class that contains two `DataFrame` objects: one for the graph vertices and one for the graph edges. Any operation on the graph is performed on these two `DataFrame`s by combining operations like `filter`, `join`, `select`, etc. The simplest example of such an operation is `inDegrees` which returns the in-degree of each vertex by simply grouping edges by the source vertex and counting the number of rows in each group.

## Algorithm Optimization Patterns

GraphFrames algorithms are relational programs, so their performance is largely determined by the size and lifetime of the DataFrames that make up each iteration. The implementations use a few recurring patterns to keep Spark plans and executor memory manageable:

### Keep intermediate state narrow

An algorithm usually needs only a small subset of the vertex and edge columns to compute its next state. Message-passing implementations join the edge DataFrame with the current state, select the columns needed by the message function, and aggregate messages by vertex. Avoiding unnecessary vertex properties in those intermediate DataFrames reduces serialization, shuffle, and join costs. This is also why `AggregateMessages` exposes separate `sendToSrc` and `sendToDst` operations rather than requiring callers to materialize every possible triplet field.

### Persist reused DataFrames, then release them

An iterative algorithm may read the same edges, frontier, or message DataFrame more than once in an iteration. GraphFrames persists these reusable intermediates at the configured storage level and unpersists older states as the iteration advances. Persisting prevents Spark from recomputing the full lineage for every consumer, while unpersisting prevents completed iterations from accumulating in executor memory. Results returned by some algorithms are also persistent; call `unpersist()` when the result is no longer needed.

### Checkpoint long-running iterations

Persistence caches data but does not shorten its logical plan. Repeated joins and projections can make the plan increasingly expensive for Spark to analyze. Iterative implementations therefore checkpoint state at a configurable interval. A regular checkpoint is fault-tolerant and requires `spark.sparkContext.setCheckpointDir(...)`; a local checkpoint is faster but can be lost with executor data. The tradeoff and configuration examples are described in the [configuration guide](../04-user-guide/13-configurations.html).

### Tune the algorithm implementation for the workload

Several algorithms offer more than one implementation because the best execution strategy depends on graph size and data distribution. For example, Connected Components can use the GraphFrames or GraphX implementation. The GraphFrames implementation also has a broadcast threshold for high-degree vertices: assignments below the threshold use joins, while larger assignments can be broadcast to avoid an oversized join. These options change how Spark moves data; they do not change the graph semantics.

When diagnosing a slow run, inspect the operation's algorithm and storage-level options together with Spark's SQL plan and stage metrics. A wider input schema, a long uncheckpointed lineage, an unsuitable storage level, or a skewed high-degree vertex can each dominate runtime even when the graph itself is unchanged.

## Vertex-centric Algorithms

Let’s look at a concrete example – PageRank. This algorithm became famous for powering Google Search (fun fact: “Page” is actually the last name of Google co-founder Larry Page, not just about web pages). PageRank helps find the most “important” nodes in a graph, like ranking web pages by relevance.

@:image(/img/graphframes-internals/pregel-pagerank.png) {
    intrinsicWidth = 600
    alt = "PageRank algorithm workflow in terms of relational operations"
    title = "PageRank Algorithm"
}

In GraphFrames, many algorithms are built on the Pregel framework ([*Malewicz, Grzegorz, et al. "Pregel: a system for large-scale graph processing." Proceedings of the 2010 ACM SIGMOD International Conference on Management of data. 2010.*](https://blog.lavaplanets.com/wp-content/uploads/2023/12/p135-malewicz.pdf)). Some algorithms, such as PageRank, currently rely on GraphX-backed implementations. We represent the graph as two `DataFrames`, which you can think of as tables: one for edges and one for vertices. The PageRank table is initialized by assigning every vertex a starting rank of `1.0`.

Each iteration of PageRank works like a series of SQL operations. The process starts by joining the edges table with the current PageRank values for each vertex. This creates a triplets table, where each row contains a source, destination, and their current ranks. Next, we generate messages: each source sends its rank to its destination. These messages are grouped by destination and summed up. Finally, we join the results back to the PageRank table and update the rank using a simple formula: `new_rank = sum_rank * 0.85 + 0.15`, where `0.85` is the damping factor and `0.15` is the reset probability (alpha) that vertices with no in-links converge to.

This whole process is repeated – each step is just a combination of joins, group by, and aggregates over tables – until the ranks stop changing much. The algorithm converges quickly, usually in about 15–20 iterations. Since it relies entirely on SQL operations, running PageRank on an Apache Spark cluster gives you excellent horizontal scalability. As long as your tables fit in Spark, you can compute PageRank using Pregel. In practice, this means you can almost infinitely scale just by adding more hardware.
