# About

GraphFrames is a package for Apache Spark which provides DataFrame-based Graphs. It provides high-level APIs in Scala, Java, and Python. It aims to provide both the functionality of GraphX and extended functionality taking advantage of Spark DataFrames. This extended functionality includes motif finding, DataFrame-based serialization, and highly expressive graph queries.

# What are GraphFrames?

GraphFrames represent graphs: vertices (e.g., users) and edges (e.g., relationships between users) in the form of Apache Spark DataFrame objects. On top of this, GraphFrames provides not only basic APIs like `filterVertices` or `outDegrees`, but also a set of powerful APIs for graph algorithms and complex graph processing.

## GraphFrames vs GraphX

GraphFrames provides most of the algorithm and routines in two ways:

- Native DataFrame-based implementation;
- Wrapper over GraphX implementation.

**NOTE:** GraphX is deprecated in the upstream Apache Spark and is not maintained anymore. GraphFrames project comes with its own fork of GraphX: `org.apache.spark.graphframes.graphx`. While we are trying to not make any breaking changes in GraphFrames' GraphX, it is still considered as a part of the internal API. The best way to use it is via GraphFrame-GraphX conversion utils, instead of directly manipulate GraphX structures.

### Graph Representation

- GraphX represents graphs by the pair of `RDD`: `VertexRDD` and `EdgeRDD`.
- GraphFrames represent graphs by the pair of `DataFrame`: `vertices` and `edges`.

While `RDD` may provide slightly more flexible API and, in theory, processing of RDDs may be faster, they require much more memory to process them. For example, `VertexRDD[Unit]` that contains de-facto only `Long` vertex IDs will require much more memory to store and process compared to the `DataFrame` of vertices with a single `Long` column. The reason is serialization of `RDD` are done by serializing the underlying JVM objects, but serialization of data in `DataFrame` rely on the `Tungsten` with its own serialization format. On benchmarks, memory overhead of serializing Java objects may be up to five times, while the compute overhead of creating JVM objects from tungsten format is less than 10–15%.

### Optimizations

- GraphX relies on its own partitioning strategy and building and maintaining partition index.
- GraphFrames rely on the Apache Spark Catalyst optimizer and Adaptive Query Execution.

In most of the cases that include real-world complex transformations, especially on huge data, Catalyst + AQE will provide better results compared to the manual index of partitions.

### If DataFrames are better, why do GraphFrames still provide conversion methods?

Our [benchmarks](03-benchmarks.md) show that on small and medium graphs GraphX may be a better choice. With GraphX users can sacrifice memory consumption in favor of better running time without query optimization overhead. That may be suitable, for example, for Spark Structured Streaming scenarios.

# Use-cases of GraphFrames

Refer to the [User Guide](/04-user-guide/01-creating-graphframes.md) for a full list of queries and algorithms.

## Ranking in search systems

`PageRank` is a fundamental algorithm originally developed by Google for ranking web pages in search results. It works by measuring the importance of nodes in a graph based on the link structure, where links from highly ranked pages contribute more to the rank of target pages. This principle can be extended to ranking documents in search systems, where documents are treated as nodes and hyperlinks or semantic relationships as edges.

GraphFrames provides a fully distributed Spark-based implementation of the `PageRank` algorithm, enabling efficient computation of document rankings at scale. This implementation leverages the power of Apache Spark's distributed computing model, allowing organizations to analyze large-scale document networks without sacrificing performance.

## Graph Clustering

GraphClustering algorithms like `Label Propagation` and `Power Iteration Clustering` are built into GraphFrames and provide efficient ways to perform unsupervised clustering on large graphs. These algorithms leverage the distributed nature of Apache Spark to scale clustering operations across massive datasets while maintaining accuracy and performance.

Label Propagation is a fast and efficient method for detecting communities in large graphs. It works by iteratively updating node labels based on the majority label of neighboring nodes, eventually leading to clusters where nodes within the same community share similar labels. This algorithm is particularly effective for identifying overlapping communities and is well-suited for real-time applications due to its simplicity and low computational overhead.

Power Iteration Clustering (PIC) is another powerful clustering algorithm included in GraphFrames. PIC uses the eigenvectors of the graph's normalized adjacency matrix to assign nodes to clusters. It is especially effective for finding well-separated clusters and can handle large-scale graphs efficiently through Spark's distributed computing capabilities. The algorithm is based on the principle that nodes belonging to the same cluster tend to have similar values in the dominant eigenvector, making it a robust choice for various graph clustering tasks.

## Anti-fraud and compliance applications

GraphFrames provides powerful tools for analyzing complex networks, offering distributed implementations that scale seamlessly with Apache Spark. Here are two notable algorithms usable for anti-fraud and compliance analysis.

### ShortestPaths Algorithm

The `ShortestPaths` algorithm can be used for identifying the shortest paths within a graph. This is particularly valuable for analyzing financial networks to find the minimum distances to known suspicious nodes. Such insights can be applied to enhance compliance scoring and detect suspicious activities with greater efficiency. In GraphFrames `ShortestPaths` algorithm is implemented in a vertic-centric Pregel framework that effectively distributes the work across the whole Apache Spark cluster.

### Cycles Detection with Rocha-Thatte Algorithm

GraphFrames includes an implementation of the [Rocha-Thatte cycles detection algorithm](https://en.wikipedia.org/wiki/Rocha%E2%80%93Thatte_cycle_detection_algorithm). This algorithm is designed to find all cycles in large graphs, making it an essential tool for uncovering suspicious activities like circular money flows. By efficiently detecting cycles, it enables analysts to better understand the structure of data and identify potential fraud.

### Motifs finding

Motifs finding is a powerful technique for identifying recurring patterns within graphs, which proves especially useful in detecting suspicious transactions and actions in financial networks. By analyzing the structural patterns of interactions between entities such as accounts, transactions, and merchants, motif finding can reveal common fraud schemes like money laundering, identity theft, or collusion among bad actors. For instance, specific motifs might indicate unusual transaction sequences that are typical of pump-and-dump schemes or layered fraudulent transfers. This capability allows financial institutions to proactively identify and investigate potentially risky behaviors before they escalate into significant losses.

GraphFrames provides a `find` API to find motifs at scale with a fully distributed algorithm powered by Apache Spark.

## Data deduplication and identity resolution

GraphFrames provides a highly efficient distributed algorithm called ["big-star small-star"](https://dl.acm.org/doi/pdf/10.1145/2670979.2670997) for finding connected components in large graphs. This algorithm is particularly useful for data deduplication and fingerprinting of massive datasets containing billions of rows. By constructing an interaction graph where each entity is represented as a vertex and relationships between entities are represented as edges, the connected components algorithm can group together rows that refer to the same real-world entity, even if they have different IDs across various systems or sessions.

For example, consider a scenario where a user has multiple accounts across different platforms or systems, each with its own unique identifier. By creating a graph where vertices represent these accounts and edges represent known relationships (such as shared email addresses, IP addresses, or transaction histories), the connected components algorithm can identify all the vertices that belong to the same user. These vertices are then grouped together and assigned a unified ID, effectively deduplicating the data while preserving the integrity of the underlying entity relationships.

This approach is especially powerful in scenarios involving customer data management, fraud detection, and identity resolution, where entities may appear under different identifiers across various data sources. The distributed nature of the "big-star small-star" algorithm ensures that such operations can be performed efficiently at scale, making it possible to process and deduplicate massive datasets in a reasonable amount of time.

## Custom graph algorithms

GraphFrames provides two powerful APIs: `AggregateMessages` and `Pregel` that allow users to write and run custom algorithms using a scalable and distributed vertex-centric approach on top of Apache Spark. These APIs enable developers to implement complex graph algorithms efficiently by leveraging Spark's distributed computing capabilities. The `AggregateMessages` API facilitates message passing between vertices by aggregating messages from neighboring vertices, making it ideal for implementing iterative graph algorithms. Meanwhile, the `Pregel` API offers a more traditional vertex-centric programming model, allowing users to define custom computations that run in iterations until convergence. Together, these APIs provide the flexibility needed to build sophisticated graph analytics solutions that can handle large-scale data processing requirements while maintaining the performance and reliability of the Apache Spark ecosystem.

# Downloading

Get GraphFrames from the [Maven Central](https://central.sonatype.com/namespace/io.graphframes). GraphFrames depends on Apache Spark, which is available for download from the [Apache Spark website](http://spark.apache.org).

GraphFrames should be compatible with any platform that runs the open-source Spark. Refer to the [Apache Spark documentation](http://spark.apache.org/docs/latest) for more information.

**WARNING:** Some vendors are maintain their own internal forks of the Apache Spark that may be not fully compatible with an OSS version. While GraphFrames project is trying to rely only on public and stable APIs of the Apache Spark, some incompatibility is still possible. Fell free to open an issue in case you are facing problems in modified Spark environments like Databricks Platform.

GraphFrames is compatible with Spark 3.4+. However, later versions of Spark include major improvements to DataFrames, so GraphFrames may be more efficient when running on more recent Spark versions.

GraphFrames is tested with Java 8, 11 and 17, Python 3, Spark 3.5 and Spark 4.0 (Scala 2.12 / Scala 2.13).

# Applications, the Apache Spark shell, and clusters

See the [Apache Spark User Guide](http://spark.apache.org/docs/latest/) for more information about submitting Spark jobs to clusters, running the Spark shell, and launching Spark clusters. The [GraphFrame Quick-Start guide](/02-quick-start/02-quick-start.md) also shows how to run the Spark shell with GraphFrames supplied as a package.

# Where to Go from Here

**User Guides:**

- [Quick Start](/02-quick-start/02-quick-start.md): a quick introduction to the GraphFrames API; start here!
- [GraphFrames User Guide](/04-user-guide/01-creating-graphframes.md): detailed overview of GraphFrames
  in all supported languages (Scala, Java, Python)
- [Motif Finding Tutorial](/03-tutorials/02-motif-tutorial.md): learn to perform pattern recognition with GraphFrames using a technique called network motif finding over the knowledge graph for the `stackexchange.com` subdomain [data dump](https://archive.org/details/stackexchange)
- [GraphFrames Configurations](/04-user-guide/13-configurations.md): detailed information about GraphFrames configurations, their descriptions, and usage examples

**Community Forums:**

- [GraphFrames Mailing List](https://groups.google.com/g/graphframes/): ask questions about GraphFrames here
- [#graphframes Discord Channel on GraphGeeks](https://discord.com/channels/1162999022819225631/1326257052368113674)

**External Resources:**

- [Apache Spark Homepage](http://spark.apache.org)
- [Apache Spark Wiki](https://cwiki.apache.org/confluence/display/SPARK)
- [Apache Spark Mailing Lists](http://spark.apache.org/mailing-lists.html)
- [GraphFrames on Stack Overflow](https://stackoverflow.com/questions/tagged/graphframes)
