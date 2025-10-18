# GraphFrames 0.10.0 release

- **Published:** 2025-10-11T00:00:00Z
- **Title:** GraphFrames 0.10.0 release
- **Summary:** This release comes with significant performance improvements to most algorithms, as well as fixed memory leaks. The PySpark APIs for Spark Connect and Spark Classic are now synchronized with the Scala core, allowing PySpark users to benefit from the latest improvements in the GraphFrame APIs and configurations. This is the first release in which GraphFrames relies on its own internal fork of GraphX instead of Spark's built-in version. There are also improvements in motif finding. Undirected, bidirectional, and an arbitrary amount of edges can now be included in the pattern string. New algorithm for cycle detection was added. The documentation has also been significantly improved.

## Performance

### GraphX LabelPropagation

An implementation of the LabelPropagation in Apache Spark' GraphX is very inefficient. On each iteration of CDLP, each vertex should collect labels from all neighbors and choose the most common os a new label. An existing GraphX implementation sends `Map[VertexID, Label]` from each vertex and on the reduce step applies a logic like:

```scala
val map = mutable.Map[VertexId, Long]()
  (count1.keySet ++ count2.keySet).foreach { i =>
    val count1Val = count1.getOrElse(i, 0L)
    val count2Val = count2.getOrElse(i, 0L)
    map.put(i, count1Val + count2Val)
  }
map
```

The problem here is when we are folding the messages `RDD`, the computational complexity is about *O(N^2)* because to reduce the messages `RDD` of length `N` (where `N` is amount of neighbors) we should do at worsest case `N` iteration iside each we need to iterate over at most `N` keys (because in the worsest case, for example on the first step of algorithm, all the labels are unique). On top of that we have some additional overhead of creating, serializing and deserializing the `Map`.


Because GraphX was deprecated in Apache Spark starting from the version `4.0` and does not accept patches anymore, GraphFrame maintainers made a decision to create an internal fork of GraphX. The fist change in the new fork was an improvement in the LabelPropagation. Instead of sending `Map[VertexID, Label]` the new implementation send `Vector[Label]`. In that case, the reduce step has a complexity *O(N)* because concatenations of `Vector` in Scala has a constant complexity. And the final reduction to compute the most common label is done on the step of label updating. While collecting the whole vector of labels may increase the average memory consumption, it reduce the peak memory consumption of algorithm. On the first iteration of LabelPropagation all the labels are unique, so old implementation will materialize the `Map[VertexID, Label]` with a size equal to amount of neighbors. At the same time, the new implementation will materialize only the `Vector[Label]` with the same size. Based on [benchmarking of Scala collections](https://www.lihaoyi.com/post/BenchmarkingScalaCollections.html#memory-use-of-immutable-collections), the memory consumption of the `Vector` is around 5 times less compared to the `Map`.

The result of the new impleentation is around 70x boost: on a LDBC' Wiki-talk test graph (2M vertices, 5M edges) Spark' implementation runs in ~3500 seconds wile the new one runs in ~50 seconds.

### GraphX memory management

Because main structures in GraphFrames are `DataFrame` objects but GraphX operates on `EdgeRDD` and `VertexRDD`, all the algorithm from GraphX are accessable from GraphFrames via conversion. Spark GraphX obviously was not designed to this way of usage. Under the hood GraphX do a lot of `RDD.persist` operations. For example, creating `Graph` from edges returns a persistent graph as well as result of all the graph algorithms are persistent. But on the GraphFrame side it is hard to unpersist all the intermediate RDDs. That tends to memory leaks. While it may be not a big problem in batch jobs, calling any GraphX algorithm in Spark Structured Streaming will lead to OOM errors after around 30-50 iterations.

With a full control of the GraphX fork, the team of GraphFrame maintainers was able to resolve the problem by removing from the GraphX code overpersisting as well, as handling manual unpersisting of GraphX structures after conversion. The result is no more memory leaks without loosing in performance.

### Connected Components & AQE

Connected Components is one of the important and used algorithm from GraphFrames. The reason is very simple: if you have a dataset of tens billions of records that are conncted logically to each other with hundred billions of edges and you want to do data deduplication (or identity resolution / fingerprinting), you have almost no other choice except GraphFrames. I do not know another graph library or tool that can compute connected components at that scale.

---

**NOTE**

*What is the link between connected components and identity resolution? The answer may be not obvious, but these two are actually the same task. Let's see it on the example of identification of users based only on the meta information from their web browsers. If we have records about someone is wisiting our website from the same browser, the same IP adress, etc., most probably it is the same user. But when we have hundred billions of session logs, it may be not so obvious how to resolve the problem. And here conncted components algorithm appears. If we imagine each link like `session -> IP` or `session -> browser` as a link in the graph all that we need is to find a connected clusters from such a links and each cluster will be a user. This task is known as a [weakly connected components](https://en.wikipedia.org/wiki/Weak_component).*

---

GraphFrames uses a very efficient algorithm for connected components, named "Two Phase" or "Big star - Small start" ([Kiveris, Raimondas, et al. "Connected components in mapreduce and beyond." Proceedings of the ACM Symposium on Cloud Computing. 2014.](https://dl.acm.org/doi/abs/10.1145/2670979.2670997)). While the algorithm has an excelent convergence complexity, there is a problem that during iterations there are appearing vertices with a very huigh degree. It creates a disbalance in workload distribution across the Spark cluster (it is known also as a skewness problem). Old versions of GraphFrames adrressed it by manual broadcasting of high-degree nodes via `DataFrame.collect`. While it works fine, such an operation breaks the Spark lineage.

This leads to inability to use Spark Adaptive Query Execution. Ángel Álvarez Pascua [make a nice blogpost with explanation](https://medium.com/towards-data-engineering/apache-spark-wtf-i-like-it-when-a-plan-comes-together-part-ii-dc59def302b3) if you are interested in details.

After do some additional research and experiments, GraphFrames maintainers team was able to make Connected Components work with AQE. Passing a `-1` as a `broadcastThreshold` in a new version of GraphFrames will completely disable manual broadcasting and parquet-based checkpointing and allows AQE to handle skewness.

The new approach was tested on subste of LDBC graphs up to 8M vertices and 260M edges and passed all the tests. Because GraphFrames follows the semantic versioning approach, we cannot just change the default value of the `broadcastThreshold`, but it is strongly recommended to use the new approach! Based on benchmarks, it provides 5-8x speed-up compared to the old one.

### Pregel performance

A couple of important improvements were made in `Pregel API`. In a new version of GraphFrames Pregel more aggresively persist intermediate results of computations. But compared to GraphX Pregel that is built around triplets (and persist triplets on iterations), GraphFrames Pregel API is built arount vertices state and persist only vertices, not triplets. Triplets in GraphFrames API is only an intermediate not materialized result.

To avoid problems with memory and to provide more flexibility, the Spark `StorageLevel` used for persisting in Pregel was made configurable (with a default one equal to `MEMORY_AND_DISK`). This configuration was also propagated to all the Pregel based algorithms like `ShortestPaths`, `LabelPropagation`, etc.

The result is around 2-3x performance boost in GraphFrames based implementation of SP and CDLP.

## PySpark APIs

GraphFrames is a Scala-first library and all other clients (PySpark Classic, PySpark Connect, etc.) are implemented as wrappers/bindings. In the last few releases the Scala core got a lot of updates not all of which were propagated to PySpark APIs.

This release finally did a sync between Scala API and PySpark Connect / Classic APIs, so PySpark and Scala Spark have a features parity now.

## Motifs finding

Improvements were made in GraphFrames motifs finding API. The new syntax:

- bidirectional edges
- undirected edges
- arbitrary amount of hops

With this new features users can find even more complex motifs and sib structures in graphs at scale!

## Cycles detection

New algorithm for cycles detection was added. It is based on the [Rocha, Rodrigo Caetano, and Bhalchandra D. Thatte. "Distributed cycle detection in large-scale sparse graphs." Proceedings of Simpósio Brasileiro de Pesquisa Operacional (SBPO’15) (2015): 1-11.](https://assets-eu.researchsquare.com/files/rs-4619085/v1_covered_22e633ca-157a-4302-adef-eb249909efc3.pdf) paper.

API is provided for both Core and PySpark. Such an algorithm significantly improve application of GraphFrames in fraud-detection when we need to detect cyclic transactions, iteractions, etc.

## Compatibility with Scala 3

We are continuing to work on the compatibility with Scala 3. In 0.10.0 the new scalafix flags were introduced to improve
the compatibility.

## Future steps

- More improvements in GraphFrames Pregel API are planned, especially related to the memory consumption;
- In the next release we are planning to finally provide Random Walks API as well as Graph Machine Learning features like node embeddings (like `node2vec` / `deepWalk` algorithms);
