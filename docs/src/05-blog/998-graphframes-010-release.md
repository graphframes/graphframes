# GraphFrames 0.10.0 release

- **Published:** 2025-10-08T00:00:00Z
- **Title:** GraphFrames 0.10.0 release
- **Summary:** This release comes with significant performance improvements to most algorithms, as well as fixed memory leaks. The PySpark APIs for Spark Connect and Spark Classic are now synchronized with the Scala core, allowing PySpark users to benefit from the latest improvements in the GraphFrame APIs and configurations. This is the first release in which GraphFrames relies on its own internal fork of GraphX instead of Spark's built-in version. There are also improvements in motif finding. Undirected, bidirectional, and an arbitrary amount of edges can now be included in the pattern string. New algorithm for cycle detection was added. The documentation has also been significantly improved.

## Performance

## GraphX memory

## PySpark APIs

## Motifs finding

## Cycles detection

## Compatibility with Scala 3

We are continuing to work on the compatibility with Scala 3. In 0.10.0 the new scalafix flags were introduced to improve
the compatibility.

## Future steps

- During the benchmarking we found that the DataFrame-based Pregel is not as fast as the GraphX-based Pregel. At the
  moment there is an ongoing discussion about moving the whole GraphX project to GraphFrames and starting to work on
  improvement and fixes of GraphX based algorithms;
- With a new `PropertyGraph` model, we can start to work on the pattern matching support and better integration with
  native graph storage formats;
- A lot of other improvements can be made in documentation, website, and infrastructure;
