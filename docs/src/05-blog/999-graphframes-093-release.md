# GraphFrames 0.9.3 release

- **Published:** 2025-09-10T00:00:00Z
- **Title:** GraphFrames 0.9.3 release
- **Summary:** This release comes with redesigned documentation and improvements. A new PropertyGraph model was
  introduced. An option has been added to use local checkpoints in DataFrame-based Pregel and Connected Components. A
  bug in DataFrame-based LabelPropagation was fixed. Benchmarks for GraphFrames have been introduced. There is better
  compatibility with Scala 3. There are also minor updates and improvements.

## The brand-new website and documentation

Documentation was significantly improved. The website is now built with [TypeLevel Laika](https://typelevel.org/Laika/).
Laika was chosen because GraphFrames is a Scala first library, and it is nice to have a website that is built with Scala
and well integrated to the sbt build. Documentation was split into multiple sections for better navigation and
readability. A new section was added for the [DataFrame-based Pregel API](/04-user-guide/10-pregel.md). A lot of things
were fixed or updated.

## The new Property Graph Model

While `Graphframes` was always good at providing graph algorithms, the API for constructing property graphs was very
low-level and with a high level of variations about how to do the same thing. As well, the full pattern matching support
was not available. The first step towards a more user-friendly API was to introduce a new `PropertyGraph` model. This
model is a pure logical structure that wraps multiple tables or dataframes to `VertexPropertyGroup` and
`EdgePropertyGroup` objects. This objects can be used to construct a `PropertyGraphFrame` that handles everything and
provides an API to convert it to a `GraphFrame` and back to run graph algorithms. What is cool is that the
`PropertyGraphFrame` supports arbitrary multigraphs that may have directed and undirected, weighted and unweighted
edges, etc. Next steps are to add support for pattern matching with an ability to match by property group type as well
as filtering by any property internally that is already very close to the OpenCypher standard. Additional benefit of the
new graph abstraction is a possibility to integrate `GraphFrames` with native graph storage formats
like [Apache GraphAr (incubating)](https://graphar.apache.org/).

## Local checkpoints in DataFrame-based Pregel and Connected Components

It was a long-awaited feature to be able to not rely on persistent checkpoints for the DataFrame-based Pregel and
Connected Components. With the 0.9.3 release, this is now possible by either direct API or global configuration.

## Bug fixes in DataFrame-based LabelPropagation

Due to a bug in tests code, the DataFrame-based LabelPropagation was not tested and was not working. This has been
fixed.

## Benchmarks for GraphFrames

For the first time, we have introduced benchmarks for GraphFrames. Benchmarks are run on the LDBC Graphalytics benchmark
suite. The results are not "official" because they were not checked by the LDBC Graphalytics team. However, they are a
good starting point for future improvements and tracking performance. The results are
available [here](/01-about/03-benchmarks.md).

## Compatibility with Scala 3

We are continuing to work on the compatibility with Scala 3. In 0.9.3 the new scalac flags were introduced to improve
the compatibility. As well as some deprecation warnings were fixed.

## Future steps

- During the benchmarking we found that the DataFrame-based Pregel is not as fast as the GraphX-based Pregel. At the
  moment there is an ongoing discussion about moving the whole GraphX project to GraphFrames and starting to work on
  improvement and fixes of GraphX based algorithms;
- With a new `PropertyGraph` model, we can start to work on the pattern matching support and better integration with
  native graph storage formats;
- A lot of other improvements can be made in documentation, website, and infrastructure;
