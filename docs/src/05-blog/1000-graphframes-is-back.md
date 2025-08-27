<meta itemprop="blogTitle" content="GraphFrames is back!">
<meta itemprop="blogDatePublished" content="2025-08-26T12:00:00">
<meta itemprop="blogSummary" content="GraphFrames is back: new contributors, new logo, new documentation, new Spark Connect support, and more!">
<meta itemprop="blogAuthors" content="Russell Jurney">

# GraphFrames is back!

*Originally published in the [Graphlet Ai Blog](https://blog.graphlet.ai/graphframes-is-back-with-v0-9-2-5773d55d3291)*

GraphFrames 0.9.2 is out on [PyPi](https://pypi.org/project/graphframes-py/) as [graphframes-py](https://pypi.org/project/graphframes-py/) and as [io.graphframes](https://central.sonatype.com/namespace/io.graphframes) on [Maven Sonatype Central](https://central.sonatype.com/search?q=io.graphframes)! Documentation is now available on [graphframes.io](https://graphframes.io/)… and we even have a new logo!

@:image(/img/logo-dark.png) {
  intrinsicWidth = 600
  alt = "The new GraphFrames logo is new for this release :)"
  title = The new GraphFrames logo
}

## GraphFrames is BACK!

You can see below that GraphFrames is back! It has seen contributions every week for most of the year — we have half a dozen active contributors now. This release is due to the efforts of many people but I need to express our deep gratitude to [Sem Sinchenko](https://www.linkedin.com/in/semyon-a-sinchenko/), who drove this release.

@:image(https://miro.medium.com/v2/resize:fit:1100/format:webp/1*xJnVeh6LbQ3gNoPVQ4gwmw.png) {
  intrinsicWidth = 600
  alt = "GraphFrames is back! Semyon Sinchenko deserves the appreciation and respect of all GraphFrames users :)The project has gone from dead to lively since GraphX was deprecated from Spark — prompting us to work on a replacement."
  title = "GraphFrames commits chart"
}

The project has gone from *effectively dead* to *vibrant* in the six months since [GraphX was deprecated](https://lists.apache.org/thread/qrvo6xrt8zvp5ss73z5spt9q89r0htwo) from Spark, which prompted us to get to work on an all-DataFrame replacement. You can see in the chart below that there is more frequent contributions than since the project’s inception!

@:image(https://miro.medium.com/v2/resize:fit:1100/format:webp/1*GYINCDjQN9LIng9ntA27iQ.png) {
  intrinsicWidth = 600
  alt = "Contributions chart of the GraphFrames project"
  title = "After a six year gap in additions, GraphFrames is back with Spark Connect support!"
}

## New Features in GraphFrames 0.9.2

It was necessary for GraphFrames to support both Spark 4 and Spark Connect to remain integral to the Spark community. There were many issues resolved in the release, but the core of it was:

- [Spark Connect support](https://github.com/graphframes/graphframes/pull/506)
- [Spark 4.x support](https://github.com/graphframes/graphframes/pull/608)
- [Performance improvements in Connected Components](https://github.com/graphframes/graphframes/pull/552)
- [Updated API for Pregel](https://github.com/graphframes/graphframes/issues?q=is%3Aissue+state%3Aclosed+Pregel)
- DataFrame implementation of [LabelPropagation](https://graphframes.io/api/scaladoc/org/graphframes/lib/LabelPropagation.html), GraphX-free
- DataFrame implementation of [ShortestPaths](https://graphframes.io/api/scaladoc/org/graphframes/lib/ShortestPaths.html), GraphX-free
- [New groupId](https://central.sonatype.com/namespace/io.graphframes) `io.graphframes`
- [New PyPi ID](https://pypi.org/project/graphframes-py/): `graphframes-py`
- A new website: [graphframes.io](https://graphframes.io/) with [Updated documentation](https://graphframes.io/)
- A new [Network Motif Finding Tutorial](/03-tutorials/02-motif-tutorial.md)
- [A lot of additional changes and fixes](https://github.com/graphframes/graphframes/releases/tag/v0.9.0)

## State of the Union

The GraphFrames community has achieved our first goal: make the project viable again! Still in the future?

## Property Graphs

Sem has [started implementing](https://github.com/graphframes/graphframes/pull/613) Property Graphs for GraphFrames, which currently has `relationship` for edges but not `type` for nodes. In current practice, this means property graph processing requires you to merge all your node schemas together into a kitchen sink schema before using GraphFrames’ algorithms. it is a real drag… property graphs will be a huge improvement! Sem recently [outlined a beautiful vision](https://semyonsinchenko.github.io/ssinchenko/post/dreams-about-graph-in-lakehouse/) for property graphs as part of the Open Lakehouse. Check it out!

## Inclusion in Spark

This is actively debated: it would be a lot of trouble to release with Spark, but based on the number of search hits for [GraphX](https://www.google.com/search?q=GraphX) versus [GraphFrames](https://www.google.com/search?q=GraphFrames), it would get us 10x as many users. When I put that way, GraphFrames in Spark sounds pretty good!

## GraphX is Deprecated

Spark deprecating GraphX was the call to action that led us to revive GraphFrames, and we heard it well. We’re building DataFrame implementations of all GraphX components. GraphX has already been removed [from ShortestPaths](https://github.com/graphframes/graphframes/pull/587) and [from LabelPropagation](https://graphframes.io/api/python/graphframes.html#graphframes.GraphFrame.labelPropagation). The rest of the work is being tracked [here](https://github.com/graphframes/graphframes/issues/556) and is underway. **GraphX will be deprecated from GraphFrames** as of 1.0. GraphFrames 2.0 will remove GraphX completely. Soon GraphFrames will be entirely built on DataFrames!

## The Sedona Alliance!

Developers from [Apache Sedona](https://sedona.apache.org/latest/) joined the development of GraphFrames 0.9. Sedona 1.80 [will depend on](https://github.com/apache/sedona/pull/2098) the new version. They’ve been a huge help! [James Willis](https://www.linkedin.com/in/james-willis/), [Adam Binford](https://www.linkedin.com/in/adam-binford-a10b0321/) and the Apache Sedona team gave us new configurations, helped us fix our CI to enable the 0.9 release and drove Spark 4 support. James Willis became an official maintainer of GraphFrames to coordinate efforts between these projects.

## New Contributors

We have a lot of new contributors for this release!

- [@bjornjorgensen](github.com/bjornjorgensen) made their first contribution in [#471](https://github.com/graphframes/graphframes/pull/471)
- [@Nassizouz](github.com/Nassizouz) made their first contribution in [#474](https://github.com/graphframes/graphframes/pull/474)
- [@SauronShepherd](github.com/SauronShepherd) made their first contribution in [#495](https://github.com/graphframes/graphframes/pull/495)
- [@SemyonSinchenko](github.com/SemyonSinchenko) made their first contribution in [#487](https://github.com/graphframes/graphframes/pull/487)
- [@dmatrix](github.com/dmatrix) made their first contribution in [#535](https://github.com/graphframes/graphframes/pull/535)
- [@architch](github.com/architch) made their first contribution in [#320](https://github.com/graphframes/graphframes/pull/320)
- [@james-willis](github.com/james-willis) made their first contribution in [#563](https://github.com/graphframes/graphframes/pull/563)
- [@Conor0Callaghan](github.com/@Conor0Callaghan) made their first contribution in [#592](https://github.com/graphframes/graphframes/pull/592) 
- [@Kimahriman](@Kimahriman) made their first contribution in [#608](https://github.com/graphframes/graphframes/pull/608)

## A Call for Help

We are building a [list of dependent projects](https://github.com/graphframes/graphframes/discussions/616), so if you use GraphFrames, please let us know! We want your help testing new versions before the release.

Got questions or concerns? Let us know what you think! Find us on Discord in [#graphframes on GraphGeeks](https://discord.com/channels/1162999022819225631/1326257052368113674), or join the [GraphFrames Google Group](https://groups.google.com/g/graphframes/).