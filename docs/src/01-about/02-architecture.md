# GraphFrames Internals

To learn how GraphFrames works internally to combine graph and relational queries, check out the paper [GraphFrames: An Integrated API for Mixing Graph and Relational Queries, Dave et al. 2016](https://people.eecs.berkeley.edu/~matei/papers/2016/grades_graphframes.pdf).

## Overview

Internally, GraphFrames translates graph queries into relational queries and then combines the results.

@:image(/img/graphframes-internals/graphframes-overview.png) {
    intrinsicWidth = 600
    alt = "An overview of GraphFrames and Apache Spark connection"
    title = "GraphFrames Overview"
}

## Graph Representation

The main abstraction is the @:scaladoc(org.graphframes.GraphFrame) class that contains two `DataFrame` objects: one for the graph vertices and one for the graph edges. Any operation on the graph is performed on these two `DataFrame`s by combining operations like `filter`, `join`, `select`, etc. The simplest example of such an operation is `inDegrees` which returns the in-degree of each vertex by simply grouping edges by the source vertex and counting the number of rows in each group.

## Vertex-centric Algorithms

